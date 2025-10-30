package sqlite

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/eatonphil/gosqlite"
	api "github.com/kocubinski/costor-api"

	"github.com/SaharaLabsAI/iavl/v2/common/constants"
	"github.com/SaharaLabsAI/iavl/v2/common/logger"
	"github.com/SaharaLabsAI/iavl/v2/common/metrics"
	nodepool "github.com/SaharaLabsAI/iavl/v2/common/pool/node"
	"github.com/SaharaLabsAI/iavl/v2/db"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

type DB struct {
	opts Options

	write       *WriteConn
	writeEv     *WriteEventLoop
	writeCancel context.CancelFunc

	// Used by block producer or syncer
	read *ReadConn

	// Separate read conn configuration from main read, typical used by rpc query
	readPool *ReadConnPool
	// Pool for calculate hash only
	hashPool *ConnPool

	metrics metrics.Proxy
	logger  logger.Logger
}

func NewInMemoryDB() (*DB, error) {
	opts := defaultOptions(Options{ConnArgs: "mode=memory&cache=shared"})
	return NewDB(opts)
}

func NewDB(opts Options) (*DB, error) {
	var err error
	opts = defaultOptions(opts)

	sql := &DB{
		opts:    opts,
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	if !api.IsFileExistent(opts.Path) {
		err = os.MkdirAll(opts.Path, 0o755)
		if err != nil {
			return nil, err
		}
	}

	sql.write, err = NewWriteConn(opts)
	if err != nil {
		return nil, err
	}

	sql.writeEv, sql.writeCancel = NewWriteEventLoop(sql.write, opts.Logger, opts.Metrics)

	if sql.opts.OptimizeOnStart {
		if err = runAnalyze(sql.write); err != nil {
			return nil, err
		}

		if err = runOptimize(sql.write); err != nil {
			return nil, err
		}
	}

	// if err = runQuickCheck(sql.writeDb); err != nil {
	// 	return nil, err
	// }

	sql.readPool, err = NewReadConnPool(&opts, opts.MaxPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize read connection pool: %w", err)
	}
	sql.hashPool = NewConnPool(&opts, 100, opts.Logger)

	return sql, nil
}

func (sql *DB) Readonly() db.ReadonlyDB {
	return &DB{
		opts:     sql.opts,
		readPool: sql.readPool,
		metrics:  sql.metrics,
		logger:   sql.logger,
	}
}

func (sql *DB) isReadonlyDB() bool {
	return sql.write == nil
}

func (sql *DB) Path() string {
	return sql.opts.Path
}

func (sql *DB) Type() db.Type {
	return db.SQLITE
}

func (sql *DB) LatestVersion() (int64, error) {
	return latestVersion(sql.opts)
}

func (sql *DB) HasRoot(version int64) (bool, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	rootQuery, err := conn.Prepare("SELECT node_version FROM root WHERE version = ? LIMIT 1", version)
	if err != nil {
		return false, err
	}
	defer rootQuery.Close()

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (sql *DB) LoadRoot(nodePool *nodepool.NodePool, version int64) (*inode.Node, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence, bytes FROM root WHERE version = ? LIMIT 1", version)
	if err != nil {
		return nil, err
	}
	defer rootQuery.Close()

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return nil, fmt.Errorf("root not found for version %d", version)
	}
	if err != nil {
		return nil, err
	}
	var (
		nodeSeq     int
		nodeVersion int64
		nodeBz      []byte
	)
	err = rootQuery.Scan(&nodeVersion, &nodeSeq, &nodeBz)
	if err != nil {
		return nil, err
	}

	// if nodeBz is nil then a (valid) empty tree was saved, which a nil root represents
	var root *inode.Node
	if nodeBz != nil {
		// node seq in sqlite is uint32
		//nolint:gosec
		rootKey := inode.NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = inode.Decode(nodePool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (sql *DB) GetConn() (db.ReadConn, error) {
	return sql.hashPool.getConn()
}

func (sql *DB) GetNode(nodePool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error) {
	// Fallback to old method for backward compatibility
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, constants.MetricsNamespace, "db_get_node")

		target := "db_get_branch"
		if constants.IsLeafSeq(nodekey.Sequence()) {
			target = "db_get_leaf"
		}
		sql.metrics.IncrCounter(1, constants.MetricsNamespace, target)
	}()

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.GetNode(nodePool, nodekey)
}

func (sql *DB) GetValue(key []byte, version int64) ([]byte, error) {
	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.GetValue(version, key)
}

func (sql *DB) SaveTree(version int64, root *inode.Node, updates *db.DirtyNodes) error {
	sql.readPool.SetSavingTree()
	defer sql.readPool.UnsetSavingTree()

	if updates == nil {
		return sql.write.SaveRoot(version, root)
	}

	if err := sql.writeEv.SaveTree(root, version, updates); err != nil {
		return err
	}

	return nil
}

func (sql *DB) Revert(toVersion int64) error {
	return sql.write.Revert(toVersion)
}

func (sql *DB) PausePruning(pause bool) {
	sql.writeEv.pausePruning.Store(pause)
}

func (sql *DB) DeleteVersionsTo(toVersion int64) error {
	sql.writeEv.treePruneCh <- &pruneSignal{pruneVersion: toVersion}
	sql.writeEv.leafPruneCh <- &pruneSignal{pruneVersion: toVersion}
	return nil
}

func (sql *DB) DeleteVersionsToSync(toVersion int64) error {
	sql.writeEv.awaitTreePruned = make(chan struct{})

	err := sql.DeleteVersionsTo(toVersion)
	if err != nil {
		sql.writeEv.awaitTreePruned = nil
		return err
	}

	<-sql.writeEv.awaitTreePruned
	sql.writeEv.awaitTreePruned = nil

	return nil
}

func (sql *DB) Close() error {
	if sql.isReadonlyDB() {
		// Readonly DB, nothing to close
		return nil
	}

	if sql.writeCancel != nil {
		sql.writeCancel()
		sql.writeEv.awaitStop()
	}

	if sql.write != nil {
		if err := sql.write.Close(); err != nil {
			return err
		}
	}

	if sql.readPool != nil {
		if err := sql.readPool.Close(); err != nil {
			return err
		}
	}

	if sql.hashPool != nil {
		if err := sql.hashPool.close(); err != nil {
			return err
		}
	}

	if sql.read != nil {
		if err := sql.read.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (sql *DB) getReadConn() (*ReadConn, error) {
	if sql.isReadonlyDB() {
		return sql.readPool.GetConn()
	}

	var err error
	if sql.read == nil {
		sql.read, err = NewMainReadConn(&sql.opts, sql.logger)
	}

	return sql.read, err
}

func latestVersion(opts Options) (version int64, err error) {
	conn, err := gosqlite.Open(opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	err = conn.Exec("PRAGMA immutable=1;")
	if err != nil {
		return 0, err
	}

	rootQuery, err := conn.Prepare("SELECT MAX(version) FROM root LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer rootQuery.Close()

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	err = rootQuery.Scan(&version)
	if err != nil {
		return 0, err
	}

	return version, nil
}
