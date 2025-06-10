package sqlite

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	api "github.com/kocubinski/costor-api"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/metrics"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	"github.com/cosmos/iavl/v2/db"
	inode "github.com/cosmos/iavl/v2/node"
)

type SqliteDb struct {
	opts Options

	writeDb     *WriteDB
	writeEv     *WriteEventLoop
	writeCancel context.CancelFunc

	// Used by block producer or syncer
	read *SqliteReadConn

	// Separate read conn configuration from main read, typical used by rpc query
	readPool *SqliteReadonlyConnPool
	hashPool []*SqliteReadConn

	metrics metrics.Proxy
	logger  logger.Logger

	useReadPool bool

	rw sync.RWMutex
}

func NewInMemorySqliteDb() (*SqliteDb, error) {
	opts := defaultOptions(Options{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(opts)
}

func NewSqliteDb(opts Options) (*SqliteDb, error) {
	var err error
	opts = defaultOptions(opts)

	sql := &SqliteDb{
		opts:        opts,
		metrics:     opts.Metrics,
		logger:      opts.Logger,
		useReadPool: false,
	}

	if !api.IsFileExistent(opts.Path) {
		err = os.MkdirAll(opts.Path, 0755)
		if err != nil {
			return nil, err
		}
	}

	sql.writeDb, err = NewWriteDB(opts)
	if err != nil {
		return nil, err
	}

	sql.writeEv, sql.writeCancel = NewWriteEventLoop(sql.writeDb, opts.Logger, opts.Metrics)

	if sql.opts.OptimizeOnStart {
		if err = runAnalyze(sql.writeDb); err != nil {
			return nil, err
		}

		if err = runOptimize(sql.writeDb); err != nil {
			return nil, err
		}
	}

	// if err = runQuickCheck(sql.writeDb); err != nil {
	// 	return nil, err
	// }

	sql.hashPool = make([]*SqliteReadConn, 0)

	sql.readPool, err = NewSqliteReadonlyConnPool(&opts, opts.MaxPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize read connection pool: %w", err)
	}

	return sql, nil
}

func (sql *SqliteDb) Readonly() db.ReadonlyDB {
	return &SqliteDb{
		opts:        sql.opts,
		readPool:    sql.readPool,
		metrics:     sql.metrics,
		logger:      sql.logger,
		useReadPool: true,
	}
}

func (sql *SqliteDb) Type() db.Type {
	return db.SQLITE
}

func (sql *SqliteDb) SaveTree(version int64, root *inode.Node, updates *db.DirtyNodes) error {
	sql.readPool.SetSavingTree()
	defer sql.readPool.UnsetSavingTree()

	if updates == nil {
		return sql.writeDb.SaveRoot(version, root)
	}

	return sql.writeEv.SaveTree(root, version, updates)
}

func (sql *SqliteDb) ReadPool() *SqliteReadonlyConnPool {
	return sql.readPool
}

// TODO: Remove
func (sql *SqliteDb) SetInitTreeVersion(version *atomic.Int64) {
	sql.readPool.LinkTreeVersion(version)
}

func (sql *SqliteDb) Get(key []byte, version int64) ([]byte, error) {
	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.getVersioned(version, key)
}

func (sql *SqliteDb) LatestVersion() (int64, error) {
	return latestVersion(sql.opts)
}

func (sql *SqliteDb) ResetRead() error {
	return sql.resetReadConn()
}

func (sql *SqliteDb) Path() string {
	return sql.opts.Path
}

func (sql *SqliteDb) Revert(toVersion int64) error {
	return sql.writeDb.Revert(toVersion)
}

func (sql *SqliteDb) PausePruning(pause bool) {
	sql.writeEv.pausePruning.Store(pause)
}

func (sql *SqliteDb) DeleteVersionsTo(toVersion int64) error {
	sql.writeEv.treePruneCh <- &pruneSignal{pruneVersion: toVersion}
	sql.writeEv.leafPruneCh <- &pruneSignal{pruneVersion: toVersion}
	return nil
}

func (sql *SqliteDb) DeleteVersionsToSync(toVersion int64) error {
	sql.writeEv.awaitTreePruned = make(chan struct{})

	err := sql.DeleteVersionsTo(toVersion)
	if err != nil {
		sql.writeEv.awaitTreePruned = nil
		return err
	}

	<-sql.writeEv.awaitTreePruned
	return nil
}

func (sql *SqliteDb) newReadConn() (*SqliteReadConn, error) {
	var (
		conn *gosqlite.Conn
		err  error
	)

	conn, err = gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString(ReadOnly)))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", sql.opts.MmapSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", sql.opts.CacheSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return nil, err
	}

	// Below configuration may cause issues
	// err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", sql.opts.ThreadsCount))
	// if err != nil {
	// 	return nil, err
	// }
	// err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", sql.opts.StatementCache))
	// if err != nil {
	// 	return nil, err
	// }
	// err = conn.Exec("PRAGMA read_uncommitted=ON;")
	// if err != nil {
	// 	return nil, err
	// }

	c := NewSqliteReadConn(conn, &sql.opts, sql.logger)

	return c, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	// if sql.read != nil {
	// 	err = sql.read.Close()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// sql.read, err = sql.newReadConn()

	if sql.read == nil {
		sql.read, err = sql.newReadConn()
		return err
	}

	err = sql.read.conn.Exec("PRAGMA query_only=OFF;")
	if err != nil {
		return err
	}

	err = sql.read.conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) getReadConn() (*SqliteReadConn, error) {
	if sql.useReadPool {
		return sql.readPool.GetConn()
	}

	var err error
	if sql.read == nil {
		sql.read, err = sql.newReadConn()
	}

	return sql.read, err
}

func (sql *SqliteDb) newHashConnection() (*SqliteReadConn, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString(ReadOnly)))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", 100))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA read_uncommitted=OFF;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return nil, err
	}

	return &SqliteReadConn{
		conn:        conn,
		treeVersion: 0,
		opts:        &sql.opts,
		logger:      sql.logger,
	}, nil
}

func (sql *SqliteDb) GetReadConn() (db.ReadConn, error) {
	sql.rw.Lock()
	defer sql.rw.Unlock()

	for _, conn := range sql.hashPool {
		if conn.IsInUse() {
			continue
		}

		conn.MarkInUse()
		return conn, nil
	}

	conn, err := sql.newHashConnection()
	if err != nil {
		return nil, err
	}

	conn.MarkInUse()
	sql.hashPool = append(sql.hashPool, conn)

	return conn, nil
}

func (sql *SqliteDb) GetNode(nodePool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error) {
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

func (sql *SqliteDb) Close() error {
	if sql.writeDb != nil {
		if err := sql.writeDb.Close(); err != nil {
			return err
		}
	}

	if sql.writeCancel != nil {
		sql.writeCancel()
		sql.writeEv.awaitStop()
	}

	if err := sql.closeHangingIterators(); err != nil {
		return err
	}

	if sql.readPool != nil {
		if err := sql.readPool.Close(); err != nil {
			return err
		}
	}

	if sql.hashPool != nil {
		for _, conn := range sql.hashPool {
			if err := conn.Close(); err != nil {
				return err
			}
		}
	}

	if sql.read != nil {
		if err := sql.read.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (sql *SqliteDb) LoadRoot(nodePool *nodepool.NodePool, version int64) (*inode.Node, error) {
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
		rootKey := inode.NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = inode.Decode(nodePool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (sql *SqliteDb) WarmLeaves() error {
	start := time.Now()

	var stmt *gosqlite.Stmt

	// Use the connection pool if available
	if sql.readPool != nil {
		conn, err := sql.readPool.GetConn()
		if err != nil {
			return err
		}
		defer conn.MarkIdle()

		stmt, err = conn.conn.Prepare("SELECT version, sequence, key_hash, bytes FROM changelog.leaf")
		if err != nil {
			return err
		}
	} else {
		read, err := sql.getReadConn()
		if err != nil {
			return err
		}

		stmt, err = read.Prepare("SELECT version, sequence, key_hash, bytes FROM leaf")
		if err != nil {
			return err
		}
	}

	var (
		cnt, version, seq int64
		kz, vz            []byte
	)
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		err = stmt.Scan(&version, &seq, &kz, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}

	sql.logger.Info(fmt.Sprintf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start)))

	return stmt.Close()
}

func (sql *SqliteDb) closeHangingIterators() error {
	if sql.readPool != nil {
		return sql.readPool.CloseHangingIterators()
	}

	return nil
}

// FIXME:
// TODO:
// func (sql *SqliteDb) replayChangelog(tree *Tree, toVersion int64, targetHash []byte) error {
// 	var (
// 		version     int
// 		lastVersion int
// 		sequence    int
// 		bz          []byte
// 		key         []byte
// 		count       int64
// 		start       = time.Now()
// 		since       = time.Now()
// 		logPath     = []interface{}{"path", sql.opts.Path}
// 	)
// 	tree.isReplaying = true
// 	defer func() {
// 		tree.isReplaying = false
// 	}()
//
// 	sql.opts.Logger.Info(fmt.Sprintf("replaying changelog from=%d to=%d", tree.version.Load(), toVersion), logPath...)
//
// 	var q *gosqlite.Stmt
// 	var conn *SqliteReadConn
// 	var err error
//
// 	conn, err = sql.getReadConn()
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.MarkIdle()
//
// 	q, err = conn.Prepare(`SELECT * FROM (
// 			SELECT version, sequence, key_hash, bytes
// 		FROM leaf WHERE version > ? AND version <= ?
// 		) as ops
// 		ORDER BY version, sequence`)
// 	if err != nil {
// 		return err
// 	}
// 	defer q.Reset()
//
// 	if err = q.Bind(tree.version.Load(), toVersion); err != nil {
// 		return err
// 	}
//
// 	for {
// 		ok, err := q.Step()
// 		if err != nil {
// 			return err
// 		}
// 		if !ok {
// 			break
// 		}
// 		count++
// 		if err = q.Scan(&version, &sequence, &key, &bz); err != nil {
// 			return err
// 		}
// 		if version-1 != lastVersion {
// 			tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
// 			tree.version.Store(int64(version - 1))
// 			tree.resetSequences()
// 			lastVersion = version - 1
// 		}
// 		if bz != nil {
// 			nk := inode.NewNodeKey(0, 0)
// 			node, err := inode.Decode(tree.pool, nk, bz)
// 			if err != nil {
// 				return err
// 			}
// 			if _, err = tree.Set(node.key, node.hash); err != nil {
// 				return err
// 			}
// 			if sequence != int(tree.leafSequence) {
// 				return fmt.Errorf("sequence mismatch version=%d; expected %d got %d; path=%s",
// 					version, sequence, tree.leafSequence, sql.opts.Path)
// 			}
// 		} else {
// 			if _, _, err = tree.Remove(key); err != nil {
// 				return err
// 			}
// 			deleteSequence := tree.deletes[len(tree.deletes)-1].deleteKey.Sequence()
// 			if sequence != int(deleteSequence) {
// 				return fmt.Errorf("sequence delete mismatch; version=%d expected %d got %d; path=%s",
// 					version, sequence, tree.leafSequence, sql.opts.Path)
// 			}
// 		}
// 		if count%250_000 == 0 {
// 			sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s node/s=%s",
// 				version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds()))), logPath)
// 			since = time.Now()
// 		}
// 	}
// 	rootHash := tree.computeHash()
// 	if !bytes.Equal(targetHash, rootHash) {
// 		return fmt.Errorf("root hash mismatch; expected %x got %x", targetHash, rootHash)
// 	}
// 	tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
// 	tree.resetSequences()
// 	tree.version.Store(toVersion)
// 	sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s dur=%s root=%v",
// 		tree.version.Load(), humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root), logPath)
// 	return q.Close()
// }

func (sql *SqliteDb) Logger() logger.Logger {
	return sql.logger
}

func DefaultOptions(opts Options) Options {
	return defaultOptions(opts)
}

func (sql *SqliteDb) GetAt(version int64, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("get value with key length 0")
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.getVersioned(version, key)
}

func (sql *SqliteDb) HasRoot(version int64) (bool, error) {
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

func (sql *SqliteDb) GetHeightOneBranchesIteratorQuery(start, end int64) (stmt *gosqlite.Stmt, err error) {
	fromShardID := ToShardID(start)
	toShardID := ToShardID(end)
	if fromShardID != toShardID {
		return nil, fmt.Errorf("from shard %d to shard %d, cross different branch shards are not support", fromShardID, toShardID)
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	shardID := ToShardID(start)

	stmt, err = conn.Prepare(
		fmt.Sprintf("SELECT version, sequence, bytes FROM tree_%d WHERE version >= ? AND version <= ? ORDER BY version ASC", shardID))
	if err != nil {
		return nil, err
	}

	if err = stmt.Bind(start, end); err != nil {
		return nil, err
	}

	return stmt, err
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
