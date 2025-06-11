package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	api "github.com/kocubinski/costor-api"
	"github.com/kocubinski/costor-api/logz"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/pool"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	inode "github.com/cosmos/iavl/v2/node"
)

var ErrorExportDone = errors.New("export done")

type sqliteSnapshot struct {
	ctx context.Context

	snapshotInsert *gosqlite.Stmt

	sql        *DB
	leafInsert *gosqlite.Stmt
	treeInsert *gosqlite.Stmt

	// if set will flush nodes to a tree & leaf tables as well as a snapshot table during import
	writeTree bool

	lastWrite time.Time
	ordinal   int
	batchSize int
	version   int64
	getLeft   func(*inode.Node) *inode.Node
	getRight  func(*inode.Node) *inode.Node
	log       logger.Logger
}

type ExpectedTree interface {
	Version() int64
	Root() *inode.Node
	EnsureLeftNode(*inode.Node) *inode.Node
	EnsureRightNode(*inode.Node) *inode.Node
}

func (sql *DB) Snapshot(ctx context.Context, tree ExpectedTree) error {
	version := tree.Version()
	err := sql.write.leafWrite.Exec(
		fmt.Sprintf("CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);", version))
	if err != nil {
		return err
	}

	snapshot := &sqliteSnapshot{
		ctx:       ctx,
		sql:       sql,
		batchSize: 200_000,
		version:   version,
		log:       sql.logger,
		getLeft: func(node *inode.Node) *inode.Node {
			return tree.EnsureLeftNode(node)
		},
		getRight: func(node *inode.Node) *inode.Node {
			return tree.EnsureRightNode(node)
		},
	}
	if err = snapshot.prepareWrite(); err != nil {
		return err
	}
	if err = snapshot.writeStep(tree.Root()); err != nil {
		return err
	}
	if err = snapshot.flush(); err != nil {
		return err
	}
	sql.logger.Info(fmt.Sprintf("creating index on snapshot_%d", version), "path", sql.opts.Path)
	err = sql.write.leafWrite.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	return err
}

type SnapshotOptions struct {
	DontWriteSnapshot bool
	TraverseOrder     constants.TraverseOrderType
}

func NewIngestSnapshotConnection(snapshotDbPath string) (*gosqlite.Conn, error) {
	newDb := !api.IsFileExistent(snapshotDbPath)

	conn, err := gosqlite.Open(fmt.Sprintf("file:%s", snapshotDbPath), gosqlite.OPEN_READWRITE|gosqlite.OPEN_CREATE|gosqlite.OPEN_NOMUTEX)
	if err != nil {
		return nil, err
	}
	pageSize := os.Getpagesize()
	if newDb {
		err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return nil, err
		}

		err = conn.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return nil, err
		}
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	walSize := 1024 * 1024 * 1024
	if err = conn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", walSize/pageSize)); err != nil {
		return nil, err
	}
	return conn, err
}

func IngestSnapshot(conn *gosqlite.Conn, prefix string, version int64, nextFn func() (*SnapshotNode, error)) (*inode.Node, error) {
	var (
		insert    *gosqlite.Stmt
		tableName = fmt.Sprintf("snapshot_%s_%d", prefix, version)
		ordinal   int
		batchSize = 200_000
		log       = logz.Logger.With().Str("prefix", prefix).Logger()
		step      func() (*inode.Node, error)
		lastWrite = time.Now()
	)

	err := conn.Exec(fmt.Sprintf("CREATE TABLE %s (ordinal int, version int, sequence int, bytes blob);", tableName))
	if err != nil {
		return nil, err
	}
	prepare := func() error {
		if err = conn.Begin(); err != nil {
			return err
		}
		insert, err = conn.Prepare(
			fmt.Sprintf("INSERT INTO %s (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);", tableName))
		if err != nil {
			return err
		}
		return nil
	}
	flush := func() error {
		log.Info().Msgf("flush total=%s size=%s dur=%s wr/s=%s",
			humanize.Comma(int64(ordinal)),
			humanize.Comma(int64(batchSize)),
			time.Since(lastWrite).Round(time.Millisecond),
			humanize.Comma(int64(float64(batchSize)/time.Since(lastWrite).Seconds())),
		)
		err = errors.Join(conn.Commit(), insert.Close())
		lastWrite = time.Now()
		return err
	}
	maybeFlush := func() error {
		if ordinal%batchSize == 0 {
			if err = flush(); err != nil {
				return err
			}
			if err = prepare(); err != nil {
				return err
			}
		}
		return nil
	}
	if err = prepare(); err != nil {
		return nil, err
	}
	step = func() (*inode.Node, error) {
		snapshotNode, err := nextFn()
		if err != nil {
			return nil, err
		}
		ordinal++

		buf := pool.BufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer pool.BufPool.Put(buf)

		node := &inode.Node{}
		node.SetKey(snapshotNode.Key)
		node.SetSubTreeHeight(snapshotNode.Height)
		node.SetNodeKey(inode.NewNodeKey(snapshotNode.Version, uint32(ordinal)))

		// Leaf
		if node.SubTreeHeight() == 0 {
			node.SetValue(snapshotNode.Value)
			node.SetSize(1)
			node.HashSelf()

			buf.Reset()
			err := node.EncodeWithBuffer(buf)
			if err != nil {
				return nil, err
			}
			nodeBz := buf.Bytes()

			if err = insert.Exec(ordinal, snapshotNode.Version, ordinal, nodeBz); err != nil {
				return nil, err
			}
			if err = maybeFlush(); err != nil {
				return nil, err
			}
			return node, nil
		}

		leftNode, err := step()
		if err != nil {
			return nil, err
		}
		node.SetLeft(leftNode)
		rightNode, err := step()
		if err != nil {
			return nil, err
		}
		node.SetRight(rightNode)

		node.SetSize(node.LeftNode().Size() + node.RightNode().Size())
		node.HashSelf()

		// Release memory
		node.SetLeft(nil)
		node.SetRight(nil)

		buf.Reset()
		err = node.EncodeWithBuffer(buf)
		if err != nil {
			return nil, err
		}
		nodeBz := buf.Bytes()

		if err = insert.Exec(ordinal, snapshotNode.Version, ordinal, nodeBz); err != nil {
			return nil, err
		}
		if err = maybeFlush(); err != nil {
			return nil, err
		}

		return node, nil
	}
	root, err := step()
	if err != nil {
		return nil, err
	}
	if err = flush(); err != nil {
		return nil, err
	}
	if err = conn.Exec(fmt.Sprintf("CREATE INDEX %s_idx ON %s (ordinal);", tableName, tableName)); err != nil {
		return nil, err
	}
	if err = conn.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

func (sql *DB) WriteSnapshot(
	ctx context.Context, version int64, nextFn func() (*SnapshotNode, error), opts SnapshotOptions,
) (*inode.Node, error) {
	snap := &sqliteSnapshot{
		ctx:       ctx,
		sql:       sql,
		batchSize: 200_000,
		version:   version,
		lastWrite: time.Now(),
		log:       sql.logger,
		writeTree: true,
	}
	err := snap.sql.write.leafWrite.Exec(
		fmt.Sprintf(`CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);`, version))
	if err != nil {
		return nil, err
	}
	if err = snap.prepareWrite(); err != nil {
		return nil, err
	}

	var (
		root           *inode.Node
		uniqueVersions map[int64]struct{}
	)
	switch opts.TraverseOrder {
	case constants.PostOrder:
		root, uniqueVersions, err = snap.restorePostOrderStep(nextFn)
	case constants.PreOrder:
		root, uniqueVersions, err = snap.restorePreOrderStep(nextFn)
	}
	if err != nil {
		return nil, err
	}

	if err = snap.flush(); err != nil {
		return nil, err
	}

	var versions []int64 // where is this used?
	for v := range uniqueVersions {
		versions = append(versions, v)
	}

	if err = sql.SaveTree(version, root, nil); err != nil {
		return nil, err
	}

	sql.logger.Info("creating table indexes")
	err = sql.write.leafWrite.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	if err != nil {
		return nil, err
	}
	err = snap.sql.write.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);")
	if err != nil {
		return nil, err
	}

	return root, nil
}

type SnapshotNode struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}

func (sql *DB) ImportSnapshotFromTable(version int64, traverseOrder constants.TraverseOrderType, loadLeaves bool, nodePool *nodepool.NodePool) (*inode.Node, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	var q *gosqlite.Stmt
	switch traverseOrder {
	case constants.PostOrder:
		q, err = read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal DESC", version))
	case constants.PreOrder:
		q, err = read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal ASC", version))
	}
	if err != nil {
		return nil, err
	}
	defer func(q *gosqlite.Stmt) {
		err = q.Close()
		if err != nil {
			sql.logger.Error("error closing import query", "error", err)
		}
	}(q)

	imp := &sqliteImport{
		query:      q,
		pool:       nodePool,
		loadLeaves: loadLeaves,
		since:      time.Now(),
		log:        sql.logger,
	}

	var root *inode.Node
	switch traverseOrder {
	case constants.PostOrder:
		root, err = imp.queryStepPostOrder()
	case constants.PreOrder:
		root, err = imp.queryStepPreOrder()
	}
	if err != nil {
		return nil, err
	}

	if !loadLeaves {
		return root, nil
	}

	h := root.Hash()
	rehashTree(root)
	if !bytes.Equal(h, root.Hash()) {
		return nil, fmt.Errorf("rehash failed; expected=%x, got=%x", h, root.Hash())
	}

	return root, nil
}

func (sql *DB) ImportMostRecentSnapshot(targetVersion int64, traverseOrder constants.TraverseOrderType, loadLeaves bool, nodePool *nodepool.NodePool) (*inode.Node, int64, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, 0, err
	}
	q, err := read.Prepare("SELECT tbl_name FROM changelog.sqlite_master WHERE type='table' AND name LIKE 'snapshot_%' ORDER BY name DESC")
	defer func(q *gosqlite.Stmt) {
		err = q.Close()
		if err != nil {
			sql.logger.Error("error closing import query", "error", err)
		}
	}(q)
	if err != nil {
		return nil, 0, err
	}

	var (
		name    string
		version int64
	)
	for {
		ok, err := q.Step()
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, fmt.Errorf("no prior snapshot found version=%d path=%s", targetVersion, sql.opts.Path)
		}
		err = q.Scan(&name)
		if err != nil {
			return nil, 0, err
		}
		vs := name[len("snapshot_"):]
		if vs == "" {
			return nil, 0, fmt.Errorf("unexpected snapshot table name %s", name)
		}
		version, err = strconv.ParseInt(vs, 10, 64)
		if err != nil {
			return nil, 0, err
		}
		if version <= targetVersion {
			break
		}
	}

	root, err := sql.ImportSnapshotFromTable(version, traverseOrder, loadLeaves, nodePool)
	if err != nil {
		return nil, 0, err
	}
	return root, version, err
}

func FindDbsInPath(path string) ([]string, error) {
	var paths []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == "changelog.sqlite" {
			paths = append(paths, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

// TODO
// merge these two functions

func (snap *sqliteSnapshot) writeStep(node *inode.Node) error {
	snap.ordinal++
	// Pre-order, NLR traversal
	// Visit this node
	buf := pool.BufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer pool.BufPool.Put(buf)

	err := node.EncodeWithBuffer(buf)
	if err != nil {
		return err
	}
	nodeBz := buf.Bytes()

	err = snap.snapshotInsert.Exec(snap.ordinal, node.Version(), int(node.NodeKey().Sequence()), nodeBz)
	if err != nil {
		return err
	}

	if snap.ordinal%snap.batchSize == 0 {
		if err = snap.flush(); err != nil {
			return err
		}
		if err = snap.prepareWrite(); err != nil {
			return err
		}
	}

	if node.IsLeaf() {
		return nil
	}

	// traverse left
	err = snap.writeStep(snap.getLeft(node))
	if err != nil {
		return err
	}

	// traverse right
	return snap.writeStep(snap.getRight(node))
}

func (snap *sqliteSnapshot) flush() error {
	select {
	case <-snap.ctx.Done():
		snap.log.Info(fmt.Sprintf("snapshot cancelled at ordinal=%s", humanize.Comma(int64(snap.ordinal))))
		errs := errors.Join(
			snap.snapshotInsert.Reset(),
			snap.snapshotInsert.Close(),
		)
		if errs != nil {
			return errs
		}
		if snap.writeTree {
			errs = errors.Join(
				snap.leafInsert.Reset(),
				snap.leafInsert.Close(),
				snap.treeInsert.Reset(),
				snap.treeInsert.Close(),
			)
		}
		errs = errors.Join(
			errs,
			snap.sql.write.leafWrite.Rollback(),
			snap.sql.write.leafWrite.Close(),
		)
		if errs != nil {
			return errs
		}
		if snap.writeTree {
			errs = errors.Join(
				errs,
				snap.sql.write.treeWrite.Rollback(),
				snap.sql.write.treeWrite.Close(),
			)
		}

		return errs
	default:
	}

	snap.log.Info(fmt.Sprintf("flush total=%s size=%s dur=%s wr/s=%s",
		humanize.Comma(int64(snap.ordinal)),
		humanize.Comma(int64(snap.batchSize)),
		time.Since(snap.lastWrite).Round(time.Millisecond),
		humanize.Comma(int64(float64(snap.batchSize)/time.Since(snap.lastWrite).Seconds())),
	))

	err := errors.Join(
		snap.sql.write.leafWrite.Commit(),
		snap.snapshotInsert.Close(),
	)
	if err != nil {
		return err
	}
	if snap.writeTree {
		err = errors.Join(
			snap.leafInsert.Close(),
			snap.sql.write.treeWrite.Commit(),
			snap.treeInsert.Close(),
		)
	}
	snap.lastWrite = time.Now()
	return err
}

func (snap *sqliteSnapshot) prepareWrite() error {
	err := snap.sql.write.leafWrite.Begin()
	if err != nil {
		return err
	}

	snap.snapshotInsert, err = snap.sql.write.leafWrite.Prepare(
		fmt.Sprintf("INSERT INTO snapshot_%d (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);",
			snap.version))

	if snap.writeTree {
		err = snap.sql.write.treeWrite.Begin()
		if err != nil {
			return err
		}

		snap.leafInsert, err = snap.sql.write.leafWrite.Prepare("INSERT INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
		if err != nil {
			return err
		}
		snap.treeInsert, err = snap.sql.write.treeWrite.Prepare(
			fmt.Sprintf("INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", snap.version))
	}

	return err
}

func (snap *sqliteSnapshot) restorePostOrderStep(nextFn func() (*SnapshotNode, error)) (*inode.Node, map[int64]struct{}, error) {
	var (
		snapshotNode   *SnapshotNode
		err            error
		count          int
		stack          []*inode.Node
		uniqueVersions = make(map[int64]struct{})
	)

	for {
		snapshotNode, err = nextFn()
		if err != nil || snapshotNode == nil {
			break
		}

		ordinal := snap.ordinal

		uniqueVersions[snapshotNode.Version] = struct{}{}
		node := &inode.Node{}
		node.SetKey(snapshotNode.Key)
		node.SetSubTreeHeight(snapshotNode.Height)
		node.SetNodeKey(inode.NewNodeKey(snapshotNode.Version, uint32(ordinal)))

		stackSize := len(stack)
		if node.IsLeaf() {
			node.SetValue(snapshotNode.Value)
			node.SetSize(1)
			node.HashSelf()

			count++
			if err := snap.writeSnapNode(node, snapshotNode.Version, count, ordinal, count); err != nil {
				return nil, nil, err
			}
		} else if stackSize >= 2 && stack[stackSize-1].SubTreeHeight() < node.SubTreeHeight() && stack[stackSize-2].SubTreeHeight() < node.SubTreeHeight() {
			node.SetLeft(stack[stackSize-2])
			node.SetRight(stack[stackSize-1])
			node.SetSize(node.LeftNode().Size() + node.RightNode().Size())
			node.HashSelf()
			stack = stack[:stackSize-2]

			node.SetLeft(nil)
			node.SetRight(nil)

			count++
			if err := snap.writeSnapNode(node, snapshotNode.Version, count, ordinal, count); err != nil {
				return nil, nil, err
			}
		}

		stack = append(stack, node)
		snap.ordinal++
	}

	if err != nil && !errors.Is(err, ErrorExportDone) {
		return nil, nil, err
	}

	if len(stack) != 1 {
		return nil, nil, fmt.Errorf("expected stack size 1, got %d", len(stack))
	}

	return stack[0], uniqueVersions, nil
}

func (snap *sqliteSnapshot) restorePreOrderStep(nextFn func() (*SnapshotNode, error)) (*inode.Node, map[int64]struct{}, error) {
	var (
		count          int
		step           func() (*inode.Node, error)
		uniqueVersions = make(map[int64]struct{})
	)

	step = func() (*inode.Node, error) {
		snapshotNode, err := nextFn()
		if err != nil {
			return nil, err
		}

		ordinal := snap.ordinal
		snap.ordinal++

		node := &inode.Node{}
		node.SetKey(snapshotNode.Key)
		node.SetSubTreeHeight(snapshotNode.Height)
		node.SetNodeKey(inode.NewNodeKey(snapshotNode.Version, uint32(ordinal)))

		if node.IsLeaf() {
			node.SetValue(snapshotNode.Value)
			node.SetSize(1)
			node.HashSelf()
		} else {
			leftNode, err := step()
			if err != nil {
				return nil, err
			}
			node.SetLeft(leftNode)

			rightNode, err := step()
			if err != nil {
				return nil, err
			}
			node.SetRight(rightNode)

			node.SetSize(node.LeftNode().Size() + node.RightNode().Size())
			node.HashSelf()

			node.SetLeft(nil)
			node.SetRight(nil)

			uniqueVersions[snapshotNode.Version] = struct{}{}
		}

		count++
		if err := snap.writeSnapNode(node, snapshotNode.Version, ordinal, ordinal, count); err != nil {
			return nil, err
		}
		snap.ordinal++

		return node, nil
	}

	node, err := step()

	return node, uniqueVersions, err
}

func (snap *sqliteSnapshot) writeSnapNode(node *inode.Node, version int64, ordinal, sequence, count int) error {
	buf := pool.BufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer pool.BufPool.Put(buf)

	err := node.EncodeWithBuffer(buf)
	if err != nil {
		return err
	}
	nodeBz := buf.Bytes()

	if err = snap.snapshotInsert.Exec(ordinal, version, sequence, nodeBz); err != nil {
		return err
	}
	if snap.writeTree {
		if node.IsLeaf() {
			if err = snap.leafInsert.Exec(version, ordinal, nodeBz); err != nil {
				return err
			}
		} else {
			if err = snap.treeInsert.Exec(version, sequence, nodeBz); err != nil {
				return err
			}
		}
	}

	if count%snap.batchSize == 0 {
		if err := snap.flush(); err != nil {
			return err
		}
		if err := snap.prepareWrite(); err != nil {
			return err
		}
	}

	return nil
}

func rehashTree(node *inode.Node) {
	if node.IsLeaf() {
		return
	}
	node.SetHash(nil)

	rehashTree(node.LeftNode())
	rehashTree(node.RightNode())

	node.HashSelf()
}

type sqliteImport struct {
	query      *gosqlite.Stmt
	pool       *nodepool.NodePool
	loadLeaves bool

	i     int64
	since time.Time
	log   logger.Logger
}

func (sqlImport *sqliteImport) queryStepPreOrder() (node *inode.Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		sqlImport.log.Debug(fmt.Sprintf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		))
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bz gosqlite.RawBytes
	var version, seq int
	err = sqlImport.query.Scan(&version, &seq, &bz)
	if err != nil {
		return nil, err
	}
	nodeKey := inode.NewNodeKey(int64(version), uint32(seq))
	node, err = inode.Decode(sqlImport.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}

	if node.IsLeaf() && sqlImport.i > 1 {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	leftNode, err := sqlImport.queryStepPreOrder()
	if err != nil {
		return nil, err
	}
	node.SetLeft(leftNode)

	rightNode, err := sqlImport.queryStepPreOrder()
	if err != nil {
		return nil, err
	}
	node.SetRight(rightNode)

	return node, nil
}

func (sqlImport *sqliteImport) queryStepPostOrder() (node *inode.Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		sqlImport.log.Debug(fmt.Sprintf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		))
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bz gosqlite.RawBytes
	var version, seq int
	err = sqlImport.query.Scan(&version, &seq, &bz)
	if err != nil {
		return nil, err
	}
	nodeKey := inode.NewNodeKey(int64(version), uint32(seq))
	node, err = inode.Decode(sqlImport.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}

	if node.IsLeaf() && sqlImport.i > 1 {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	rightNode, err := sqlImport.queryStepPostOrder()
	if err != nil {
		return nil, err
	}
	node.SetRight(rightNode)

	leftNode, err := sqlImport.queryStepPostOrder()
	if err != nil {
		return nil, err
	}
	node.SetLeft(leftNode)

	return node, nil
}
