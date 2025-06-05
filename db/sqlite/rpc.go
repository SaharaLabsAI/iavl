package sqlite

import (
	"fmt"
	"sync"

	"github.com/eatonphil/gosqlite"
	"lukechampine.com/blake3"

	"github.com/cosmos/iavl/v2/common/logger"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	inode "github.com/cosmos/iavl/v2/node"
)

type SqliteReadConn struct {
	conn *gosqlite.Conn

	treeVersion int64

	queryLeaf *gosqlite.Stmt
	queryKV   *gosqlite.Stmt

	queryBranch *BranchShardQuery

	opts *Options

	inUse  bool
	logger logger.Logger

	mu sync.RWMutex
}

func NewSqliteReadConn(conn *gosqlite.Conn, opts *Options, logger logger.Logger) *SqliteReadConn {
	return &SqliteReadConn{
		conn:        conn,
		treeVersion: 0,
		opts:        opts,
		inUse:       false,
		logger:      logger,
	}
}

func NewSqliteImmutableReadConn(treeVersion int64, opts *Options, logger logger.Logger) *SqliteReadConn {
	return &SqliteReadConn{
		treeVersion: treeVersion,
		opts:        opts,
		inUse:       false,
		logger:      logger,
	}
}

func (c *SqliteReadConn) ResetToTreeVersion(version int64) error {
	if c.treeVersion >= version {
		// No need to reset
		return nil
	}

	if c.conn != nil {
		if err := c.Close(); err != nil {
			return err
		}
		c.conn = nil
	}

	conn, err := gosqlite.Open(c.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", c.opts.leafConnectionString(ReadOnly)))
	if err != nil {
		conn.Close()
		return err
	}

	err = conn.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		conn.Close()
		return err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", 100))
	if err != nil {
		conn.Close()
		return err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", c.opts.BusyTimeout))
	if err != nil {
		return err
	}

	err = conn.Exec("PRAGMA read_uncommitted=OFF;")
	if err != nil {
		return err
	}

	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return err
	}

	// Below configuration may cause issues
	// err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", c.opts.ThreadsCount))
	// if err != nil {
	// 	return err
	// }

	// err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", c.opts.StatementCache))
	// if err != nil {
	// 	return err
	// }

	c.conn = conn

	return nil
}

func (c *SqliteReadConn) Prepare(statement string, args ...interface{}) (*gosqlite.Stmt, error) {
	return c.conn.Prepare(statement, args...)
}

func (c *SqliteReadConn) getVersioned(version int64, key []byte) ([]byte, error) {
	defer c.MarkIdle()

	if len(key) == 0 {
		return nil, fmt.Errorf("get value with key length 0")
	}

	keyHash := blake3.Sum256(key)

	var err error
	if c.queryKV == nil {
		c.queryKV, err = c.conn.Prepare("SELECT bytes FROM changelog.leaf WHERE key_hash = ? AND version <= ? ORDER BY version DESC LIMIT 1")
		if err != nil {
			return nil, err
		}
	}
	defer c.queryKV.Reset()

	if err = c.queryKV.Bind(keyHash[:], version); err != nil {
		return nil, err
	}

	hasRow, err := c.queryKV.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}

	var nodeBz gosqlite.RawBytes
	err = c.queryKV.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	if nodeBz == nil {
		return nil, nil
	}

	return inode.DecodeValueOnly(nodeBz)
}

func (c *SqliteReadConn) GetLeaf(pool *nodepool.NodePool, nodeKey inode.NodeKey) (*inode.Node, error) {
	defer c.MarkIdle()

	var err error
	if c.queryLeaf == nil {
		c.queryLeaf, err = c.conn.Prepare("SELECT bytes FROM changelog.leaf WHERE version = ? AND sequence = ? LIMIT 1")
		if err != nil {
			return nil, err
		}
	}
	defer c.queryLeaf.Reset()

	if err = c.queryLeaf.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}

	hasRow, err := c.queryLeaf.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var nodeBz gosqlite.RawBytes
	err = c.queryLeaf.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	node, err := inode.Decode(pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *SqliteReadConn) GetNode(pool *nodepool.NodePool, nodeKey inode.NodeKey) (*inode.Node, error) {
	defer c.MarkIdle()

	var err error
	if c.queryBranch == nil {
		c.queryBranch = PrepareBranchShardQuery(c)
	}

	if err := c.queryBranch.PrepareVersion(c, nodeKey.Version()); err != nil {
		return nil, err
	}

	q, err := c.queryBranch.Bind(nodeKey.Version(), nodeKey.Sequence())
	if err != nil {
		return nil, err
	}
	defer q.Reset()

	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d; path=%s",
			nodeKey, ToShardID(nodeKey.Version()), c.opts.Path)
	}
	if err != nil {
		return nil, err
	}

	var nodeBz gosqlite.RawBytes
	err = q.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	node, err := inode.Decode(pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *SqliteReadConn) IsInUse() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.inUse
}

func (c *SqliteReadConn) MarkInUse() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.inUse = true
}

func (c *SqliteReadConn) MarkIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.inUse = false
}

func (c *SqliteReadConn) Close() error {
	if c.queryLeaf != nil {
		if err := c.queryLeaf.Close(); err != nil {
			return err
		}
		c.queryLeaf = nil
	}

	if c.queryKV != nil {
		if err := c.queryKV.Close(); err != nil {
			return err
		}
		c.queryKV = nil
	}

	if c.queryBranch != nil {
		if err := c.queryBranch.Close(); err != nil {
			return err
		}
		c.queryBranch = nil
	}

	return c.conn.Close()
}
