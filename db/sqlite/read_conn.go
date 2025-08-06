package sqlite

import (
	"fmt"
	"hash"
	"sync/atomic"

	"github.com/eatonphil/gosqlite"

	"github.com/SaharaLabsAI/iavl/v2/common/constants"
	"github.com/SaharaLabsAI/iavl/v2/common/logger"
	hashpool "github.com/SaharaLabsAI/iavl/v2/common/pool/hash"
	nodepool "github.com/SaharaLabsAI/iavl/v2/common/pool/node"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

type ReadConn struct {
	conn *gosqlite.Conn

	opts   *Options
	logger logger.Logger

	queryLeaf   *gosqlite.Stmt
	queryKV     *gosqlite.Stmt
	queryBranch *BranchShardQuery

	busy atomic.Bool
}

func NewMainReadConn(opts *Options, logger logger.Logger) (*ReadConn, error) {
	conn, err := NewReadConn(opts, logger)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", opts.CacheSize))
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", opts.TempStoreSize))
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func NewReadConn(opts *Options, logger logger.Logger) (*ReadConn, error) {
	conn, err := gosqlite.Open(opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", opts.leafConnectionString(ReadOnly)))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		conn.Close()
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

	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", opts.BusyTimeout))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA read_uncommitted=OFF;")
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Below configuration may cause issues

	// err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", opts.ThreadsCount))
	// if err != nil {
	// conn.Close()
	// 	return err
	// }

	// err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", opts.StatementCache))
	// if err != nil {
	// conn.Close()
	// 	return err
	// }

	c := &ReadConn{
		conn:   conn,
		opts:   opts,
		logger: logger,
	}

	return c, nil
}

func (c *ReadConn) Refresh() error {
	err := c.conn.Exec("PRAGMA query_only=OFF;")
	if err != nil {
		return err
	}

	err = c.conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return err
	}

	return nil
}

func (c *ReadConn) Prepare(statement string, args ...any) (*gosqlite.Stmt, error) {
	return c.conn.Prepare(statement, args...)
}

func (c *ReadConn) Exec(stmt string, args ...any) error {
	defer c.Release()

	return c.conn.Exec(stmt, args...)
}

func (c *ReadConn) GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error) {
	defer c.Release()

	if constants.IsLeafSeq(nodekey.Sequence()) {
		return c.getLeaf(pool, nodekey)
	}

	return c.getNode(pool, nodekey)
}

func (c *ReadConn) GetValue(version int64, key []byte) ([]byte, error) {
	defer c.Release()

	if len(key) == 0 {
		return nil, fmt.Errorf("get value with key length 0")
	}

	var err error
	if c.queryKV == nil {
		c.queryKV, err = c.conn.Prepare("SELECT bytes FROM changelog.leaf WHERE key_hash = ? AND version <= ? ORDER BY version DESC LIMIT 1")
		if err != nil {
			return nil, err
		}
	}
	defer c.queryKV.Reset()

	h := hashpool.Blake3Pool.Get().(hash.Hash)
	defer hashpool.Blake3Pool.Put(h)

	h.Reset()
	h.Write(key)
	keyHash := h.Sum(nil)

	if err = c.queryKV.Bind(keyHash, version); err != nil {
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

func (c *ReadConn) IsBusy() bool {
	return c.busy.Load()
}

func (c *ReadConn) SetBusy() {
	c.busy.Store(true)
}

func (c *ReadConn) Release() error {
	c.busy.Store(false)

	return nil
}

func (c *ReadConn) Close() error {
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

func (c *ReadConn) getLeaf(pool *nodepool.NodePool, nodeKey inode.NodeKey) (*inode.Node, error) {
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

func (c *ReadConn) getNode(pool *nodepool.NodePool, nodeKey inode.NodeKey) (*inode.Node, error) {
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
