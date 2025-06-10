package sqlite

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/metrics"
)

type SqliteReadonlyConnPool struct {
	opts *Options

	treeVersion atomic.Int64
	savingTree  atomic.Bool

	conns *ConnPool
	iters *IterPool

	metrics metrics.Proxy
	logger  logger.Logger

	mu sync.RWMutex
}

func NewSqliteReadonlyConnPool(opts *Options, MaxPoolSize int) (*SqliteReadonlyConnPool, error) {
	if MaxPoolSize <= 0 {
		MaxPoolSize = defaultMaxPoolSize
	}

	pool := &SqliteReadonlyConnPool{
		opts:    opts,
		conns:   NewConnPool(opts, MaxPoolSize, opts.Logger),
		iters:   NewIterPool(opts.Logger),
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	// pool.logger.Info(fmt.Sprintf("Created readonly connection pool with max size %d", MaxPoolSize))

	return pool, nil
}

func (pool *SqliteReadonlyConnPool) SetTreeVersion(version int64) {
	pool.treeVersion.Store(version)
}

func (pool *SqliteReadonlyConnPool) SetSavingTree() {
	pool.savingTree.Store(true)
	pool.mu.Lock()
}

func (pool *SqliteReadonlyConnPool) UnsetSavingTree() {
	pool.savingTree.Store(false)
	pool.mu.Unlock()
}

func (pool *SqliteReadonlyConnPool) GetConn() (*SqliteReadConn, error) {
	pool.mu.RLock()

	if pool.savingTree.Load() {
		pool.mu.RUnlock()

		ctx := context.Background()
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				// Check if checkpoint finished
				pool.mu.RLock()
				if pool.savingTree.Load() {
					pool.mu.RUnlock()
					continue
				}

				// Checkpoint done, proceed with connection
				defer pool.mu.RUnlock()
				// Continue with existing GetConn logic
				return pool.conns.getConn(pool.treeVersion.Load())
			}
		}
	}

	defer pool.mu.RUnlock()
	return pool.conns.getConn(pool.treeVersion.Load())
}

// Close closes all connections in the pool
func (pool *SqliteReadonlyConnPool) Close() error {
	if err := pool.iters.closeHangingIterators(); err != nil {
		return err
	}

	return pool.conns.close()
}

func (pool *SqliteReadonlyConnPool) ResetShardQueries() {
	// disable now because we don't enable sharding

	// pool.mu.Lock()
	// defer pool.mu.Unlock()
	//
	// for _, conn := range pool.conns {
	// 	conn.SetPendingResetShard()
	// }
}

func (pool *SqliteReadonlyConnPool) CloseKVIterstor(idx int) error {
	return pool.iters.closeKVIterstor(idx)
}

func (pool *SqliteReadonlyConnPool) CloseHangingIterators() error {
	return pool.iters.closeHangingIterators()
}

func (pool *SqliteReadonlyConnPool) GetVersionDescLeafIterator(version int64, limit int) (stmt *gosqlite.Stmt, idx int, err error) {
	conn, err := pool.GetConn()
	if err != nil {
		return nil, 0, err
	}

	idx = pool.iters.nextIdx()

	stmt, err = conn.Prepare(`
		SELECT l.key_hash, l.bytes, l.version
		FROM changelog.leaf l
		INNER JOIN (
			SELECT key_hash, MAX(version) as max_version
			FROM changelog.leaf
			WHERE bytes IS NOT NULL AND version <= ?
			GROUP BY key_hash
		) m ON l.key_hash = m.key_hash AND l.version = m.max_version
		LIMIT ?;
	`)
	if err != nil {
		return nil, idx, err
	}

	if err = stmt.Bind(version, limit); err != nil {
		return nil, idx, err
	}

	pool.iters.setIterator(idx, stmt, conn)

	return stmt, idx, nil
}

type ConnPool struct {
	opts *Options

	conns []*SqliteReadConn

	logger logger.Logger

	mu sync.Mutex
}

func NewConnPool(opts *Options, MaxPoolSize int, logger logger.Logger) *ConnPool {
	return &ConnPool{
		opts:   opts,
		conns:  make([]*SqliteReadConn, 0, MaxPoolSize),
		logger: logger,
	}
}

func (c *ConnPool) getConn(version int64) (*SqliteReadConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		if !conn.IsInUse() {
			conn.MarkInUse()
			conn.ResetToTreeVersion(version)
			return conn, nil
		}
	}

	if len(c.conns) > c.opts.MaxPoolSize {
		return nil, fmt.Errorf("service busy, try again later")
	}

	conn := NewSqliteImmutableReadConn(version, c.opts, c.logger)
	conn.MarkInUse()
	conn.ResetToTreeVersion(conn.treeVersion + 1) // Force reset on first connect
	c.conns = append(c.conns, conn)

	c.logger.Debug(fmt.Sprintf("Created new connection, pool size now: %d", len(c.conns)))

	return conn, nil
}

func (c *ConnPool) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for _, conn := range c.conns {
		lastErr = conn.conn.Close()
	}

	c.conns = nil

	return lastErr
}

type IterPool struct {
	kvItrIdx    int
	kvIterators map[int]*gosqlite.Stmt
	kvItrConns  map[int]*SqliteReadConn

	logger logger.Logger

	mu sync.Mutex
}

func NewIterPool(logger logger.Logger) *IterPool {
	return &IterPool{
		kvIterators: make(map[int]*gosqlite.Stmt),
		kvItrConns:  make(map[int]*SqliteReadConn),
		logger:      logger,
	}
}

func (i *IterPool) nextIdx() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.kvItrIdx++

	return i.kvItrIdx
}

func (i *IterPool) setIterator(idx int, stmt *gosqlite.Stmt, conn *SqliteReadConn) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.kvIterators[idx] = stmt
	i.kvItrConns[idx] = conn
}

func (i *IterPool) closeKVIterstor(idx int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	var err error
	stmt, exists := i.kvIterators[idx]
	if exists {
		err = stmt.Close()
		delete(i.kvIterators, idx)
	}

	conn, exists := i.kvItrConns[idx]
	if exists {
		conn.MarkIdle()
		delete(i.kvItrConns, idx)
	}

	return err
}

func (i *IterPool) closeHangingIterators() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for idx, stmt := range i.kvIterators {
		i.logger.Info(fmt.Sprintf("closing hanging iterator idx=%d", idx))

		if err := stmt.Close(); err != nil {
			return err
		}

		if i.kvItrConns[idx] != nil {
			i.kvItrConns[idx].MarkIdle()
			delete(i.kvItrConns, idx)
		}

		delete(i.kvIterators, idx)
	}

	i.kvItrIdx = 0

	return nil
}
