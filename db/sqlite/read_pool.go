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
	inode "github.com/cosmos/iavl/v2/node"
)

type ReadConnPool struct {
	opts *Options

	savingTree atomic.Bool

	conns *ConnPool
	iters *IterPool

	metrics metrics.Proxy
	logger  logger.Logger

	mu sync.RWMutex
}

// NOTE: This pool is primary used for rpc query
func NewReadConnPool(opts *Options, MaxPoolSize int) (*ReadConnPool, error) {
	if MaxPoolSize <= 0 {
		MaxPoolSize = defaultMaxPoolSize
	}

	pool := &ReadConnPool{
		opts:    opts,
		conns:   NewConnPool(opts, MaxPoolSize, opts.Logger),
		iters:   NewIterPool(opts.Logger),
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	// pool.logger.Info(fmt.Sprintf("Created readonly connection pool with max size %d", MaxPoolSize))

	return pool, nil
}

func (pool *ReadConnPool) SetSavingTree() {
	pool.savingTree.Store(true)
	pool.mu.Lock()
}

func (pool *ReadConnPool) UnsetSavingTree() {
	pool.savingTree.Store(false)
	pool.mu.Unlock()
}

func (pool *ReadConnPool) GetConn() (*ReadConn, error) {
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
				return pool.conns.getConn()
			}
		}
	}

	defer pool.mu.RUnlock()
	return pool.conns.getConn()
}

// Close closes all connections in the pool
func (pool *ReadConnPool) Close() error {
	if err := pool.iters.closeHangingIterators(); err != nil {
		return err
	}

	return pool.conns.close()
}

func (pool *ReadConnPool) CloseKVIterstor(idx int) error {
	return pool.iters.closeKVIterstor(idx)
}

func (pool *ReadConnPool) CloseHangingIterators() error {
	return pool.iters.closeHangingIterators()
}

func (pool *ReadConnPool) getLatestLeavesIterator(version int64, limit int) (*KVIterator, error) {
	conn, err := pool.GetConn()
	if err != nil {
		return nil, err
	}

	idx := pool.iters.nextIdx()

	stmt, err := conn.Prepare(`
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
		return nil, err
	}

	if err = stmt.Bind(version, limit); err != nil {
		return nil, err
	}

	pool.iters.setIterator(idx, stmt, conn)

	iter := &KVIterator{
		IterPool: pool.iters,
		valid:    true,
		metrics:  pool.metrics,
	}

	iter.Next()

	return iter, nil
}

type ConnPool struct {
	opts *Options

	conns []*ReadConn

	logger logger.Logger

	mu sync.Mutex
}

func NewConnPool(opts *Options, MaxPoolSize int, logger logger.Logger) *ConnPool {
	return &ConnPool{
		opts:   opts,
		conns:  make([]*ReadConn, 0, MaxPoolSize),
		logger: logger,
	}
}

func (c *ConnPool) getConn() (*ReadConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		if !conn.IsBusy() {
			conn.SetBusy()
			return conn, nil
		}
	}

	if len(c.conns) > c.opts.MaxPoolSize {
		return nil, fmt.Errorf("service busy, try again later")
	}

	conn, err := NewReadConn(c.opts, c.logger)
	if err != nil {
		return nil, err
	}

	conn.SetBusy()
	c.conns = append(c.conns, conn)

	c.logger.Debug(fmt.Sprintf("Created new connection, pool size now: %d", len(c.conns)))

	return conn, nil
}

func (c *ConnPool) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for _, conn := range c.conns {
		lastErr = conn.Close()
	}

	c.conns = nil

	return lastErr
}

type IterPool struct {
	kvItrIdx    int
	kvIterators map[int]*gosqlite.Stmt
	kvItrConns  map[int]*ReadConn

	logger logger.Logger

	mu sync.Mutex
}

func NewIterPool(logger logger.Logger) *IterPool {
	return &IterPool{
		kvIterators: make(map[int]*gosqlite.Stmt),
		kvItrConns:  make(map[int]*ReadConn),
		logger:      logger,
	}
}

func (i *IterPool) nextIdx() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.kvItrIdx++

	return i.kvItrIdx
}

func (i *IterPool) setIterator(idx int, stmt *gosqlite.Stmt, conn *ReadConn) {
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
		conn.Release()
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
			i.kvItrConns[idx].Release()
			delete(i.kvItrConns, idx)
		}

		delete(i.kvIterators, idx)
	}

	i.kvItrIdx = 0

	return nil
}

type KVIterator struct {
	IterPool *IterPool
	itrStmt  *gosqlite.Stmt
	start    []byte
	end      []byte
	valid    bool
	err      error
	key      []byte
	value    []byte
	metrics  metrics.Proxy
	itrIdx   int
}

func (i *KVIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *KVIterator) Valid() bool {
	return i.valid
}

func (i *KVIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "kv iterator", "next")
	}
	if !i.valid {
		return
	}

	hasRow, err := i.itrStmt.Step()
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
	if !hasRow {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	var nodeBz gosqlite.RawBytes
	if err = i.itrStmt.Scan(&i.key, &nodeBz); err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	i.value, err = inode.DecodeValueOnly(nodeBz)
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
}

func (i *KVIterator) Key() (key []byte) {
	return i.key
}

func (i *KVIterator) Value() (value []byte) {
	return i.value
}

func (i *KVIterator) Error() error {
	return i.err
}

func (i *KVIterator) Close() error {
	if i.valid {
		if i.metrics != nil {
			i.metrics.IncrCounter(1, "iavl2", "iterator", "close")
		}
		i.valid = false
	}

	return i.IterPool.closeKVIterstor(i.itrIdx)
}
