package sqlite

import (
	"bytes"
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"

	"github.com/SaharaLabsAI/iavl/v2/common/logger"
	"github.com/SaharaLabsAI/iavl/v2/common/metrics"
	"github.com/SaharaLabsAI/iavl/v2/common/pool"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

type WriteConn struct {
	// 2 separate databases and 2 separate connections.
	// the underlying databases have different WAL policies therefore separation is required.
	leafWrite *gosqlite.Conn
	treeWrite *gosqlite.Conn

	branchShards *BranchShards

	leafInsert *gosqlite.Stmt
	leafOrphan *gosqlite.Stmt
	treeInsert *BranchShardInsert
	treeOrphan *gosqlite.Stmt

	opts Options

	metrics metrics.Proxy
	logger  logger.Logger
}

func NewWriteConn(opts Options) (*WriteConn, error) {
	wdb := &WriteConn{
		opts:    opts,
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	if err := wdb.resetWriteConn(); err != nil {
		return nil, err
	}

	if err := wdb.createTableIfNotExists(); err != nil {
		return nil, err
	}

	var err error
	wdb.branchShards, err = NewBranchShards(wdb)
	if err != nil {
		return nil, err
	}

	if err = wdb.prepareInsertStatements(); err != nil {
		return nil, err
	}

	return wdb, nil
}

func (conn *WriteConn) SaveRoot(version int64, node *inode.Node) error {
	if node != nil {
		buf := pool.BufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer pool.BufPool.Put(buf)

		err := node.EncodeWithBuffer(buf)
		if err != nil {
			return err
		}
		bz := buf.Bytes()

		err = conn.treeWrite.Exec(StmtInsertRoot,
			version,
			node.NodeKey().Version(),
			int(node.NodeKey().Sequence()),
			bz)
		if err != nil {
			return err
		}

		return nil
	}
	// for an empty root a sentinel is saved
	return conn.treeWrite.Exec("INSERT OR REPLACE INTO root(version) VALUES (?)", version)
}

func (conn *WriteConn) Revert(version int64) error {
	if err := conn.leafWrite.Exec("DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if err := conn.leafWrite.Exec("DELETE FROM leaf_orphan WHERE at > ?", version); err != nil {
		return err
	}
	if err := conn.treeWrite.Exec("DELETE FROM branch_orphan WHERE at > ?", version); err != nil {
		return err
	}

	latestVersion, err := latestVersion(conn.opts)
	if err != nil {
		return err
	}

	toShardID := ToShardID(latestVersion)
	fromShardID := ToShardID(version)

	for shardID := fromShardID; shardID <= toShardID; shardID++ {
		if err := conn.treeWrite.Exec(fmt.Sprintf("DELETE FROM tree_%d WHERE version > ?", shardID), version); err != nil {
			return err
		}
	}

	if err := conn.treeWrite.Exec("DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}

	return nil
}

func (conn *WriteConn) Close() error {
	if conn.leafInsert != nil {
		if err := conn.leafInsert.Close(); err != nil {
			conn.logger.Warn("failed to close leaf insert statement", "err", err)
		}
	}

	if conn.leafOrphan != nil {
		if err := conn.leafOrphan.Close(); err != nil {
			conn.logger.Warn("failed to close leaf orphan statement", "err", err)
		}
	}

	if conn.treeInsert != nil {
		if err := conn.treeInsert.Close(); err != nil {
			conn.logger.Warn("failed to close tree insert statement", "err", err)
		}
	}

	if conn.treeOrphan != nil {
		if err := conn.treeOrphan.Close(); err != nil {
			conn.logger.Warn("failed to close tree orphan statement", "err", err)
		}
	}

	if conn.leafWrite != nil {
		if err := conn.leafWrite.Close(); err != nil {
			return err
		}
	}

	if conn.treeWrite != nil {
		if err := conn.treeWrite.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (conn *WriteConn) createShardTableIfNotExists(shardID int) error {
	tableName := fmt.Sprintf("tree_%d", shardID)
	q, err := conn.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
	if err != nil {
		return err
	}
	defer q.Close()

	if err = q.Bind(tableName); err != nil {
		return err
	}

	hasRow, err := q.Step()
	if err != nil {
		return err
	}

	if hasRow {
		return nil
	}

	conn.logger.Info(fmt.Sprintf("creating %s shard %d", conn.opts.Path, shardID))
	return conn.treeWrite.Exec(fmt.Sprintf(StmtCreateTreeBranchShardTableFormat, shardID))
}

func (conn *WriteConn) createTableIfNotExists() error {
	q, err := conn.treeWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='root'")
	if err != nil {
		return err
	}
	hasRow, err := q.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		pageSize := getPageSize()
		//nolint:gosec
		conn.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = conn.treeWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

		err = conn.treeWrite.Exec(StmtCreateTreeTables)
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	q, err = conn.leafWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='leaf'")
	if err != nil {
		return err
	}
	if !hasRow {
		pageSize := getPageSize()
		err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = conn.leafWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

		err = conn.leafWrite.Exec(StmtCreateLeafTables)
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	return nil
}

func (conn *WriteConn) resetWriteConn() (err error) {
	if conn.treeWrite != nil {
		err = conn.treeWrite.Close()
		if err != nil {
			return err
		}
	}
	conn.treeWrite, err = gosqlite.Open(conn.opts.treeConnectionString(UseOption), conn.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = conn.treeWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", conn.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", conn.opts.walPages)); err != nil {
		return err
	}

	err = conn.treeWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", conn.opts.BusyTimeout))
	if err != nil {
		return err
	}

	conn.leafWrite, err = gosqlite.Open(conn.opts.leafConnectionString(UseOption), conn.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = conn.leafWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", conn.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", conn.opts.walPages)); err != nil {
		return err
	}

	err = conn.leafWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", conn.opts.BusyTimeout))
	if err != nil {
		return err
	}

	return err
}

func (conn *WriteConn) preapreBranchShardInsertStatement(shardID int64) (*gosqlite.Stmt, error) {
	return conn.treeWrite.Prepare(fmt.Sprintf(StmtInsertBranchShardFormat, shardID))
}

func (conn *WriteConn) prepareInsertStatements() (err error) {
	if conn.leafInsert != nil {
		if err = conn.leafInsert.Close(); err != nil {
			return err
		}
	}
	conn.leafInsert, err = conn.leafWrite.Prepare(StmtInsertLeaf)
	if err != nil {
		return err
	}

	if conn.leafOrphan != nil {
		if err = conn.leafOrphan.Close(); err != nil {
			return err
		}
	}
	conn.leafOrphan, err = conn.leafWrite.Prepare(StmtInsertLeafOrphan)
	if err != nil {
		return err
	}

	if conn.treeOrphan != nil {
		if err = conn.treeOrphan.Close(); err != nil {
			return err
		}
	}
	conn.treeOrphan, err = conn.treeWrite.Prepare(StmtInsertBranchOrphan)
	if err != nil {
		return err
	}

	if conn.treeInsert != nil {
		if err = conn.treeInsert.Close(); err != nil {
			return err
		}
	}
	conn.treeInsert, err = PrepareBranchShardInsert(conn, conn.branchShards)
	if err != nil {
		return err
	}

	return err
}
