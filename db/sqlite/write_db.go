package sqlite

import (
	"bytes"
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/metrics"
	"github.com/cosmos/iavl/v2/common/pool"
	inode "github.com/cosmos/iavl/v2/node"
)

type WriteDB struct {
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

func NewWriteDB(opts Options) (*WriteDB, error) {
	wdb := &WriteDB{
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

func (sql *WriteDB) SaveRoot(version int64, node *inode.Node) error {
	if node != nil {
		buf := pool.BufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer pool.BufPool.Put(buf)

		err := node.EncodeWithBuffer(buf)
		if err != nil {
			return err
		}
		bz := buf.Bytes()

		err = sql.treeWrite.Exec(StmtInsertRoot,
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
	return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version) VALUES (?)", version)
}

func (sql *WriteDB) Revert(version int64) error {
	if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.leafWrite.Exec("DELETE FROM leaf_orphan WHERE at > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM branch_orphan WHERE at > ?", version); err != nil {
		return err
	}

	latestVersion, err := latestVersion(sql.opts)
	if err != nil {
		return err
	}

	toShardID := ToShardID(latestVersion)
	fromShardID := ToShardID(version)

	for shardID := fromShardID; shardID <= toShardID; shardID++ {
		if err := sql.treeWrite.Exec(fmt.Sprintf("DELETE FROM tree_%d WHERE version > ?", shardID), version); err != nil {
			return err
		}
	}

	if err := sql.treeWrite.Exec("DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}

	return nil
}

func (sql *WriteDB) Close() error {
	if sql.leafInsert != nil {
		if err := sql.leafInsert.Close(); err != nil {
			sql.logger.Warn("failed to close leaf insert statement", "err", err)
		}
	}

	if sql.leafOrphan != nil {
		if err := sql.leafOrphan.Close(); err != nil {
			sql.logger.Warn("failed to close leaf orphan statement", "err", err)
		}
	}

	if sql.treeInsert != nil {
		if err := sql.treeInsert.Close(); err != nil {
			sql.logger.Warn("failed to close tree insert statement", "err", err)
		}
	}

	if sql.treeOrphan != nil {
		if err := sql.treeOrphan.Close(); err != nil {
			sql.logger.Warn("failed to close tree orphan statement", "err", err)
		}
	}

	if sql.leafWrite != nil {
		if err := sql.leafWrite.Close(); err != nil {
			return err
		}
	}

	if sql.treeWrite != nil {
		if err := sql.treeWrite.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (sql *WriteDB) createShardTableIfNotExists(shardID int) error {
	tableName := fmt.Sprintf("tree_%d", shardID)
	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
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

	sql.logger.Info(fmt.Sprintf("creating %s shard %d", sql.opts.Path, shardID))
	return sql.treeWrite.Exec(fmt.Sprintf(StmtCreateTreeBranchShardTableFormat, shardID))
}

func (sql *WriteDB) createTableIfNotExists() error {
	q, err := sql.treeWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='root'")
	if err != nil {
		return err
	}
	hasRow, err := q.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		pageSize := getPageSize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

		err = sql.treeWrite.Exec(StmtCreateTreeTables)
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	q, err = sql.leafWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='leaf'")
	if err != nil {
		return err
	}
	if !hasRow {
		pageSize := getPageSize()
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

		err = sql.leafWrite.Exec(StmtCreateLeafTables)
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	return nil
}

func (sql *WriteDB) resetWriteConn() (err error) {
	if sql.treeWrite != nil {
		err = sql.treeWrite.Close()
		if err != nil {
			return err
		}
	}
	sql.treeWrite, err = gosqlite.Open(sql.opts.treeConnectionString(UseOption), sql.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return err
	}

	sql.leafWrite, err = gosqlite.Open(sql.opts.leafConnectionString(UseOption), sql.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return err
	}

	return err
}

func (sql *WriteDB) preapreBranchShardInsertStatement(shardID int64) (*gosqlite.Stmt, error) {
	return sql.treeWrite.Prepare(fmt.Sprintf(StmtInsertBranchShardFormat, shardID))
}

func (sql *WriteDB) prepareInsertStatements() (err error) {
	if sql.leafInsert != nil {
		if err = sql.leafInsert.Close(); err != nil {
			return err
		}
	}
	sql.leafInsert, err = sql.leafWrite.Prepare(StmtInsertLeaf)
	if err != nil {
		return err
	}

	if sql.leafOrphan != nil {
		if err = sql.leafOrphan.Close(); err != nil {
			return err
		}
	}
	sql.leafOrphan, err = sql.leafWrite.Prepare(StmtInsertLeafOrphan)
	if err != nil {
		return err
	}

	if sql.treeOrphan != nil {
		if err = sql.treeOrphan.Close(); err != nil {
			return err
		}
	}
	sql.treeOrphan, err = sql.treeWrite.Prepare(StmtInsertBranchOrphan)
	if err != nil {
		return err
	}

	if sql.treeInsert != nil {
		if err = sql.treeInsert.Close(); err != nil {
			return err
		}
	}
	sql.treeInsert, err = PrepareBranchShardInsert(sql, sql.branchShards)
	if err != nil {
		return err
	}

	return err
}
