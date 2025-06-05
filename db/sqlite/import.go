package sqlite

import (
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/cosmos/iavl/v2/db"
)

func (sql *SqliteDb) PrepareImport() error {
	err := sql.writeDb.treeWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 10*1024*1024*1024)) // 8G
	if err != nil {
		return err
	}
	err = sql.writeDb.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -2*1024*1024)) // 2G
	if err != nil {
		return err
	}

	err = sql.writeDb.leafWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 14*1024*1024*1024)) // 16G
	if err != nil {
		return err
	}
	err = sql.writeDb.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -2*1024*1024)) // 2G
	if err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) FinishImport() error {
	err := sql.writeDb.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		return fmt.Errorf("failed tree checkpoint; %w", err)
	}

	err = sql.writeDb.treeWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		return err
	}

	err = sql.writeDb.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}

	err = sql.writeDb.leafWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		return err
	}

	err = sql.writeDb.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) WriteBatch(importedNodes *db.DirtyNodes) error {
	batch := WriteBatch{
		updates: importedNodes,
		sql:     sql.writeDb,
		size:    int64(len(importedNodes.Leaves)) + int64(len(importedNodes.Branches)),
		logger:  sql.logger,
		metrics: sql.metrics,
	}

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		_, err := batch.saveLeaves()
		return err
	})

	eg.Go(func() error {
		_, err := batch.saveBranches()
		return err
	})

	return eg.Wait()
}
