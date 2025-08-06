package sqlite

import (
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/SaharaLabsAI/iavl/v2/db"
)

func (sql *DB) PrepareImport() error {
	err := sql.write.treeWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 10*1024*1024*1024)) // 8G
	if err != nil {
		return err
	}
	err = sql.write.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -2*1024*1024)) // 2G
	if err != nil {
		return err
	}

	err = sql.write.leafWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 14*1024*1024*1024)) // 16G
	if err != nil {
		return err
	}
	err = sql.write.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -2*1024*1024)) // 2G
	if err != nil {
		return err
	}

	return nil
}

func (sql *DB) FinishImport() error {
	err := sql.write.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		return fmt.Errorf("failed tree checkpoint; %w", err)
	}

	err = sql.write.treeWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		return err
	}

	err = sql.write.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}

	err = sql.write.leafWrite.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		return err
	}

	err = sql.write.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}

	return nil
}

func (sql *DB) WriteBatch(importedNodes *db.DirtyNodes) error {
	size := len(importedNodes.Leaves) + len(importedNodes.Branches)
	if size == 0 {
		size = defaultWriteBatchSize
	}

	batch := WriteBatch{
		updates: importedNodes,
		conn:    sql.write,
		size:    int64(size),
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
