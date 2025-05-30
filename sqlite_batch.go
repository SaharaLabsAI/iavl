package iavl

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	"lukechampine.com/blake3"

	"github.com/cosmos/iavl/v2/metrics"
)

type sqliteBatch struct {
	tree    *Tree
	sql     *SqliteDb
	size    int64
	logger  Logger
	metrics metrics.Proxy

	treeCount int64
	treeSince time.Time
	leafCount int64
	leafSince time.Time

	leafInsert *gosqlite.Stmt
	treeInsert *BranchShardInsert
	leafOrphan *gosqlite.Stmt
	treeOrphan *gosqlite.Stmt
}

func (b *sqliteBatch) newChangeLogBatch() (err error) {
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}

	b.leafSince = time.Now()
	return nil
}

func (b *sqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) changelogBatchCommitBegin() error {
	return b.sql.leafWrite.Exec("Commit; Begin")
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.version.Load())
}

func (b *sqliteBatch) newTreeBatch() (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}

	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommitBegin() error {
	if err := b.sql.treeWrite.Exec("Commit; Begin"); err != nil {
		return err
	}

	if b.treeCount >= b.size {
		batchSize := b.treeCount % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug(fmt.Sprintf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(b.treeCount),
			time.Since(b.treeSince).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.treeSince).Seconds()))))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit() (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) saveLeaves() (int64, error) {
	b.leafInsert = b.sql.leafInsert
	b.leafOrphan = b.sql.leafOrphan
	b.leafCount = 0

	tree := b.tree

	err := b.newChangeLogBatch()
	if err != nil {
		return 0, err
	}

	defer func() {
		if err := b.leafInsert.Reset(); err != nil {
			b.logger.Warn("failed to reset leaf insert", "err", err)
		}
		if err := b.leafOrphan.Reset(); err != nil {
			b.logger.Warn("failed to reset leaf orphan", "err", err)
		}
	}()

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	for i, leaf := range tree.leaves {
		b.leafCount++

		buf.Reset()
		err := leaf.BytesWithBuffer(buf)
		if err != nil {
			return b.leafCount, err
		}

		keyHash := blake3.Sum256(leaf.key)

		if err = b.leafInsert.Exec(leaf.Version(), int(leaf.nodeKey.Sequence()), keyHash[:], buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}

		if tree.heightFilter > 0 {
			originalLeaf := tree.leaves[i]
			if i != 0 {
				// evict leaf
				tree.returnNode(originalLeaf)
			} else if originalLeaf.nodeKey != tree.root.nodeKey {
				// never evict the root if it's a leaf
				tree.returnNode(originalLeaf)
			}
		}
	}

	for _, leafDelete := range tree.deletes {
		b.leafCount++
		if err = b.leafInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey, nil); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range tree.leafOrphans {
		b.leafCount++
		if err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.version.Load()); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommitBegin(); err != nil {
		return 0, err
	}

	if err = b.sql.leafWrite.Commit(); err != nil {
		return 0, err
	}

	err = tree.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);")
	if err != nil {
		return b.leafCount, err
	}

	return b.leafCount, nil
}

func (b *sqliteBatch) saveBranches() (n int64, err error) {
	b.treeInsert = b.sql.treeInsert
	b.treeOrphan = b.sql.treeOrphan
	b.treeCount = 0

	tree := b.tree

	shardID := ToShardID(tree.version.Load())
	if err := b.treeInsert.EnsureShardTable(b.sql, shardID); err != nil {
		return 0, err
	}

	b.logger.Debug(fmt.Sprintf("save branches db=tree version=%d shard=%d orphans=%s",
		tree.version.Load(), shardID, humanize.Comma(int64(len(tree.branchOrphans)))))

	if err = b.newTreeBatch(); err != nil {
		return 0, err
	}

	defer func() {
		if err := b.treeInsert.Reset(); err != nil {
			b.logger.Warn("failed to reset tree insert", "err", err)
		}
		if err := b.treeOrphan.Reset(); err != nil {
			b.logger.Warn("failed to reset tree orphan", "err", err)
		}
	}()

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	for i, branch := range tree.branches {
		b.treeCount++

		shardID := ToShardID(branch.Version())
		if err := b.treeInsert.EnsureShardTable(b.sql, shardID); err != nil {
			return 0, err
		}

		buf.Reset()
		err := branch.BytesWithBuffer(buf)
		if err != nil {
			return b.leafCount, err
		}

		if err = b.treeInsert.Exec(branch.Version(), int(branch.nodeKey.Sequence()), buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.treeMaybeCommit(); err != nil {
			return 0, err
		}

		originalNode := tree.branches[i]
		originalNode.dirty = false

		if originalNode.evict {
			tree.returnNode(originalNode)
		}
	}

	for _, orphan := range tree.branchOrphans {
		b.treeCount++
		err = b.execBranchOrphan(orphan)
		if err != nil {
			return 0, err
		}
		if err = b.treeMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.treeBatchCommitBegin(); err != nil {
		return 0, err
	}

	if err = b.sql.treeWrite.Commit(); err != nil {
		return 0, err
	}

	return b.treeCount, nil
}
