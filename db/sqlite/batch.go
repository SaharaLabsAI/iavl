package sqlite

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	"lukechampine.com/blake3"

	"github.com/cosmos/iavl/v2/logger"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/pool"
	"github.com/cosmos/iavl/v2/types"
)

type SqliteBatch struct {
	tree types.UpdatedTree

	sql     *SqliteDb
	size    int64
	logger  logger.Logger
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

func (b *SqliteBatch) newChangeLogBatch() (err error) {
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}

	b.leafSince = time.Now()
	return nil
}

func (b *SqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *SqliteBatch) changelogBatchCommitBegin() error {
	return b.sql.leafWrite.Exec("Commit; Begin")
}

func (b *SqliteBatch) execBranchOrphan(nodeKey types.NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.Version())
}

func (b *SqliteBatch) newTreeBatch() (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}

	b.treeSince = time.Now()
	return err
}

func (b *SqliteBatch) treeBatchCommitBegin() error {
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

func (b *SqliteBatch) treeMaybeCommit() (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *SqliteBatch) saveLeaves() (int64, error) {
	b.leafInsert = b.sql.leafInsert
	b.leafOrphan = b.sql.leafOrphan
	b.leafCount = 0

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

	buf := pool.BufPool.Get().(*bytes.Buffer)
	defer pool.BufPool.Put(buf)

	for i, leaf := range b.tree.Updates().Leaves {
		b.leafCount++

		buf.Reset()
		err := leaf.BytesWithBuffer(buf)
		if err != nil {
			return b.leafCount, err
		}

		keyHash := blake3.Sum256(leaf.Key())

		if err = b.leafInsert.Exec(leaf.Version(), int(leaf.NodeKey().Sequence()), keyHash[:], buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}

		if b.tree.HeightFilter() > 0 {
			originalLeaf := b.tree.Updates().Leaves[i]
			if i != 0 {
				// evict leaf
				b.tree.ReturnNode(originalLeaf)
			} else if originalLeaf.NodeKey() != b.tree.Root().NodeKey() {
				// never evict the root if it's a leaf
				b.tree.ReturnNode(originalLeaf)
			}
		}
	}

	for _, leafDelete := range b.tree.Updates().Deletes {
		b.leafCount++
		if err = b.leafInsert.Exec(leafDelete.DeleteKey.Version(), int(leafDelete.DeleteKey.Sequence()), leafDelete.LeafKey, nil); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range b.tree.Updates().LeafOrphans {
		b.leafCount++
		if err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.Version()); err != nil {
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

	err = b.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);")
	if err != nil {
		return b.leafCount, err
	}

	return b.leafCount, nil
}

func (b *SqliteBatch) saveBranches() (n int64, err error) {
	b.treeInsert = b.sql.treeInsert
	b.treeOrphan = b.sql.treeOrphan
	b.treeCount = 0

	shardID := ToShardID(b.tree.Version())
	if err := b.treeInsert.EnsureShardTable(b.sql, shardID); err != nil {
		return 0, err
	}

	b.logger.Debug(fmt.Sprintf("save branches db=tree version=%d shard=%d orphans=%s",
		b.tree.Version(), shardID, humanize.Comma(int64(len(b.tree.Updates().BranchOrphans)))))

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

	buf := pool.BufPool.Get().(*bytes.Buffer)
	defer pool.BufPool.Put(buf)

	for i, branch := range b.tree.Updates().Branches {
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

		if err = b.treeInsert.Exec(branch.Version(), int(branch.NodeKey().Sequence()), buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.treeMaybeCommit(); err != nil {
			return 0, err
		}

		originalNode := b.tree.Updates().Branches[i]
		originalNode.SetDirty(false)

		if originalNode.Evict() {
			b.tree.ReturnNode(originalNode)
		}
	}

	for _, orphan := range b.tree.Updates().BranchOrphans {
		b.treeCount++
		err = b.execBranchOrphan(*orphan)
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
