package sqlite

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	"lukechampine.com/blake3"

	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/metrics"
	"github.com/cosmos/iavl/v2/common/pool"
	"github.com/cosmos/iavl/v2/db"
	inode "github.com/cosmos/iavl/v2/node"
)

const defaultWriteBatchSize = 200_000

type WriteBatch struct {
	updates *db.DirtyNodes

	conn    *WriteConn
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

func (b *WriteBatch) newChangeLogBatch() (err error) {
	if err = b.conn.leafWrite.Begin(); err != nil {
		return err
	}

	b.leafSince = time.Now()
	return nil
}

func (b *WriteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *WriteBatch) changelogBatchCommitBegin() error {
	return b.conn.leafWrite.Exec("Commit; Begin")
}

func (b *WriteBatch) execBranchOrphan(nodeKey inode.NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.updates.Version)
}

func (b *WriteBatch) newTreeBatch() (err error) {
	if err = b.conn.treeWrite.Begin(); err != nil {
		return err
	}

	b.treeSince = time.Now()
	return err
}

func (b *WriteBatch) treeBatchCommitBegin() error {
	if err := b.conn.treeWrite.Exec("Commit; Begin"); err != nil {
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

func (b *WriteBatch) treeMaybeCommit() (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommitBegin(); err != nil {
			return err
		}
	}
	return nil
}

func (b *WriteBatch) saveLeaves() (int64, error) {
	b.leafInsert = b.conn.leafInsert
	b.leafOrphan = b.conn.leafOrphan
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

	for _, leaf := range b.updates.Leaves {
		b.leafCount++

		buf.Reset()
		err := leaf.EncodeWithBuffer(buf)
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
	}

	for _, leafDelete := range b.updates.Deletes {
		b.leafCount++
		if err = b.leafInsert.Exec(leafDelete.DeleteKey.Version(), int(leafDelete.DeleteKey.Sequence()), leafDelete.LeafKey, nil); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range b.updates.LeafOrphans {
		b.leafCount++
		if err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.updates.Version); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommitBegin(); err != nil {
		return 0, err
	}

	if err = b.conn.leafWrite.Commit(); err != nil {
		return 0, err
	}

	err = b.conn.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);")
	if err != nil {
		return b.leafCount, err
	}

	return b.leafCount, nil
}

func (b *WriteBatch) saveBranches() (n int64, err error) {
	b.treeInsert = b.conn.treeInsert
	b.treeOrphan = b.conn.treeOrphan
	b.treeCount = 0

	shardID := ToShardID(b.updates.Version)
	if err := b.treeInsert.EnsureShardTable(b.conn, shardID); err != nil {
		return 0, err
	}

	b.logger.Debug(fmt.Sprintf("save branches db=tree version=%d shard=%d orphans=%s",
		b.updates.Version, shardID, humanize.Comma(int64(len(b.updates.BranchOrphans)))))

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

	for i, branch := range b.updates.Branches {
		b.treeCount++

		shardID := ToShardID(branch.Version())
		if err := b.treeInsert.EnsureShardTable(b.conn, shardID); err != nil {
			return 0, err
		}

		buf.Reset()
		err := branch.EncodeWithBuffer(buf)
		if err != nil {
			return b.leafCount, err
		}

		if err = b.treeInsert.Exec(branch.Version(), int(branch.NodeKey().Sequence()), buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.treeMaybeCommit(); err != nil {
			return 0, err
		}

		originalNode := b.updates.Branches[i]
		originalNode.SetDirty(false)
	}

	for _, orphan := range b.updates.BranchOrphans {
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

	if err = b.conn.treeWrite.Commit(); err != nil {
		return 0, err
	}

	return b.treeCount, nil
}
