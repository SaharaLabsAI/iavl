package iavl

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	"lukechampine.com/blake3"

	"github.com/klauspost/compress/zstd"

	"github.com/cosmos/iavl/v2/compress"
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
	treeInsert *gosqlite.Stmt
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
		if err = b.changelogBatchCommit(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) changelogBatchCommit() error {
	return b.sql.leafWrite.Exec("Commit; Begin")
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.version.Load())
}

func (b *sqliteBatch) newTreeBatch(shardID int64) (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}

	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
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

func (b *sqliteBatch) treeMaybeCommit(_shardID int64) (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
	}
	return nil
}

// CompressedLeafData holds the compressed data for a leaf node
type CompressedLeafData struct {
	version    int64
	sequence   int
	keyHash    []byte
	compressed []byte
	index      int
	err        error
}

// CompressedBranchData holds the compressed data for a branch node
type CompressedBranchData struct {
	version    int64
	sequence   int
	compressed []byte
	index      int
	err        error
}

// parallelCompressLeaves compresses all leaves in parallel
func (b *sqliteBatch) parallelCompressLeaves(leaves []*Node) ([]CompressedLeafData, error) {
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > len(leaves) {
		numWorkers = len(leaves)
	}
	if numWorkers == 0 {
		numWorkers = 1
	}

	input := make(chan struct {
		leaf  *Node
		index int
	}, len(leaves))
	output := make(chan CompressedLeafData, len(leaves))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			buf := bufPool.Get().(*bytes.Buffer)
			defer bufPool.Put(buf)

			compressBuf := bufPool.Get().(*bytes.Buffer)
			defer bufPool.Put(compressBuf)

			encoder := compress.ZstdEncoderPool.Get().(*zstd.Encoder)
			defer compress.ZstdEncoderPool.Put(encoder)

			for work := range input {
				leaf := work.leaf
				index := work.index

				buf.Reset()
				err := leaf.BytesWithBuffer(buf)
				if err != nil {
					output <- CompressedLeafData{index: index, err: err}
					continue
				}

				compressBuf.Reset()
				encoder.Reset(compressBuf)
				if _, err := encoder.Write(buf.Bytes()); err != nil {
					output <- CompressedLeafData{index: index, err: err}
					continue
				}
				if err := encoder.Close(); err != nil {
					output <- CompressedLeafData{index: index, err: err}
					continue
				}

				keyHash := blake3.Sum256(leaf.key)
				compressedData := make([]byte, len(compressBuf.Bytes()))
				copy(compressedData, compressBuf.Bytes())

				output <- CompressedLeafData{
					version:    leaf.nodeKey.Version(),
					sequence:   int(leaf.nodeKey.Sequence()),
					keyHash:    keyHash[:],
					compressed: compressedData,
					index:      index,
					err:        nil,
				}
			}
		}()
	}

	// Send work
	go func() {
		defer close(input)
		for i, leaf := range leaves {
			input <- struct {
				leaf  *Node
				index int
			}{leaf: leaf, index: i}
		}
	}()

	// Collect results
	results := make([]CompressedLeafData, len(leaves))
	for i := 0; i < len(leaves); i++ {
		result := <-output
		if result.err != nil {
			// Wait for all workers to finish before returning error
			wg.Wait()
			return nil, result.err
		}
		results[result.index] = result
	}

	wg.Wait()
	close(output)

	return results, nil
}

// parallelCompressBranches compresses all branches in parallel
func (b *sqliteBatch) parallelCompressBranches(branches []*Node) ([]CompressedBranchData, error) {
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > len(branches) {
		numWorkers = len(branches)
	}
	if numWorkers == 0 {
		numWorkers = 1
	}

	input := make(chan struct {
		branch *Node
		index  int
	}, len(branches))
	output := make(chan CompressedBranchData, len(branches))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			buf := bufPool.Get().(*bytes.Buffer)
			defer bufPool.Put(buf)

			compressBuf := bufPool.Get().(*bytes.Buffer)
			defer bufPool.Put(compressBuf)

			encoder := compress.ZstdEncoderPool.Get().(*zstd.Encoder)
			defer compress.ZstdEncoderPool.Put(encoder)

			for work := range input {
				branch := work.branch
				index := work.index

				buf.Reset()
				err := branch.BytesWithBuffer(buf)
				if err != nil {
					output <- CompressedBranchData{index: index, err: err}
					continue
				}

				compressBuf.Reset()
				encoder.Reset(compressBuf)
				if _, err := encoder.Write(buf.Bytes()); err != nil {
					output <- CompressedBranchData{index: index, err: err}
					continue
				}
				if err := encoder.Close(); err != nil {
					output <- CompressedBranchData{index: index, err: err}
					continue
				}

				compressedData := make([]byte, len(compressBuf.Bytes()))
				copy(compressedData, compressBuf.Bytes())

				output <- CompressedBranchData{
					version:    branch.nodeKey.Version(),
					sequence:   int(branch.nodeKey.Sequence()),
					compressed: compressedData,
					index:      index,
					err:        nil,
				}
			}
		}()
	}

	// Send work
	go func() {
		defer close(input)
		for i, branch := range branches {
			input <- struct {
				branch *Node
				index  int
			}{branch: branch, index: i}
		}
	}()

	// Collect results
	results := make([]CompressedBranchData, len(branches))
	for i := 0; i < len(branches); i++ {
		result := <-output
		if result.err != nil {
			// Wait for all workers to finish before returning error
			wg.Wait()
			return nil, result.err
		}
		results[result.index] = result
	}

	wg.Wait()
	close(output)

	return results, nil
}

func (b *sqliteBatch) saveLeaves() (int64, error) {
	b.leafInsert = b.sql.leafInsert
	b.leafOrphan = b.sql.leafOrphan
	b.leafCount = 0

	tree := b.tree

	compressedLeaves, err := b.parallelCompressLeaves(tree.leaves)
	if err != nil {
		return 0, fmt.Errorf("failed to compress leaves: %w", err)
	}

	err = b.newChangeLogBatch()
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

	for i, leaf := range compressedLeaves {
		b.leafCount++

		if err = b.leafInsert.Exec(leaf.version, leaf.sequence, leaf.keyHash, leaf.compressed); err != nil {
			return 0, err
		}

		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}

		if tree.heightFilter > 0 {
			leaf := tree.leaves[i]
			if i != 0 {
				// evict leaf
				tree.returnNode(leaf)
			} else if leaf.nodeKey != tree.root.nodeKey {
				// never evict the root if it's a leaf
				tree.returnNode(leaf)
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

	if err = b.changelogBatchCommit(); err != nil {
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

	shardID, err := tree.sql.nextShard(tree.version.Load())
	if err != nil {
		return 0, err
	}
	b.logger.Debug(fmt.Sprintf("save branches db=tree version=%d shard=%d orphans=%s",
		tree.version.Load(), shardID, humanize.Comma(int64(len(tree.branchOrphans)))))

	compressedBranches, err := b.parallelCompressBranches(tree.branches)
	if err != nil {
		return 0, fmt.Errorf("failed to compress branches: %w", err)
	}

	if err = b.newTreeBatch(shardID); err != nil {
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

	for i, branch := range compressedBranches {
		b.treeCount++

		if err = b.treeInsert.Exec(branch.version, branch.sequence, branch.compressed); err != nil {
			return 0, err
		}

		if err = b.treeMaybeCommit(shardID); err != nil {
			return 0, err
		}

		node := tree.branches[i]
		node.dirty = false

		if node.evict {
			tree.returnNode(node)
		}
	}

	for _, orphan := range tree.branchOrphans {
		b.treeCount++
		err = b.execBranchOrphan(orphan)
		if err != nil {
			return 0, err
		}
		if err = b.treeMaybeCommit(shardID); err != nil {
			return 0, err
		}
	}

	if err = b.treeBatchCommit(); err != nil {
		return 0, err
	}

	if err = b.sql.treeWrite.Commit(); err != nil {
		return 0, err
	}

	err = b.sql.treeWrite.Exec(fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", shardID, shardID))
	if err != nil {
		return 0, err
	}

	return b.treeCount, nil
}
