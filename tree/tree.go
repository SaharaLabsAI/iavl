package tree

import (
	"sync"
	"sync/atomic"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/common/metrics"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	idb "github.com/cosmos/iavl/v2/db"
	inode "github.com/cosmos/iavl/v2/node"
)

type Tree struct {
	version atomic.Int64
	root    *inode.Node

	metrics  metrics.Proxy
	nodePool *nodepool.NodePool

	// options
	maxWorkingSize uint64
	workingBytes   uint64
	workingSize    int64
	heightFilter   int8
	metricsProxy   metrics.Proxy

	// state
	db         idb.DB
	dirtyNodes *idb.DirtyNodes

	leafSequence   uint32
	branchSequence uint32
	isReplaying    bool

	hashedVersion     int64
	modificationCount int64
	cache             map[string][]byte
	deleted           map[string]bool

	rw sync.RWMutex
}

func NewTree(db idb.DB, pool *nodepool.NodePool, opts Options) *Tree {
	tree := &Tree{
		db:             db,
		dirtyNodes:     &idb.DirtyNodes{},
		nodePool:       pool,
		metrics:        opts.MetricsProxy,
		maxWorkingSize: 1.5 * 1024 * 1024 * 1024,
		heightFilter:   opts.HeightFilter,
		metricsProxy:   opts.MetricsProxy,
		leafSequence:   constants.LeafSequenceStart,
		hashedVersion:  -1,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	tree.version.Store(0)

	return tree
}

func (tree *Tree) Root() *inode.Node {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root
}

func (tree *Tree) Path() string {
	return tree.db.Path()
}

func (tree *Tree) Metrics() metrics.Proxy {
	return tree.metrics
}

func (tree *Tree) WorkingSize() int64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.workingSize
}

func (tree *Tree) WorkingBytes() uint64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.workingBytes
}

func (tree *Tree) Size() int64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.Size()
}

func (tree *Tree) Height() int8 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.SubTreeHeight()
}

func (tree *Tree) Close() error {
	return tree.db.Close()
}

// FIXME
func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	// return tree.sql.replayChangelog(tree, toVersion, targetHash)
	return nil
}
