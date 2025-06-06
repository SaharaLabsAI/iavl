package tree

import (
	"errors"
	"fmt"
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

	immutable         bool
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
		immutable:      false,
		hashedVersion:  -1,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	tree.version.Store(0)
	if tree.db != nil {
		tree.db.SetInitTreeVersion(&tree.version)
	}

	return tree
}

func (tree *Tree) VersionExists(version int64) (bool, error) {
	exists, err := tree.db.HasRoot(version)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (tree *Tree) Version() int64 {
	return tree.version.Load()
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.db == nil {
		return errors.New("db is nil")
	}

	if version == 0 {
		return nil
	}

	tree.version.Store(version)
	if tree.immutable {
		exists, err := tree.db.HasRoot(version)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("root not found for version %d", version)
		}

		return nil
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.root, err = tree.db.LoadRoot(version)
	if err != nil {
		return err
	}

	tree.hashedVersion = version
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)

	return nil
}

func (tree *Tree) SetInitialVersion(version int64) error {
	if tree.immutable {
		panic("set initial version on immutable tree")
	}

	var err error

	tree.version.Store(version - 1)

	return err
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

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	return nil, 0, err
	// }

	dirtyTreeVersion := tree.version.Load()
	savedTreeVersion := dirtyTreeVersion + 1

	rootHash := tree.computeHash()

	tree.version.Add(1)
	tree.resetSequences()
	tree.dirtyNodes.Version = savedTreeVersion

	err := tree.db.SaveTree(savedTreeVersion, tree.root, tree.dirtyNodes)
	if err != nil {
		return nil, dirtyTreeVersion, err
	}

	if tree.heightFilter > 0 {
		for i, leaf := range tree.dirtyNodes.Leaves {
			if i != 0 {
				// evict leaf
				tree.returnNode(leaf)
			} else if leaf.NodeKey() != tree.root.NodeKey() {
				// never evict the root if it's a leaf
				tree.returnNode(leaf)
			}
		}
	}
	for _, branch := range tree.dirtyNodes.Branches {
		if branch.Evict() {
			tree.returnNode(branch)
		}
	}

	if err := tree.db.ResetRead(); err != nil {
		return nil, savedTreeVersion, err
	}

	tree.resetSequences()
	tree.dirtyNodes.Reset()
	tree.deleted = make(map[string]bool)
	tree.cache = make(map[string][]byte)
	tree.modificationCount = 0

	return rootHash, savedTreeVersion, nil
}

func (tree *Tree) Revert(version int64) error {
	return tree.db.Revert(version)
}

// FIXME
func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	// return tree.sql.replayChangelog(tree, toVersion, targetHash)
	return nil
}
