package tree

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/common/metrics"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	ndb "github.com/cosmos/iavl/v2/db"
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
	db         ndb.DB
	dirtyNodes *ndb.DirtyNodes

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

func NewTree(db ndb.DB, pool *nodepool.NodePool, opts Options) *Tree {
	tree := &Tree{
		db:             db,
		dirtyNodes:     &ndb.DirtyNodes{},
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

func (tree *Tree) Root() *inode.Node {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root
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

func (tree *Tree) Hash() []byte {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return inode.EmptyHash
	}
	return tree.root.Hash()
}

func (tree *Tree) WorkingHash() []byte {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return inode.EmptyHash
	}

	if tree.root.Hash() != nil {
		return tree.root.Hash()
	}

	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	panic(err)
	// }

	hash := tree.computeHash()

	return hash
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

	err := tree.db.SaveTree(tree.root, savedTreeVersion, tree.dirtyNodes)
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

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	if tree.immutable {
		panic("set on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_set")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_update")
	} else {
		tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_new_node")
	}

	tree.modificationCount++
	tree.cache[string(key)] = value
	delete(tree.deleted, string(key))

	return updated, nil
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.immutable {
		panic("Remove on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_remove")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return nil, false, nil
	}

	delete(tree.cache, string(key))

	newRoot, _, value, removed, err := tree.iterativeRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.deleted[string(key)] = true
	tree.modificationCount++

	tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_delete")

	tree.root = newRoot
	return value, true, nil
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

func (tree *Tree) PausePruning(pause bool) {
	tree.db.PausePruning(pause)
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	return tree.db.DeleteVersionsTo(toVersion)
}

func (tree *Tree) DeleteVersionsToSync(toVersion int64) error {
	return tree.db.DeleteVersionsToSync(toVersion)
}

func (tree *Tree) GetWithIndex(key []byte) (int64, []byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return 0, nil, nil
	}

	return tree.get(tree.root, key)
}

func (tree *Tree) GetByIndex(index int64) (key []byte, value []byte, err error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return nil, nil, nil
	}

	return tree.getByIndex(tree.root, index)
}

func (tree *Tree) getByIndex(node *inode.Node, index int64) (key []byte, value []byte, err error) {
	if node.IsLeaf() {
		if index == 0 {
			return node.Key(), node.Value(), nil
		}
		return nil, nil, nil
	}

	leftNode, err := tree.getLeftNode(node)
	if err != nil {
		return nil, nil, err
	}

	if index < leftNode.Size() {
		return tree.getByIndex(leftNode, index)
	}

	rightNode, err := tree.getRightNode(node)
	if err != nil {
		return nil, nil, err
	}

	return tree.getByIndex(rightNode, index-leftNode.Size())
}

func (tree *Tree) SetInitialVersion(version int64) error {
	if tree.immutable {
		panic("set initial version on immutable tree")
	}

	var err error

	tree.version.Store(version - 1)

	return err
}

func (tree *Tree) GetRecent(version int64, key []byte) (bool, []byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	got, root := tree.getRecentRoot(version)
	if !got {
		return false, nil, nil
	}
	if root == nil {
		return true, nil, nil
	}

	_, val, err := tree.get(root, key)
	return true, val, err
}

func (tree *Tree) getRecentRoot(version int64) (bool, *inode.Node) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if version != tree.version.Load() {
		return false, nil
	}

	// Version == 0
	if tree.root == nil {
		return false, nil
	}

	root := *tree.root

	return true, &root
}

// func (tree *Tree) Import(version int64) (*Importer, error) {
// 	return newImporter(tree, version)
// }

// func (tree *Tree) GetImmutable(version int64) (*Tree, error) {
// 	// We can discard whole pool after usage
// 	pool := NewNodePool()
//
// 	sql := &SqliteDb{
// 		opts:        tree.sql.opts,
// 		pool:        pool,
// 		readPool:    tree.sql.readPool,
// 		metrics:     tree.sql.metrics,
// 		logger:      tree.sql.logger,
// 		useReadPool: true,
// 	}
//
// 	imTree := &Tree{
// 		sql:            sql,
// 		WriteEventLoop: nil,
// 		writerCancel:   nil,
// 		nodePool:       sql.pool,
// 		metrics:        tree.metrics,
// 		maxWorkingSize: tree.maxWorkingSize,
// 		heightFilter:   tree.heightFilter,
// 		metricsProxy:   tree.metricsProxy,
// 		leafSequence:   leafSequenceStart,
// 		hashedVersion:  version,
// 		cache:          make(map[string][]byte),
// 		deleted:        make(map[string]bool),
// 	}
//
// 	if err := imTree.LoadVersion(version); err != nil {
// 		return nil, err
// 	}
//
// 	imTree.immutable = true
//
// 	return imTree, nil
// }
//
// func (tree *Tree) GetImmutableProvable(version int64) (*Tree, error) {
// 	// We can discard whole pool after usage
// 	pool := NewNodePool()
//
// 	sql := &SqliteDb{
// 		opts:        tree.sql.opts,
// 		pool:        pool,
// 		readPool:    tree.sql.readPool,
// 		metrics:     tree.sql.metrics,
// 		logger:      tree.sql.logger,
// 		useReadPool: true,
// 	}
//
// 	imTree := &Tree{
// 		sql:            sql,
// 		WriteEventLoop: nil,
// 		writerCancel:   nil,
// 		nodePool:       sql.pool,
// 		metrics:        tree.metrics,
// 		maxWorkingSize: tree.maxWorkingSize,
// 		heightFilter:   tree.heightFilter,
// 		metricsProxy:   tree.metricsProxy,
// 		leafSequence:   leafSequenceStart,
// 		hashedVersion:  version,
// 		cache:          make(map[string][]byte),
// 		deleted:        make(map[string]bool),
// 	}
//
// 	if err := imTree.LoadVersion(version); err != nil {
// 		return nil, err
// 	}
//
// 	imTree.immutable = true
//
// 	return imTree, nil
// }

func (tree *Tree) IsImmutable() bool {
	return tree.immutable
}

// FIXME: implement Immutable Tree struct, which use readonly DB
func (tree *Tree) DiscardImmutableTree() error {
	// tree.sql.leafWrite = nil
	// tree.sql.treeWrite = nil
	// tree.sql.read = nil
	// tree.sql.readPool = nil
	// tree.sql.pool = nil
	return tree.Close()
}

func (tree *Tree) GetFromRoot(key []byte) ([]byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	_, val, err := tree.get(tree.root, key)
	return val, err
}

func (tree *Tree) Path() string {
	return tree.db.Path()
}

func (tree *Tree) Revert(version int64) error {
	return tree.db.Revert(version)
}

func (tree *Tree) nextVersion() int64 {
	return tree.version.Load() + 1
}

// FIXME
func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	// return tree.sql.replayChangelog(tree, toVersion, targetHash)
	return nil
}
