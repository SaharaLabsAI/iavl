package tree

import (
	"errors"
	"fmt"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/db/sqlite"
	inode "github.com/cosmos/iavl/v2/node"
)

// maxBatchSize is the maximum size of the import batch before flushing it to the database
const importBatchSize = 600_000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
type Importer struct {
	tree      *Tree
	version   int64
	batchSize uint32
	batch     *sqlite.WriteBatch

	stack         []*inode.Node
	leafSequences []uint32
	nodeSequences []uint32
}

func NewImportNode(key, value []byte, version int64, height int8) *inode.Node {
	return inode.NewNode(key, value, version, height)
}

func (tree *Tree) Import(version int64) (*Importer, error) {
	return NewImporter(tree, version)
}

// newImporter creates a new Importer for an empty Tree
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func NewImporter(tree *Tree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	versionExists, err := tree.db.LatestVersion()
	if err != nil {
		return nil, err
	}
	if versionExists > 0 {
		return nil, fmt.Errorf("found version %v, must be empty", versionExists)
	}

	if err := tree.db.PrepareImport(); err != nil {
		return nil, err
	}

	// NOTE: Must turn off heightFilter, otherwise imported tree root may not be consistent
	tree.heightFilter = 0

	return &Importer{
		tree:          tree,
		version:       version,
		stack:         make([]*inode.Node, 0, 8),
		leafSequences: make([]uint32, version+1),
		nodeSequences: make([]uint32, version+1),
	}, nil
}

// writeNode writes the node content to the storage.
func (i *Importer) writeNode(node *inode.Node) error {
	node.HashSelf()

	if node.IsLeaf() {
		i.tree.dirtyNodes.AddLeaf(node)
	} else {
		i.tree.dirtyNodes.AddBranch(node)
	}

	i.batchSize++
	if i.batchSize >= importBatchSize {
		err := i.tree.db.WriteBatch(i.tree.dirtyNodes)
		if err != nil {
			return err
		}

		i.tree.dirtyNodes.Reset()
		i.batchSize = 0
	}

	return nil
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	i.batch = nil
	i.tree = nil
}

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(node *inode.Node) error {
	if i.tree == nil {
		return ErrNoImport
	}
	if node == nil {
		return errors.New("node cannot be nil")
	}
	nodeVersion := node.Version()
	if nodeVersion > i.version {
		return fmt.Errorf("node version %v can't be greater than import version %v",
			nodeVersion, i.version)
	}
	node.SetSource(inode.ManualNode)

	// We build the tree from the bottom-left up. The stack is used to store unresolved left
	// children while constructing right children. When all children are built, the parent can
	// be constructed and the resolved children can be discarded from the stack. Using a stack
	// ensures that we can handle additional unresolved left children while building a right branch.
	//
	// We don't modify the stack until we've verified the built node, to avoid leaving the
	// importer in an inconsistent state when we return an error.
	stackSize := len(i.stack)
	if node.SubTreeHeight() == 0 {
		node.SetSize(1)
	} else if stackSize >= 2 && i.stack[stackSize-1].SubTreeHeight() < node.SubTreeHeight() && i.stack[stackSize-2].SubTreeHeight() < node.SubTreeHeight() {
		leftNode := i.stack[stackSize-2]
		rightNode := i.stack[stackSize-1]

		node.SetLeft(leftNode)
		node.SetRight(rightNode)
		node.SetSize(leftNode.Size() + rightNode.Size())

		// Update the stack now.
		if err := i.writeNode(leftNode); err != nil {
			return err
		}
		if err := i.writeNode(rightNode); err != nil {
			return err
		}
		i.stack = i.stack[:stackSize-2]

		// remove the recursive references to avoid memory leak
		leftNode.SetLeft(nil)
		leftNode.SetRight(nil)
		rightNode.SetLeft(nil)
		rightNode.SetRight(nil)
	}

	if node.IsLeaf() {
		node.SetNodeKey(inode.NewNodeKey(nodeVersion, i.nextLeafSequence(nodeVersion)))
	} else {
		node.SetNodeKey(inode.NewNodeKey(nodeVersion, i.nextNodeSequence(nodeVersion)))
	}

	i.stack = append(i.stack, node)

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		// Should handle tree.root == nil special case
		if err := i.tree.db.SaveTree(i.version, nil, nil); err != nil {
			return err
		}
		i.tree.root = nil
	case 1:
		n := i.stack[0]
		if n.IsLeaf() {
			n.SetNodeKey(inode.NewNodeKey(n.Version(), i.nextLeafSequence(n.Version())))
		} else {
			n.SetNodeKey(inode.NewNodeKey(n.Version(), i.nextNodeSequence(n.Version())))
		}
		if err := i.writeNode(n); err != nil {
			return err
		}
		if err := i.tree.db.SaveTree(i.version, n, nil); err != nil {
			return err
		}
		i.tree.root = n
	default:
		return fmt.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	err := i.tree.db.WriteBatch(i.tree.dirtyNodes)
	if err != nil {
		return err
	}

	err = i.tree.db.FinishImport()
	if err != nil {
		return err
	}

	err = i.tree.LoadVersion(i.version)
	if err != nil {
		return err
	}

	i.Close()
	return nil
}

func (i *Importer) nextLeafSequence(version int64) uint32 {
	if i.leafSequences[version] == 0 {
		i.leafSequences[version] = constants.LeafSequenceStart
	}

	i.leafSequences[version]++
	if i.leafSequences[version] < constants.LeafSequenceStart {
		panic("leaf sequence underflow")
	}

	return i.leafSequences[version]
}

func (i *Importer) nextNodeSequence(version int64) uint32 {
	i.nodeSequences[version]++
	if i.nodeSequences[version] >= constants.LeafSequenceStart {
		panic("node sequence overflow")
	}

	return i.nodeSequences[version]
}
