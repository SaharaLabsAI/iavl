package tree

import (
	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

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

func (tree *Tree) mutateNode(node *inode.Node) {
	// this seems to be true only in certain cases
	// we should investigate if we can remove this check
	alreadyMutatedForNextVersion := node.Hash() == nil && node.Version() == tree.version.Load()+1
	if alreadyMutatedForNextVersion {
		// This node has already been mutated for the next version, so we can skip
		// This can happen when we have already processed this node during a recursive operation
		return
	}

	// Always mark the hash as nil to ensure recomputation
	node.SetHash(nil)

	// Create a new nodeKey with the next version
	if node.IsLeaf() {
		node.SetNodeKey(tree.nextLeafNodeKey())
	} else {
		node.SetNodeKey(tree.nextNodeKey())
	}

	// Mark the node as dirty for tracking
	node.SetDirty(true)

	// Mark the tree hash as dirty
	tree.markHashDirty()
}

func (tree *Tree) returnNode(node *inode.Node) {
	if node == nil {
		return
	}

	node.CheckValid()

	// Make sure node is not the tree's root before recycling
	if node == tree.root {
		return
	}

	// Check for lingering references before returning to pool
	if node.LeftNode() != nil || node.RightNode() != nil {
		panic("Attempted to return node with active child references")
	}

	if node.Dirty() {
		tree.workingBytes -= node.SizeBytes()
		tree.workingSize--
	}

	// Return node to the pool for reuse
	tree.nodePool.Put(node)
}

// markHashDirty marks the tree's hash as needing recalculation
// Call this function whenever the tree structure changes
func (tree *Tree) markHashDirty() {
	tree.hashedVersion = -1 // Invalidate hash by setting to impossible version
}

func (tree *Tree) resetSequences() {
	tree.leafSequence = constants.LeafSequenceStart
	tree.branchSequence = 0
}

func (tree *Tree) addOrphan(node *inode.Node) {
	if node.Hash() == nil {
		return
	}

	tree.dirtyNodes.AddOrphan(node)
}

func (tree *Tree) addDelete(node *inode.Node) {
	// added and removed in the same version; no op.
	if node.Version() == tree.nextVersion() {
		return
	}

	tree.dirtyNodes.AddDelete(tree.nextLeafNodeKey(), node.Key())
}

func (tree *Tree) nextVersion() int64 {
	return tree.version.Load() + 1
}
