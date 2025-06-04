package tree

import (
	"errors"

	"github.com/cosmos/iavl/v2/constants"
	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

func (t *Tree) ensureLeftNode(node *nodetypes.Node) *nodetypes.Node {
	leftNode, err := t.getLeftNode(node)
	if err != nil {
		panic(err)
	}

	return leftNode
}

func (t *Tree) ensureRightNode(node *nodetypes.Node) *nodetypes.Node {
	rightNode, err := t.getRightNode(node)
	if err != nil {
		panic(err)
	}

	return rightNode
}

func (t *Tree) getLeftNode(node *nodetypes.Node) (*nodetypes.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no left node")
	}
	if node.LeftNode() != nil {
		return node.LeftNode(), nil
	}

	leftNode, err := t.db.GetLeftNode(node)
	if err != nil {
		return nil, err
	}

	node.SetLeft(leftNode)

	return node.LeftNode(), nil
}

func (t *Tree) getRightNode(node *nodetypes.Node) (*nodetypes.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no right node")
	}
	if node.RightNode() != nil {
		return node.RightNode(), nil
	}

	rightNode, err := t.db.GetRightNode(node)
	if err != nil {
		return nil, err
	}

	node.SetRight(rightNode)

	return node.RightNode(), nil
}

// newLeafNode returns a new node from a key, value and version.
func (tree *Tree) newLeafNode(key []byte, value []byte) *nodetypes.Node {
	node := tree.nodePool.Get()

	node.SetNodeKey(tree.nextLeafNodeKey())
	node.SetKey(key)
	node.SetSubTreeHeight(0)
	node.SetSize(1)

	if tree.isReplaying {
		node.SetHash(value)
	} else {
		node.SetValue(value)
		// node._hash() // parallel hash in deepHashParallel
	}

	node.SetDirty(true)
	tree.workingBytes += node.SizeBytes()
	tree.workingSize++

	return node
}

func (tree *Tree) nextNodeKey() nodetypes.NodeKey {
	tree.branchSequence++
	nk := nodetypes.NewNodeKey(tree.version.Load()+1, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() nodetypes.NodeKey {
	tree.leafSequence++
	if tree.leafSequence < constants.LeafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := nodetypes.NewNodeKey(tree.version.Load()+1, tree.leafSequence)
	return nk
}
