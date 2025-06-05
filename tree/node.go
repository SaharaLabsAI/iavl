package tree

import (
	"errors"

	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

func (tree *Tree) ensureLeftNode(node *inode.Node) *inode.Node {
	leftNode, err := tree.getLeftNode(node)
	if err != nil {
		panic(err)
	}

	return leftNode
}

func (tree *Tree) ensureRightNode(node *inode.Node) *inode.Node {
	rightNode, err := tree.getRightNode(node)
	if err != nil {
		panic(err)
	}

	return rightNode
}

func (tree *Tree) getLeftNode(node *inode.Node) (*inode.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no left node")
	}
	if node.LeftNode() != nil {
		return node.LeftNode(), nil
	}

	leftNode, err := tree.db.GetLeftNode(node)
	if err != nil {
		return nil, err
	}

	node.SetLeft(leftNode)

	return node.LeftNode(), nil
}

func (tree *Tree) getRightNode(node *inode.Node) (*inode.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no right node")
	}
	if node.RightNode() != nil {
		return node.RightNode(), nil
	}

	rightNode, err := tree.db.GetRightNode(node)
	if err != nil {
		return nil, err
	}

	node.SetRight(rightNode)

	return node.RightNode(), nil
}

// newLeafNode returns a new node from a key, value and version.
func (tree *Tree) newLeafNode(key []byte, value []byte) *inode.Node {
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

func (tree *Tree) nextNodeKey() inode.NodeKey {
	tree.branchSequence++
	nk := inode.NewNodeKey(tree.version.Load()+1, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() inode.NodeKey {
	tree.leafSequence++
	if tree.leafSequence < constants.LeafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := inode.NewNodeKey(tree.version.Load()+1, tree.leafSequence)
	return nk
}
