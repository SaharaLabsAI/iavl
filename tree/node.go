package tree

import (
	"errors"

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

	leftNode, err := t.sql.GetLeftNode(node)
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

	rightNode, err := t.sql.GetRightNode(node)
	if err != nil {
		return nil, err
	}

	node.SetRight(rightNode)

	return node.RightNode(), nil
}
