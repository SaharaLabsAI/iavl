package iavl

import (
	"bytes"
	"errors"

	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

func (t *Tree) get(node *nodetypes.Node, key []byte) (index int64, value []byte, err error) {
	if node.IsLeaf() {
		switch bytes.Compare(node.Key(), key) {
		case -1:
			return 1, nil, nil
		case 1:
			return 0, nil, nil
		default:
			return 0, node.Value(), nil
		}
	}

	if bytes.Compare(key, node.Key()) < 0 {
		leftNode, err := t.getLeftNode(node)
		if err != nil {
			return 0, nil, err
		}

		return t.get(leftNode, key)
	}

	rightNode, err := t.getRightNode(node)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = t.get(rightNode, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.Size() - rightNode.Size()

	return index, value, nil
}

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
