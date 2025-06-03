package tree

import (
	"bytes"

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
