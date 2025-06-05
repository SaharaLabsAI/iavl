package tree

import (
	"bytes"
	"time"

	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_has")
	}

	val, err := tree.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_get")
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	treeVersion := tree.version.Load()

	if val, exists := tree.cache[string(key)]; exists {
		return val, nil
	}
	if _, exists := tree.deleted[string(key)]; exists {
		return nil, nil
	}

	return tree.db.GetVersioned(key, treeVersion)
}

func (t *Tree) get(node *inode.Node, key []byte) (index int64, value []byte, err error) {
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
