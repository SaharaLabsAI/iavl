package tree

import (
	"bytes"
	"time"

	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_has")
	}

	val, err := tree.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_db_get")
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

	return tree.db.GetValue(key, treeVersion)
}

func (tree *Tree) GetFromRoot(key []byte) ([]byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	_, val, err := tree.get(tree.root, key)
	return val, err
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

func (tree *Tree) get(node *inode.Node, key []byte) (index int64, value []byte, err error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_get")
	}

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
		leftNode, err := tree.getLeftNode(node)
		if err != nil {
			return 0, nil, err
		}

		return tree.get(leftNode, key)
	}

	rightNode, err := tree.getRightNode(node)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = tree.get(rightNode, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.Size() - rightNode.Size()

	return index, value, nil
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
