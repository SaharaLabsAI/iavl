package tree

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	ics23 "github.com/cosmos/ics23/go"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/common/metrics"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	"github.com/cosmos/iavl/v2/db"
	inode "github.com/cosmos/iavl/v2/node"
)

type ImmutableTree struct {
	version int64
	root    *inode.Node

	db       db.ReadonlyDB
	nodePool *nodepool.NodePool

	metrics metrics.Proxy
}

func (tree *Tree) GetImmutable(version int64) (*ImmutableTree, error) {
	// We can discard whole pool after usage
	pool := nodepool.NewNodePool()
	db := tree.db.Readonly()

	imTree := &ImmutableTree{
		db:       db,
		nodePool: pool,
		metrics:  tree.metrics,
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	return imTree, nil
}

func (tree *ImmutableTree) Close() error {
	if tree.db == nil {
		return nil
	}

	return tree.db.Close()
}

func (tree *ImmutableTree) Version() int64 {
	return tree.version
}

func (tree *ImmutableTree) VersionExists(version int64) (bool, error) {
	exists, err := tree.db.HasRoot(version)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (tree *ImmutableTree) LoadVersion(version int64) (err error) {
	if tree.db == nil {
		return errors.New("db is nil")
	}

	if version == 0 {
		return nil
	}

	tree.root, err = tree.db.LoadRoot(tree.nodePool, version)
	if err != nil {
		return err
	}
	tree.version = version

	return nil
}

func (tree *ImmutableTree) Has(key []byte) (bool, error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_has")
	}

	val, err := tree.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (tree *ImmutableTree) Get(key []byte) ([]byte, error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_db_get")
	}

	return tree.db.GetValue(key, tree.version)
}

func (tree *ImmutableTree) Hash() []byte {
	if tree.root == nil {
		return inode.EmptyHash
	}
	return tree.root.Hash()
}

func (tree *ImmutableTree) GetWithIndex(key []byte) (int64, []byte, error) {
	if tree.root == nil {
		return 0, nil, nil
	}

	return tree.get(tree.root, key)
}

func (tree *ImmutableTree) GetByIndex(index int64) (key []byte, value []byte, err error) {
	if tree.root == nil {
		return nil, nil, nil
	}

	return tree.getByIndex(tree.root, index)
}

func (tree *ImmutableTree) GetProof(key []byte) (proof *ics23.CommitmentProof, err error) {
	return tree.getProof(key)
}

func (tree *ImmutableTree) get(node *inode.Node, key []byte) (index int64, value []byte, err error) {
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

func (tree *ImmutableTree) getByIndex(node *inode.Node, index int64) (key []byte, value []byte, err error) {
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

func (tree *ImmutableTree) getLeftNode(node *inode.Node) (*inode.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no left node")
	}
	if node.LeftNode() != nil {
		return node.LeftNode(), nil
	}

	leftNode, err := tree.db.GetNode(tree.nodePool, node.LeftNodeKey())
	if err != nil {
		return nil, fmt.Errorf("failed to get left node node_key=%s height=%d path=%s: %w",
			node.LeftNodeKey(), node.SubTreeHeight(), tree.db.Path(), err)
	}

	node.SetLeft(leftNode)

	return node.LeftNode(), nil
}

func (tree *ImmutableTree) getRightNode(node *inode.Node) (*inode.Node, error) {
	node.CheckValid()
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no right node")
	}
	if node.RightNode() != nil {
		return node.RightNode(), nil
	}

	rightNode, err := tree.db.GetNode(tree.nodePool, node.RightNodeKey())
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.RightNodeKey(), node.SubTreeHeight(), tree.db.Path(), err)
	}

	node.SetRight(rightNode)

	return node.RightNode(), nil
}

func (tree *ImmutableTree) returnNode(node *inode.Node) {
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

	// Return node to the pool for reuse
	tree.nodePool.Put(node)
}

func (tree *ImmutableTree) Iterator(start, end []byte, inclusive bool) (itr Iterator, err error) {
	itr = &LeafIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: true,
		inclusive: inclusive,
		valid:     tree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metrics,
	}

	treeItr := itr.(*LeafIterator)
	treeItr.initializeIteratorStack(tree.root)

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if tree.root != nil {
		itr.Next()
	}

	return itr, err
}

func (tree *ImmutableTree) ReverseIterator(start, end []byte) (itr Iterator, err error) {
	itr = &LeafIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: false,
		inclusive: false,
		valid:     tree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metrics,
	}

	treeItr := itr.(*LeafIterator)
	treeItr.initializeIteratorStack(tree.root)

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if tree.root != nil {
		itr.Next()
	}

	return itr, nil
}
