package sqlite

import (
	"errors"
	"fmt"

	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

func (sql *SqliteDb) GetRightNode(node *inode.Node) (*inode.Node, error) {
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no children")
	}

	var (
		rightNode *inode.Node
		err       error
	)

	if constants.IsLeafSeq(node.RightNodeKey().Sequence()) {
		rightNode, err = sql.GetLeaf(node.RightNodeKey())
	} else {
		rightNode, err = sql.getNode(node.RightNodeKey())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.RightNodeKey(), node.SubTreeHeight(), sql.opts.Path, err)
	}

	return rightNode, nil
}

func (sql *SqliteDb) GetLeftNode(node *inode.Node) (*inode.Node, error) {
	if node.IsLeaf() {
		return nil, errors.New("leaf node has no children")
	}

	var (
		leftNode *inode.Node
		err      error
	)

	if constants.IsLeafSeq(node.LeftNodeKey().Sequence()) {
		leftNode, err = sql.GetLeaf(node.LeftNodeKey())
	} else {
		leftNode, err = sql.getNode(node.LeftNodeKey())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get left node node_key=%s height=%d path=%s: %w",
			node.LeftNodeKey(), node.SubTreeHeight(), sql.opts.Path, err)
	}

	return leftNode, nil
}
