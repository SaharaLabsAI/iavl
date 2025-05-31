package iavl

import (
	"bytes"
	"errors"
	"fmt"
)

func (node *Node) left(t *Tree) *Node {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		panic(err)
	}
	return leftNode
}

func (node *Node) right(t *Tree) *Node {
	rightNode, err := node.getRightNode(t)
	if err != nil {
		panic(err)
	}
	return rightNode
}

// getLeftNode will never be called on leaf nodes. all tree nodes have 2 children.
func (node *Node) getLeftNode(t *Tree) (*Node, error) {
	node.CheckValid()
	if node.isLeaf() {
		return nil, errors.New("leaf node has no left node")
	}
	if node.leftNode != nil {
		return node.leftNode, nil
	}
	var err error
	node.leftNode, err = t.sql.getLeftNode(node)
	if err != nil {
		return nil, err
	}
	return node.leftNode, nil
}

func (node *Node) getRightNode(t *Tree) (*Node, error) {
	node.CheckValid()
	if node.isLeaf() {
		return nil, errors.New("leaf node has no right node")
	}
	if node.rightNode != nil {
		return node.rightNode, nil
	}
	var err error
	node.rightNode, err = t.sql.getRightNode(node)
	if err != nil {
		return nil, err
	}
	return node.rightNode, nil
}

// NOTE: mutates height and size
func (node *Node) calcHeightAndSize(t *Tree) error {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return err
	}

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size

	// make sure we should update hash
	node.dirty = true
	node.hash = nil

	return nil
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (tree *Tree) balance(node *Node) (newSelf *Node, err error) {
	if node.hash != nil {
		return nil, errors.New("unexpected balance() call on persisted node")
	}

	// Pre-fetch left and right nodes once to avoid repeated fetches
	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, err
	}

	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, err
	}

	// Calculate balance directly instead of through calcBalance
	balance := int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight)

	// No balancing needed - fast path
	if balance >= -1 && balance <= 1 {
		return node, nil
	}

	// Left heavy subtree
	if balance > 1 {
		// Only fetch left child's nodes if we need to determine rotation type
		leftLeftNode, err := leftNode.getLeftNode(tree)
		if err != nil {
			return nil, err
		}

		// Check if we need a double rotation by looking at left-left height
		if leftLeftNode.subtreeHeight >= leftNode.subtreeHeight-1 {
			// Left-Left case: single right rotation (leftLeftNode height is sufficient)
			return tree.rotateRight(node)
		} else {
			// Left-Right case: left rotation on left child, then right rotation on node
			newLeftNode, err := tree.rotateLeft(leftNode)
			if err != nil {
				return nil, err
			}
			node.setLeft(newLeftNode)

			return tree.rotateRight(node)
		}
	} else { // balance < -1, right heavy subtree
		// Only fetch right child's nodes if we need to determine rotation type
		rightRightNode, err := rightNode.getRightNode(tree)
		if err != nil {
			return nil, err
		}

		// Check if we need a double rotation by looking at right-right height
		if rightRightNode.subtreeHeight >= rightNode.subtreeHeight-1 {
			// Right-Right case: single left rotation (rightRightNode height is sufficient)
			return tree.rotateLeft(node)
		} else {
			// Right-Left case: right rotation on right child, then left rotation on node
			newRightNode, err := tree.rotateRight(rightNode)
			if err != nil {
				return nil, err
			}
			node.setRight(newRightNode)

			return tree.rotateLeft(node)
		}
	}
}

func (node *Node) calcBalance(t *Tree) (int, error) {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

// Rotate right and return the new node and orphan.
func (tree *Tree) rotateRight(node *Node) (*Node, error) {
	var err error

	// Early validation to prevent operations on nil nodes
	if node == nil {
		return nil, errors.New("cannot rotate nil node")
	}

	// Get the left node before any modifications
	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to get left node: %w", err)
	}
	if leftNode == nil {
		return nil, errors.New("cannot rotate right with nil left child")
	}

	// Get right node of left child before any modifications
	rightOfLeft, err := leftNode.getRightNode(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to get right of left node: %w", err)
	}

	// Add original nodes to orphans for tracking
	tree.addOrphan(node)
	tree.addOrphan(leftNode)

	// Mutate nodes - this changes their keys and marks them dirty
	tree.mutateNode(node)
	tree.mutateNode(leftNode)

	// Update node references in a clear sequence
	// 1. First update the left child connection
	node.setLeft(rightOfLeft)

	// 2. Then update the right child connection of leftNode
	leftNode.setRight(node)

	// Update heights and sizes after rotation
	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate node height after rotation: %w", err)
	}

	err = leftNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate leftNode height after rotation: %w", err)
	}

	// Mark hash as dirty since tree structure changed
	tree.markHashDirty()

	return leftNode, nil
}

// Rotate left and return the new node and orphan.
func (tree *Tree) rotateLeft(node *Node) (*Node, error) {
	var err error

	// Early validation to prevent operations on nil nodes
	if node == nil {
		return nil, errors.New("cannot rotate nil node")
	}

	// Get the right node before any modifications
	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to get right node: %w", err)
	}
	if rightNode == nil {
		return nil, errors.New("cannot rotate left with nil right child")
	}

	// Get left node of right child before any modifications
	leftOfRight, err := rightNode.getLeftNode(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to get left of right node: %w", err)
	}

	// Add original nodes to orphans for tracking
	tree.addOrphan(node)
	tree.addOrphan(rightNode)

	// Mutate nodes - this changes their keys and marks them dirty
	tree.mutateNode(node)
	tree.mutateNode(rightNode)

	// Update node references in a clear sequence
	// 1. First update the right child connection
	node.setRight(leftOfRight)

	// 2. Then update the left child connection of rightNode
	rightNode.setLeft(node)

	// Update heights and sizes after rotation
	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate node height after rotation: %w", err)
	}

	err = rightNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate rightNode height after rotation: %w", err)
	}

	// Mark hash as dirty since tree structure changed
	tree.markHashDirty()

	return rightNode, nil
}

func (node *Node) get(t *Tree, key []byte) (index int64, value []byte, err error) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil, nil
		case 1:
			return 0, nil, nil
		default:
			return 0, node.value, nil
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		leftNode, err := node.getLeftNode(t)
		if err != nil {
			return 0, nil, err
		}

		return leftNode.get(t, key)
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = rightNode.get(t, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.size - rightNode.size
	return index, value, nil
}
