package tree

import (
	"errors"
	"fmt"

	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

// NOTE: assumes that node can be modified
func (t *Tree) balance(node *inode.Node) (newSelf *inode.Node, err error) {
	if node.Hash() != nil {
		return nil, errors.New("unexpected balance() call on persisted node")
	}

	// Pre-fetch left and right nodes once to avoid repeated fetches
	leftNode, err := t.getLeftNode(node)
	if err != nil {
		return nil, err
	}

	rightNode, err := t.getRightNode(node)
	if err != nil {
		return nil, err
	}

	// Calculate balance directly instead of through calcBalance
	balance := int(leftNode.SubTreeHeight()) - int(rightNode.SubTreeHeight())

	// No balancing needed - fast path
	if balance >= -1 && balance <= 1 {
		return node, nil
	}

	// Left heavy subtree
	if balance > 1 {
		// Only fetch left child's nodes if we need to determine rotation type
		leftLeftNode, err := t.getLeftNode(leftNode)
		if err != nil {
			return nil, err
		}

		// Check if we need a double rotation by looking at left-left height
		if leftLeftNode.SubTreeHeight() >= leftNode.SubTreeHeight()-1 {
			// Left-Left case: single right rotation (leftLeftNode height is sufficient)
			return t.rotateRight(node)
		}

		// Left-Right case: left rotation on left child, then right rotation on node
		newLeftNode, err := t.rotateLeft(leftNode)
		if err != nil {
			return nil, err
		}
		node.SetLeft(newLeftNode)

		return t.rotateRight(node)
	}

	// balance < -1, right heavy subtree
	// Only fetch right child's nodes if we need to determine rotation type
	rightRightNode, err := t.getRightNode(rightNode)
	if err != nil {
		return nil, err
	}

	// Check if we need a double rotation by looking at right-right height
	if rightRightNode.SubTreeHeight() >= rightNode.SubTreeHeight()-1 {
		// Right-Right case: single left rotation (rightRightNode height is sufficient)
		return t.rotateLeft(node)
	}

	// Right-Left case: right rotation on right child, then left rotation on node
	newRightNode, err := t.rotateRight(rightNode)
	if err != nil {
		return nil, err
	}
	node.SetRight(newRightNode)

	return t.rotateLeft(node)
}

// NOTE: mutates height and size
func (t *Tree) calcHeightAndSize(node *inode.Node) error {
	leftNode, err := t.getLeftNode(node)
	if err != nil {
		return err
	}

	rightNode, err := t.getRightNode(node)
	if err != nil {
		return err
	}

	node.SetSubTreeHeight(max(leftNode.SubTreeHeight(), rightNode.SubTreeHeight()) + 1)
	node.SetSize(leftNode.Size() + rightNode.Size())

	// make sure we should update hash
	node.SetDirty(true)
	node.SetHash(nil)

	return nil
}

// Rotate right and return the new node and orphan.
func (t *Tree) rotateRight(node *inode.Node) (*inode.Node, error) {
	var err error

	// Early validation to prevent operations on nil nodes
	if node == nil {
		return nil, errors.New("cannot rotate nil node")
	}

	// Get the left node before any modifications
	leftNode, err := t.getLeftNode(node)
	if err != nil {
		return nil, fmt.Errorf("failed to get left node: %w", err)
	}
	if leftNode == nil {
		return nil, errors.New("cannot rotate right with nil left child")
	}

	// Get right node of left child before any modifications
	rightOfLeft, err := t.getRightNode(leftNode)
	if err != nil {
		return nil, fmt.Errorf("failed to get right of left node: %w", err)
	}

	// Add original nodes to orphans for tracking
	t.addOrphan(node)
	t.addOrphan(leftNode)

	// Mutate nodes - this changes their keys and marks them dirty
	t.mutateNode(node)
	t.mutateNode(leftNode)

	// Update node references in a clear sequence
	// 1. First update the left child connection
	node.SetLeft(rightOfLeft)

	// 2. Then update the right child connection of leftNode
	leftNode.SetRight(node)

	// Update heights and sizes after rotation
	err = t.calcHeightAndSize(node)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate node height after rotation: %w", err)
	}

	err = t.calcHeightAndSize(leftNode)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate leftNode height after rotation: %w", err)
	}

	// Mark hash as dirty since tree structure changed
	t.markHashDirty()

	return leftNode, nil
}

// Rotate left and return the new node and orphan.
func (t *Tree) rotateLeft(node *inode.Node) (*inode.Node, error) {
	var err error

	// Early validation to prevent operations on nil nodes
	if node == nil {
		return nil, errors.New("cannot rotate nil node")
	}

	// Get the right node before any modifications
	rightNode, err := t.getRightNode(node)
	if err != nil {
		return nil, fmt.Errorf("failed to get right node: %w", err)
	}
	if rightNode == nil {
		return nil, errors.New("cannot rotate left with nil right child")
	}

	// Get left node of right child before any modifications
	leftOfRight, err := t.getLeftNode(rightNode)
	if err != nil {
		return nil, fmt.Errorf("failed to get left of right node: %w", err)
	}

	// Add original nodes to orphans for tracking
	t.addOrphan(node)
	t.addOrphan(rightNode)

	// Mutate nodes - this changes their keys and marks them dirty
	t.mutateNode(node)
	t.mutateNode(rightNode)

	// Update node references in a clear sequence
	// 1. First update the right child connection
	node.SetRight(leftOfRight)

	// 2. Then update the left child connection of rightNode
	rightNode.SetLeft(node)

	// Update heights and sizes after rotation
	err = t.calcHeightAndSize(node)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate node height after rotation: %w", err)
	}

	err = t.calcHeightAndSize(rightNode)
	if err != nil {
		return nil, fmt.Errorf("failed to recalculate rightNode height after rotation: %w", err)
	}

	// Mark hash as dirty since tree structure changed
	t.markHashDirty()

	return rightNode, nil
}
