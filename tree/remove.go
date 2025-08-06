package tree

import (
	"bytes"
	"fmt"
	"time"

	"github.com/SaharaLabsAI/iavl/v2/common/constants"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.metrics != nil {
		defer tree.metrics.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_remove")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return nil, false, nil
	}

	delete(tree.cache, string(key))

	newRoot, _, value, removed, err := tree.iterativeRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.deleted[string(key)] = true
	tree.modificationCount++

	tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_delete")

	tree.root = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) iterativeRemove(node *inode.Node, key []byte) (newSelf *inode.Node, newKey []byte, newValue []byte, removed bool, err error) {
	// Define a struct to track our traversal state
	type removeFrame struct {
		node    *inode.Node
		key     []byte
		visited bool // Whether this node's children have been processed
		goLeft  bool // Whether we go left or right from this node
	}

	// Create stack of frames to process
	stack := make([]removeFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, removeFrame{
		node:    node,
		key:     key,
		visited: false,
		goLeft:  bytes.Compare(key, node.Key()) < 0,
	})

	// Maps parent nodes to their processed children and results
	type resultInfo struct {
		node       *inode.Node // The new node replacing the old one
		newKey     []byte      // New key (if any)
		value      []byte      // Value removed (if any)
		wasRemoved bool        // Whether a removal occurred in this subtree
	}
	resultMap := make(map[*inode.Node]resultInfo)

	// Track nodes that should be returned to the pool after the operation
	nodesToReturn := make(map[*inode.Node]bool)

	// Process frames until the stack is empty
	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode := currentFrame.node

		// Handle leaf nodes directly
		if currentNode.IsLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			var result resultInfo

			// Check if this is the leaf we're looking for
			if bytes.Equal(currentFrame.key, currentNode.Key()) {
				// Found the node to remove
				tree.addDelete(currentNode)
				nodesToReturn[currentNode] = true

				result = resultInfo{
					node:       nil,
					newKey:     nil,
					value:      currentNode.Value(),
					wasRemoved: true,
				}
			} else {
				// This leaf doesn't match the key, keep it
				result = resultInfo{
					node:       currentNode,
					newKey:     nil,
					value:      nil,
					wasRemoved: false,
				}
			}

			// Store result for parent frame
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
			continue
		}

		// Handle internal nodes
		if !currentFrame.visited {
			// Mark as visited for the next iteration
			currentFrame.visited = true
			stack[currentIndex] = *currentFrame

			// Visit the appropriate child node based on key comparison
			var childNode *inode.Node
			if currentFrame.goLeft {
				childNode = tree.EnsureLeftNode(currentNode)
			} else {
				childNode = tree.EnsureRightNode(currentNode)
			}

			// Add child to the stack
			stack = append(stack, removeFrame{
				node:    childNode,
				key:     currentFrame.key,
				visited: false,
				goLeft:  bytes.Compare(currentFrame.key, childNode.Key()) < 0,
			})
		} else {
			// Pop the frame since we've processed its children
			stack = stack[:currentIndex]

			// Get the result from the processed child
			childResult, exists := resultMap[currentNode]
			if !exists {
				return nil, nil, nil, false, fmt.Errorf("internal error: child result not found")
			}

			// Clean up the result map to prevent memory leaks
			delete(resultMap, currentNode)

			// If nothing was removed in the subtree, just pass it up
			if !childResult.wasRemoved {
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					resultMap[parentFrame.node] = childResult
				} else {
					// We're at the root
					// Return nodes to pool now that we're done
					for n := range nodesToReturn {
						tree.returnNode(n)
					}
					return childResult.node, childResult.newKey, childResult.value, childResult.wasRemoved, nil
				}
				continue
			}

			// We need to update the current node based on the removal result
			tree.addOrphan(currentNode)

			var resultNode *inode.Node
			var resultKey []byte

			if currentFrame.goLeft {
				// Left child was affected
				if childResult.node == nil {
					// Left node held value, was removed
					// Collapse `node.rightNode` into `node`
					rightNode := tree.EnsureRightNode(currentNode)
					resultKey = currentNode.Key() // Important: pass the current node's key up
					nodesToReturn[currentNode] = true
					resultNode = rightNode
				} else {
					// Left subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.SetLeft(childResult.node)

					// Update node's height and size
					err := tree.calcHeightAndSize(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Important: propagate the new key if there is one from child
					resultKey = childResult.newKey
				}
			} else {
				// Right child was affected
				if childResult.node == nil {
					// Right node held value, was removed
					// Collapse `node.leftNode` into `node`
					leftNode := tree.EnsureLeftNode(currentNode)
					nodesToReturn[currentNode] = true
					resultNode = leftNode
					// No new key when right node is removed and replaced with left node
				} else {
					// Right subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.SetRight(childResult.node)

					// Update key if needed (this is crucial for correct hash calculation)
					if childResult.newKey != nil {
						currentNode.SetKey(childResult.newKey)
					}

					// Update node's height and size
					err := tree.calcHeightAndSize(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}
				}
			}

			// Create result for this node
			result := resultInfo{
				node:       resultNode,
				newKey:     resultKey,
				value:      childResult.value,
				wasRemoved: true,
			}

			// Store result for parent frame or return final result
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					n.SetLeft(nil)
					n.SetRight(nil)
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
		}
	}

	// We should never reach here
	// If we somehow do, make sure to clean up
	for n := range nodesToReturn {
		tree.returnNode(n)
	}
	return nil, nil, nil, false, fmt.Errorf("unexpected exit from iterativeRemove")
}
