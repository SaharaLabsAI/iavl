package tree

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cosmos/iavl/v2/common/constants"
	inode "github.com/cosmos/iavl/v2/node"
)

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	if tree.immutable {
		panic("set on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_set")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_update")
	} else {
		tree.metrics.IncrCounter(1, constants.MetricsNamespace, "tree_new_node")
	}

	tree.modificationCount++
	tree.cache[string(key)] = value
	delete(tree.deleted, string(key))

	return updated, nil
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.immutable {
		panic("Remove on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), constants.MetricsNamespace, "tree_remove")
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

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.newLeafNode(key, value)
		return updated, nil
	}

	tree.root, updated, err = tree.iterativeSet(tree.root, key, value)

	return updated, err
}

func (tree *Tree) iterativeSet(node *inode.Node, key []byte, value []byte) (
	newSelf *inode.Node, updated bool, err error,
) {
	// Define a struct to track our traversal state
	type setFrame struct {
		node    *inode.Node
		key     []byte
		value   []byte
		goLeft  bool // Whether we should go left or right from this node
		visited bool // Whether this node's children have been processed
	}

	// Create stack of frames to process
	stack := make([]setFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, setFrame{
		node:    node,
		key:     key,
		value:   value,
		goLeft:  bytes.Compare(key, node.Key()) < 0,
		visited: false,
	})

	// Process frames in a loop until stack is empty
	var currentNode *inode.Node
	childMap := make(map[*inode.Node]*inode.Node) // Maps parent nodes to their processed children

	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode = currentFrame.node

		// Handle nil node (should never happen)
		if currentNode == nil {
			panic("node is nil")
		}

		// Handle leaf nodes directly - no recursion needed
		if currentNode.IsLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			switch bytes.Compare(currentFrame.key, currentNode.Key()) {
			case -1: // setKey < leafKey
				tree.metrics.IncrCounter(2, constants.MetricsNamespace, "pool_get")
				parent := tree.nodePool.Get()
				parent.SetNodeKey(tree.nextNodeKey())
				parent.SetKey(currentNode.Key())
				parent.SetSubTreeHeight(1)
				parent.SetSize(2)
				parent.SetDirty(true)
				parent.SetLeft(tree.newLeafNode(currentFrame.key, currentFrame.value))
				parent.SetRight(currentNode)

				tree.workingBytes += parent.SizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			case 1: // setKey > leafKey
				tree.metrics.IncrCounter(2, constants.MetricsNamespace, "pool_get")
				parent := tree.nodePool.Get()
				parent.SetNodeKey(tree.nextNodeKey())
				parent.SetKey(currentFrame.key)
				parent.SetSubTreeHeight(1)
				parent.SetSize(2)
				parent.SetDirty(true)
				parent.SetLeft(currentNode)
				parent.SetRight(tree.newLeafNode(currentFrame.key, currentFrame.value))

				tree.workingBytes += parent.SizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			default: // Equal keys - update the value
				tree.addOrphan(currentNode)
				wasDirty := currentNode.Dirty()
				tree.mutateNode(currentNode)
				if tree.isReplaying {
					currentNode.SetHash(currentFrame.value)
				} else {
					if wasDirty {
						tree.workingBytes -= currentNode.SizeBytes()
					}
					currentNode.SetValue(currentFrame.value)
					// currentNode._hash() // parallel hash in deepHashParallel
					tree.workingBytes += currentNode.SizeBytes()
				}

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = currentNode
					updated = true
				} else {
					return currentNode, true, nil
				}
			}
		} else {
			// Handle internal nodes
			if !currentFrame.visited {
				// Mark as visited to avoid repeated work
				currentFrame.visited = true
				stack[currentIndex] = *currentFrame

				// First visit: add orphan and mutate node
				tree.addOrphan(currentNode)
				tree.mutateNode(currentNode)

				// Add frame for child node traversal
				var childNode *inode.Node
				if currentFrame.goLeft {
					childNode = tree.ensureLeftNode(currentNode)
				} else {
					childNode = tree.ensureRightNode(currentNode)
				}

				// Push child onto stack
				stack = append(stack, setFrame{
					node:    childNode,
					key:     currentFrame.key,
					value:   currentFrame.value,
					goLeft:  bytes.Compare(currentFrame.key, childNode.Key()) < 0,
					visited: false,
				})
			} else {
				// Pop frame from stack
				stack = stack[:currentIndex]

				// Get processed child node
				childNode, exists := childMap[currentNode]
				if !exists {
					return nil, false, fmt.Errorf("internal error: child not found for node")
				}

				// Delete from map to prevent memory leaks
				delete(childMap, currentNode)

				// Apply the child node to the current node
				if currentFrame.goLeft {
					currentNode.SetLeft(childNode)
				} else {
					currentNode.SetRight(childNode)
				}

				// If the child was updated, no need for balancing
				if updated {
					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = currentNode
					} else {
						return currentNode, updated, nil
					}
				} else {
					// Perform height/size calculation and balancing
					err = tree.calcHeightAndSize(currentNode)
					if err != nil {
						return nil, false, err
					}

					newNode, err := tree.balance(currentNode)
					if err != nil {
						return nil, false, err
					}

					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = newNode
					} else {
						return newNode, updated, nil
					}
				}
			}
		}
	}

	// We should never reach here if the algorithm is implemented correctly
	return node, false, fmt.Errorf("unexpected exit from recursiveSet")
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
				childNode = tree.ensureLeftNode(currentNode)
			} else {
				childNode = tree.ensureRightNode(currentNode)
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
					rightNode := tree.ensureRightNode(currentNode)
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
					leftNode := tree.ensureLeftNode(currentNode)
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

func (tree *Tree) mutateNode(node *inode.Node) {
	// this seems to be true only in certain cases
	// we should investigate if we can remove this check
	alreadyMutatedForNextVersion := node.Hash() == nil && node.Version() == tree.version.Load()+1
	if alreadyMutatedForNextVersion {
		// This node has already been mutated for the next version, so we can skip
		// This can happen when we have already processed this node during a recursive operation
		return
	}

	// Always mark the hash as nil to ensure recomputation
	node.SetHash(nil)

	// Create a new nodeKey with the next version
	if node.IsLeaf() {
		node.SetNodeKey(tree.nextLeafNodeKey())
	} else {
		node.SetNodeKey(tree.nextNodeKey())
	}

	// Mark the node as dirty for tracking
	node.SetDirty(true)

	// Mark the tree hash as dirty
	tree.markHashDirty()
}
