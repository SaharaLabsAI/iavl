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
					childNode = tree.EnsureLeftNode(currentNode)
				} else {
					childNode = tree.EnsureRightNode(currentNode)
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
