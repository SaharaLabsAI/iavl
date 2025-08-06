package tree

import (
	"bytes"
	"time"

	"github.com/SaharaLabsAI/iavl/v2/common/metrics"
	nodepool "github.com/SaharaLabsAI/iavl/v2/common/pool/node"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

type Iterator interface {
	// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
	// CONTRACT: start, end readonly []byte
	Domain() (start []byte, end []byte)

	// Valid returns whether the current iterator is valid. Once invalid, the LeafIterator remains
	// invalid forever.
	Valid() bool

	// Next moves the iterator to the next key in the database, as defined by order of iteration.
	// If Valid returns false, this method will panic.
	Next()

	// Key returns the key at the current position. Panics if the iterator is invalid.
	// CONTRACT: key readonly []byte
	Key() (key []byte)

	// Value returns the value at the current position. Panics if the iterator is invalid.
	// CONTRACT: value readonly []byte
	Value() (value []byte)

	// Error returns the last error encountered by the iterator, if any.
	Error() error

	// Close closes the iterator, releasing any allocated resources.
	Close() error
}

var _ Iterator = (*LeafIterator)(nil)

type iteratorStackEntry struct {
	node  *inode.Node
	state int // 0: process left, 1: process right, 2: process self
}

type LeafIterator struct {
	tree       *ImmutableTree
	start, end []byte // iteration domain
	ascending  bool   // ascending traversal
	inclusive  bool   // end key inclusiveness

	stack   []iteratorStackEntry
	started bool

	key, value []byte // current key, value
	err        error  // current error
	valid      bool   // iteration status

	metrics metrics.Proxy
}

func (i *LeafIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *LeafIterator) Valid() bool {
	return i.valid
}

func (i *LeafIterator) Next() {
	defer func() {
		if !i.valid {
			i.Close()
		}
	}()

	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "iterator", "next")
	}
	if !i.valid {
		return
	}
	if len(i.stack) == 0 {
		i.valid = false
		return
	}
	if i.ascending {
		i.stepAscend()
	} else {
		i.stepDescend()
	}
	i.started = true
}

func (i *LeafIterator) stepAscend() {
	for len(i.stack) > 0 {
		currentEntry := &i.stack[len(i.stack)-1]
		node := currentEntry.node

		if node.IsLeaf() {
			if !i.started && bytes.Compare(node.Key(), i.start) < 0 {
				// Skip this leaf and remove from stack
				removedNode := i.removeNodeFromStack()
				if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
					i.tree.nodePool.Put(removedNode)
				}
				continue
			}
			if i.isPastEndAscend(node.Key()) {
				i.valid = false
				return
			}

			// Found valid leaf
			i.key = node.Key()
			i.value = node.Value()

			removedNode := i.removeNodeFromStack()
			if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
				i.tree.nodePool.Put(removedNode)
			}
			return
		}

		// Handle internal node based on state
		switch currentEntry.state {
		case 0: // Process left subtree first (for ascending order)
			currentEntry.state = 1 // Advance state for next iteration

			// For ascending order, we need to check if we should traverse left subtree
			// We traverse left if start is less than current node's key
			if i.start == nil || bytes.Compare(i.start, node.Key()) < 0 {
				left, err := i.tree.getLeftNode(node)
				if err != nil {
					i.err = err
					i.valid = false
					return
				}
				if left != nil {
					// Make a copy of the node if it's from a different pool
					leftToAdd := left
					if left.PoolID() != i.tree.nodePool.PoolID() {
						leftCopy := i.tree.nodePool.Get()
						*leftCopy = *left // Shallow copy
						leftToAdd = leftCopy
					}
					i.addNodeToStack(leftToAdd, 0)
					// Continue to process the left child
					continue
				}
			}
			// No left child to process or shouldn't traverse left, continue to state 1 in next iteration

		case 1: // Process right subtree
			currentEntry.state = 2 // Advance state for next iteration

			right, err := i.tree.getRightNode(node)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if right != nil {
				// Make a copy of the node if it's from a different pool
				rightToAdd := right
				if right.PoolID() != i.tree.nodePool.PoolID() {
					rightCopy := i.tree.nodePool.Get()
					*rightCopy = *right // Shallow copy
					rightToAdd = rightCopy
				}
				i.addNodeToStack(rightToAdd, 0)
				// Continue to process the right child
				continue
			}
			// No right child to process, continue to state 2 in next iteration

		case 2: // Done with this internal node
			removedNode := i.removeNodeFromStack()
			if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
				i.tree.nodePool.Put(removedNode)
			}
		}
	}

	i.valid = false
}

func (i *LeafIterator) stepDescend() {
	for len(i.stack) > 0 {
		currentEntry := &i.stack[len(i.stack)-1]
		node := currentEntry.node

		if node.IsLeaf() {
			if !i.started && i.end != nil {
				res := bytes.Compare(i.end, node.Key())
				// if end is inclusive and the key is greater than end, skip
				if i.inclusive && res < 0 {
					// Skip this leaf and remove from stack
					removedNode := i.removeNodeFromStack()
					// Return leaf node to pool if it belongs to iterator pool
					if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
						i.tree.nodePool.Put(removedNode)
					}
					continue
				}
				// if end is not inclusive (default) and the key is greater than or equal to end, skip
				if res <= 0 {
					// Skip this leaf and remove from stack
					removedNode := i.removeNodeFromStack()
					if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
						i.tree.nodePool.Put(removedNode)
					}
					continue
				}
			}
			if i.isPastEndDescend(node.Key()) {
				i.valid = false
				return
			}

			// Found valid leaf
			i.key = node.Key()
			i.value = node.Value()

			removedNode := i.removeNodeFromStack()
			if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
				i.tree.nodePool.Put(removedNode)
			}
			return
		}

		// Handle internal node based on state
		switch currentEntry.state {
		case 0: // Process right subtree first (for descending order)
			currentEntry.state = 1 // Advance state for next iteration

			// For descending order, we need to check if we should traverse right subtree
			// We traverse right if end is nil or current node's key is <= end
			if i.end == nil || bytes.Compare(node.Key(), i.end) <= 0 {
				right, err := i.tree.getRightNode(node)
				if err != nil {
					i.err = err
					i.valid = false
					return
				}
				if right != nil {
					// Make a copy of the node if it's from a different pool
					rightToAdd := right
					if right.PoolID() != i.tree.nodePool.PoolID() {
						rightCopy := i.tree.nodePool.Get()
						*rightCopy = *right // Shallow copy
						rightToAdd = rightCopy
					}
					i.addNodeToStack(rightToAdd, 0)
					// Continue to process the right child
					continue
				}
			}
			// No right child to process or shouldn't traverse right, continue to state 1 in next iteration

		case 1: // Process left subtree
			currentEntry.state = 2 // Advance state for next iteration

			left, err := i.tree.getLeftNode(node)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if left != nil {
				// Make a copy of the node if it's from a different pool
				leftToAdd := left
				if left.PoolID() != i.tree.nodePool.PoolID() {
					leftCopy := i.tree.nodePool.Get()
					*leftCopy = *left // Shallow copy
					leftToAdd = leftCopy
				}
				i.addNodeToStack(leftToAdd, 0)
				// Continue to process the left child
				continue
			}
			// No left child to process, continue to state 2 in next iteration

		case 2: // Done with this internal node
			removedNode := i.removeNodeFromStack()
			if removedNode != nil && i.tree != nil && i.tree.nodePool != nil && removedNode.PoolID() == i.tree.nodePool.PoolID() {
				i.tree.nodePool.Put(removedNode)
			}
		}
	}

	// Stack is empty
	i.valid = false
}

func (i *LeafIterator) isPastEndAscend(key []byte) bool {
	if i.end == nil {
		return false
	}
	if i.inclusive {
		return bytes.Compare(key, i.end) > 0
	}
	return bytes.Compare(key, i.end) >= 0
}

func (i *LeafIterator) isPastEndDescend(key []byte) bool {
	if i.start == nil {
		return false
	}
	return bytes.Compare(key, i.start) < 0
}

func (i *LeafIterator) Key() (key []byte) {
	return i.key
}

func (i *LeafIterator) Value() (value []byte) {
	v := make([]byte, len(i.value))
	copy(v, i.value)

	return v
}

func (i *LeafIterator) Error() error {
	return i.err
}

func (i *LeafIterator) Close() error {
	if i.tree != nil && i.tree.nodePool != nil {
		for len(i.stack) > 0 {
			entry := i.stack[len(i.stack)-1]
			node := entry.node
			i.stack = i.stack[:len(i.stack)-1]
			if node != nil && node.PoolID() == i.tree.nodePool.PoolID() {
				i.tree.nodePool.Put(node)
			}
		}
	}

	i.stack = nil
	i.valid = false

	i.tree.root = nil
	i.tree.nodePool = nil
	i.tree.db = nil

	return i.err
}

func (i *LeafIterator) addNodeToStack(node *inode.Node, state int) {
	i.stack = append(i.stack, iteratorStackEntry{node: node, state: state})
}

func (i *LeafIterator) removeNodeFromStack() *inode.Node {
	if len(i.stack) == 0 {
		return nil
	}

	lastEntry := i.stack[len(i.stack)-1]
	node := lastEntry.node

	i.stack = i.stack[:len(i.stack)-1]

	return node
}

func (i *LeafIterator) initializeIteratorStack(root *inode.Node) {
	if root != nil {
		// Make a copy of the root if it's from a different pool
		rootToAdd := root
		if root.PoolID() != i.tree.nodePool.PoolID() {
			rootCopy := i.tree.nodePool.Get()
			*rootCopy = *root // Shallow copy
			rootToAdd = rootCopy
		}
		i.addNodeToStack(rootToAdd, 0)
	}
}

func newIterTree(tree *Tree) *ImmutableTree {
	pool := nodepool.NewNodePool()
	db := tree.db.Readonly()

	itTree := &ImmutableTree{
		version:  tree.version.Load(),
		db:       db,
		nodePool: pool,
		metrics:  tree.metrics,
	}

	if tree.root != nil {
		itTree.root = tree.root.DeepCopy(nil)
	}

	return itTree
}

func (tree *Tree) Iterator(start, end []byte, inclusive bool) (itr Iterator, err error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	itTree := newIterTree(tree)

	itr = &LeafIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: true,
		inclusive: inclusive,
		valid:     itTree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metrics,
	}

	treeItr := itr.(*LeafIterator)
	treeItr.initializeIteratorStack(itTree.root)

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}
	return itr, err
}

func (tree *Tree) ReverseIterator(start, end []byte) (itr Iterator, err error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	itTree := newIterTree(tree)

	itr = &LeafIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: false,
		inclusive: false,
		valid:     itTree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metrics,
	}

	treeItr := itr.(*LeafIterator)
	treeItr.initializeIteratorStack(itTree.root)

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}
	return itr, nil
}

func (tree *Tree) IterateRecent(version int64, start, end []byte, ascending bool) (bool, Iterator) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	got, _ := tree.getRecentRoot(version)
	if !got {
		return false, nil
	}

	itTree := newIterTree(tree)

	itr := &LeafIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: ascending,
		inclusive: false,
		valid:     true,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metrics,
	}

	itr.initializeIteratorStack(itTree.root)

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}

	return true, itr
}
