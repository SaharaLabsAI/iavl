package tree

import (
	"errors"
	"time"

	inode "github.com/cosmos/iavl/v2/node"
)

// TraverseOrderType is the type of the order in which the tree is traversed.
type TraverseOrderType uint8

const (
	PreOrder TraverseOrderType = iota
	PostOrder
)

const maxOutChanSize = 8192

type stackEntry struct {
	node  *inode.Node
	state int // 0: process left, 1: process right, 2: process self
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	imTree, err := tree.GetImmutableProvable(tree.version.Load())
	if err != nil {
		panic(err)
	}

	exporter := &Exporter{
		tree:    imTree,
		out:     make(chan *inode.Node, maxOutChanSize),
		errCh:   make(chan error),
		count:   0,
		startAt: time.Now(),
		version: imTree.version.Load(),
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		defer close(exporter.errCh)

		switch traverseOrder {
		case PostOrder:
			exporter.postOrderNext(imTree.root)
		case PreOrder:
			exporter.preOrderNext(imTree.root)
		}
	}(order)

	return exporter
}

func (tree *Tree) ExportVersion(version int64, order TraverseOrderType) (*Exporter, error) {
	got, _ := tree.getRecentRoot(version)
	if got {
		return tree.Export(order), nil
	}

	imTree, err := tree.GetImmutableProvable(version)
	if err != nil {
		return nil, err
	}

	exporter := &Exporter{
		tree:    imTree,
		out:     make(chan *inode.Node, maxOutChanSize),
		errCh:   make(chan error),
		count:   0,
		version: version,
	}

	if imTree.root == nil {
		close(exporter.out)
		close(exporter.errCh)
		return exporter, nil
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		defer close(exporter.errCh)

		switch traverseOrder {

		case PostOrder:
			exporter.postOrderNext(imTree.root)
		case PreOrder:
			exporter.preOrderNext(imTree.root)
		}
	}(order)

	return exporter, nil
}

type Exporter struct {
	tree    *Tree
	out     chan *inode.Node
	errCh   chan error
	count   int
	startAt time.Time
	version int64
}

func (e *Exporter) postOrderNext(root *inode.Node) {
	if root == nil {
		return
	}

	s := []stackEntry{{node: root, state: 0}}

	for len(s) > 0 {
		currentEntry := &s[len(s)-1]

		if currentEntry.node.IsLeaf() {
			e.out <- currentEntry.node
			s = s[:len(s)-1]
			continue
		}

		// For non-leaf nodes, proceed based on state
		switch currentEntry.state {
		case 0: // State 0: Attempt to process the left child
			currentEntry.state = 1 // Advance this node's state for the next time it's processed

			left, err := e.tree.getLeftNode(currentEntry.node)
			if err != nil {
				e.errCh <- err
				return
			}
			if left != nil {
				s = append(s, stackEntry{node: left, state: 0})
				// The loop will now process the new top (left child)
			}
			// If no left child, currentEntry (now in state 1) remains at the top
			// and will be processed in the next iteration.

		case 1: // State 1: Attempt to process the right child
			currentEntry.state = 2

			right, err := e.tree.getRightNode(currentEntry.node)
			if err != nil {
				e.errCh <- err
				return
			}
			if right != nil {
				s = append(s, stackEntry{node: right, state: 0})
				// The loop will now process the new top (right child)
			}
			// If no right child, currentEntry (now in state 2) remains at the top
			// and will be processed in the next iteration.

		case 2: // State 2: Process the node itself (both children have been handled)
			processedNode := currentEntry.node
			e.out <- processedNode

			// Explicitly nil out child pointers in the processedNode after it's been sent.
			if processedNode != nil {
				processedNode.SetLeft(nil)
				processedNode.SetRight(nil)
			}

			s = s[:len(s)-1]
		}
	}
}

func (e *Exporter) preOrderNext(root *inode.Node) {
	if root == nil {
		return
	}

	stack := []*inode.Node{root}

	for len(stack) > 0 {
		n := len(stack) - 1
		node := stack[n]
		stack = stack[:n]

		e.out <- node

		if !node.IsLeaf() {
			right, err := e.tree.getRightNode(node)
			if err != nil {
				e.errCh <- err
				return
			}
			if right != nil {
				stack = append(stack, right)
			}

			left, err := e.tree.getLeftNode(node)
			if err != nil {
				e.errCh <- err
				return
			}
			if left != nil {
				stack = append(stack, left)
			}
		}
	}
}

func (e *Exporter) Next() (*inode.SnapshotNode, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			// Channel is closed, check for errors
			select {
			case err, ok := <-e.errCh:
				if ok {
					return nil, err
				}
			default:
			}
			return nil, ErrorExportDone
		}
		e.count++

		snapNode := &inode.SnapshotNode{
			Key:     node.Key(),
			Value:   node.Value(),
			Version: node.Version(),
			Height:  node.SubTreeHeight(),
		}

		e.tree.returnNode(node)

		return snapNode, nil
	case err, ok := <-e.errCh:
		if !ok {
			// Error channel closed, check if out channel still has items
			select {
			case node, ok := <-e.out:
				if ok {
					e.count++

					snapNode := &inode.SnapshotNode{
						Key:     node.Key(),
						Value:   node.Value(),
						Version: node.Version(),
						Height:  node.SubTreeHeight(),
					}
					e.tree.returnNode(node)

					return snapNode, nil
				}
			default:
			}
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

// Primary used for unit tests
func (e *Exporter) NextRawNode() (*inode.Node, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			// Channel is closed, check for errors
			select {
			case err, ok := <-e.errCh:
				if ok {
					return nil, err
				}
			default:
			}
			return nil, ErrorExportDone
		}
		e.count++
		return node, nil
	case err, ok := <-e.errCh:
		if !ok {
			// Error channel closed, check if out channel still has items
			select {
			case node, ok := <-e.out:
				if ok {
					e.count++
					return node, nil
				}
			default:
			}
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

var ErrorExportDone = errors.New("export done")

func (e *Exporter) Close() error {
	return e.tree.DiscardImmutableTree()
}
