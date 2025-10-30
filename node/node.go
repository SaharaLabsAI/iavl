package node

import (
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"unsafe"
)

const (
	hashSize = 32
	nodeSize = uint64(unsafe.Sizeof(Node{})) + hashSize
)

type Source int

const (
	PoolNode Source = iota
	ManualNode
)

// Node represents a node in a Tree.
type Node struct {
	key           []byte
	value         []byte
	hash          []byte
	nodeKey       NodeKey
	leftNodeKey   NodeKey
	rightNodeKey  NodeKey
	size          int64
	leftNode      *Node
	rightNode     *Node
	subtreeHeight int8

	dirty  bool
	evict  bool
	poolID uint64
	source Source
}

func NewNode(key, value []byte, version int64, height int8) *Node {
	return &Node{
		nodeKey:       NewNodeKey(version, 0),
		key:           key,
		value:         value,
		subtreeHeight: height,
		source:        ManualNode,
	}
}

func (node *Node) ShadowCopy(nodepool Pool) *Node {
	n := &Node{source: ManualNode}
	if nodepool != nil {
		n = nodepool.Get()
		n.source = PoolNode
	}

	*n = *node // Shadow copy

	return n
}

func (node *Node) DeepCopy(nodepool Pool) *Node {
	n := &Node{source: ManualNode}
	if nodepool != nil {
		n = nodepool.Get()
		n.source = PoolNode
	}

	key := make([]byte, len(node.key))
	copy(key, node.key)

	value := make([]byte, len(node.value))
	copy(value, node.value)

	n.subtreeHeight = node.subtreeHeight
	n.nodeKey = node.nodeKey
	n.size = node.size
	n.key = key
	n.hash = node.hash
	n.value = value
	n.leftNodeKey = node.leftNodeKey
	n.rightNodeKey = node.rightNodeKey
	n.leftNode = node.leftNode
	n.rightNode = node.rightNode

	return n
}

func (node *Node) NodeKey() NodeKey {
	node.CheckValid()
	return node.nodeKey
}

func (node *Node) Key() []byte {
	node.CheckValid()
	return node.key
}

func (node *Node) Hash() []byte {
	node.CheckValid()
	return node.hash
}

func (node *Node) Version() int64 {
	return node.nodeKey.Version()
}

func (node *Node) Value() []byte {
	node.CheckValid()
	return node.value
}

func (node *Node) LeftNode() *Node {
	node.CheckValid()
	return node.leftNode
}

func (node *Node) RightNode() *Node {
	node.CheckValid()
	return node.rightNode
}

func (node *Node) RightNodeKey() NodeKey {
	node.CheckValid()
	return node.rightNodeKey
}

func (node *Node) LeftNodeKey() NodeKey {
	node.CheckValid()
	return node.leftNodeKey
}

func (node *Node) varSize() uint64 {
	//nolint:gosec
	return uint64(len(node.key) + len(node.value))
}

func (node *Node) SizeBytes() uint64 {
	return nodeSize + node.varSize()
}

func (node *Node) SubTreeHeight() int8 {
	node.CheckValid()
	return node.subtreeHeight
}

func (node *Node) Size() int64 {
	node.CheckValid()
	return node.size
}

func (node *Node) Dirty() bool {
	node.CheckValid()
	return node.dirty
}

func (node *Node) Evict() bool {
	node.CheckValid()
	return node.evict
}

func (node *Node) PoolID() uint64 {
	return node.poolID
}

func (node *Node) Source() Source {
	return node.source
}

func (node *Node) String() string {
	return fmt.Sprintf("Node{hash: %x, nodeKey: %s, leftNodeKey: %v, rightNodeKey: %v, size: %d, subtreeHeight: %d, poolId: %d}",
		node.hash, node.nodeKey, node.leftNodeKey, node.rightNodeKey, node.size, node.subtreeHeight, node.poolID)
}

func (node *Node) CheckValid() {
	if node.source == PoolNode && node.poolID == 0 {
		_, file, line, _ := runtime.Caller(1)
		caller := fmt.Sprintf("%s:%d", filepath.Base(file), line)
		stack := debug.Stack()
		panic(fmt.Sprintf("attempt to use node (key: %s, nk: %s) after it was returned to pool or not properly initialized\n\ncaller: %s\nstack trace:\n%s", node.key, node.nodeKey, caller, string(stack)))
	}
}

func (node *Node) IsLeaf() bool {
	node.CheckValid()
	return node.subtreeHeight == 0
}

func (node *Node) SetLeft(leftNode *Node) {
	node.CheckValid()
	node.leftNode = leftNode
	if leftNode != nil {
		node.leftNodeKey = leftNode.nodeKey
	}
}

func (node *Node) SetLeftNodeKey(key NodeKey) {
	node.CheckValid()
	node.leftNodeKey = key
}

func (node *Node) SetRight(rightNode *Node) {
	node.CheckValid()
	node.rightNode = rightNode
	if rightNode != nil {
		node.rightNodeKey = rightNode.nodeKey
	}
}

func (node *Node) SetRightNodeKey(key NodeKey) {
	node.CheckValid()
	node.rightNodeKey = key
}

func (node *Node) EvictChildren() {
	if node.leftNode != nil {
		node.leftNode.evict = true
		node.leftNode = nil
	}
	if node.rightNode != nil {
		node.rightNode.evict = true
		node.rightNode = nil
	}
}

func (node *Node) SetValue(value []byte) {
	node.CheckValid()
	node.value = value
}

func (node *Node) SetSubTreeHeight(height int8) {
	node.CheckValid()
	node.subtreeHeight = height
}

func (node *Node) SetSize(size int64) {
	node.CheckValid()
	node.size = size
}

func (node *Node) SetNodeKey(nk NodeKey) {
	node.CheckValid()
	node.nodeKey = nk
}

func (node *Node) SetHash(hash []byte) {
	node.CheckValid()
	node.hash = hash
}

func (node *Node) SetKey(key []byte) {
	node.CheckValid()
	node.key = key
}

func (node *Node) SetDirty(dirty bool) {
	node.CheckValid()
	node.dirty = dirty
}

func (node *Node) SetPoolID(id uint64) {
	node.poolID = id
}

func (node *Node) SetSource(src Source) {
	node.source = src
}

func (node *Node) Reset() {
	node.leftNodeKey = emptyNodeKey
	node.rightNodeKey = emptyNodeKey
	node.rightNode = nil
	node.leftNode = nil
	node.nodeKey = emptyNodeKey
	node.hash = nil
	node.key = nil
	node.value = nil
	node.subtreeHeight = -1
	node.size = 0
	node.dirty = false
	node.evict = false
	node.source = PoolNode

	node.poolID = 0
}
