package constants

const (
	MetricsNamespace  = "iavl2"
	LeafSequenceStart = uint32(1 << 31)
)

// TraverseOrderType is the type of the order in which the tree is traversed.
type TraverseOrderType uint8

const (
	PreOrder TraverseOrderType = iota
	PostOrder
)

func IsLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}
