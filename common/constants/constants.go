package constants

const (
	MetricsNamespace  = "iavl2"
	LeafSequenceStart = uint32(1 << 31)
)

func IsLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}
