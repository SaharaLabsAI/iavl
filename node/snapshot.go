package node

type SnapshotNode struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}
