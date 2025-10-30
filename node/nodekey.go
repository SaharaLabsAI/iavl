package node

import (
	"encoding/binary"
	"fmt"
)

var emptyNodeKey = NodeKey{}

// NodeKey represents a key of node in the DB.
//
//nolint:revive
type NodeKey [12]byte

func NewNodeKey(version int64, sequence uint32) NodeKey {
	var nk NodeKey
	//nolint:gosec
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], sequence)
	return nk
}

func (nk NodeKey) Version() int64 {
	//nolint:gosec
	return int64(binary.BigEndian.Uint64(nk[:]))
}

func (nk NodeKey) Sequence() uint32 {
	return binary.BigEndian.Uint32(nk[8:])
}

// String returns a string representation of the node key.
func (nk NodeKey) String() string {
	return fmt.Sprintf("(%d, %d)", nk.Version(), nk.Sequence())
}

func (nk NodeKey) IsEmpty() bool {
	return nk == emptyNodeKey
}
