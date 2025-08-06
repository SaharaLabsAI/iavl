package node

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/SaharaLabsAI/iavl/v2/common/encoding"
)

type NodePool interface {
	Get() *Node
}

func (node *Node) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := node.writeBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (node *Node) EncodeWithBuffer(buf *bytes.Buffer) error {
	buf.Reset()
	return node.writeBytes(buf)
}

// Decode constructs a *Node from an encoded byte slice.
func Decode(pool NodePool, nodeKey NodeKey, buf []byte) (*Node, error) {
	// Read node header (height, size, version, key).
	height, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.height, %w", err)
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.size, %w", err)
	}
	buf = buf[n:]

	key, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.key, %w", err)
	}
	buf = buf[n:]

	hash, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.hash, %w", err)
	}
	buf = buf[n:]

	node := pool.Get()
	node.subtreeHeight = int8(height)
	node.nodeKey = nodeKey
	node.size = size
	node.key = key
	node.hash = hash

	if node.IsLeaf() {
		val, _, cause := encoding.DecodeBytes(buf)
		if cause != nil {
			return nil, fmt.Errorf("decoding node.value, %w", cause)
		}
		node.value = val
	} else {
		leftNk, n, err := decodeNodeKey(buf)
		// leftNodeKey, n, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.leftKey, %w", err)
		}
		buf = buf[n:]

		rightNk, _, err := decodeNodeKey(buf)
		// rightNodeKey, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.rightKey, %w", err)
		}

		node.leftNodeKey = *leftNk
		node.rightNodeKey = *rightNk
	}

	return node, nil
}

func DecodeValueOnly(buf []byte) ([]byte, error) {
	// Read node header (height, size, version, key).
	height, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding leaf.height, %w", err)
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	_, n, err = encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding leaf.size, %w", err)
	}
	buf = buf[n:]

	// Decoding leaf.key
	s, n, err := encoding.DecodeUvarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding leaf.key, %w", err)
	}

	// Make sure size doesn't overflow. ^uint(0) >> 1 will help determine the
	// max int value variably on 32-bit and 64-bit machines. We also doublecheck
	// that size is positive.
	size := int(s)
	if s >= uint64(^uint(0)>>1) || size < 0 {
		return nil, fmt.Errorf("decoding leaf.key, invalid out of range length %v decoding []byte", s)
	}
	// Make sure end index doesn't overflow. We know n>0 from decodeUvarint().
	end := n + size
	if end < n {
		return nil, fmt.Errorf("decoding leaf.key, invalid out of range length %v decoding []byte", size)
	}
	// Make sure the end index is within bounds.
	if len(buf) < end {
		return nil, fmt.Errorf("decoding leaf.key, insufficient bytes decoding []byte of length %v", size)
	}
	buf = buf[end:]

	// Decoding leaf.hash
	s, n, err = encoding.DecodeUvarint(buf)
	if err != nil {
		return nil, err
	}
	// Make sure size doesn't overflow. ^uint(0) >> 1 will help determine the
	// max int value variably on 32-bit and 64-bit machines. We also doublecheck
	// that size is positive.
	size = int(s)
	if s >= uint64(^uint(0)>>1) || size < 0 {
		return nil, fmt.Errorf("decoding leaf.hash, invalid out of range length %v decoding []byte", s)
	}
	// Make sure end index doesn't overflow. We know n>0 from decodeUvarint().
	end = n + size
	if end < n {
		return nil, fmt.Errorf("decoding leaf.hash, invalid out of range length %v decoding []byte", size)
	}
	// Make sure the end index is within bounds.
	if len(buf) < end {
		return nil, fmt.Errorf("decoding leaf.hash, insufficient bytes decoding []byte of length %v", size)
	}
	buf = buf[end:]

	val, _, cause := encoding.DecodeBytes(buf)
	if cause != nil {
		return nil, fmt.Errorf("decoding leaf.value, %w", cause)
	}

	return val, nil
}

func (node *Node) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot leafWrite nil node")
	}
	cause := encoding.EncodeVarint(w, int64(node.subtreeHeight))
	if cause != nil {
		return fmt.Errorf("writing height; %w", cause)
	}
	cause = encoding.EncodeVarint(w, node.size)
	if cause != nil {
		return fmt.Errorf("writing size; %w", cause)
	}

	cause = encoding.EncodeBytes(w, node.key)
	if cause != nil {
		return fmt.Errorf("writing key; %w", cause)
	}

	if len(node.hash) != hashSize {
		return fmt.Errorf("hash has unexpected length: %d", len(node.hash))
	}
	cause = encoding.EncodeBytes(w, node.hash)
	if cause != nil {
		return fmt.Errorf("writing hash; %w", cause)
	}

	if node.IsLeaf() {
		cause = encoding.EncodeBytes(w, node.value)
		if cause != nil {
			return fmt.Errorf("writing value; %w", cause)
		}
	} else {
		if node.leftNodeKey.IsEmpty() {
			return fmt.Errorf("left node key is nil")
		}
		cause = encoding.EncodeBytes(w, node.leftNodeKey[:])
		if cause != nil {
			return fmt.Errorf("writing left node key; %w", cause)
		}

		if node.rightNodeKey.IsEmpty() {
			return fmt.Errorf("right node key is nil")
		}
		cause = encoding.EncodeBytes(w, node.rightNodeKey[:])
		if cause != nil {
			return fmt.Errorf("writing right node key; %w", cause)
		}
	}
	return nil
}

func decodeNodeKey(bz []byte) (*NodeKey, int, error) {
	s, n, err := encoding.DecodeUvarint(bz)
	if err != nil {
		return nil, n, err
	}
	// Make sure size doesn't overflow. ^uint(0) >> 1 will help determine the
	// max int value variably on 32-bit and 64-bit machines. We also doublecheck
	// that size is positive.
	size := int(s)
	if s >= uint64(^uint(0)>>1) || size < 0 {
		return nil, n, fmt.Errorf("invalid out of range length %v decoding []byte", s)
	}
	if size != 12 {
		return nil, n, fmt.Errorf("unexpected node key size %d", size)
	}
	// Make sure end index doesn't overflow. We know n>0 from decodeUvarint().
	end := n + size
	if end < n {
		return nil, n, fmt.Errorf("invalid out of range length %v decoding []byte", size)
	}
	// Make sure the end index is within bounds.
	if len(bz) < end {
		return nil, n, fmt.Errorf("insufficient bytes decoding []byte of length %v", size)
	}

	version := binary.BigEndian.Uint64(bz[n : n+8])
	sequence := binary.BigEndian.Uint32(bz[n+8 : end])

	key := NewNodeKey(int64(version), sequence)

	return &key, end, nil
}
