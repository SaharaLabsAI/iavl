package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"github.com/cosmos/iavl/v2/pool"
	hashpool "github.com/cosmos/iavl/v2/pool/hash"
)

var (
	EmptyHash = sha256.New().Sum(nil)
)

func (node *Node) HashWith(h hash.Hash, buf *bytes.Buffer) []byte {
	node.CheckValid()
	if node.hash != nil {
		return node.hash
	}

	node.writeHashBytesToBuffer(buf)
	h.Write(buf.Bytes())
	node.hash = h.Sum(nil)

	return node.hash
}

func (node *Node) writeHashBytesToBuffer(buf *bytes.Buffer) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], int64(node.subtreeHeight))
	buf.Write(tmp[:n])

	n = binary.PutVarint(tmp[:], node.size)
	buf.Write(tmp[:n])

	n = binary.PutVarint(tmp[:], node.nodeKey.Version())
	buf.Write(tmp[:n])

	if node.IsLeaf() {
		n = binary.PutUvarint(tmp[:], uint64(len(node.key)))
		buf.Write(tmp[:n])
		buf.Write(node.key)

		h := hashpool.Sha256Pool.Get().(hash.Hash)
		defer hashpool.Sha256Pool.Put(h)

		h.Reset()
		h.Write(node.value)
		valueHash := h.Sum(nil)

		n = binary.PutUvarint(tmp[:], uint64(len(valueHash)))
		buf.Write(tmp[:n])
		buf.Write(valueHash[:])
	} else {
		// Safely handle the left node hash
		var leftHash []byte
		if node.leftNode == nil {
			panic("left child node cannot be nil during hash calculation")
		} else {
			leftHash = node.leftNode.hash
			if leftHash == nil {
				// Compute hash if needed - this is safer than panicking
				leftHash = node.leftNode._hash()
			}
		}

		n = binary.PutUvarint(tmp[:], uint64(len(leftHash)))
		buf.Write(tmp[:n])
		buf.Write(leftHash)

		// Safely handle the right node hash
		var rightHash []byte
		if node.rightNode == nil {
			panic("right child node cannot be nil during hash calculation")
		} else {
			rightHash = node.rightNode.hash
			if rightHash == nil {
				// Compute hash if needed - this is safer than panicking
				rightHash = node.rightNode._hash()
			}
		}

		n = binary.PutUvarint(tmp[:], uint64(len(rightHash)))
		buf.Write(tmp[:n])
		buf.Write(rightHash)
	}
}

// writeHashBytes is kept for backward compatibility
func (node *Node) writeHashBytes(w io.Writer) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.subtreeHeight))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], node.nodeKey.Version())
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	if node.IsLeaf() {
		if err := encodeBytes(w, node.key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		if err := encodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if err := encodeBytes(w, node.leftNode.hash); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := encodeBytes(w, node.rightNode.hash); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash() []byte {
	node.CheckValid()
	if node.hash != nil {
		return node.hash
	}

	h := hashpool.Sha256Pool.Get().(hash.Hash)
	h.Reset() // Ensure the hash is clean

	buf := pool.BufPool.Get().(*bytes.Buffer)
	buf.Reset()

	node.writeHashBytesToBuffer(buf)
	h.Write(buf.Bytes())

	node.hash = h.Sum(nil)

	pool.BufPool.Put(buf)
	hashpool.Sha256Pool.Put(h)

	return node.hash
}

func encodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}
