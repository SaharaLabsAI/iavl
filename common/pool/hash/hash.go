package pool

import (
	"crypto/sha256"
	"sync"

	"lukechampine.com/blake3"
)

var Sha256Pool = &sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

var blake3Personal = blake3.Sum256([]byte("sahara-iavl"))

var Blake3Pool = &sync.Pool{
	New: func() any {
		return blake3.New(32, blake3Personal[:])
	},
}
