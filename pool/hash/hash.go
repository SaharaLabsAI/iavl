package pool

import (
	"crypto/sha256"
	"sync"
)

var Sha256Pool = &sync.Pool{
	New: func() any {
		return sha256.New()
	},
}
