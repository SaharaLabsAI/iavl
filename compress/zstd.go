package compress

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	compressLevel   = zstd.WithEncoderLevel(zstd.SpeedFastest)
	memoryOptimized = zstd.WithLowerEncoderMem(true)
	windowSize      = zstd.WithWindowSize(64 * 1024)

	ZstdEncoderPool = &sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(nil, compressLevel, memoryOptimized, windowSize)
			return w
		},
	}

	ZstdDecoderPool = &sync.Pool{
		New: func() any {
			r, _ := zstd.NewReader(nil)
			return r
		},
	}
)
