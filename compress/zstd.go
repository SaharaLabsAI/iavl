package compress

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	_ Encoder = &ZstdEncoder{}
	_ Decoder = &ZstdDecoder{}
)

var (
	compressLevel   = zstd.WithEncoderLevel(zstd.SpeedBetterCompression)
	memoryOptimized = zstd.WithLowerEncoderMem(true)
	windowSize      = zstd.WithWindowSize(64 * 1024)

	ZstdEncoderPool = &sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(nil, compressLevel, memoryOptimized, windowSize)
			return &ZstdEncoder{
				Encoder: w,
			}
		},
	}

	ZstdDecoderPool = &sync.Pool{
		New: func() any {
			r, _ := zstd.NewReader(nil)
			return &ZstdDecoder{
				Decoder: r,
			}
		},
	}
)

type ZstdEncoder struct {
	*zstd.Encoder
}

func (z *ZstdEncoder) Type() CompressType {
	return ZSTD
}

type ZstdDecoder struct {
	*zstd.Decoder
}

func (z *ZstdDecoder) Type() CompressType {
	return ZSTD
}
