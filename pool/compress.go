package pool

import "github.com/cosmos/iavl/v2/compress"

type CompressType int

const (
	S2 = iota
	ZSTD
)

const UseCompress = S2

type Compress struct{}

func (c Compress) GetEncoder() compress.Encoder {
	switch UseCompress {
	case S2:
		return compress.S2EncoderPool.Get().(compress.Encoder)
	case ZSTD:
		return compress.ZstdEncoderPool.Get().(compress.Encoder)
	default:
		return compress.S2EncoderPool.Get().(compress.Encoder)
	}
}

func (c Compress) GetDecoder() compress.Decoder {
	switch UseCompress {
	case S2:
		return compress.S2DecoderPool.Get().(compress.Decoder)
	case ZSTD:
		return compress.ZstdDecoderPool.Get().(compress.Decoder)
	default:
		return compress.S2DecoderPool.Get().(compress.Decoder)
	}
}

func (c Compress) PutEncoder(e compress.Encoder) {
	switch UseCompress {
	case S2:
		compress.S2EncoderPool.Put(e)
	case ZSTD:
		compress.ZstdEncoderPool.Put(e)
	default:
		compress.S2EncoderPool.Put(e)
	}
}

func (c Compress) PutDecoder(d compress.Decoder) {
	switch UseCompress {
	case S2:
		compress.S2DecoderPool.Put(d)
	case ZSTD:
		compress.ZstdDecoderPool.Put(d)
	default:
		compress.S2DecoderPool.Put(d)
	}
}
