package pool

import "github.com/cosmos/iavl/v2/common/compress"

type Compress struct{}

func (c Compress) GetEncoder(ty compress.CompressType) compress.Encoder {
	switch ty {
	case compress.S2:
		return compress.S2EncoderPool.Get().(compress.Encoder)
	case compress.ZSTD:
		return compress.ZstdEncoderPool.Get().(compress.Encoder)
	default:
		return compress.S2EncoderPool.Get().(compress.Encoder)
	}
}

func (c Compress) GetDecoder(ty compress.CompressType) compress.Decoder {
	switch ty {
	case compress.S2:
		return compress.S2DecoderPool.Get().(compress.Decoder)
	case compress.ZSTD:
		return compress.ZstdDecoderPool.Get().(compress.Decoder)
	default:
		return compress.S2DecoderPool.Get().(compress.Decoder)
	}
}

func (c Compress) PutEncoder(e compress.Encoder) {
	switch e.Type() {
	case compress.S2:
		compress.S2EncoderPool.Put(e)
	case compress.ZSTD:
		compress.ZstdEncoderPool.Put(e)
	default:
		compress.S2EncoderPool.Put(e)
	}
}

func (c Compress) PutDecoder(d compress.Decoder) {
	switch d.Type() {
	case compress.S2:
		compress.S2DecoderPool.Put(d)
	case compress.ZSTD:
		compress.ZstdDecoderPool.Put(d)
	default:
		compress.S2DecoderPool.Put(d)
	}
}
