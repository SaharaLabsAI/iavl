package compress

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestZstdCompress(t *testing.T) {
	source := []byte(`
		This is the source file. Compression of the target file with
		the source file as the dictionary will produce a compressed
		delta encoding of the target file.`)

	e := ZstdEncoderPool.Get().(*zstd.Encoder)
	defer ZstdEncoderPool.Put(e)

	delta := e.EncodeAll(source, nil)

	d := ZstdDecoderPool.Get().(*zstd.Decoder)
	defer ZstdDecoderPool.Put(d)

	out, err := d.DecodeAll(delta, nil)
	require.NoError(t, err)

	require.Equal(t, source, out)
}
