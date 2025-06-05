package compress

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS2Compress(t *testing.T) {
	source := []byte(`
		This is the source file. Compression of the target file with
		the source file as the dictionary will produce a compressed
		delta encoding of the target file.`)

	e := S2EncoderPool.Get().(Encoder)
	defer S2EncoderPool.Put(e)

	var (
		delta bytes.Buffer
		out   bytes.Buffer
	)

	e.Reset(&delta)
	_, err := e.Write(source)
	require.NoError(t, err)
	err = e.Close()
	require.NoError(t, err)

	d := S2DecoderPool.Get().(Decoder)
	defer S2DecoderPool.Put(d)

	d.Reset(&delta)
	_, err = io.Copy(&out, d)
	require.NoError(t, err)

	require.Equal(t, source, out.Bytes())
}
