package compress

import (
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

var (
	_ Encoder = &s2.Writer{}
	_ Decoder = &DecoderWrapper{}
)

var (
	S2EncoderPool = &sync.Pool{
		New: func() any {
			w := s2.NewWriter(nil)
			return w
		},
	}

	S2DecoderPool = &sync.Pool{
		New: func() any {
			r := s2.NewReader(nil)
			return &DecoderWrapper{
				Reader: r,
			}
		},
	}
)

type DecoderWrapper struct {
	*s2.Reader
}

func (s2 *DecoderWrapper) Reset(r io.Reader) error {
	s2.Reader.Reset(r)
	return nil
}
