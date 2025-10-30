package compress

import (
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

var (
	_ Encoder = &S2Encoder{}
	_ Decoder = &S2Decoder{}
)

var (
	S2EncoderPool = &sync.Pool{
		New: func() any {
			w := s2.NewWriter(nil, s2.WriterBetterCompression(), s2.WriterBlockSize(256*1024))
			return &S2Encoder{
				Writer: w,
			}
		},
	}

	S2DecoderPool = &sync.Pool{
		New: func() any {
			r := s2.NewReader(nil)
			return &S2Decoder{
				Reader: r,
			}
		},
	}
)

type S2Encoder struct {
	*s2.Writer
}

func (s2 *S2Encoder) Type() Type {
	return S2
}

type S2Decoder struct {
	*s2.Reader
}

func (s2 *S2Decoder) Reset(r io.Reader) error {
	s2.Reader.Reset(r)
	return nil
}

func (s2 *S2Decoder) Type() Type {
	return S2
}
