package compress

import "io"

type CompressType int

const (
	S2 = iota
	ZSTD
)

type Encoder interface {
	Type() CompressType
	Reset(io.Writer)
	Write([]byte) (int, error)
	Close() error
}

type Decoder interface {
	Type() CompressType
	Reset(io.Reader) error
	io.Reader
}
