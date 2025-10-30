package compress

import "io"

type Type int

const (
	S2 = iota
	ZSTD
)

type Encoder interface {
	Type() Type
	Reset(io.Writer)
	Write([]byte) (int, error)
	Close() error
}

type Decoder interface {
	Type() Type
	Reset(io.Reader) error
	io.Reader
}
