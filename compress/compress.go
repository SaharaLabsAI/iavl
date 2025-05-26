package compress

import "io"

type Encoder interface {
	Reset(io.Writer)
	Write([]byte) (int, error)
	Close() error
}

type Decoder interface {
	Reset(io.Reader) error
	io.Reader
}
