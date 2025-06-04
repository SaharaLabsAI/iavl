package iavl

type Tree interface {
	Get(key []byte) ([]byte, error)
}
