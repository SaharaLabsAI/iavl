package pool

import (
	"bytes"
	"sync"
)

var BufPool = &sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}
