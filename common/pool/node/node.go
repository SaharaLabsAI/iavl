package pool

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	inode "github.com/cosmos/iavl/v2/node"
)

var GlobalPoolId atomic.Uint64

type NodePool struct {
	syncPool *sync.Pool

	poolID atomic.Uint64
}

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() any {
				return &inode.Node{}
			},
		},
	}

	if GlobalPoolId.Load() == math.MaxUint64 {
		np.poolID.Store(1)
		GlobalPoolId.Store(1)
	} else {
		GlobalPoolId.Add(1)
		np.poolID.Store(GlobalPoolId.Load())
	}

	return np
}

func (np *NodePool) PoolID() uint64 {
	return np.poolID.Load()
}

func (np *NodePool) Get() *inode.Node {
	n := np.syncPool.Get().(*inode.Node)
	n.SetPoolID(np.poolID.Load())
	n.SetSource(inode.PoolNode)

	return n
}

func (np *NodePool) Put(node *inode.Node) {
	if node.PoolID() == 0 {
		panic(fmt.Sprintf("NodePool.Put: detected attempt to Put node with poolId 0 (key: %s). Possible double Put or invalid node.", node.Key()))
	}
	if node.PoolID() != np.poolID.Load() {
		panic(fmt.Sprintf("NodePool.Put: attempt to Put node to wrong pool, node %d pool %d", node.PoolID(), np.poolID.Load()))
	}

	node.Reset()
	np.syncPool.Put(node)
}
