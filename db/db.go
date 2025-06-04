package db

import (
	"sync/atomic"

	nodepool "github.com/cosmos/iavl/v2/pool/node"
	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

type DBType int

const (
	SQLITE = iota
)

type DB interface {
	Type() DBType
	// Readonly() ReadonlyDB
	DBRead
	DBWrite
	Close() error
}

// FIXME: later
type ReadonlyDB interface {
	Type() DBType
	// Readonly(node *nodepool.NodePool) ReadonlyDB
	DBRead
	DBWrite
	Close() error
}

type HashConn interface {
	GetLeaf(pool *nodepool.NodePool, nodekey nodetypes.NodeKey) (*nodetypes.Node, error)
	GetNode(pool *nodepool.NodePool, nodekey nodetypes.NodeKey) (*nodetypes.Node, error)
}

type HashConnPool interface {
	GetHashConn() (HashConn, error)
}

type DBRead interface {
	Path() string
	ResetRead() error
	GetLeftNode(node *nodetypes.Node) (*nodetypes.Node, error)
	GetRightNode(node *nodetypes.Node) (*nodetypes.Node, error)
	GetVersioned(key []byte, version int64) ([]byte, error)
	LatestVersion() (int64, error)
	HasRoot(version int64) (bool, error)
	LoadRoot(version int64) (*nodetypes.Node, error)
	SetInitTreeVersion(version *atomic.Int64)
}

type DBWrite interface {
	SaveRoot(version int64, node *nodetypes.Node) error
	SaveTree(root *nodetypes.Node, version int64, dirtyNodes *DirtyNodes) error
	Revert(version int64) error
	PausePruning(pause bool)
	DeleteVersionsTo(toVersion int64) error
	DeleteVersionsToSync(toVersion int64) error
}
