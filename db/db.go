package db

import (
	"sync/atomic"

	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	inode "github.com/cosmos/iavl/v2/node"
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
	GetLeaf(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
}

type HashConnPool interface {
	GetHashConn() (HashConn, error)
}

type DBRead interface {
	Path() string
	ResetRead() error
	GetLeftNode(node *inode.Node) (*inode.Node, error)
	GetRightNode(node *inode.Node) (*inode.Node, error)
	GetVersioned(key []byte, version int64) ([]byte, error)
	LatestVersion() (int64, error)
	HasRoot(version int64) (bool, error)
	LoadRoot(version int64) (*inode.Node, error)
	SetInitTreeVersion(version *atomic.Int64)
}

type DBWrite interface {
	SaveRoot(version int64, node *inode.Node) error
	SaveTree(root *inode.Node, version int64, dirtyNodes *DirtyNodes) error
	Revert(version int64) error
	PausePruning(pause bool)
	DeleteVersionsTo(toVersion int64) error
	DeleteVersionsToSync(toVersion int64) error
}
