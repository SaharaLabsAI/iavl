package db

import (
	"sync/atomic"

	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	inode "github.com/cosmos/iavl/v2/node"
)

type Type int

const (
	SQLITE = iota
)

type DB interface {
	Type() Type
	Readonly() ReadonlyDB
	Import
	Read
	Write
	Close() error
}

// FIXME: later
type ReadonlyDB interface {
	Type() Type
	Readonly() ReadonlyDB
	Import
	Read
	Write
	Close() error
}

type HashConn interface {
	GetLeaf(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
}

type HashConnPool interface {
	GetHashConn() (HashConn, error)
	ReturnHashConns([]HashConn)
}

type Read interface {
	Path() string
	HashConnPool
	ResetRead() error
	Get(key []byte, version int64) ([]byte, error)
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	LatestVersion() (int64, error)
	HasRoot(version int64) (bool, error)
	LoadRoot(pool *nodepool.NodePool, version int64) (*inode.Node, error)
	SetInitTreeVersion(version *atomic.Int64)
}

type Write interface {
	SaveTree(version int64, root *inode.Node, dirtyNodes *DirtyNodes) error
	Revert(version int64) error
	PausePruning(pause bool)
	DeleteVersionsTo(toVersion int64) error
	DeleteVersionsToSync(toVersion int64) error
}

type Import interface {
	PrepareImport() error
	FinishImport() error
	WriteBatch(*DirtyNodes) error
}
