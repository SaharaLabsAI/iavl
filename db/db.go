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
	Read
	Write
	Import
	Type() Type
	Close() error
	Readonly() ReadonlyDB
}

type ReadonlyDB interface {
	Read
	Type() Type
	Close() error
}

type ReadConn interface {
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	Release() error
}

type Read interface {
	Path() string
	ResetRead() error
	Get(key []byte, version int64) ([]byte, error)
	GetReadConn() (ReadConn, error)
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	HasRoot(version int64) (bool, error)
	LoadRoot(pool *nodepool.NodePool, version int64) (*inode.Node, error)
	LatestVersion() (int64, error)
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
