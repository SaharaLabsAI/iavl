package db

import (
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
	GetConn() (ReadConn, error)
	GetValue(key []byte, version int64) ([]byte, error)
	GetNode(pool *nodepool.NodePool, nodekey inode.NodeKey) (*inode.Node, error)
	HasRoot(version int64) (bool, error)
	LoadRoot(pool *nodepool.NodePool, version int64) (*inode.Node, error)
	LatestVersion() (int64, error)
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
