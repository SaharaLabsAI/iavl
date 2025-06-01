package db

import (
	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

type DBType int

const (
	SQLITE = iota
)

type DB interface {
	Type() DBType
}

type DBRead interface {
	GetLeftNode(node *nodetypes.Node) (*nodetypes.Node, error)
	GetRightNode(node *nodetypes.Node) (*nodetypes.Node, error)
}

type DBWrite interface {
	SaveRoot(version int64, node *nodetypes.Node) error
}
