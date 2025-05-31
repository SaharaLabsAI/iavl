package types

import (
	"github.com/cosmos/iavl/v2/db"
	node "github.com/cosmos/iavl/v2/types/node"
)

type UpdatedTree interface {
	Root() *node.Node
	HeightFilter() int
	ReturnNode(*node.Node)
	DB() db.DB
	Version() int64
	Updates() *NodeUpdates
}
