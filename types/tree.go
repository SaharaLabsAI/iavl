package types

import "github.com/cosmos/iavl/v2/db"

type UpdatedTree interface {
	Root() *Node
	HeightFilter() int
	ReturnNode(*Node)
	DB() db.DB
	Version() int64
	Updates() *NodeUpdates
}
