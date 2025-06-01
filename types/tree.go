package types

import (
	node "github.com/cosmos/iavl/v2/types/node"
)

type Tree interface {
	Root() *node.Node
	HeightFilter() int
	ReturnNode(*node.Node)
	DB()
	Version() int64
}

type UpdatedTree interface {
	Tree
	Updates() *NodeUpdates
}

type NodeDelete struct {
	// the sequence in which this deletion was processed
	DeleteKey node.NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	LeafKey []byte
}

type NodeUpdates struct {
	Version       int64
	Leaves        []*node.Node
	Branches      []*node.Node
	BranchOrphans []*node.NodeKey
	LeafOrphans   []*node.NodeKey
	Deletes       []*NodeDelete
}
