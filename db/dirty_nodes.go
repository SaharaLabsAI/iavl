package db

import "github.com/cosmos/iavl/v2/types/node"

type DeletedNode struct {
	// the sequence in which this deletion was processed
	DeleteKey node.NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	LeafKey []byte
}

type DirtyNodes struct {
	Version       int64
	Leaves        []*node.Node
	LeafOrphans   []node.NodeKey
	Branches      []*node.Node
	BranchOrphans []node.NodeKey
	Deletes       []*DeletedNode
}

func (n *DirtyNodes) AddBranch(node *node.Node) {
	n.Branches = append(n.Branches, node)
}

func (n *DirtyNodes) AddLeaf(node *node.Node) {
	n.Leaves = append(n.Leaves, node)
}

func (n *DirtyNodes) AddOrphan(node *node.Node) {
	if !node.IsLeaf() {
		n.BranchOrphans = append(n.BranchOrphans, node.NodeKey())
	} else if node.IsLeaf() && !node.Dirty() {
		n.LeafOrphans = append(n.LeafOrphans, node.NodeKey())
	}
}

func (n *DirtyNodes) AddDelete(nodeKey node.NodeKey, leafKey []byte) {
	del := &DeletedNode{
		DeleteKey: nodeKey,
		LeafKey:   leafKey,
	}

	n.Deletes = append(n.Deletes, del)
}

func (n *DirtyNodes) Reset() {
	n.Version = -1
	n.Leaves = nil
	n.LeafOrphans = nil
	n.Branches = nil
	n.BranchOrphans = nil
	n.Deletes = nil
}
