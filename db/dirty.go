package db

import inode "github.com/cosmos/iavl/v2/node"

type DeletedNode struct {
	// the sequence in which this deletion was processed
	DeleteKey inode.NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	LeafKey []byte
}

type DirtyNodes struct {
	Version       int64
	Leaves        []*inode.Node
	LeafOrphans   []inode.NodeKey
	Branches      []*inode.Node
	BranchOrphans []inode.NodeKey
	Deletes       []*DeletedNode
}

func (n *DirtyNodes) AddBranch(node *inode.Node) {
	n.Branches = append(n.Branches, node)
}

func (n *DirtyNodes) AddLeaf(node *inode.Node) {
	n.Leaves = append(n.Leaves, node)
}

func (n *DirtyNodes) AddOrphan(node *inode.Node) {
	if !node.IsLeaf() {
		n.BranchOrphans = append(n.BranchOrphans, node.NodeKey())
	} else if node.IsLeaf() && !node.Dirty() {
		n.LeafOrphans = append(n.LeafOrphans, node.NodeKey())
	}
}

func (n *DirtyNodes) AddDelete(nodeKey inode.NodeKey, leafKey []byte) {
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
