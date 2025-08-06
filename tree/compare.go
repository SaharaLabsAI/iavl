package tree

import (
	"bytes"

	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

func (tree *Tree) Compare(other *Tree) error {
	_, err := doCompare(tree, tree.root, other, other.root)
	return err
}

func doCompare(tree1 *Tree, node1 *inode.Node, tree2 *Tree, node2 *inode.Node) (bool, error) {
	if node1.IsLeaf() {
		if !bytes.Equal(node1.Hash(), node2.Hash()) {
			return false, nil
		}
		return true, nil
	}

	if bytes.Equal(node1.Hash(), node2.Hash()) {
		return true, nil
	}

	// fmt.Printf("height %d hash %x  2 height %d %x\n", node1.subtreeHeight, node1.hash, node2.subtreeHeight, node2.hash)

	left1, err := tree1.db.GetNode(tree1.nodePool, node1.LeftNodeKey())
	if err != nil {
		return false, err
	}
	left2, err := tree2.db.GetNode(tree2.nodePool, node2.LeftNodeKey())
	if err != nil {
		return false, err
	}
	equal, err := doCompare(tree1, left1, tree2, left2)
	if err != nil {
		return false, err
	}
	if !equal && !left1.IsLeaf() {
		return doCompare(tree1, left1, tree2, left2)
	}

	right1, err := tree1.db.GetNode(tree1.nodePool, node1.RightNodeKey())
	if err != nil {
		return false, err
	}
	right2, err := tree2.db.GetNode(tree2.nodePool, node2.RightNodeKey())
	if err != nil {
		return false, err
	}
	equal, err = doCompare(tree1, right1, tree2, right2)
	if err != nil {
		return false, err
	}
	if !equal && !right1.IsLeaf() {
		return doCompare(tree1, right1, tree2, right2)
	}

	return true, nil
}
