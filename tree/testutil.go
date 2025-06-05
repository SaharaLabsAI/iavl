package tree

// NOTE: This func is primary for unit test(no db, pure memory tree)
func (tree *Tree) AdvanceVersion() {
	tree.version.Add(1)
}
