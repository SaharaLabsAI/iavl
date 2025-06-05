package tree

func (tree *Tree) PausePruning(pause bool) {
	tree.db.PausePruning(pause)
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	return tree.db.DeleteVersionsTo(toVersion)
}

func (tree *Tree) DeleteVersionsToSync(toVersion int64) error {
	return tree.db.DeleteVersionsToSync(toVersion)
}
