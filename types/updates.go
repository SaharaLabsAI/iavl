package types

type NodeDelete struct {
	// the sequence in which this deletion was processed
	DeleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	LeafKey []byte
}

type NodeUpdates struct {
	Version       int64
	Leaves        []*Node
	Branches      []*Node
	BranchOrphans []*NodeKey
	LeafOrphans   []*NodeKey
	Deletes       []*NodeDelete
}
