package tree

import (
	"github.com/cosmos/iavl/v2/common/constants"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
)

func (tree *Tree) GetImmutable(version int64) (*Tree, error) {
	// We can discard whole pool after usage
	pool := nodepool.NewNodePool()
	db := tree.db.Readonly()

	imTree := &Tree{
		db:             db,
		nodePool:       pool,
		metrics:        tree.metrics,
		maxWorkingSize: tree.maxWorkingSize,
		heightFilter:   tree.heightFilter,
		metricsProxy:   tree.metricsProxy,
		leafSequence:   constants.LeafSequenceStart,
		hashedVersion:  version,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	imTree.immutable = true

	return imTree, nil
}

func (tree *Tree) GetImmutableProvable(version int64) (*Tree, error) {
	// We can discard whole pool after usage
	pool := nodepool.NewNodePool()
	db := tree.db.Readonly()

	imTree := &Tree{
		db:             db,
		nodePool:       pool,
		metrics:        tree.metrics,
		maxWorkingSize: tree.maxWorkingSize,
		heightFilter:   tree.heightFilter,
		metricsProxy:   tree.metricsProxy,
		leafSequence:   constants.LeafSequenceStart,
		hashedVersion:  version,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	imTree.immutable = true

	return imTree, nil
}

func (tree *Tree) IsImmutable() bool {
	return tree.immutable
}

// FIXME: implement Immutable Tree struct, which use readonly DB
func (tree *Tree) DiscardImmutableTree() error {
	// tree.sql.leafWrite = nil
	// tree.sql.treeWrite = nil
	// tree.sql.read = nil
	// tree.sql.readPool = nil
	// tree.sql.pool = nil
	return tree.Close()
}
