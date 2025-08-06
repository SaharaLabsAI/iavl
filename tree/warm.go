package tree

import (
	"fmt"

	"github.com/SaharaLabsAI/iavl/v2/db"
	"github.com/SaharaLabsAI/iavl/v2/db/sqlite"
)

func (tree *Tree) WarmLeaves() error {
	if tree.db.Type() != db.SQLITE {
		return fmt.Errorf("WarmLeaves only support SQLITE")
	}

	sql := tree.db.(*sqlite.DB)

	return sql.WarmLeaves()
}

func (tree *Tree) IteratorLatestLeaves(version int64, limit int) (Iterator, error) {
	if tree.db.Type() != db.SQLITE {
		return nil, fmt.Errorf("IteratorLatestLeaves only support SQLITE")
	}

	db := tree.db.Readonly()
	sql := db.(*sqlite.DB)

	return sql.GetLatestLeavesIterator(version, limit)
}
