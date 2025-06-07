package tree

import (
	"fmt"

	"github.com/cosmos/iavl/v2/db"
	"github.com/cosmos/iavl/v2/db/sqlite"
)

func (tree *Tree) WarmLeaves() error {
	if tree.db.Type() != db.SQLITE {
		return fmt.Errorf("WarmLeaves only support SQLITE")
	}

	sql := tree.db.(*sqlite.SqliteDb)

	return sql.WarmLeaves()
}
