package tree

import (
	"context"
	"fmt"

	"github.com/cosmos/iavl/v2/common/constants"
	"github.com/cosmos/iavl/v2/db"
	"github.com/cosmos/iavl/v2/db/sqlite"
)

func (tree *Tree) LoadSnapshot(version int64, traverseOrder constants.TraverseOrderType) (err error) {
	if tree.db.Type() != db.SQLITE {
		return fmt.Errorf("LoadSnapshot only support SQLITE")
	}
	sql := tree.db.(*sqlite.SqliteDb)

	tree.rw.Lock()
	defer tree.rw.Unlock()

	var v int64
	tree.root, v, err = sql.ImportMostRecentSnapshot(version, traverseOrder, true)
	if err != nil {
		return err
	}
	if v < version {
		return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
	}

	tree.version.Store(v)
	tree.hashedVersion = v
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)

	return nil
}

func (tree *Tree) SaveSnapshot() (err error) {
	if tree.db.Type() != db.SQLITE {
		return fmt.Errorf("LoadSnapshot only support SQLITE")
	}
	sql := tree.db.(*sqlite.SqliteDb)

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	ctx := context.Background()

	return sql.Snapshot(ctx, tree)
}
