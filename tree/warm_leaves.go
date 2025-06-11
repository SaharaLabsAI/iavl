package tree

import (
	"fmt"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/common/metrics"
	"github.com/cosmos/iavl/v2/db"
	"github.com/cosmos/iavl/v2/db/sqlite"
	inode "github.com/cosmos/iavl/v2/node"
)

func (tree *Tree) WarmLeaves() error {
	if tree.db.Type() != db.SQLITE {
		return fmt.Errorf("WarmLeaves only support SQLITE")
	}

	sql := tree.db.(*sqlite.SqliteDb)

	return sql.WarmLeaves()
}

var _ Iterator = (*KVIterator)(nil)

type KVIterator struct {
	sql       *sqlite.SqliteDb
	itrStmt   *gosqlite.Stmt
	start     []byte
	end       []byte
	valid     bool
	err       error
	key       []byte
	value     []byte
	metrics   metrics.Proxy
	itrIdx    int
	ascending bool
	inclusive bool
}

func (i *KVIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *KVIterator) Valid() bool {
	return i.valid
}

func (i *KVIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "kv iterator", "next")
	}
	if !i.valid {
		return
	}

	hasRow, err := i.itrStmt.Step()
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
	if !hasRow {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	var nodeBz gosqlite.RawBytes
	if err = i.itrStmt.Scan(&i.key, &nodeBz); err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	i.value, err = inode.DecodeValueOnly(nodeBz)
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
}

func (i *KVIterator) Key() (key []byte) {
	return i.key
}

func (i *KVIterator) Value() (value []byte) {
	return i.value
}

func (i *KVIterator) Error() error {
	return i.err
}

func (i *KVIterator) Close() error {
	if i.valid {
		if i.metrics != nil {
			i.metrics.IncrCounter(1, "iavl2", "iterator", "close")
		}
		i.valid = false
		return i.sql.ReadPool().CloseKVIterstor(i.itrIdx)
	}
	return nil
}

func (tree *Tree) IteratorVersionDescLeaves(version int64, limit int) (Iterator, error) {
	if tree.db.Type() != db.SQLITE {
		return nil, fmt.Errorf("IteratorVersionDescLeaves only support SQLITE")
	}
	db := tree.db.Readonly()
	sql := db.(*sqlite.SqliteDb)

	var err error
	kvItr := &KVIterator{
		sql:     sql,
		valid:   true,
		metrics: tree.metricsProxy,
	}

	kvItr.itrStmt, kvItr.itrIdx, err = sql.ReadPool().GetVersionDescLeafIterator(version, limit)
	if err != nil {
		return nil, err
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	kvItr.Next()

	return kvItr, err

}
