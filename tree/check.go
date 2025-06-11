package tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/common/metrics"
	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	"github.com/cosmos/iavl/v2/db"
	"github.com/cosmos/iavl/v2/db/sqlite"
	inode "github.com/cosmos/iavl/v2/node"
)

type WrongVersionKey struct {
	version int64
	key     []byte
}

func (tree *Tree) DetectWrongBranchHashes(start, end int64) ([]*WrongVersionKey, error) {
	iter, err := tree.WrongBranchHashIterator(start, end)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keys := make([]*WrongVersionKey, 0)
	for ; iter.Valid(); iter.Next() {
		version := binary.BigEndian.Uint64(iter.Value())

		keys = append(keys, &WrongVersionKey{
			key:     iter.Key(),
			version: int64(version),
		})
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].version < keys[j].version
	})

	return keys, iter.Error()
}

var _ Iterator = (*WrongBranchHashIterator)(nil)

func (tree *Tree) WrongBranchHashIterator(start, end int64) (Iterator, error) {
	if tree.db.Type() != db.SQLITE {
		return nil, fmt.Errorf("IteratorLatestLeaves only support SQLITE")
	}

	pool := nodepool.NewNodePool()
	db := tree.db.Readonly()
	sql := db.(*sqlite.DB)

	itr := &WrongBranchHashIterator{
		sql:      sql,
		nodePool: pool,
		start:    start,
		end:      end,
		valid:    true,
		metrics:  tree.metrics,
	}

	var err error
	itr.itrStmt, err = sql.GetHeightOneBranchesIteratorQuery(start, end)
	if err != nil {
		return nil, err
	}

	if tree.metrics != nil {
		tree.metrics.IncrCounter(1, "iavl2", "iterator", "open")
	}

	itr.Next()

	return itr, err
}

type WrongBranchHashIterator struct {
	sql      *sqlite.DB
	nodePool *nodepool.NodePool
	itrStmt  *gosqlite.Stmt
	valid    bool
	start    int64
	end      int64
	err      error
	key      []byte
	value    []byte
	metrics  metrics.Proxy
}

func (i *WrongBranchHashIterator) Domain() (strat []byte, end []byte) {
	s := make([]byte, 8)
	binary.BigEndian.PutUint64(s, uint64(i.start))

	e := make([]byte, 8)
	binary.BigEndian.PutUint64(e, uint64(i.end))

	return s, e
}

func (i *WrongBranchHashIterator) Valid() bool {
	return i.valid
}

func (i *WrongBranchHashIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "kv iterator", "next")
	}
	if !i.valid {
		return
	}

	for {
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

		var (
			version  int64
			sequence int
			nodeBz   gosqlite.RawBytes
		)

		if err = i.itrStmt.Scan(&version, &sequence, &nodeBz); err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		nodeKey := inode.NewNodeKey(version, uint32(sequence))
		node, err := inode.Decode(i.nodePool, nodeKey, nodeBz)
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		if node.SubTreeHeight() != 1 {
			continue
		}

		leftNode, err := i.sql.GetNode(i.nodePool, node.LeftNodeKey())
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}
		node.SetLeft(leftNode)

		rightNode, err := i.sql.GetNode(i.nodePool, node.RightNodeKey())
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}
		node.SetRight(rightNode)

		oldHash := slices.Clone(node.Hash())
		node.SetHash(nil)
		node.HashSelf()

		if !bytes.Equal(node.Hash(), oldHash) {
			i.key = node.LeftNode().Key()

			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(node.Version()))
			i.value = b

			break
		}
	}
}

func (i *WrongBranchHashIterator) Key() (key []byte) {
	return i.key
}

func (i *WrongBranchHashIterator) Value() (key []byte) {
	return i.value
}

func (i *WrongBranchHashIterator) Error() error {
	return i.err
}

func (i *WrongBranchHashIterator) Close() error {
	if i.valid {
		if i.metrics != nil {
			i.metrics.IncrCounter(1, "iavl2", "iterator", "close")
		}
		i.valid = false

		return i.itrStmt.Close()
	}
	return nil
}
