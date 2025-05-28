package iavl

import (
	"fmt"
	"strconv"

	"github.com/eatonphil/gosqlite"
)

const defaultStartShardID = int64(1)
const defaultTreeShardSize = 500_000

func ToShardID(version int64) int64 {
	if version <= 0 {
		return defaultStartShardID
	}
	return (version-1)/defaultTreeShardSize + defaultStartShardID
}

type BranchShards struct {
	shardIDs map[int64]bool
}

func NewBranchShards() *BranchShards {
	s := &BranchShards{}

	return s
}

func (s *BranchShards) ReloadShardIDs(sql *SqliteDb) error {
	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return err
	}
	defer q.Close()

	s.shardIDs = make(map[int64]bool)
	for {
		hasRow, err := q.Step()
		if err != nil {
			return err
		}

		if !hasRow {
			break
		}

		var shard string
		err = q.Scan(&shard)
		if err != nil {
			return err
		}

		shardID, err := strconv.Atoi(shard[5:])
		if err != nil {
			return err
		}

		s.shardIDs[int64(shardID)] = true
	}

	return nil
}

func (s *BranchShards) HasShardID(shardID int64) bool {
	_, exists := s.shardIDs[shardID]
	return exists
}

func (s *BranchShards) AddShardID(shardID int64) {
	s.shardIDs[shardID] = true
}

type BranchShardInsert struct {
	stmts map[int64]*gosqlite.Stmt // shardID -> stmt
}

func PrepareBranchShardInsert(sql *SqliteDb, _ *BranchShards) (*BranchShardInsert, error) {
	stmts := make(map[int64]*gosqlite.Stmt)
	return &BranchShardInsert{stmts: stmts}, nil
}

func (ss *BranchShardInsert) EnsureShardTable(sql *SqliteDb, shardID int64) error {
	if sql.branchShards.HasShardID(shardID) {
		return nil
	}

	if err := sql.createShardTableIfNotExists(int(shardID)); err != nil {
		return err
	}

	st, err := sql.preapreBranchShardInsertStatement(shardID)
	if err != nil {
		return err
	}

	sql.branchShards.AddShardID(shardID)
	ss.stmts[shardID] = st

	return nil
}

func (ss *BranchShardInsert) Exec(version int64, sequence int, bz []byte) error {
	shardID := ToShardID(version)

	st, exists := ss.stmts[shardID]
	if !exists {
		return fmt.Errorf("unexpected shard %d insert statment not found", shardID)
	}
	defer st.Reset()

	return st.Exec(version, sequence, bz)
}

func (ss *BranchShardInsert) Reset() error {
	for shardID, st := range ss.stmts {
		if err := st.Reset(); err != nil {
			return fmt.Errorf("failed to reset shard %d insert stmt: %w", shardID, err)
		}
	}

	return nil
}

func (ss *BranchShardInsert) Close() error {
	for shardID, st := range ss.stmts {
		if err := st.Close(); err != nil {
			return fmt.Errorf("failed to close shard %d insert stmt: %w", shardID, err)
		}
		delete(ss.stmts, shardID)
	}

	return nil
}

type BranchShardDelete struct {
	stmts map[int64]*gosqlite.Stmt // shardID -> stmt
}

func PrepareBranchShardDelete(sql *SqliteDb, _ *BranchShards) (*BranchShardDelete, error) {
	stmts := make(map[int64]*gosqlite.Stmt)
	return &BranchShardDelete{stmts: stmts}, nil
}

func (sd *BranchShardDelete) PrepareVersion(sql *SqliteDb, version int64) error {
	shardID := ToShardID(version)

	if _, exists := sd.stmts[shardID]; exists {
		return nil
	}

	st, err := sql.treeWrite.Prepare(fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ? LIMIT 1", shardID))
	if err != nil {
		return err
	}

	sd.stmts[shardID] = st

	return nil
}

func (sd *BranchShardDelete) Exec(version int64, sequence int) error {
	shardID := ToShardID(version)

	st, exists := sd.stmts[shardID]
	if !exists {
		return fmt.Errorf("unexpected shard %d delete statment not found", shardID)
	}
	defer st.Reset()

	return st.Exec(version, sequence)
}

func (sd *BranchShardDelete) Reset() error {
	for shardID, st := range sd.stmts {
		if err := st.Reset(); err != nil {
			return fmt.Errorf("failed to reset shard %d delete stmt: %w", shardID, err)
		}
	}

	return nil
}

func (sd *BranchShardDelete) Close() error {
	for shardID, st := range sd.stmts {
		if err := st.Close(); err != nil {
			return fmt.Errorf("failed to close shard %d delete stmt: %w", shardID, err)
		}
		delete(sd.stmts, shardID)
	}

	return nil
}

type BranchShardQuery struct {
	stmts map[int64]*gosqlite.Stmt // shardID -> stmt
}

func PrepareBranchShardQuery(c *SqliteReadConn) *BranchShardQuery {
	stmts := make(map[int64]*gosqlite.Stmt)
	return &BranchShardQuery{stmts: stmts}
}

func (sq *BranchShardQuery) PrepareVersion(c *SqliteReadConn, version int64) error {
	shardID := ToShardID(version)

	if _, exists := sq.stmts[shardID]; exists {
		return nil
	}

	sqlQuery := fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ? LIMIT 1", shardID)
	st, err := c.conn.Prepare(sqlQuery)
	if err != nil {
		return err
	}

	sq.stmts[shardID] = st

	return nil
}

func (sq *BranchShardQuery) Bind(version int64, sequence uint32) (*gosqlite.Stmt, error) {
	shardID := ToShardID(version)

	st, exists := sq.stmts[shardID]
	if !exists {
		return nil, fmt.Errorf("unexpected shard %d query statment not found", shardID)
	}

	if err := st.Bind(version, int(sequence)); err != nil {
		return nil, err
	}

	return st, nil
}

func (sq *BranchShardQuery) Reset() error {
	for shardID, st := range sq.stmts {
		if err := st.Reset(); err != nil {
			return fmt.Errorf("failed to reset shard %d query stmt: %w", shardID, err)
		}
	}

	return nil
}

func (sq *BranchShardQuery) Close() error {
	for shardID, st := range sq.stmts {
		if err := st.Close(); err != nil {
			return fmt.Errorf("failed to close shard %d query stmt: %w", shardID, err)
		}
		delete(sq.stmts, shardID)
	}

	return nil
}
