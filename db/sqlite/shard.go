package sqlite

import (
	"fmt"
	"strconv"
	"strings"

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

func NewBranchShards(sql *WriteDB) (*BranchShards, error) {
	s := &BranchShards{}
	if err := s.reloadShardIDs(sql); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *BranchShards) reloadShardIDs(sql *WriteDB) error {
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

func (s *BranchShards) isSharded(sql *WriteDB) (bool, error) {
	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return false, err
	}
	var cnt int
	for {
		hasRow, err := q.Step()
		if err != nil {
			return false, err
		}
		if !hasRow {
			break
		}
		cnt++
		if cnt > 1 {
			break
		}
	}
	return cnt > 1, q.Close()
}

func (s *BranchShards) hasShardID(shardID int64) bool {
	_, exists := s.shardIDs[shardID]
	return exists
}

func (s *BranchShards) addShardID(shardID int64) {
	s.shardIDs[shardID] = true
}

type BranchShardInsert struct {
	stmts map[int64]*gosqlite.Stmt // shardID -> stmt
}

func PrepareBranchShardInsert(sql *WriteDB, shards *BranchShards) (*BranchShardInsert, error) {
	stmts := make(map[int64]*gosqlite.Stmt)

	for shardID, _ := range shards.shardIDs {
		st, err := sql.preapreBranchShardInsertStatement(shardID)
		if err != nil {
			return nil, err
		}

		stmts[shardID] = st
	}

	return &BranchShardInsert{stmts: stmts}, nil
}

func (ss *BranchShardInsert) EnsureShardTable(sql *WriteDB, shardID int64) error {
	if sql.branchShards.hasShardID(shardID) {
		return nil
	}

	if err := sql.createShardTableIfNotExists(int(shardID)); err != nil {
		return err
	}

	st, err := sql.preapreBranchShardInsertStatement(shardID)
	if err != nil {
		return err
	}

	sql.branchShards.addShardID(shardID)
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
	stmts     map[int64]*gosqlite.Stmt // shardID -> stmt
	nonexists map[int64]bool
}

func PrepareBranchShardDelete(sql *WriteDB, _ *BranchShards) (*BranchShardDelete, error) {
	stmts := make(map[int64]*gosqlite.Stmt)
	nonexists := make(map[int64]bool)
	return &BranchShardDelete{stmts: stmts, nonexists: nonexists}, nil
}

func (sd *BranchShardDelete) PrepareVersion(sql *WriteDB, version int64) error {
	shardID := ToShardID(version)

	if _, exists := sd.stmts[shardID]; exists {
		return nil
	}

	st, err := sql.treeWrite.Prepare(fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ? LIMIT 1", shardID))
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("no such table: tree_%d", shardID)) {
			sd.nonexists[shardID] = true
			return nil
		}

		return err
	}

	sd.stmts[shardID] = st
	delete(sd.nonexists, shardID)

	return nil
}

func (sd *BranchShardDelete) Exec(version int64, sequence int) error {
	shardID := ToShardID(version)

	if _, nonexists := sd.nonexists[shardID]; nonexists {
		return nil
	}

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

	for shardID := range sd.nonexists {
		delete(sd.nonexists, shardID)
	}

	return nil
}

type BranchShardQuery struct {
	stmts map[int64]*gosqlite.Stmt // shardID -> stmt
}

func PrepareBranchShardQuery(c *ReadConn) *BranchShardQuery {
	stmts := make(map[int64]*gosqlite.Stmt)
	return &BranchShardQuery{stmts: stmts}
}

func (sq *BranchShardQuery) PrepareVersion(c *ReadConn, version int64) error {
	shardID := ToShardID(version)

	if _, exists := sq.stmts[shardID]; exists {
		return nil
	}

	sqlQuery := fmt.Sprintf(StmtQueryBranchShardFormat, shardID)
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
