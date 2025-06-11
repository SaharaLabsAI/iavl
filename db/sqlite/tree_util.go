package sqlite

import (
	"fmt"

	"github.com/eatonphil/gosqlite"
)

func (sql *DB) GetHeightOneBranchesIteratorQuery(start, end int64) (stmt *gosqlite.Stmt, err error) {
	fromShardID := ToShardID(start)
	toShardID := ToShardID(end)
	if fromShardID != toShardID {
		return nil, fmt.Errorf("from shard %d to shard %d, cross different branch shards are not support", fromShardID, toShardID)
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	shardID := ToShardID(start)

	stmt, err = conn.Prepare(
		fmt.Sprintf("SELECT version, sequence, bytes FROM tree_%d WHERE version >= ? AND version <= ? ORDER BY version ASC", shardID))
	if err != nil {
		return nil, err
	}

	if err = stmt.Bind(start, end); err != nil {
		return nil, err
	}

	return stmt, err
}

func (sql *DB) GetLatestLeavesIterator(version int64, limit int) (*KVIterator, error) {
	return sql.readPool.getLatestLeavesIterator(version, limit)
}
