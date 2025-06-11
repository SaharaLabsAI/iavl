package sqlite

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
)

const defaultWarmLeafSize = 100_0000

func (sql *DB) WarmLeaves() error {
	start := time.Now()

	conn, err := sql.getReadConn()
	if err != nil {
		return err
	}

	stmt, err := conn.Prepare(fmt.Sprintf("SELECT version, sequence, key_hash, bytes FROM changelog.leaf ORDER BY version DESC LIMIT %d", defaultWarmLeafSize))
	if err != nil {
		return err
	}
	defer stmt.Close()

	var (
		cnt, version, seq int64
		kz, vz            []byte
	)
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		err = stmt.Scan(&version, &seq, &kz, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}

	sql.logger.Info(fmt.Sprintf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start)))

	return nil
}
