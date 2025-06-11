package sqlite

import (
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cosmos/iavl/v2/common/constants"
)

func runQuickCheck(sql *WriteConn) error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, constants.MetricsNamespace, "db_quick_check")
		sql.logger.Warn(fmt.Sprintf("tree %s quick check, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.treeWrite.Exec("PRAGMA quick_check"); err != nil {
			return fmt.Errorf("failed to quick check tree %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("PRAGMA quick_check;"); err != nil {
			return fmt.Errorf("failed to quick check tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s quick checked", sql.opts.Path))

	return nil
}
