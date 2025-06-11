package sqlite

import (
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cosmos/iavl/v2/common/constants"
)

func runAnalyze(sql *WriteConn) error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, constants.MetricsNamespace, "db_analyze")
		sql.logger.Warn(fmt.Sprintf("tree %s indexes analyzed, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("ANALYZE;"); err != nil {
			return fmt.Errorf("failed to analyze tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.treeWrite.Exec("ANALYZE;"); err != nil {
			return fmt.Errorf("failed to analyze tree branch %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s analyzed", sql.opts.Path))

	return nil
}

func runOptimize(sql *WriteConn) error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, constants.MetricsNamespace, "db_optimize")
		sql.logger.Warn(fmt.Sprintf("tree %s indexes optimized, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("PRAGMA optimize();"); err != nil {
			return fmt.Errorf("failed to optimize tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.treeWrite.Exec("PRAGMA optimize();"); err != nil {
			return fmt.Errorf("failed to optimize tree branch %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s optimized", sql.opts.Path))

	return nil
}
