package sqlite

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/common/logger"
	"github.com/cosmos/iavl/v2/common/metrics"
)

const defaultSQLitePath = "/tmp/iavl2"
const defaultMaxPoolSize = 1000
const defaultPageSize = 4096 * 2 // 8K
const defaultThreadsCount = 8
const defaultAnalysisLimit = 2000
const defaultIncrementalVacuum = 50
const defaultWriteCacheSize = -256 * 1024 // 256M

// journal mode is database wide, only need to set once on write connections
const defaultJournalMode = "WAL"

// We cannot guarantee that read connection are not shared between different go routine
const openReadOnlyMode = gosqlite.OPEN_READONLY | gosqlite.OPEN_FULLMUTEX

type ConnectionType int

const (
	UseOption ConnectionType = iota
	Immutable
	ReadOnly
)

type Options struct {
	Path          string
	Mode          int
	MmapSize      uint64
	WalSize       int
	CacheSize     int
	ConnArgs      string
	TempStoreSize int
	ShardTrees    bool
	MaxPoolSize   int

	BusyTimeout    int
	ThreadsCount   int
	StatementCache int

	Logger  logger.Logger
	Metrics metrics.Proxy

	walPages int

	OptimizeOnStart bool
}

func getPageSize() int {
	pageSize := os.Getpagesize()

	for pageSize < defaultPageSize {
		pageSize *= 2
	}

	return pageSize
}

func defaultOptions(opts Options) Options {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	// NOTE: mutex mode is set on open func call not here
	if opts.Mode == 0 {
		opts.Mode = gosqlite.OPEN_READWRITE | gosqlite.OPEN_CREATE
	}
	if opts.MmapSize == 0 {
		// opts.MmapSize = 512 * 1024 * 1024
		opts.MmapSize = 0 // disable mmap, it only map the first N bytes of data into memory
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	if opts.CacheSize == 0 {
		// 1G
		opts.CacheSize = -1 * 1024 * 1024
	}
	if opts.TempStoreSize == 0 {
		// 200M
		opts.TempStoreSize = 200 * 1024 * 1024
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}

	opts.walPages = opts.WalSize / getPageSize()

	opts.ShardTrees = false

	if opts.MaxPoolSize == 0 {
		opts.MaxPoolSize = defaultMaxPoolSize
	}

	if opts.BusyTimeout == 0 {
		opts.BusyTimeout = 2000
	}

	if opts.ThreadsCount == 0 {
		opts.ThreadsCount = defaultThreadsCount
	}

	if opts.StatementCache == 0 {
		opts.StatementCache = 100
	}

	if opts.Logger == nil {
		opts.Logger = logger.NewNopLogger()
	}

	return opts
}

func (opts Options) connArgs(ty ConnectionType) string {
	// Short circuit for unit tests
	if strings.Contains(opts.ConnArgs, "mode=memory&cache=shared") {
		return opts.ConnArgs
	}

	var args string

	switch ty {
	case UseOption:
		if opts.ConnArgs == "" {
			return ""
		}
		args = opts.ConnArgs
	case ReadOnly:
		args = "mode=ro"
	// NOTE: immutable freeze database view on connection creation, it may not see latest changes compare to
	// first readonly then pragma immutable=1 later.
	case Immutable:
		args = "mode=ro&immutable=1"
	}

	return fmt.Sprintf("?%s", args)
}

func (opts Options) leafConnectionString(ty ConnectionType) string {
	return fmt.Sprintf("file:%s/changelog.sqlite%s", opts.Path, opts.connArgs(ty))
}

func (opts Options) treeConnectionString(ty ConnectionType) string {
	return fmt.Sprintf("file:%s/tree.sqlite%s", opts.Path, opts.connArgs(ty))
}

func (opts Options) EstimateMmapSize() (uint64, error) {
	opts.Logger.Info("calculate mmap size")
	opts.Logger.Info(fmt.Sprintf("leaf connection string: %s", opts.leafConnectionString(ReadOnly)))
	conn, err := gosqlite.Open(opts.leafConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return 0, err
	}
	q, err := conn.Prepare("SELECT SUM(pgsize) FROM dbstat WHERE name = 'leaf'")
	if err != nil {
		return 0, err
	}
	hasRow, err := q.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, errors.New("no row")
	}
	var leafSize int64
	err = q.Scan(&leafSize)
	if err != nil {
		return 0, err
	}
	if err = q.Close(); err != nil {
		return 0, err
	}
	if err = conn.Close(); err != nil {
		return 0, err
	}
	mmapSize := uint64(float64(leafSize) * 1.3)
	opts.Logger.Info(fmt.Sprintf("leaf mmap size: %s", humanize.Bytes(mmapSize)))

	return mmapSize, nil
}

func (opts Options) latestVersion() (version int64, err error) {
	conn, err := gosqlite.Open(opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	err = conn.Exec("PRAGMA immutable=1;")
	if err != nil {
		return 0, err
	}

	rootQuery, err := conn.Prepare("SELECT MAX(version) FROM root LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer rootQuery.Close()

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	err = rootQuery.Scan(&version)
	if err != nil {
		return 0, err
	}

	return version, nil
}
