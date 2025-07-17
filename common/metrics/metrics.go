package metrics

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/dustin/go-humanize"
)

type Label struct {
	Name  string
	Value string
}

type Proxy interface {
	IncrCounter(val float32, keys ...string)
	SetGauge(val float32, keys ...string)
	MeasureSince(start time.Time, keys ...string)
}

var (
	_ Proxy = &StructMetrics{}
	_ Proxy = &NilMetrics{}
)

type NilMetrics struct{}

func (n NilMetrics) IncrCounter(_ float32, _ ...string) {}

func (n NilMetrics) SetGauge(_ float32, _ ...string) {}

func (n NilMetrics) MeasureSince(_ time.Time, _ ...string) {}

type StructMetrics struct {
	*TreeMetrics
	*DbMetrics
}

func NewStructMetrics() *StructMetrics {
	return &StructMetrics{
		TreeMetrics: &TreeMetrics{},
		DbMetrics:   &DbMetrics{},
	}
}

func (s *StructMetrics) IncrCounter(val float32, keys ...string) {
	if len(keys) != 2 {
		return
	}
	k := keys[1]
	switch k {
	case "pool_get":
		s.PoolGet.Add(int64(val))
	case "pool_return":
		s.PoolReturn.Add(int64(val))
	case "pool_evict":
		s.PoolEvict.Add(int64(val))
	case "pool_evict_miss":
		s.PoolEvictMiss.Add(int64(val))
	case "pool_fault":
		s.PoolFault.Add(int64(val))

	case "tree_update":
		s.TreeUpdate.Add(int64(val))
	case "tree_new_node":
		s.TreeNewNode.Add(int64(val))
	case "tree_delete":
		s.TreeDelete.Add(int64(val))
	case "tree_hash":
		s.TreeHash.Add(int64(val))

	case "db_get_leaf":
		s.QueryLeafCount.Add(int64(val))
	case "db_get_branch":
		s.QueryBranchCount.Add(int64(val))
	case "db_leaf_miss":
		s.QueryLeafMiss.Add(int64(val))
	case "db_write_leaf":
		s.WriteLeaves.Add(int64(val))
	case "db_write_branch":
		s.WriteBranch.Add(int64(val))
	}
}

func (s *StructMetrics) SetGauge(_ float32, _ ...string) {}

func (s *StructMetrics) MeasureSince(start time.Time, keys ...string) {
	dur := time.Since(start)
	if len(keys) != 2 {
		return
	}
	k := keys[1]
	switch k {
	case "db_get":
		s.QueryDurations = append(s.QueryDurations, dur)
		s.QueryTime += dur
		s.QueryCount.Add(1)
	case "db_write":
		s.WriteDurations = append(s.WriteDurations, dur)
		s.WriteTime += dur
	}
}

type TreeMetrics struct {
	PoolGet       atomic.Int64
	PoolReturn    atomic.Int64
	PoolEvict     atomic.Int64
	PoolEvictMiss atomic.Int64
	PoolFault     atomic.Int64

	TreeUpdate  atomic.Int64
	TreeNewNode atomic.Int64
	TreeDelete  atomic.Int64
	TreeHash    atomic.Int64
}

type DbMetrics struct {
	WriteDurations []time.Duration
	WriteTime      time.Duration
	WriteLeaves    atomic.Int64
	WriteBranch    atomic.Int64

	QueryDurations   []time.Duration
	QueryTime        time.Duration
	QueryCount       atomic.Int64
	QueryLeafMiss    atomic.Int64
	QueryLeafCount   atomic.Int64
	QueryBranchCount atomic.Int64
}

func (m *TreeMetrics) Report() {
	fmt.Printf("Pool:\n gets: %s, returns: %s, faults: %s, evicts: %s, evict miss %s\n",
		humanize.Comma(m.PoolGet.Load()),
		humanize.Comma(m.PoolReturn.Load()),
		humanize.Comma(m.PoolFault.Load()),
		humanize.Comma(m.PoolEvict.Load()),
		humanize.Comma(m.PoolEvictMiss.Load()),
	)

	fmt.Printf("\nTree:\n update: %s, new node: %s, delete: %s\n",
		humanize.Comma(m.TreeUpdate.Load()),
		humanize.Comma(m.TreeNewNode.Load()),
		humanize.Comma(m.TreeDelete.Load()))
}

func (s *StructMetrics) QueryReport(bins int) error {
	if s.QueryCount.Load() == 0 {
		return nil
	}

	fmt.Printf("queries=%s q/s=%s dur/q=%s dur=%s leaf-q=%s branch-q=%s leaf-miss=%s\n",
		humanize.Comma(s.QueryCount.Load()),
		humanize.Comma(int64(float64(s.QueryCount.Load())/s.QueryTime.Seconds())),
		time.Duration(int64(s.QueryTime)/s.QueryCount.Load()),
		s.QueryTime.Round(time.Millisecond),
		humanize.Comma(s.QueryLeafCount.Load()),
		humanize.Comma(s.QueryBranchCount.Load()),
		humanize.Comma(s.QueryLeafMiss.Load()),
	)

	if bins > 0 {
		var histData []float64
		for _, d := range s.QueryDurations {
			if d > 50*time.Microsecond {
				continue
			}
			histData = append(histData, float64(d))
		}
		hist := histogram.Hist(bins, histData)
		err := histogram.Fprintf(os.Stdout, hist, histogram.Linear(10), func(v float64) string {
			return time.Duration(v).String()
		})
		if err != nil {
			return err
		}
	}

	s.SetQueryZero()

	return nil
}

func (s *StructMetrics) SetQueryZero() {
	s.QueryDurations = nil
	s.QueryTime = 0
	s.QueryCount.Store(0)
	s.QueryLeafMiss.Store(0)
	s.QueryLeafCount.Store(0)
	s.QueryBranchCount.Store(0)
}

func (s *StructMetrics) Add(os *StructMetrics) {
	s.WriteDurations = append(s.WriteDurations, os.WriteDurations...)
	s.WriteTime += os.WriteTime
	s.WriteLeaves.Add(os.WriteLeaves.Load())
	s.WriteBranch.Add(os.WriteBranch.Load())

	s.QueryDurations = append(s.QueryDurations, os.QueryDurations...)
	s.QueryTime += os.QueryTime
	s.QueryCount.Add(os.QueryCount.Load())
	s.QueryLeafMiss.Add(os.QueryLeafMiss.Load())
	s.QueryLeafCount.Add(os.QueryLeafCount.Load())
	s.QueryBranchCount.Add(os.QueryBranchCount.Load())
}
