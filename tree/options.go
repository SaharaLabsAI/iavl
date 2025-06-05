package tree

import "github.com/cosmos/iavl/v2/common/metrics"

type Options struct {
	StateStorage  bool
	HeightFilter  int8
	EvictionDepth int8
	MetricsProxy  metrics.Proxy
}

func DefaultOptions() Options {
	return Options{
		StateStorage:  true,
		HeightFilter:  1,
		EvictionDepth: 18,
		MetricsProxy:  &metrics.NilMetrics{},
	}
}
