package metrics

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const metricsNamespace = "iavl2"

// Verify TelemetryMetrics implements the Proxy interface
var _ Proxy = &OtelMetrics{}

// OtelMetrics implements the Proxy interface using OpenTelemetry metrics
type OtelMetrics struct {
	meter otelmetric.Meter

	// Pool metrics
	poolOperations otelmetric.Int64Counter

	// Tree metrics
	treeOperations otelmetric.Int64Counter

	// Database metrics
	dbOperations otelmetric.Int64Counter
	dbDuration   otelmetric.Float64Histogram

	// Query metrics
	queryCount    otelmetric.Int64Counter
	queryDuration otelmetric.Float64Histogram

	// Write metrics
	writeDuration otelmetric.Float64Histogram

	// Working metrics
	workingSize  otelmetric.Int64UpDownCounter
	workingBytes otelmetric.Int64UpDownCounter
}

func NewOtelMetrics() *OtelMetrics {
	meter := otel.Meter(metricsNamespace)

	// Create instruments
	poolOperations, err := meter.Int64Counter(
		"iavl2.pool.operations",
		otelmetric.WithDescription("Total number of pool operations"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		log.Printf("Failed to create pool operations counter: %v", err)
	}

	treeOperations, err := meter.Int64Counter(
		"iavl2.tree.operations",
		otelmetric.WithDescription("Total number of tree operations"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		log.Printf("Failed to create tree operations counter: %v", err)
	}

	dbOperations, err := meter.Int64Counter(
		"iavl2.db.operations",
		otelmetric.WithDescription("Total number of database operations"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		log.Printf("Failed to create db operations counter: %v", err)
	}

	dbDuration, err := meter.Float64Histogram(
		"iavl2.db.operation_duration",
		otelmetric.WithDescription("Duration of database operations"),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(
			0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
		),
	)
	if err != nil {
		log.Printf("Failed to create db duration histogram: %v", err)
	}

	queryCount, err := meter.Int64Counter(
		"iavl2.query.total",
		otelmetric.WithDescription("Total number of queries"),
		otelmetric.WithUnit("{query}"),
	)
	if err != nil {
		log.Printf("Failed to create query count counter: %v", err)
	}

	queryDuration, err := meter.Float64Histogram(
		"iavl2.query.duration",
		otelmetric.WithDescription("Duration of queries"),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(
			0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
		),
	)
	if err != nil {
		log.Printf("Failed to create query duration histogram: %v", err)
	}

	writeDuration, err := meter.Float64Histogram(
		"iavl2.write.duration",
		otelmetric.WithDescription("Duration of write operations"),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(
			0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
		),
	)
	if err != nil {
		log.Printf("Failed to create write duration histogram: %v", err)
	}

	workingSize, err := meter.Int64UpDownCounter(
		"iavl2.tree.working_size",
		otelmetric.WithDescription("Current working size of the tree"),
		otelmetric.WithUnit("{item}"),
	)
	if err != nil {
		log.Printf("Failed to create working size gauge: %v", err)
	}

	workingBytes, err := meter.Int64UpDownCounter(
		"iavl2.tree.working_bytes",
		otelmetric.WithDescription("Current working bytes of the tree"),
		otelmetric.WithUnit("By"),
	)
	if err != nil {
		log.Printf("Failed to create working bytes gauge: %v", err)
	}

	return &OtelMetrics{
		meter:          meter,
		poolOperations: poolOperations,
		treeOperations: treeOperations,
		dbOperations:   dbOperations,
		dbDuration:     dbDuration,
		queryCount:     queryCount,
		queryDuration:  queryDuration,
		writeDuration:  writeDuration,
		workingSize:    workingSize,
		workingBytes:   workingBytes,
	}
}

// IncrCounter increments the appropriate counter based on the metric key
func (t *OtelMetrics) IncrCounter(val float32, keys ...string) {
	if len(keys) != 2 || keys[0] != metricsNamespace {
		return
	}

	ctx := context.Background()
	metricName := keys[1]

	switch metricName {
	// Pool operations
	case "pool_get":
		t.poolOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "get"),
		))
	case "pool_return":
		t.poolOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "return"),
		))
	case "pool_evict":
		t.poolOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "evict"),
		))
	case "pool_evict_miss":
		t.poolOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "evict_miss"),
		))
	case "pool_fault":
		t.poolOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "fault"),
		))

	// Tree operations
	case "tree_update":
		t.treeOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "update"),
		))
	case "tree_new_node":
		t.treeOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "new_node"),
		))
	case "tree_delete":
		t.treeOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "delete"),
		))
	case "tree_hash":
		t.treeOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "hash"),
		))

	// Database operations
	case "db_get_leaf":
		t.dbOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "get_leaf"),
		))
	case "db_get_branch":
		t.dbOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "get_branch"),
		))
	case "db_leaf_miss":
		t.dbOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "leaf_miss"),
		))
	case "db_write_leaf":
		t.dbOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "write_leaf"),
		))
	case "db_write_branch":
		t.dbOperations.Add(ctx, int64(val), otelmetric.WithAttributes(
			attribute.String("operation", "write_branch"),
		))
	}
}

// SetGauge sets the appropriate gauge metric
func (t *OtelMetrics) SetGauge(val float32, keys ...string) {
	if len(keys) != 2 || keys[0] != metricsNamespace {
		return
	}

	ctx := context.Background()
	metricName := keys[1]

	switch metricName {
	case "working_size":
		t.workingSize.Add(ctx, int64(val))
	case "working_bytes":
		t.workingBytes.Add(ctx, int64(val))
	}
}

// MeasureSince measures the duration since the start time and records it in the appropriate histogram
func (t *OtelMetrics) MeasureSince(start time.Time, keys ...string) {
	if len(keys) != 2 || keys[0] != metricsNamespace {
		return
	}

	ctx := context.Background()
	duration := time.Since(start)
	metricName := keys[1]

	switch metricName {
	case "db_get":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "get"),
		))
		t.queryCount.Add(ctx, 1)
		t.queryDuration.Record(ctx, duration.Seconds())
	case "db_write":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "write"),
		))
		t.writeDuration.Record(ctx, duration.Seconds())
	case "tree_set":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "tree_set"),
		))
	case "tree_has":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "tree_has"),
		))
	case "tree_get":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "tree_get"),
		))
	case "tree_batch_set_remove_deferred":
		t.dbDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(
			attribute.String("operation", "batch_set_remove"),
		))
	}
}
