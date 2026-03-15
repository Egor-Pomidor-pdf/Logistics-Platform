package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Collector stores business-level metrics instruments for eta-data-service.
type Collector struct {
	dbQueryDuration metric.Float64Histogram
	httpRequests    metric.Int64Counter
	batchRuns       metric.Int64Counter
}

// New creates all metrics instruments.
func New(meter metric.Meter) (*Collector, error) {
	dbQueryDuration, err := meter.Float64Histogram(
		"eta_data_db_query_duration_ms",
		metric.WithDescription("eta-data-service database query duration in milliseconds"),
	)
	if err != nil {
		return nil, fmt.Errorf("create eta_data_db_query_duration_ms: %w", err)
	}

	httpRequests, err := meter.Int64Counter(
		"eta_data_http_requests_total",
		metric.WithDescription("eta-data-service HTTP requests grouped by route and status code"),
	)
	if err != nil {
		return nil, fmt.Errorf("create eta_data_http_requests_total: %w", err)
	}

	batchRuns, err := meter.Int64Counter(
		"eta_data_batches_total",
		metric.WithDescription("eta-data-service batch jobs grouped by type and result"),
	)
	if err != nil {
		return nil, fmt.Errorf("create eta_data_batches_total: %w", err)
	}

	return &Collector{
		dbQueryDuration: dbQueryDuration,
		httpRequests:    httpRequests,
		batchRuns:       batchRuns,
	}, nil
}

// ObserveDBQueryDuration tracks query latency.
func (c *Collector) ObserveDBQueryDuration(ctx context.Context, operation string, duration time.Duration) {
	c.dbQueryDuration.Record(
		ctx,
		float64(duration.Milliseconds()),
		metric.WithAttributes(attribute.String("operation", operation)),
	)
}

// ObserveHTTPRequest tracks HTTP requests.
func (c *Collector) ObserveHTTPRequest(ctx context.Context, route string, statusCode int) {
	c.httpRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("route", route),
		attribute.Int("status_code", statusCode),
	))
}

// ObserveBatchRun tracks batch executions.
func (c *Collector) ObserveBatchRun(ctx context.Context, batchType, result string) {
	c.batchRuns.Add(ctx, 1, metric.WithAttributes(
		attribute.String("type", batchType),
		attribute.String("result", result),
	))
}

