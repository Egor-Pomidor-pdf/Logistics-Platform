package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Collector stores business-level metrics instruments for analytics-exporter.
type Collector struct {
	dbQueryDuration metric.Float64Histogram
	exportDuration  metric.Float64Histogram
	exportedRows    metric.Int64Counter
	exportedBatches metric.Int64Counter
}

// New constructs metrics collector.
func New(meter metric.Meter) (*Collector, error) {
	dbQueryDuration, err := meter.Float64Histogram(
		"analytics_exporter_db_query_duration_ms",
		metric.WithDescription("analytics-exporter database query duration in milliseconds"),
	)
	if err != nil {
		return nil, err
	}

	exportDuration, err := meter.Float64Histogram(
		"analytics_exporter_export_duration_ms",
		metric.WithDescription("analytics-exporter export duration in milliseconds"),
	)
	if err != nil {
		return nil, err
	}

	exportedRows, err := meter.Int64Counter(
		"analytics_exporter_rows_total",
		metric.WithDescription("analytics-exporter exported rows count"),
	)
	if err != nil {
		return nil, err
	}

	exportedBatches, err := meter.Int64Counter(
		"analytics_exporter_batches_total",
		metric.WithDescription("analytics-exporter exported batches count"),
	)
	if err != nil {
		return nil, err
	}

	return &Collector{
		dbQueryDuration: dbQueryDuration,
		exportDuration:  exportDuration,
		exportedRows:    exportedRows,
		exportedBatches: exportedBatches,
	}, nil
}

// ObserveDBQueryDuration records DB query timing.
func (c *Collector) ObserveDBQueryDuration(ctx context.Context, op string, duration time.Duration) {
	if c == nil {
		return
	}
	c.dbQueryDuration.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(
		attribute.String("operation", op),
	))
}

// ObserveExport records export batch timing and row count.
func (c *Collector) ObserveExport(ctx context.Context, stream, result string, duration time.Duration, rows int) {
	if c == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("stream", stream),
		attribute.String("result", result),
	)
	c.exportDuration.Record(ctx, float64(duration.Milliseconds()), attrs)
	c.exportedBatches.Add(ctx, 1, attrs)
	if rows > 0 {
		c.exportedRows.Add(ctx, int64(rows), attrs)
	}
}
