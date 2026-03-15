package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Collector stores business-level metrics instruments.
type Collector struct {
	normalizedEvents metric.Int64Counter
	stateUpserts     metric.Int64Counter
	dbQueryDuration  metric.Float64Histogram
	httpRequests     metric.Int64Counter
	grpcRequests     metric.Int64Counter
	kafkaMessages    metric.Int64Counter
}

// New creates all metrics instruments.
func New(meter metric.Meter) (*Collector, error) {
	normalizedEvents, err := meter.Int64Counter(
		"shipment_core_events_written_total",
		metric.WithDescription("Total persisted shipment events grouped by status and source"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_events_written_total: %w", err)
	}

	stateUpserts, err := meter.Int64Counter(
		"shipment_core_state_upserts_total",
		metric.WithDescription("Total shipment_state upserts grouped by result"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_state_upserts_total: %w", err)
	}

	dbQueryDuration, err := meter.Float64Histogram(
		"shipment_core_db_query_duration_ms",
		metric.WithDescription("Database query duration in milliseconds"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_db_query_duration_ms: %w", err)
	}

	httpRequests, err := meter.Int64Counter(
		"shipment_core_http_requests_total",
		metric.WithDescription("HTTP requests total grouped by route and status code"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_http_requests_total: %w", err)
	}

	grpcRequests, err := meter.Int64Counter(
		"shipment_core_grpc_requests_total",
		metric.WithDescription("gRPC requests total grouped by method and status"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_grpc_requests_total: %w", err)
	}

	kafkaMessages, err := meter.Int64Counter(
		"shipment_core_kafka_messages_total",
		metric.WithDescription("Kafka consumer messages grouped by processing result"),
	)
	if err != nil {
		return nil, fmt.Errorf("create shipment_core_kafka_messages_total: %w", err)
	}

	return &Collector{
		normalizedEvents: normalizedEvents,
		stateUpserts:     stateUpserts,
		dbQueryDuration:  dbQueryDuration,
		httpRequests:     httpRequests,
		grpcRequests:     grpcRequests,
		kafkaMessages:    kafkaMessages,
	}, nil
}

// ObserveEventWritten adds business metric for written event.
func (c *Collector) ObserveEventWritten(ctx context.Context, statusCode, sourceSystem string) {
	c.normalizedEvents.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status_code", statusCode),
		attribute.String("source_system", sourceSystem),
	))
}

// ObserveStateUpsert tracks upsert result.
func (c *Collector) ObserveStateUpsert(ctx context.Context, updated bool) {
	result := "skipped"
	if updated {
		result = "updated"
	}
	c.stateUpserts.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
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

// ObserveGRPCRequest tracks gRPC requests.
func (c *Collector) ObserveGRPCRequest(ctx context.Context, method, status string) {
	c.grpcRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("status", status),
	))
}

// ObserveKafkaMessage tracks consumer processing outcome.
func (c *Collector) ObserveKafkaMessage(ctx context.Context, result string) {
	c.kafkaMessages.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}
