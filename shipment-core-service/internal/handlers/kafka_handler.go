package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/logging"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
	"github.com/dns/logistics/shipment-core-service/internal/service"
)

// KafkaConsumer consumes normalized logistics events and persists state.
type KafkaConsumer struct {
	logger  *slog.Logger
	service *service.CoreService
	metrics *metrics.Collector
	reader  *kafka.Reader
	tracer  trace.Tracer
}

// NewKafkaConsumer creates reader for normalized events topic.
func NewKafkaConsumer(
	logger *slog.Logger,
	svc *service.CoreService,
	metricCollector *metrics.Collector,
	tracer trace.Tracer,
	brokers []string,
	topic string,
	groupID string,
) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &KafkaConsumer{
		logger:  logger,
		service: svc,
		metrics: metricCollector,
		reader:  reader,
		tracer:  tracer,
	}
}

// Run starts consume-process-commit loop.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		commit, err := c.processMessage(ctx, msg)
		if err != nil {
			c.metrics.ObserveKafkaMessage(ctx, "error")
			return err
		}
		if !commit {
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit kafka message offset=%d: %w", msg.Offset, err)
		}
	}
}

func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) (bool, error) {
	ctx, span := c.tracer.Start(ctx, "kafka.consume_normalized_event")
	defer span.End()
	span.SetAttributes(
		attribute.String("kafka.topic", msg.Topic),
		attribute.Int("kafka.partition", msg.Partition),
		attribute.Int64("kafka.offset", msg.Offset),
	)

	var normalized domain.NormalizedEvent
	if err := json.Unmarshal(msg.Value, &normalized); err != nil {
		c.metrics.ObserveKafkaMessage(ctx, "invalid_json")
		c.logger.Warn("invalid normalized event payload",
			append([]any{"error", err, "payload", string(msg.Value)}, logging.TraceFields(ctx)...)...,
		)
		return true, nil
	}

	event, err := normalized.ToShipmentEvent()
	if err != nil {
		c.metrics.ObserveKafkaMessage(ctx, "invalid_event")
		c.logger.Warn("invalid normalized event",
			append([]any{"error", err, "event_id", normalized.EventID, "shipment_id", normalized.ShipmentID}, logging.TraceFields(ctx)...)...,
		)
		return true, nil
	}

	result, err := c.service.RecordShipmentEvent(ctx, event)
	if err != nil {
		if errors.Is(err, service.ErrInvalidEvent) {
			c.metrics.ObserveKafkaMessage(ctx, "invalid_event")
			c.logger.Warn("normalized event rejected by core validation",
				append([]any{"error", err, "event_id", event.EventID, "shipment_id", event.ShipmentID.String()}, logging.TraceFields(ctx)...)...,
			)
			return true, nil
		}

		span.RecordError(err)
		return false, fmt.Errorf("record shipment event from kafka: %w", err)
	}

	c.metrics.ObserveKafkaMessage(ctx, "processed")
	c.logger.Info("normalized event persisted",
		append([]any{"event_id", event.EventID, "shipment_id", event.ShipmentID.String(), "state_updated", result.StateUpdated}, logging.TraceFields(ctx)...)...,
	)
	return true, nil
}
