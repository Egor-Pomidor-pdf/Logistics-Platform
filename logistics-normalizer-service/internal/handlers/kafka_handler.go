package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/logistics-normalizer-service/internal/domain"
	"github.com/dns/logistics/logistics-normalizer-service/internal/metrics"
	"github.com/dns/logistics/logistics-normalizer-service/internal/repository"
)

var statusMapping = map[string]map[string]string{
	"WMS": {
		"PICKED":  "picked",
		"PACKED":  "packed",
		"SHIPPED": "shipped",
	},
	"PARTNER_X": {
		"IN_TRANSIT": "in_transit",
		"DELIVERED":  "delivered",
	},
	"OMS": {
		"CREATED":   "created",
		"CANCELLED": "cancelled",
	},
}

var sourceMapping = map[string]string{
	"OMS":       "oms",
	"WMS":       "wms",
	"PARTNER_X": "partner_x",
}

// KafkaHandler processes raw events and publishes normalized events.
type KafkaHandler struct {
	logger           *slog.Logger
	metrics          *metrics.Collector
	deduplicator     repository.EventDeduplicator
	normalizedWriter *kafka.Writer
	invalidWriter    *kafka.Writer
	tracer           trace.Tracer
	normalizedEvents otelmetric.Int64Counter
	moscowLocation   *time.Location
}

// NewKafkaHandler builds handler instance.
func NewKafkaHandler(
	logger *slog.Logger,
	metricsCollector *metrics.Collector,
	deduplicator repository.EventDeduplicator,
	normalizedWriter *kafka.Writer,
	invalidWriter *kafka.Writer,
	tracer trace.Tracer,
	normalizedEventsCounter otelmetric.Int64Counter,
) *KafkaHandler {
	moscowLocation, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		moscowLocation = time.FixedZone("MSK", 3*60*60)
	}

	return &KafkaHandler{
		logger:           logger,
		metrics:          metricsCollector,
		deduplicator:     deduplicator,
		normalizedWriter: normalizedWriter,
		invalidWriter:    invalidWriter,
		tracer:           tracer,
		normalizedEvents: normalizedEventsCounter,
		moscowLocation:   moscowLocation,
	}
}

// ProcessMessage handles one Kafka raw message.
func (h *KafkaHandler) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	started := time.Now()
	defer h.metrics.ObserveKafkaHandlerDuration(time.Since(started))

	h.updateConsumerLag(msg)

	var raw domain.RawEvent
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		h.metrics.IncRawEvents("unknown")
		h.metrics.IncInvalidEvents("invalid_json")
		h.logger.Warn("invalid JSON payload", "error", err, "payload", string(msg.Value))
		return h.publishInvalid(ctx, raw, msg.Value, "invalid_json", err)
	}

	raw.SourceSystem = strings.ToUpper(strings.TrimSpace(raw.SourceSystem))
	raw.ExternalStatus = strings.ToUpper(strings.TrimSpace(raw.ExternalStatus))
	h.metrics.IncRawEvents(metricSource(raw.SourceSystem))

	ctx, span := h.tracer.Start(ctx, "process_kafka_message")
	defer span.End()
	span.SetAttributes(
		attribute.String("event_id", raw.EventID),
		attribute.String("shipment_id", raw.ShipmentID),
		attribute.String("source_system", raw.SourceSystem),
		attribute.String("external_status", raw.ExternalStatus),
	)

	statusTS, reason, err := h.validateRaw(raw)
	if err != nil {
		h.metrics.IncInvalidEvents(reason)
		h.logger.Warn(
			"raw event validation failed",
			"reason", reason,
			"event_id", raw.EventID,
			"shipment_id", raw.ShipmentID,
			"source_system", raw.SourceSystem,
			"error", err,
		)
		return h.publishInvalid(ctx, raw, msg.Value, reason, err)
	}

	if raw.EventID != "" {
		duplicate, dedupErr := h.checkDuplicate(ctx, raw.EventID)
		if dedupErr != nil {
			h.logger.Error("redis dedup check failed; continue without dedup", "event_id", raw.EventID, "error", dedupErr)
		} else if duplicate {
			h.metrics.IncDeduplicatedEvents()
			h.logger.Info("duplicate event skipped", "event_id", raw.EventID, "shipment_id", raw.ShipmentID)
			return nil
		}
	}

	statusCode, ok := mapExternalStatus(raw.SourceSystem, raw.ExternalStatus)
	if !ok {
		h.metrics.IncStatusNormalizationError(raw.SourceSystem, raw.ExternalStatus)
		h.metrics.IncInvalidEvents("unknown_external_status")
		h.logger.Warn(
			"unknown external status",
			"event_id", raw.EventID,
			"shipment_id", raw.ShipmentID,
			"source_system", raw.SourceSystem,
			"external_status", raw.ExternalStatus,
		)
		return h.publishInvalid(ctx, raw, msg.Value, "unknown_external_status", errors.New("mapping not found"))
	}

	normalized := domain.NormalizedEvent{
		EventID:      raw.EventID,
		ShipmentID:   raw.ShipmentID,
		StatusCode:   statusCode,
		StatusTS:     statusTS.UTC(),
		SourceSystem: mapSourceSystem(raw.SourceSystem),
		LocationCode: raw.Location,
		PartnerID:    raw.PartnerID,
		RouteID:      raw.RouteID,
		Payload:      json.RawMessage(append([]byte(nil), msg.Value...)),
	}

	if eta, err := h.parseEstimatedDelivery(raw.EstimatedDelivery); err == nil && eta != nil {
		normalized.EstimatedDelivery = eta
	} else if err != nil {
		h.logger.Warn("failed to parse estimated_delivery, skipping field",
			"event_id", raw.EventID,
			"estimated_delivery", raw.EstimatedDelivery,
			"error", err,
		)
	}

	if err := h.publishNormalized(ctx, normalized); err != nil {
		h.metrics.IncNormalizationError()
		return err
	}

	h.metrics.IncNormalizedEvents(normalized.SourceSystem, normalized.StatusCode)
	if h.normalizedEvents != nil {
		h.normalizedEvents.Add(
			ctx,
			1,
			otelmetric.WithAttributes(
				attribute.String("source_system", normalized.SourceSystem),
				attribute.String("status_code", normalized.StatusCode),
			),
		)
	}
	h.logger.Info(
		"event normalized",
		"event_id", normalized.EventID,
		"shipment_id", normalized.ShipmentID,
		"status_code", normalized.StatusCode,
		"source_system", normalized.SourceSystem,
	)

	return nil
}

func (h *KafkaHandler) validateRaw(raw domain.RawEvent) (time.Time, string, error) {
	if strings.TrimSpace(raw.EventID) == "" {
		return time.Time{}, "missing_event_id", errors.New("event_id is empty")
	}
	if strings.TrimSpace(raw.ShipmentID) == "" {
		return time.Time{}, "missing_shipment_id", errors.New("shipment_id is empty")
	}
	if _, err := uuid.Parse(raw.ShipmentID); err != nil {
		return time.Time{}, "invalid_shipment_uuid", fmt.Errorf("shipment_id must be UUID: %w", err)
	}
	if strings.TrimSpace(raw.ExternalStatus) == "" {
		return time.Time{}, "missing_external_status", errors.New("external_status is empty")
	}
	if strings.TrimSpace(raw.SourceSystem) == "" {
		return time.Time{}, "missing_source_system", errors.New("source_system is empty")
	}
	if _, ok := statusMapping[raw.SourceSystem]; !ok {
		return time.Time{}, "unknown_source_system", fmt.Errorf("source_system %q is not supported", raw.SourceSystem)
	}
	if strings.TrimSpace(raw.StatusTS) == "" {
		return time.Time{}, "missing_status_ts", errors.New("status_ts is empty")
	}

	timestamp, err := h.parseStatusTS(raw.StatusTS)
	if err != nil {
		return time.Time{}, "invalid_status_ts", err
	}
	return timestamp, "", nil
}

func (h *KafkaHandler) parseStatusTS(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	withTZLayouts := []string{time.RFC3339Nano, time.RFC3339}
	for _, layout := range withTZLayouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC(), nil
		}
	}

	withoutTZLayouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05.000",
	}
	for _, layout := range withoutTZLayouts {
		if ts, err := time.ParseInLocation(layout, value, h.moscowLocation); err == nil {
			return ts.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported status_ts format: %q", raw)
}

func (h *KafkaHandler) checkDuplicate(ctx context.Context, eventID string) (bool, error) {
	if h.deduplicator == nil {
		return false, nil
	}

	ctx, span := h.tracer.Start(ctx, "redis_dedup_check")
	defer span.End()

	duplicate, err := h.deduplicator.CheckAndMark(ctx, eventID)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	span.SetAttributes(attribute.Bool("duplicate", duplicate))

	return duplicate, nil
}

func (h *KafkaHandler) publishNormalized(ctx context.Context, event domain.NormalizedEvent) error {
	ctx, span := h.tracer.Start(ctx, "kafka_publish_normalized_event")
	defer span.End()

	payload, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal normalized event: %w", err)
	}

	if err := h.normalizedWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.ShipmentID),
		Value: payload,
		Time:  time.Now().UTC(),
	}); err != nil {
		span.RecordError(err)
		return fmt.Errorf("write normalized message: %w", err)
	}

	return nil
}

func (h *KafkaHandler) publishInvalid(
	ctx context.Context,
	raw domain.RawEvent,
	rawPayload []byte,
	reason string,
	cause error,
) error {
	ctx, span := h.tracer.Start(ctx, "kafka_publish_invalid_event")
	defer span.End()

	invalid := domain.InvalidEvent{
		EventID:      raw.EventID,
		ShipmentID:   raw.ShipmentID,
		SourceSystem: raw.SourceSystem,
		Reason:       reason,
		Error:        errorMessage(cause),
		RawPayload:   string(rawPayload),
		CreatedAt:    time.Now().UTC(),
	}

	payload, err := json.Marshal(invalid)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal invalid event: %w", err)
	}

	key := raw.ShipmentID
	if key == "" {
		key = raw.EventID
	}

	if err := h.invalidWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	}); err != nil {
		span.RecordError(err)
		return fmt.Errorf("write invalid event message: %w", err)
	}

	return nil
}

func mapExternalStatus(sourceSystem, externalStatus string) (string, bool) {
	sourceMap, ok := statusMapping[sourceSystem]
	if !ok {
		return "", false
	}
	status, ok := sourceMap[externalStatus]
	if !ok {
		return "", false
	}
	return status, true
}

func mapSourceSystem(source string) string {
	if mapped, ok := sourceMapping[source]; ok {
		return mapped
	}
	return strings.ToLower(source)
}

func metricSource(source string) string {
	if strings.TrimSpace(source) == "" {
		return "unknown"
	}
	return strings.ToLower(source)
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// parseEstimatedDelivery normalizes estimated delivery time from various partner formats to UTC.
// Partners send ETA in different formats: ISO dates, datetime with timezone, unix timestamps, etc.
func (h *KafkaHandler) parseEstimatedDelivery(raw string) (*time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}

	// Try RFC3339 / RFC3339Nano first (e.g. "2026-03-15T14:00:00+03:00").
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if ts, err := time.Parse(layout, value); err == nil {
			utc := ts.UTC()
			return &utc, nil
		}
	}

	// Date-only formats (e.g. "2026-03-15", "15.03.2026").
	dateLayouts := []string{
		"2006-01-02",
		"02.01.2006",
		"01/02/2006",
	}
	for _, layout := range dateLayouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return &ts, nil
		}
	}

	// Datetime without timezone — assume Moscow.
	noTZLayouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05.000",
	}
	for _, layout := range noTZLayouts {
		if ts, err := time.ParseInLocation(layout, value, h.moscowLocation); err == nil {
			utc := ts.UTC()
			return &utc, nil
		}
	}

	// Unix timestamp (seconds).
	if len(value) == 10 {
		var sec int64
		if _, err := fmt.Sscanf(value, "%d", &sec); err == nil && sec > 1e9 {
			ts := time.Unix(sec, 0).UTC()
			return &ts, nil
		}
	}

	return nil, fmt.Errorf("unsupported estimated_delivery format: %q", raw)
}

func (h *KafkaHandler) updateConsumerLag(msg kafka.Message) {
	if msg.HighWaterMark <= 0 {
		return
	}
	lag := msg.HighWaterMark - msg.Offset - 1
	h.metrics.SetKafkaConsumerLag(float64(lag))
}
