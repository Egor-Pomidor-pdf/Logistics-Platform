package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
	"github.com/dns/logistics/shipment-core-service/internal/repository"
)

var (
	ErrInvalidShipmentID = errors.New("invalid shipment_id")
	ErrInvalidEvent      = errors.New("invalid shipment event")
)

// CoreService handles shipment core business flow.
type CoreService struct {
	repo    repository.PostgresStore
	metrics *metrics.Collector
	tracer  trace.Tracer

	featureTaskPublisher func(ctx context.Context, shipmentID string, reason string, force bool) error
}

// New creates shipment core service.
func New(repo repository.PostgresStore, metricCollector *metrics.Collector, tracer trace.Tracer) *CoreService {
	return &CoreService{repo: repo, metrics: metricCollector, tracer: tracer}
}

// WithFeatureTaskPublisher настраивает callback для публикации задач пересчёта фич.
func (s *CoreService) WithFeatureTaskPublisher(publisher func(ctx context.Context, shipmentID string, reason string, force bool) error) {
	s.featureTaskPublisher = publisher
}

// RecordShipmentEvent saves immutable history and updates latest state when event is newer.
func (s *CoreService) RecordShipmentEvent(ctx context.Context, event domain.ShipmentEvent) (domain.RecordEventResult, error) {
	ctx, span := s.tracer.Start(ctx, "service.record_shipment_event")
	defer span.End()

	if err := validateEvent(event); err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}

	span.SetAttributes(
		attribute.String("shipment_id", event.ShipmentID.String()),
		attribute.String("status_code", event.StatusCode),
	)

	result, err := s.repo.RecordEvent(ctx, event)
	if err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, err
	}

	s.metrics.ObserveEventWritten(ctx, event.StatusCode, event.SourceSystem)
	s.metrics.ObserveStateUpsert(ctx, result.StateUpdated)

	// Если состояние обновилось, публикуем задачу на пересчёт фич в ETA-слой.
	if result.StateUpdated && s.featureTaskPublisher != nil {
		_ = s.featureTaskPublisher(ctx, event.ShipmentID.String(), "state_updated", false)
	}

	return result, nil
}

// GetShipment fetches latest shipment state.
func (s *CoreService) GetShipment(ctx context.Context, shipmentID string) (domain.ShipmentState, error) {
	ctx, span := s.tracer.Start(ctx, "service.get_shipment")
	defer span.End()

	id, err := uuid.Parse(strings.TrimSpace(shipmentID))
	if err != nil {
		span.RecordError(err)
		return domain.ShipmentState{}, ErrInvalidShipmentID
	}

	state, err := s.repo.GetShipmentState(ctx, id)
	if err != nil {
		span.RecordError(err)
		return domain.ShipmentState{}, err
	}
	return state, nil
}

// ListShipments returns latest states with filters and pagination.
func (s *CoreService) ListShipments(ctx context.Context, filters domain.ShipmentFilters) ([]domain.ShipmentState, error) {
	ctx, span := s.tracer.Start(ctx, "service.list_shipments")
	defer span.End()

	filters.StatusCode = strings.TrimSpace(filters.StatusCode)
	filters.SourceSystem = strings.TrimSpace(filters.SourceSystem)
	filters.RouteID = strings.TrimSpace(filters.RouteID)

	if filters.Limit <= 0 {
		filters.Limit = 100
	}
	if filters.Limit > 500 {
		filters.Limit = 500
	}
	if filters.Offset < 0 {
		filters.Offset = 0
	}

	items, err := s.repo.ListShipmentStates(ctx, filters)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return items, nil
}

// IsReady checks repository health.
func (s *CoreService) IsReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.repo.Ping(ctx)
}

func validateEvent(event domain.ShipmentEvent) error {
	if strings.TrimSpace(event.EventID) == "" {
		return errors.New("event_id is empty")
	}
	if event.ShipmentID == uuid.Nil {
		return errors.New("shipment_id is nil")
	}
	if strings.TrimSpace(event.StatusCode) == "" {
		return errors.New("status_code is empty")
	}
	if strings.TrimSpace(event.SourceSystem) == "" {
		return errors.New("source_system is empty")
	}
	if event.StatusTS.IsZero() {
		return errors.New("status_ts is zero")
	}
	return nil
}
