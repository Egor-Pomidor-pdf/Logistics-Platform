package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
)

// ---------------------------------------------------------------------------
// Mock repository
// ---------------------------------------------------------------------------

type mockRepo struct {
	recordEventFn       func(ctx context.Context, event domain.ShipmentEvent) (domain.RecordEventResult, error)
	getShipmentStateFn  func(ctx context.Context, shipmentID uuid.UUID) (domain.ShipmentState, error)
	listShipmentStatesFn func(ctx context.Context, filters domain.ShipmentFilters) ([]domain.ShipmentState, error)
	pingFn              func(ctx context.Context) error
}

func (m *mockRepo) EnsureSchema(context.Context) error { return nil }
func (m *mockRepo) Close()                              {}

func (m *mockRepo) RecordEvent(ctx context.Context, event domain.ShipmentEvent) (domain.RecordEventResult, error) {
	if m.recordEventFn != nil {
		return m.recordEventFn(ctx, event)
	}
	return domain.RecordEventResult{}, nil
}

func (m *mockRepo) GetShipmentState(ctx context.Context, id uuid.UUID) (domain.ShipmentState, error) {
	if m.getShipmentStateFn != nil {
		return m.getShipmentStateFn(ctx, id)
	}
	return domain.ShipmentState{}, nil
}

func (m *mockRepo) ListShipmentStates(ctx context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
	if m.listShipmentStatesFn != nil {
		return m.listShipmentStatesFn(ctx, f)
	}
	return nil, nil
}

func (m *mockRepo) Ping(ctx context.Context) error {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestService(repo *mockRepo) *CoreService {
	meter := metricnoop.NewMeterProvider().Meter("test")
	mc, _ := metrics.New(meter)
	tracer := tracenoop.NewTracerProvider().Tracer("test")
	return New(repo, mc, tracer)
}

func validEvent() domain.ShipmentEvent {
	return domain.ShipmentEvent{
		EventID:      "evt-001",
		ShipmentID:   uuid.New(),
		StatusCode:   "in_transit",
		StatusTS:     time.Now().UTC(),
		SourceSystem: "wms",
	}
}

// ---------------------------------------------------------------------------
// validateEvent tests
// ---------------------------------------------------------------------------

func TestValidateEvent_Valid(t *testing.T) {
	if err := validateEvent(validEvent()); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateEvent_MissingEventID(t *testing.T) {
	e := validEvent()
	e.EventID = ""
	if err := validateEvent(e); err == nil {
		t.Fatal("expected error for empty event_id")
	}
}

func TestValidateEvent_NilShipmentID(t *testing.T) {
	e := validEvent()
	e.ShipmentID = uuid.Nil
	if err := validateEvent(e); err == nil {
		t.Fatal("expected error for nil shipment_id")
	}
}

func TestValidateEvent_EmptyStatusCode(t *testing.T) {
	e := validEvent()
	e.StatusCode = "   "
	if err := validateEvent(e); err == nil {
		t.Fatal("expected error for empty status_code")
	}
}

func TestValidateEvent_EmptySourceSystem(t *testing.T) {
	e := validEvent()
	e.SourceSystem = ""
	if err := validateEvent(e); err == nil {
		t.Fatal("expected error for empty source_system")
	}
}

func TestValidateEvent_ZeroStatusTS(t *testing.T) {
	e := validEvent()
	e.StatusTS = time.Time{}
	if err := validateEvent(e); err == nil {
		t.Fatal("expected error for zero status_ts")
	}
}

// ---------------------------------------------------------------------------
// RecordShipmentEvent tests
// ---------------------------------------------------------------------------

func TestRecordShipmentEvent_Success(t *testing.T) {
	repo := &mockRepo{
		recordEventFn: func(_ context.Context, _ domain.ShipmentEvent) (domain.RecordEventResult, error) {
			return domain.RecordEventResult{StateUpdated: true}, nil
		},
	}
	svc := newTestService(repo)

	result, err := svc.RecordShipmentEvent(context.Background(), validEvent())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.StateUpdated {
		t.Fatal("expected StateUpdated=true")
	}
}

func TestRecordShipmentEvent_ValidationError(t *testing.T) {
	repo := &mockRepo{}
	svc := newTestService(repo)

	bad := validEvent()
	bad.EventID = ""

	_, err := svc.RecordShipmentEvent(context.Background(), bad)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got: %v", err)
	}
}

func TestRecordShipmentEvent_RepoError(t *testing.T) {
	repoErr := errors.New("db connection lost")
	repo := &mockRepo{
		recordEventFn: func(_ context.Context, _ domain.ShipmentEvent) (domain.RecordEventResult, error) {
			return domain.RecordEventResult{}, repoErr
		},
	}
	svc := newTestService(repo)

	_, err := svc.RecordShipmentEvent(context.Background(), validEvent())
	if !errors.Is(err, repoErr) {
		t.Fatalf("expected repo error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetShipment tests
// ---------------------------------------------------------------------------

func TestGetShipment_ValidUUID(t *testing.T) {
	id := uuid.New()
	expected := domain.ShipmentState{
		ShipmentID: id,
		StatusCode: "delivered",
	}
	repo := &mockRepo{
		getShipmentStateFn: func(_ context.Context, gotID uuid.UUID) (domain.ShipmentState, error) {
			if gotID != id {
				t.Fatalf("expected id %s, got %s", id, gotID)
			}
			return expected, nil
		},
	}
	svc := newTestService(repo)

	state, err := svc.GetShipment(context.Background(), id.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.ShipmentID != id {
		t.Fatalf("expected shipment_id %s, got %s", id, state.ShipmentID)
	}
	if state.StatusCode != "delivered" {
		t.Fatalf("expected status_code delivered, got %s", state.StatusCode)
	}
}

func TestGetShipment_InvalidUUID(t *testing.T) {
	repo := &mockRepo{}
	svc := newTestService(repo)

	_, err := svc.GetShipment(context.Background(), "not-a-uuid")
	if !errors.Is(err, ErrInvalidShipmentID) {
		t.Fatalf("expected ErrInvalidShipmentID, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ListShipments tests — filter normalization
// ---------------------------------------------------------------------------

func TestListShipments_DefaultLimit(t *testing.T) {
	repo := &mockRepo{
		listShipmentStatesFn: func(_ context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
			if f.Limit != 100 {
				t.Fatalf("expected default limit 100, got %d", f.Limit)
			}
			return nil, nil
		},
	}
	svc := newTestService(repo)

	_, err := svc.ListShipments(context.Background(), domain.ShipmentFilters{Limit: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListShipments_NegativeLimit(t *testing.T) {
	repo := &mockRepo{
		listShipmentStatesFn: func(_ context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
			if f.Limit != 100 {
				t.Fatalf("expected default limit 100 for negative input, got %d", f.Limit)
			}
			return nil, nil
		},
	}
	svc := newTestService(repo)

	_, err := svc.ListShipments(context.Background(), domain.ShipmentFilters{Limit: -5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListShipments_LimitCap(t *testing.T) {
	repo := &mockRepo{
		listShipmentStatesFn: func(_ context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
			if f.Limit != 500 {
				t.Fatalf("expected capped limit 500, got %d", f.Limit)
			}
			return nil, nil
		},
	}
	svc := newTestService(repo)

	_, err := svc.ListShipments(context.Background(), domain.ShipmentFilters{Limit: 9999})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListShipments_NegativeOffset(t *testing.T) {
	repo := &mockRepo{
		listShipmentStatesFn: func(_ context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
			if f.Offset != 0 {
				t.Fatalf("expected offset 0 for negative input, got %d", f.Offset)
			}
			return nil, nil
		},
	}
	svc := newTestService(repo)

	_, err := svc.ListShipments(context.Background(), domain.ShipmentFilters{Limit: 10, Offset: -3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListShipments_TrimFilters(t *testing.T) {
	repo := &mockRepo{
		listShipmentStatesFn: func(_ context.Context, f domain.ShipmentFilters) ([]domain.ShipmentState, error) {
			if f.StatusCode != "delivered" {
				t.Fatalf("expected trimmed status_code 'delivered', got %q", f.StatusCode)
			}
			if f.SourceSystem != "wms" {
				t.Fatalf("expected trimmed source_system 'wms', got %q", f.SourceSystem)
			}
			if f.RouteID != "R-1" {
				t.Fatalf("expected trimmed route_id 'R-1', got %q", f.RouteID)
			}
			return nil, nil
		},
	}
	svc := newTestService(repo)

	_, err := svc.ListShipments(context.Background(), domain.ShipmentFilters{
		StatusCode:   "  delivered  ",
		SourceSystem: " wms ",
		RouteID:      " R-1 ",
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
