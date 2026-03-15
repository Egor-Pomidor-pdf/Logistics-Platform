package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/eta-data-service/internal/domain"
	"github.com/dns/logistics/eta-data-service/internal/metrics"
)

// PostgresStore is storage contract used by eta-data-service.
type PostgresStore interface {
	EnsureSchema(ctx context.Context) error
	Ping(ctx context.Context) error
	Close()

	// ListShipmentHistory returns shipment_history rows filtered by time window and optional route/source.
	ListShipmentHistory(ctx context.Context, f ShipmentHistoryFilter) ([]domain.ShipmentEvent, error)

	// ListActiveShipmentStates returns latest states for shipments considered "active" by caller criteria.
	ListActiveShipmentStates(ctx context.Context, f ShipmentStateFilter) ([]domain.ShipmentState, error)
}

// ShipmentHistoryFilter defines filters for history queries.
type ShipmentHistoryFilter struct {
	Since        time.Time
	Until        time.Time
	SourceSystem string
	RouteID      string
	Limit        int
}

// ShipmentStateFilter defines filters for current state queries.
type ShipmentStateFilter struct {
	StatusCode   string
	SourceSystem string
	RouteID      string
	Limit        int
	Offset       int
}

// PostgresRepository is pgx-backed implementation.
type PostgresRepository struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
	writeTimeout time.Duration
	tracer       trace.Tracer
	metrics      *metrics.Collector
}

// NewPostgresRepository creates repository.
func NewPostgresRepository(
	pool *pgxpool.Pool,
	queryTimeout time.Duration,
	writeTimeout time.Duration,
	tracer trace.Tracer,
	metricCollector *metrics.Collector,
) *PostgresRepository {
	return &PostgresRepository{
		pool:         pool,
		queryTimeout: queryTimeout,
		writeTimeout: writeTimeout,
		tracer:       tracer,
		metrics:      metricCollector,
	}
}

// PoolOptions defines pgxpool tuning.
type PoolOptions struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// NewPool creates pgx pool with provided tuning.
func NewPool(ctx context.Context, dsn string, opts PoolOptions) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = opts.MinConns
	cfg.MaxConnLifetime = opts.MaxConnLifetime
	cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	cfg.HealthCheckPeriod = opts.HealthCheckPeriod
	cfg.ConnConfig.ConnectTimeout = opts.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	return pool, nil
}

// EnsureSchema is a no-op: schema is owned by shipment-core-service.
func (r *PostgresRepository) EnsureSchema(context.Context) error {
	return nil
}

// Ping checks DB connectivity.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	return r.pool.Ping(ctx)
}

// Close closes pool.
func (r *PostgresRepository) Close() {
	if r == nil || r.pool == nil {
		return
	}
	r.pool.Close()
}

// ListShipmentHistory returns history rows for the given filters.
func (r *PostgresRepository) ListShipmentHistory(ctx context.Context, f ShipmentHistoryFilter) ([]domain.ShipmentEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.list_shipment_history")
	defer span.End()

	if f.Limit <= 0 {
		f.Limit = 1000
	}
	if f.Limit > 10000 {
		f.Limit = 10000
	}

	args := make([]any, 0, 5)
	where := "WHERE status_ts >= $1 AND status_ts <= $2"
	args = append(args, f.Since.UTC(), f.Until.UTC())
	argIdx := 3

	if f.SourceSystem != "" {
		where += fmt.Sprintf(" AND source_system = $%d", argIdx)
		args = append(args, f.SourceSystem)
		argIdx++
	}
	if f.RouteID != "" {
		where += fmt.Sprintf(" AND route_id = $%d", argIdx)
		args = append(args, f.RouteID)
		argIdx++
	}

	args = append(args, f.Limit)

	query := `
		SELECT
			event_id,
			shipment_id,
			status_code,
			status_ts,
			source_system,
			COALESCE(location_code, ''),
			COALESCE(partner_id, ''),
			COALESCE(route_id, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM shipment_history
	` + where + `
		ORDER BY shipment_id, status_ts
		LIMIT $` + fmt.Sprint(argIdx)

	started := time.Now()
	rows, err := r.pool.Query(ctx, query, args...)
	r.metrics.ObserveDBQueryDuration(ctx, "list_shipment_history", time.Since(started))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("list shipment_history: %w", err)
	}
	defer rows.Close()

	var items []domain.ShipmentEvent
	for rows.Next() {
		var ev domain.ShipmentEvent
		var payload []byte
		if err := rows.Scan(
			&ev.EventID,
			&ev.ShipmentID,
			&ev.StatusCode,
			&ev.StatusTS,
			&ev.SourceSystem,
			&ev.LocationCode,
			&ev.PartnerID,
			&ev.RouteID,
			&payload,
		); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("scan shipment_history: %w", err)
		}
		ev.Payload = append(ev.Payload[:0], payload...)
		items = append(items, ev)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("iterate shipment_history rows: %w", err)
	}

	return items, nil
}

// ListActiveShipmentStates returns snapshot from shipment_state using filters.
func (r *PostgresRepository) ListActiveShipmentStates(ctx context.Context, f ShipmentStateFilter) ([]domain.ShipmentState, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.list_active_shipment_states")
	defer span.End()

	if f.Limit <= 0 {
		f.Limit = 500
	}
	if f.Limit > 5000 {
		f.Limit = 5000
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	args := make([]any, 0, 5)
	where := ""
	argIdx := 1

	if f.StatusCode != "" {
		where += fmt.Sprintf("status_code = $%d", argIdx)
		args = append(args, f.StatusCode)
		argIdx++
	}
	if f.SourceSystem != "" {
		if where != "" {
			where += " AND "
		}
		where += fmt.Sprintf("source_system = $%d", argIdx)
		args = append(args, f.SourceSystem)
		argIdx++
	}
	if f.RouteID != "" {
		if where != "" {
			where += " AND "
		}
		where += fmt.Sprintf("route_id = $%d", argIdx)
		args = append(args, f.RouteID)
		argIdx++
	}

	args = append(args, f.Limit, f.Offset)

	query := `
		SELECT
			shipment_id,
			last_event_id,
			status_code,
			status_ts,
			source_system,
			COALESCE(location_code, ''),
			COALESCE(partner_id, ''),
			COALESCE(route_id, ''),
			COALESCE(payload, '{}'::jsonb),
			updated_at
		FROM shipment_state
	`
	if where != "" {
		query += " WHERE " + where
	}
	query += fmt.Sprintf(" ORDER BY status_ts DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)

	started := time.Now()
	rows, err := r.pool.Query(ctx, query, args...)
	r.metrics.ObserveDBQueryDuration(ctx, "list_active_states", time.Since(started))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("list shipment_state: %w", err)
	}
	defer rows.Close()

	var items []domain.ShipmentState
	for rows.Next() {
		var s domain.ShipmentState
		var payload []byte
		if err := rows.Scan(
			&s.ShipmentID,
			&s.LastEventID,
			&s.StatusCode,
			&s.StatusTS,
			&s.SourceSystem,
			&s.LocationCode,
			&s.PartnerID,
			&s.RouteID,
			&payload,
			&s.UpdatedAt,
		); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("scan shipment_state: %w", err)
		}
		s.Payload = append(s.Payload[:0], payload...)
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("iterate shipment_state rows: %w", err)
	}

	return items, nil
}

// helper to satisfy linter when span attributes need uuid.
func uuidAttr(key string, id uuid.UUID) attribute.KeyValue {
	return attribute.String(key, id.String())
}

