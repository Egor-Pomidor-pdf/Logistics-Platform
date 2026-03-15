package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/analytics-exporter/internal/domain"
	"github.com/dns/logistics/analytics-exporter/internal/metrics"
)

// PostgresRepository provides read-only access for analytics exports.
type PostgresRepository struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
	tracer       trace.Tracer
	metrics      *metrics.Collector
}

// New creates repository.
func New(pool *pgxpool.Pool, queryTimeout time.Duration, tracer trace.Tracer, metricCollector *metrics.Collector) *PostgresRepository {
	return &PostgresRepository{
		pool:         pool,
		queryTimeout: queryTimeout,
		tracer:       tracer,
		metrics:      metricCollector,
	}
}

// Ping checks DB connectivity.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	return r.pool.Ping(ctx)
}

// ListHistorySince returns shipment_history rows from cursor to until.
func (r *PostgresRepository) ListHistorySince(
	ctx context.Context,
	cursorTS time.Time,
	cursorID string,
	until time.Time,
	limit int,
) ([]domain.ShipmentEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.list_history_since")
	defer span.End()

	if limit <= 0 {
		limit = 5000
	}
	if limit > 20000 {
		limit = 20000
	}

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
		WHERE (status_ts > $1 OR (status_ts = $1 AND event_id > $2))
			AND status_ts <= $3
		ORDER BY status_ts, event_id
		LIMIT $4
	`

	started := time.Now()
	rows, err := r.pool.Query(ctx, query, cursorTS.UTC(), cursorID, until.UTC(), limit)
	r.metrics.ObserveDBQueryDuration(ctx, "list_history_since", time.Since(started))
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

// ListStatesSince returns shipment_state rows from cursor.
func (r *PostgresRepository) ListStatesSince(
	ctx context.Context,
	cursorTS time.Time,
	cursorID string,
	limit int,
) ([]domain.ShipmentState, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.list_states_since")
	defer span.End()

	if limit <= 0 {
		limit = 5000
	}
	if limit > 20000 {
		limit = 20000
	}

	uid := uuid.Nil
	if cursorID != "" {
		if parsed, err := uuid.Parse(cursorID); err == nil {
			uid = parsed
		}
	}

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
		WHERE (updated_at > $1 OR (updated_at = $1 AND shipment_id > $2))
		ORDER BY updated_at, shipment_id
		LIMIT $3
	`

	started := time.Now()
	rows, err := r.pool.Query(ctx, query, cursorTS.UTC(), uid, limit)
	r.metrics.ObserveDBQueryDuration(ctx, "list_states_since", time.Since(started))
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
