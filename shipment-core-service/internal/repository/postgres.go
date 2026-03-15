package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
)

var ErrShipmentNotFound = errors.New("shipment not found")

// PostgresStore is storage contract used by business service.
type PostgresStore interface {
	EnsureSchema(ctx context.Context) error
	RecordEvent(ctx context.Context, event domain.ShipmentEvent) (domain.RecordEventResult, error)
	GetShipmentState(ctx context.Context, shipmentID uuid.UUID) (domain.ShipmentState, error)
	ListShipmentStates(ctx context.Context, filters domain.ShipmentFilters) ([]domain.ShipmentState, error)
	Ping(ctx context.Context) error
	Close()
}

// PostgresRepository is pgx-backed shipment storage.
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

// PoolOptions defines pgxpool tuning.
type PoolOptions struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

func (r *PostgresRepository) EnsureSchema(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.ensure_schema")
	defer span.End()

	started := time.Now()
	if _, err := r.pool.Exec(ctx, schemaSQL); err != nil {
		span.RecordError(err)
		return fmt.Errorf("ensure schema: %w", err)
	}
	r.metrics.ObserveDBQueryDuration(ctx, "ensure_schema", time.Since(started))
	return nil
}

func (r *PostgresRepository) RecordEvent(ctx context.Context, event domain.ShipmentEvent) (domain.RecordEventResult, error) {
	ctx, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.record_event")
	defer span.End()
	span.SetAttributes(attribute.String("shipment_id", event.ShipmentID.String()), attribute.String("event_id", event.EventID))

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	insertHistoryStarted := time.Now()
	_, err = tx.Exec(ctx, `
		INSERT INTO shipment_history (
			event_id, shipment_id, status_code, status_ts, source_system,
			location_code, partner_id, route_id, estimated_delivery, payload
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (event_id) DO NOTHING
	`,
		event.EventID,
		event.ShipmentID,
		event.StatusCode,
		event.StatusTS.UTC(),
		event.SourceSystem,
		nullIfEmpty(event.LocationCode),
		nullIfEmpty(event.PartnerID),
		nullIfEmpty(event.RouteID),
		event.EstimatedDelivery,
		event.Payload,
	)
	if err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, fmt.Errorf("insert history: %w", err)
	}
	r.metrics.ObserveDBQueryDuration(ctx, "insert_history", time.Since(insertHistoryStarted))

	upsertStarted := time.Now()
	result, err := tx.Exec(ctx, `
		INSERT INTO shipment_state (
			shipment_id, last_event_id, status_code, status_ts, source_system,
			location_code, partner_id, route_id, estimated_delivery, payload, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, NOW())
		ON CONFLICT (shipment_id)
		DO UPDATE SET
			last_event_id = EXCLUDED.last_event_id,
			status_code = EXCLUDED.status_code,
			status_ts = EXCLUDED.status_ts,
			source_system = EXCLUDED.source_system,
			location_code = EXCLUDED.location_code,
			partner_id = EXCLUDED.partner_id,
			route_id = EXCLUDED.route_id,
			estimated_delivery = EXCLUDED.estimated_delivery,
			payload = EXCLUDED.payload,
			updated_at = NOW()
		WHERE
			EXCLUDED.status_ts > shipment_state.status_ts
			OR (
				EXCLUDED.status_ts = shipment_state.status_ts
				AND EXCLUDED.last_event_id <> shipment_state.last_event_id
			)
	`,
		event.ShipmentID,
		event.EventID,
		event.StatusCode,
		event.StatusTS.UTC(),
		event.SourceSystem,
		nullIfEmpty(event.LocationCode),
		nullIfEmpty(event.PartnerID),
		nullIfEmpty(event.RouteID),
		event.EstimatedDelivery,
		event.Payload,
	)
	if err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, fmt.Errorf("upsert shipment_state: %w", err)
	}
	r.metrics.ObserveDBQueryDuration(ctx, "upsert_state", time.Since(upsertStarted))

	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		return domain.RecordEventResult{}, fmt.Errorf("commit tx: %w", err)
	}

	stateUpdated := result.RowsAffected() > 0
	return domain.RecordEventResult{StateUpdated: stateUpdated}, nil
}

func (r *PostgresRepository) GetShipmentState(ctx context.Context, shipmentID uuid.UUID) (domain.ShipmentState, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.get_shipment_state")
	defer span.End()
	span.SetAttributes(attribute.String("shipment_id", shipmentID.String()))

	queryStarted := time.Now()
	row := r.pool.QueryRow(ctx, `
		SELECT
			shipment_id,
			last_event_id,
			status_code,
			status_ts,
			source_system,
			COALESCE(location_code, ''),
			COALESCE(partner_id, ''),
			COALESCE(route_id, ''),
			estimated_delivery,
			COALESCE(payload, '{}'::jsonb),
			updated_at
		FROM shipment_state
		WHERE shipment_id = $1
	`, shipmentID)

	state, err := scanShipmentState(row)
	r.metrics.ObserveDBQueryDuration(ctx, "get_state", time.Since(queryStarted))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ShipmentState{}, ErrShipmentNotFound
		}
		span.RecordError(err)
		return domain.ShipmentState{}, fmt.Errorf("query shipment_state: %w", err)
	}
	return state, nil
}

func (r *PostgresRepository) ListShipmentStates(ctx context.Context, filters domain.ShipmentFilters) ([]domain.ShipmentState, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	ctx, span := r.tracer.Start(ctx, "postgres.list_shipment_states")
	defer span.End()

	query, args := buildListQuery(filters)
	queryStarted := time.Now()
	rows, err := r.pool.Query(ctx, query, args...)
	r.metrics.ObserveDBQueryDuration(ctx, "list_states", time.Since(queryStarted))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("list shipment_state: %w", err)
	}
	defer rows.Close()

	items := make([]domain.ShipmentState, 0, filters.Limit)
	for rows.Next() {
		state, err := scanShipmentState(rows)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("scan shipment_state: %w", err)
		}
		items = append(items, state)
	}

	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("iterate shipment_state rows: %w", err)
	}
	return items, nil
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) Close() {
	if r == nil || r.pool == nil {
		return
	}
	r.pool.Close()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanShipmentState(row scanner) (domain.ShipmentState, error) {
	var state domain.ShipmentState
	var payload []byte
	if err := row.Scan(
		&state.ShipmentID,
		&state.LastEventID,
		&state.StatusCode,
		&state.StatusTS,
		&state.SourceSystem,
		&state.LocationCode,
		&state.PartnerID,
		&state.RouteID,
		&state.EstimatedDelivery,
		&payload,
		&state.UpdatedAt,
	); err != nil {
		return domain.ShipmentState{}, err
	}
	state.Payload = append(state.Payload[:0], payload...)
	return state, nil
}

func buildListQuery(filters domain.ShipmentFilters) (string, []any) {
	args := make([]any, 0, 5)
	where := make([]string, 0, 3)
	index := 1

	if filters.StatusCode != "" {
		where = append(where, fmt.Sprintf("status_code = $%d", index))
		args = append(args, filters.StatusCode)
		index++
	}
	if filters.SourceSystem != "" {
		where = append(where, fmt.Sprintf("source_system = $%d", index))
		args = append(args, filters.SourceSystem)
		index++
	}
	if filters.RouteID != "" {
		where = append(where, fmt.Sprintf("route_id = $%d", index))
		args = append(args, filters.RouteID)
		index++
	}

	if filters.Limit <= 0 || filters.Limit > 500 {
		filters.Limit = 100
	}
	if filters.Offset < 0 {
		filters.Offset = 0
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
			estimated_delivery,
			COALESCE(payload, '{}'::jsonb),
			updated_at
		FROM shipment_state
	`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY status_ts DESC LIMIT $%d OFFSET $%d", index, index+1)
	args = append(args, filters.Limit, filters.Offset)

	return query, args
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS shipment_history (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    shipment_id UUID NOT NULL,
    status_code TEXT NOT NULL,
    status_ts TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    location_code TEXT NULL,
    partner_id TEXT NULL,
    route_id TEXT NULL,
    estimated_delivery TIMESTAMPTZ NULL,
    payload JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shipment_history_shipment_status_ts
    ON shipment_history (shipment_id, status_ts DESC);
CREATE INDEX IF NOT EXISTS idx_shipment_history_status_code
    ON shipment_history (status_code);
CREATE INDEX IF NOT EXISTS idx_shipment_history_source_system
    ON shipment_history (source_system);

CREATE TABLE IF NOT EXISTS shipment_state (
    shipment_id UUID PRIMARY KEY,
    last_event_id TEXT NOT NULL,
    status_code TEXT NOT NULL,
    status_ts TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    location_code TEXT NULL,
    partner_id TEXT NULL,
    route_id TEXT NULL,
    estimated_delivery TIMESTAMPTZ NULL,
    payload JSONB NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shipment_state_status_code
    ON shipment_state (status_code);
CREATE INDEX IF NOT EXISTS idx_shipment_state_route_id
    ON shipment_state (route_id);
CREATE INDEX IF NOT EXISTS idx_shipment_state_status_ts
    ON shipment_state (status_ts DESC);
`
