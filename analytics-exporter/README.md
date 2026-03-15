# analytics-exporter

Incremental data export service for ClickHouse used by analytics and SLA reporting.

The service reads shipment history and current state from PostgreSQL, exports them to ClickHouse over HTTP in batches, and tracks cursors for exactly-once-ish delivery semantics (with idempotency enforced on the ClickHouse side). It does not build ETA predictions.

## Main Features

- Incremental export of `shipment_history` and `shipment_state` using Postgres cursors.
- Advisory locks to prevent concurrent export of the same stream across instances.
- Configurable export interval, lag, and batch size.
- ClickHouse HTTP export via `JSONEachRow`.
- Observability: Prometheus metrics, OpenTelemetry tracing.

## API Endpoints

- `GET /healthz`
- `POST /jobs/export` — triggers export cycle once (useful for ops).

## Environment

Required:

- `POSTGRES_DSN`
- `CLICKHOUSE_URL`

Important runtime settings:

- `SERVICE_NAME` (default: `analytics-exporter`)
- `LOG_LEVEL` (`debug|info|warn|error`)
- `HTTP_ADDR` (default: `:8090`)
- `OTLP_ENDPOINT`
- `EXPORT_INTERVAL` (default: `1m`)
- `EXPORT_LAG` (default: `1m`)
- `EXPORT_BATCH_SIZE` (default: `5000`)
- `EXPORT_BACKFILL_WINDOW` (default: `720h`)
- `EXPORT_HISTORY` / `EXPORT_STATE` (default: `true`)
- `CLICKHOUSE_DATABASE` (default: `default`)
- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_TIMEOUT` (default: `5s`)
- `CLICKHOUSE_HISTORY_TABLE` (default: `shipment_history`)
- `CLICKHOUSE_STATE_TABLE` (default: `shipment_state`)
- `DB_QUERY_TIMEOUT`, `DB_CONNECT_TIMEOUT`
- `PG_MAX_CONNS`, `PG_MIN_CONNS`, `PG_MAX_CONN_LIFETIME`, `PG_MAX_CONN_IDLE_TIME`, `PG_HEALTH_CHECK_PERIOD`
- `ENABLE_AUTO_MIGRATE` (default: `true`)

## ClickHouse Tables (Recommended)

History:

```sql
CREATE TABLE shipment_history (
    event_id String,
    shipment_id UUID,
    status_code String,
    status_ts DateTime64(3),
    source_system String,
    location_code String,
    partner_id String,
    route_id String,
    payload String,
    exported_at DateTime64(3)
) ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (shipment_id, status_ts, event_id);
```

State:

```sql
CREATE TABLE shipment_state (
    shipment_id UUID,
    last_event_id String,
    status_code String,
    status_ts DateTime64(3),
    source_system String,
    location_code String,
    partner_id String,
    route_id String,
    payload String,
    updated_at DateTime64(3),
    exported_at DateTime64(3)
) ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (shipment_id);
```
