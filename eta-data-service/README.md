# eta-data-service

Data preparation pipeline for ETA prediction and SLA control services.

The service reads shipment history and current state from PostgreSQL, aggregates them into structured data snapshots, and serves these via HTTP to external ETA prediction and SLA analytics services. It does not perform predictions itself.

## Main Features

- PostgreSQL storage via `pgxpool` with pool tuning and timeouts.
- Aggregates shipment event history (configurable window, ~30 days) into structured data snapshots: event counts, status durations, shipment age, current state.
- HTTP API: per-shipment data on demand and bulk batches for ETA model training and scoring windows.
- Redis cache for prepared data snapshots; on cache miss — enqueues async recalculation task via RabbitMQ.
- Background worker pool consumes RabbitMQ tasks, recalculates aggregates from history, and populates cache.
- ClickHouse export is handled by `analytics-exporter` (separate service).
- Observability: Prometheus metrics, OpenTelemetry tracing.

Result: data preparation latency reduced from ~900ms to ~150ms.

## API Endpoints

- `GET /healthz`
- `GET /features/{shipment_id}` — returns cached aggregated data for a single shipment; on cache miss returns `202 Accepted` and enqueues background recalculation.
- `POST /jobs/train?since=<RFC3339>&until=<RFC3339>&limit=<int>` — prepares a historical data batch for a given time window (used by ETA model training jobs).
- `POST /jobs/score?limit=<int>&offset=<int>` — prepares a batch of active shipment aggregates (used by ETA scoring jobs).

## Environment

Required:

- `POSTGRES_DSN`

Important runtime settings:

- `SERVICE_NAME` (default: `eta-data-service`)
- `LOG_LEVEL` (`debug|info|warn|error`)
- `HTTP_ADDR` (default: `:8083`)
- `OTLP_ENDPOINT`
- `REDIS_ADDR` (default: `redis:6379`)
- `REDIS_DB` (default: `0`)
- `FEATURE_TTL` (default: `10m`)
- `RABBITMQ_URL` (default: `amqp://guest:guest@rabbitmq:5672/`)
- `RABBITMQ_EXCHANGE` (default: `eta_data_exchange`)
- `RABBITMQ_QUEUE` (default: `eta_data_recalc`)
- `RABBITMQ_ROUTING_KEY` (default: `eta_data.recalc`)
- `RABBITMQ_DLQ` (default: `eta_data_recalc.dlq`)
- `RABBITMQ_PREFETCH` (default: `64`)
- `WORKER_COUNT` (default: `8`)
- `DB_QUERY_TIMEOUT`, `DB_WRITE_TIMEOUT`, `DB_CONNECT_TIMEOUT`
- `PG_MAX_CONNS`, `PG_MIN_CONNS`, `PG_MAX_CONN_LIFETIME`, `PG_MAX_CONN_IDLE_TIME`, `PG_HEALTH_CHECK_PERIOD`
- `ENABLE_AUTO_MIGRATE` (default: `false`)

## Kubernetes

Service manifests are stored in infrastructure repo:

- `../infra-logistics/k8s/services/eta-sla-service/`
