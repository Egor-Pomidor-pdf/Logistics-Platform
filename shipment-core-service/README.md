# shipment-core-service

`shipment-core-service` is the logistics domain core for shipment state storage and retrieval.
It consumes normalized events from Kafka, writes immutable `shipment_history`, maintains latest `shipment_state`, and exposes read APIs for internal clients.

## Main Features

- PostgreSQL storage via `pgxpool` with pool tuning and timeouts.
- Schema bootstrap for `shipment_history` and `shipment_state` with indexes.
- Upsert logic updates `shipment_state` only when incoming `status_ts` is newer.
- Kafka consumer for `normalized_logistics_events` -> `CoreService.RecordShipmentEvent`.
- HTTP API for liveness/readiness, get by id, list with filters/pagination — with input validation and role-based authorization.
- gRPC API for internal services (`GetShipment`, `ListShipments`, `Ping`) — with input validation and role-based authorization.
- OpenTelemetry SDK for tracing and metrics over OTLP.
- Structured JSON logs enriched with `trace_id`, `span_id`, `shipment_id`.

## API Endpoints

HTTP:

- `GET /healthz`
- `GET /readyz`
- `GET /api/v1/shipments/{shipment_id}`
- `GET /api/v1/shipments?status_code=&source_system=&route_id=&limit=&offset=`

gRPC:

- `shipmentcore.v1.ShipmentCoreService/GetShipment`
- `shipmentcore.v1.ShipmentCoreService/ListShipments`
- `shipmentcore.v1.ShipmentCoreService/Ping`

## Environment

Required:

- `POSTGRES_DSN`

Important runtime settings:

- `SERVICE_NAME` (default: `shipment-core-service`)
- `LOG_LEVEL` (`debug|info|warn|error`)
- `HTTP_ADDR` (default: `:8080`)
- `GRPC_ADDR` (default: `:9090`)
- `OTLP_ENDPOINT` (example: `otel-agent.observability.svc.cluster.local:4317`)
- `KAFKA_BROKERS` (example: `kafka:9092`)
- `NORMALIZED_TOPIC` (default: `normalized_logistics_events`)
- `KAFKA_GROUP_ID` (default: `shipment-core-service`)
- `KAFKA_CONSUMER_ENABLED` (default: `true`)
- `DB_QUERY_TIMEOUT`, `DB_WRITE_TIMEOUT`, `DB_CONNECT_TIMEOUT`
- `PG_MAX_CONNS`, `PG_MIN_CONNS`, `PG_MAX_CONN_LIFETIME`, `PG_MAX_CONN_IDLE_TIME`, `PG_HEALTH_CHECK_PERIOD`
- `ENABLE_AUTO_MIGRATE` (default: `true`)

## Kubernetes

Service manifests are stored in infrastructure repo:

- `../infra-logistics/k8s/services/shipment-core-service/`

This repository keeps service code only.
