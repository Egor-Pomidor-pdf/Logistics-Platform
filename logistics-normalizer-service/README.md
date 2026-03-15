# logistics-normalizer-service

This service receives raw logistics events from internal systems and partners via Kafka and REST, validates and normalizes them, and publishes the result to `normalized_logistics_events`.
Normalization covers status codes, predicted delivery time fields (ETA), and timezone alignment.
Invalid payloads are redirected to `logistics_events_invalid`, while duplicate events are filtered by `event_id` via Redis.

## Environment

- `KAFKA_BROKERS` (example: `kafka:9092`)
- `RAW_TOPIC` (default: `raw_logistics_events`)
- `NORMALIZED_TOPIC` (default: `normalized_logistics_events`)
- `GROUP_ID` (default: `logistics-normalizer`)
- `REDIS_ADDR` (example: `redis:6379`)
- `SERVICE_NAME` (default: `logistics-normalizer`)
- `LOG_LEVEL` (`info` or `debug`)
- `OTLP_ENDPOINT` (example: `otel-agent.observability.svc.cluster.local:4317`)

## Observability Manifests

Kubernetes examples for OpenTelemetry agent/gateway and Prometheus scrape config are under:

- `deploy/observability/otel-agent-configmap.yaml`
- `deploy/observability/otel-agent-daemonset.yaml`
- `deploy/observability/otel-gateway-configmap.yaml`
- `deploy/observability/otel-gateway-deployment.yaml`
- `deploy/observability/prometheus-scrape-example.yaml`
