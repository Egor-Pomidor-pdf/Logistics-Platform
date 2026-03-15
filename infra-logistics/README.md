# infra-logistics

This repository contains integration infrastructure for DNS logistics microservices.
It includes local Docker Compose setup and Kubernetes observability manifests (OTEL agent/gateway + Prometheus scrape pattern).

## Local Development

Run all infra + service stubs:

```bash
docker compose up --build
```

## Kubernetes Observability

OpenTelemetry collector manifests are in `k8s/observability/`:

- `namespace.yaml`
- `otel-agent-configmap.yaml`
- `otel-agent-daemonset.yaml`
- `otel-gateway-configmap.yaml`
- `otel-gateway-deployment.yaml`
- `prometheus-scrape-example.yaml`

Flow:

- services (`shipment-core-service`, `logistics-normalizer`, etc.) -> `otel-agent` (DaemonSet)
- `otel-agent` -> `otel-gateway` (Deployment, replicas=2)
- Prometheus scrapes `otel-gateway:9464/metrics`

Apply:

```bash
kubectl apply -f k8s/observability/namespace.yaml
kubectl apply -f k8s/observability/otel-agent-configmap.yaml
kubectl apply -f k8s/observability/otel-agent-daemonset.yaml
kubectl apply -f k8s/observability/otel-gateway-configmap.yaml
kubectl apply -f k8s/observability/otel-gateway-deployment.yaml
```

## Kubernetes Services

Service manifests are centralized here:

- `k8s/services/shipment-core-service/`
- `k8s/services/eta-sla-service/`

Apply:

```bash
kubectl apply -f k8s/services/shipment-core-service/namespace.yaml
kubectl apply -f k8s/services/shipment-core-service/configmap.yaml
kubectl apply -f k8s/services/shipment-core-service/secret.example.yaml
kubectl apply -f k8s/services/shipment-core-service/deployment.yaml
kubectl apply -f k8s/services/shipment-core-service/service.yaml

kubectl apply -f k8s/services/eta-sla-service/namespace.yaml
kubectl apply -f k8s/services/eta-sla-service/configmap.yaml
kubectl apply -f k8s/services/eta-sla-service/secret.example.yaml
kubectl apply -f k8s/services/eta-sla-service/deployment.yaml
kubectl apply -f k8s/services/eta-sla-service/service.yaml
kubectl apply -f k8s/services/eta-sla-service/kafka-topics-job.yaml
kubectl apply -f k8s/services/eta-sla-service/rabbitmq-topology-job.yaml
```
