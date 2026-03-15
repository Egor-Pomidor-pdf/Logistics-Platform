MINIKUBE_PROFILE ?= logistics
K8S_DIR         := infra-logistics/k8s
INFRA_COMPOSE   := infra-logistics/docker-compose.yml
IMAGE_PREFIX    := ghcr.io/dns/logistics

SERVICES := logistics-normalizer-service shipment-core-service eta-data-service analytics-exporter

.PHONY: help infra-up infra-down minikube-start minikube-stop build-all deploy-all undeploy-all dev-up test

help:
	@echo "Logistics Platform"
	@echo ""
	@echo "Local (docker-compose infra + go run):"
	@echo "  make infra-up        Start postgres, kafka, redis, rabbitmq, clickhouse"
	@echo "  make infra-down      Stop infra"
	@echo ""
	@echo "Minikube (full k8s):"
	@echo "  make minikube-start  Start Minikube cluster (4 CPU, 6GB RAM)"
	@echo "  make build-all       Build all service images inside Minikube docker"
	@echo "  make deploy-all      Apply all k8s manifests"
	@echo "  make undeploy-all    Delete all service deployments"
	@echo "  make minikube-stop   Stop Minikube cluster"
	@echo ""
	@echo "Tests:"
	@echo "  make test            Run all unit tests"

# ── Local infra ─────────────────────────────────────────────────────────────

infra-up:
	docker compose -f $(INFRA_COMPOSE) up postgres kafka redis rabbitmq clickhouse -d

infra-down:
	docker compose -f $(INFRA_COMPOSE) down

# ── Minikube ─────────────────────────────────────────────────────────────────

minikube-start:
	minikube start --profile=$(MINIKUBE_PROFILE) --cpus=4 --memory=6144

minikube-stop:
	minikube stop --profile=$(MINIKUBE_PROFILE)

# Build all images directly inside Minikube's docker daemon (no registry needed).
build-all:
	@eval $$(minikube -p $(MINIKUBE_PROFILE) docker-env) && \
	for svc in $(SERVICES); do \
	  echo "→ Building $$svc"; \
	  docker build -t $(IMAGE_PREFIX)/$$svc:local $$svc/; \
	done

deploy-all:
	kubectl apply -f $(K8S_DIR)/services/logistics-normalizer-service/
	kubectl apply -f $(K8S_DIR)/services/shipment-core-service/
	kubectl apply -f $(K8S_DIR)/services/eta-data-service/
	kubectl apply -f $(K8S_DIR)/services/analytics-exporter/

undeploy-all:
	kubectl delete -f $(K8S_DIR)/services/logistics-normalizer-service/ --ignore-not-found
	kubectl delete -f $(K8S_DIR)/services/shipment-core-service/ --ignore-not-found
	kubectl delete -f $(K8S_DIR)/services/eta-data-service/ --ignore-not-found
	kubectl delete -f $(K8S_DIR)/services/analytics-exporter/ --ignore-not-found

# ── Tests ────────────────────────────────────────────────────────────────────

test:
	@for svc in $(SERVICES); do \
	  echo "→ Testing $$svc"; \
	  cd $$svc && go test ./internal/... && cd ..; \
	done
