.DEFAULT_GOAL := help
SHELL := /bin/bash

UV      ?= uv
COMPOSE ?= docker compose

.PHONY: help install lint fmt test test-integration ci up down nuke logs ps topics psql open dlq-peek inject-bad

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:  ## Create .venv and install the whole workspace (dev tools included)
	$(UV) sync --all-packages

lint:  ## ruff check + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt:  ## ruff format + autofix
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test:  ## Unit tests (no broker, no database, no live feed - PRD N3)
	$(UV) run pytest

test-integration:  ## Integration tests against the running compose stack
	POSTGRES_PORT=$${POSTGRES_HOST_PORT:-5433} KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
	MINIO_ENDPOINT=http://localhost:$${MINIO_API_PORT:-9010} $(UV) run pytest -m integration -o addopts=""

ci: lint test  ## Exactly what the CI workflow runs, locally

# ---------------------------------------------------------------- stack
up: .env  ## Build + start the full stack and wait until every service is healthy
	$(COMPOSE) up -d --build --wait --wait-timeout 300
	@echo
	@echo "  Dashboard        http://localhost:8501"
	@echo "  Grafana          http://localhost:3000   (admin / admin)"
	@echo "  Redpanda Console http://localhost:8080"
	@echo "  MinIO console    http://localhost:9011   (minioadmin / minioadmin)"
	@echo "  Prometheus       http://localhost:9090"

down:  ## Stop the stack (keeps volumes; use `make nuke` to wipe data)
	$(COMPOSE) down --remove-orphans

nuke:  ## Stop the stack and delete all volumes (topics, database, archive)
	$(COMPOSE) down --remove-orphans --volumes

logs:  ## Tail logs of all services (SVC=name to narrow)
	$(COMPOSE) logs -f --tail=100 $(SVC)

ps:  ## Service status
	$(COMPOSE) ps

topics:  ## List topics with partition/offset info
	$(COMPOSE) exec redpanda rpk topic list
	$(COMPOSE) exec redpanda rpk topic describe vehicle_positions arrival_predictions -p | head -40

psql:  ## Open psql against the pipeline database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-ov} -d $${POSTGRES_DB:-ov}

dlq-peek:  ## Show the newest messages on the DLQ topics with their error headers
	@for t in dlq.vehicle_positions dlq.arrival_predictions; do \
		echo "== $$t"; \
		$(COMPOSE) exec -T redpanda rpk topic consume $$t -n 3 -o -3 -f '%p@%o key=%k headers=%h\n' 2>/dev/null || true; \
	done

inject-bad:  ## Publish a malformed message to arrival_predictions to demo the DLQ path
	@echo '{"type":"arrival_prediction","schema_version":1,"trip_id":"demo","delay_seconds":"soon"}' \
		| $(COMPOSE) exec -T redpanda rpk topic produce arrival_predictions -k demo-bad
	@echo 'this is not even json' | $(COMPOSE) exec -T redpanda rpk topic produce vehicle_positions -k demo-bad
	@echo "published 2 poison messages; watch: make dlq-peek"

open:  ## Open the dashboard in the browser
	open http://localhost:8501 2>/dev/null || xdg-open http://localhost:8501

.env:
	cp .env.example .env
	@echo "created .env from .env.example — edit if needed"
