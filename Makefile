.DEFAULT_GOAL := help
SHELL := /bin/bash

UV      ?= uv
COMPOSE ?= docker compose

.PHONY: help install lint fmt test test-integration

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
	$(UV) run pytest -m "not integration"

test-integration:  ## Integration tests against the running compose stack
	$(UV) run pytest -m integration
