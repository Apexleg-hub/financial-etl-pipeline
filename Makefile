---
# =============================================================================
# MAKEFILE
# =============================================================================
# File: Makefile

.PHONY: help build up down restart logs shell test clean init-db

help:
	@echo "ETL Pipeline Commands:"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make restart    - Restart all services"
	@echo "  make logs       - View logs"
	@echo "  make shell      - Open shell in worker container"
	@echo "  make test       - Run tests"
	@echo "  make clean      - Clean up volumes and containers"
	@echo "  make init-db    - Initialize database schema"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Services starting... Access:"
	@echo "  Prefect UI:  http://localhost:4200"
	@echo "  Grafana:     http://localhost:3000"
	@echo "  PGAdmin:     http://localhost:5050"
	@echo "  MinIO:       http://localhost:9001"

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

logs-worker:
	docker-compose logs -f etl-worker

shell:
	docker-compose exec etl-worker /bin/bash

shell-db:
	docker-compose exec postgres psql -U etl_user -d etl_pipeline

test:
	docker-compose exec etl-worker pytest tests/ -v

clean:
	docker-compose down -v
	rm -rf logs/* data/raw/* data/staging/* data/processed/*

init-db:
	docker-compose exec postgres psql -U etl_user -d etl_pipeline -f /docker-entrypoint-initdb.d/schema.sql
	@echo "Database initialized"

deploy-local:
	@echo "Deploying ETL pipeline locally..."
	cp .env.template .env
	@echo "Please edit .env file with your API keys"
	@echo "Then run: make up"