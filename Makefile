

# Makefile for CI/CD operations

.PHONY: help install test lint security build deploy clean

help:
	@echo "Available commands:"
	@echo "  install     Install dependencies"
	@echo "  test        Run tests"
	@echo "  lint        Run linting checks"
	@echo "  security    Run security checks"
	@echo "  build       Build Docker image"
	@echo "  deploy      Deploy to environment"
	@echo "  clean       Clean build artifacts"

install:
	poetry install --with dev

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=html --cov-report=term

test-integration:
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit

test-all: test-unit test-integration

lint:
	black --check src tests
	flake8 src tests
	isort --check-only src tests
	mypy src

format:
	black src tests
	isort src tests

security:
	safety check -r requirements.txt
	bandit -r src -f json

build:
	docker build -t financial-etl-pipeline:latest .

build-multi:
	docker buildx build --platform linux/amd64,linux/arm64 -t your-registry/financial-etl-pipeline:latest --push .

deploy-staging:
	@echo "Deploying to staging..."
	# Add your staging deployment commands here

deploy-production:
	@echo "Deploying to production..."
	# Add your production deployment commands here

ci-local:
	@echo "Running local CI pipeline..."
	@make lint
	@make security
	@make test-all
	@echo " Local CI pipeline completed successfully"

clean:
	@echo "Cleaning up..."
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov coverage.xml .pytest_cache
	docker system prune -f

docker-clean:
	docker system prune -af --volumes

setup-git-hooks:
	@echo "Setting up Git hooks..."
	cp .githooks/* .git/hooks/
	chmod +x .git/hooks/*

pre-commit:
	@echo "Running pre-commit checks..."
	make lint
	make security
	make test-unit