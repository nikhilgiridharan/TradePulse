# MarketFlow development commands
# Run 'make help' to see all available commands

.PHONY: help up down logs test lint format clean

help:
	@echo "MarketFlow Development Commands"
	@echo "================================"
	@echo "make up       - Start full stack with Docker Compose"
	@echo "make down     - Stop all services"
	@echo "make logs     - Tail logs from all services"
	@echo "make test     - Run all tests"
	@echo "make lint     - Run flake8 and mypy"
	@echo "make format   - Run black formatter"
	@echo "make clean    - Remove all containers and volumes"

up:
	docker-compose up --build -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo "MarketFlow is running. Dashboard at http://localhost:8000"

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	pytest tests/ -v --tb=short --junitxml=pytest-results.xml

lint:
	flake8 src/ tests/ --max-line-length=100
	mypy src/ --ignore-missing-imports

format:
	black src/ tests/ --line-length=100

clean:
	docker-compose down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
