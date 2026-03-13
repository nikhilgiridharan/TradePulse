#!/usr/bin/env bash
# MarketFlow – run the full stack locally with Docker Compose.
# Usage: ./run_local.sh
# Requires: Docker and Docker Compose (or "docker compose" plugin).

set -e
cd "$(dirname "$0")"

if ! command -v docker &>/dev/null; then
  echo "Docker is not installed or not in PATH. Install Docker Desktop from https://www.docker.com/products/docker-desktop/"
  exit 1
fi

# Prefer "docker compose" (v2) then "docker-compose" (v1)
if docker compose version &>/dev/null; then
  COMPOSE="docker compose"
elif command -v docker-compose &>/dev/null; then
  COMPOSE="docker-compose"
else
  echo "Docker Compose not found. Install Docker Desktop (includes Compose) or run: pip install docker-compose"
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "Creating .env from .env.example ..."
  cp .env.example .env
  echo "Edit .env to set POLYGON_API_KEY and AWS_* if you need full functionality."
fi

echo "Starting MarketFlow stack (Zookeeper, Kafka, Schema Registry, App)..."
$COMPOSE up -d --build

echo ""
echo "Waiting for services to be healthy (~30s)..."
sleep 30

echo ""
echo "Stack is up. Checking health..."
curl -sf http://localhost:8000/health | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8000/health

echo ""
echo "Done. FastAPI: http://localhost:8000  |  Docs: http://localhost:8000/docs  |  Kafka: localhost:9092"
