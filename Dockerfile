# Dockerfile
# Production Dockerfile for TradePulse FastAPI service
# Only deploys the API layer — not Kafka, Faust, or producers
# Those services require a full cluster and are run locally
# for the complete pipeline demo

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for some Python packages
# gcc and python3-dev required for certain compiled extensions
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
# This layer only rebuilds when requirements.txt changes
COPY requirements.txt .

# Install Python dependencies
# --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/
COPY .env.example .

# Create non-root user for security
# Running as root in production is a security risk
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port (Railway injects $PORT automatically)
# 8000 is the default for local development
EXPOSE 8000

# Health check so Railway knows when the service is ready
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Start command — Railway overrides this with railway.toml startCommand
CMD uvicorn src.api.main:app --host 0.0.0.0 --port ${PORT:-8000}
