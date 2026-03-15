FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY .env.example .

ENV PYTHONPATH=/app
ENV DEMO_MODE=true

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

CMD uvicorn src.api.main:app --host 0.0.0.0 --port ${PORT:-8000}
