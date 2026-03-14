# TradePulse Application Image
# =============================
# Multi-stage not required for Python; single stage with slim base.
# Uses python:3.11-slim for smaller image and security updates.

FROM python:3.11-slim

# Prevent Python from writing .pyc and unbuffered stdout for logs.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps only if needed (e.g. for pyarrow); otherwise remove.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run FastAPI by default; override to run producer or Faust worker.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
