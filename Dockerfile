# Dockerfile for TradePulse — Hugging Face Spaces compatible
# HF Spaces requires port 7860 and a non-root user

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Create non-root user — HF Spaces requirement
# HF Spaces runs containers with restricted permissions
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user
ENV PATH=/home/user/.local/bin:$PATH

# Set working directory to user-owned location
WORKDIR $HOME/app

# Copy requirements first for better Docker layer caching
COPY --chown=user requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=user . .

# Set Python path so imports work
ENV PYTHONPATH=$HOME/app

# Demo mode is required for HF Spaces deployment
# Full pipeline requires Kafka, Faust, AWS which are not available on HF
ENV DEMO_MODE=true

# Expose Hugging Face Spaces required port
EXPOSE 7860

# Start FastAPI on port 7860 — required by Hugging Face Spaces
# Shell form so environment variables expand correctly
CMD uvicorn src.api.main:app --host 0.0.0.0 --port 7860
