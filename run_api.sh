#!/usr/bin/env bash
# Run TradePulse API locally (no Docker). Requires .env in project root.
set -e
cd "$(dirname "$0")"
export PYTHONPATH=.
if [ -d ".venv" ]; then
  .venv/bin/uvicorn src.api.main:app --host 0.0.0.0 --port 8000
else
  python3 -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
fi
