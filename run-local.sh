#!/usr/bin/env bash
# Starts the scraper service: the HTTP API and the queue worker in one process.
#
# Local only, like the rest of YuktiSetu -- no containers. The venv is created on
# first run and reused afterwards.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  echo "creating .venv"
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -e ".[dev]"
fi

[ -f .env ] || { cp .env.example .env; echo "wrote .env from .env.example"; }

PORT="${YS_API_PORT:-8090}"
echo "scraper on http://localhost:${PORT}  (health: /health)"
exec .venv/bin/uvicorn yuktisetu_scraper.api:app --host 0.0.0.0 --port "${PORT}"
