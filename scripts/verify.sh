#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${RABBITMQ_URL:?RABBITMQ_URL is required}"
export API_KEY="${API_KEY:-verify-api-key}"

uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy app tests
uv run alembic upgrade head
uv run alembic check
uv run pytest "$@"
