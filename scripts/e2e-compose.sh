set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || cp .env.example .env
set -a
. ./.env
set +a

urlencode() { python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }

compose=(docker compose -p payments-e2e -f docker-compose.yml -f docker-compose.e2e.yml)
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then "${compose[@]}" logs --no-color --tail=300; fi
  [ "${KEEP_STACK:-0}" = "1" ] || "${compose[@]}" down -v --remove-orphans
  exit "$status"
}
trap cleanup EXIT

"${compose[@]}" up -d --build
for _ in $(seq 1 60); do
  curl -fsS http://127.0.0.1:8000/health/ready -H "X-API-Key: $API_KEY" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS http://127.0.0.1:8000/health/ready -H "X-API-Key: $API_KEY"

E2E_COMPOSE="${compose[*]}" \
E2E_BASE_URL=http://127.0.0.1:8000 \
E2E_API_KEY="$API_KEY" \
E2E_WEBHOOK_SECRET="$WEBHOOK_SECRET" \
E2E_RABBITMQ_URL="amqp://$(urlencode "$RABBITMQ_USER"):$(urlencode "$RABBITMQ_PASSWORD")@127.0.0.1:5672/" \
E2E_RETRY_BASE_DELAY="$RETRY_BASE_DELAY" \
E2E_GATEWAY_MIN_DELAY="$GATEWAY_MIN_DELAY" \
E2E_GATEWAY_MAX_DELAY="$GATEWAY_MAX_DELAY" \
  uv run pytest tests/e2e -v "$@"
