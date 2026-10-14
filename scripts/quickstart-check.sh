#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8000
KEY=change-me
BODY='{
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1042",
  "metadata": {"order_id": 1042},
  "webhook_url": "https://merchant.example/webhooks/payments"
}'

if [ -e .env ] && [ -z "${CI:-}" ]; then
  printf 'quickstart-check runs README steps on a clean checkout and overwrites .env; remove .env first\n' >&2
  exit 1
fi
export COMPOSE_PROJECT_NAME=payments-quickstart

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then docker compose ps -a; docker compose logs --no-color --tail=200; fi
  [ "${KEEP_STACK:-0}" = "1" ] || docker compose down -v --remove-orphans
  exit "$status"
}
trap cleanup EXIT

step "cp .env.example .env"
cp .env.example .env

step "docker compose up -d --build"
docker compose up -d --build

step "waiting for /health/ready"
for _ in $(seq 1 90); do
  curl -fsS "$API/health/ready" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$API/health/ready" | grep -q '"ok"' || fail "api is not ready"

step "docker compose ps"
docker compose ps -a
for service in postgres rabbitmq api outbox-relay consumer; do
  state=$(docker compose ps -a --format '{{.Service}} {{.State}}' | awk -v s="$service" '$1 == s {print $2}')
  [ "$state" = "running" ] || fail "$service is $state"
done
[ "$(docker compose ps -a --format '{{.Service}} {{.ExitCode}}' | awk '$1 == "migrate" {print $2}')" = "0" ] \
  || fail "migrate did not exit with 0"

step "POST /api/v1/payments without X-API-Key -> 401"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/payments" \
  -H "Idempotency-Key: order-1042" -H "Content-Type: application/json" -d "$BODY")
[ "$code" = "401" ] || fail "expected 401, got $code"

step "POST /api/v1/payments (README example) -> 202"
first=$(curl -fsS -X POST "$API/api/v1/payments" -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-1042" -H "Content-Type: application/json" -d "$BODY" -w '\n%{http_code}')
[ "$(tail -1 <<<"$first")" = "202" ] || fail "expected 202"
payment=$(head -1 <<<"$first")
echo "$payment"
id=$(json "d['payment_id']" <<<"$payment")
[ "$(json "d['status']" <<<"$payment")" = "pending" ] || fail "new payment must be pending"

step "same Idempotency-Key and body -> 202 replay"
headers=$(curl -fsS -D - -o /tmp/replay.json -X POST "$API/api/v1/payments" -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-1042" -H "Content-Type: application/json" -d "$BODY")
grep -qi '^idempotent-replayed: true' <<<"$headers" || fail "missing Idempotent-Replayed"
[ "$(json "d['payment_id']" </tmp/replay.json)" = "$id" ] || fail "replay returned another payment"

step "same Idempotency-Key, different body -> 409"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/payments" -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-1042" -H "Content-Type: application/json" -d "${BODY/1500.00/1.00}")
[ "$code" = "409" ] || fail "expected 409, got $code"

step "GET /api/v1/payments/$id until processed"
for _ in $(seq 1 60); do
  details=$(curl -fsS "$API/api/v1/payments/$id" -H "X-API-Key: $KEY")
  [ "$(json "d['status']" <<<"$details")" != "pending" ] && break
  sleep 1
done
echo "$details"
status=$(json "d['status']" <<<"$details")
[[ "$status" =~ ^(succeeded|failed)$ ]] || fail "payment stuck in $status"

step "webhook to merchant.example fails 3 times -> payments.new.dlq"
for _ in $(seq 1 60); do
  details=$(curl -fsS "$API/api/v1/payments/$id" -H "X-API-Key: $KEY")
  [ "$(json "d['webhook']['attempts']" <<<"$details")" = "3" ] && break
  sleep 1
done
[ "$(json "d['webhook']['attempts']" <<<"$details")" = "3" ] || fail "expected 3 webhook attempts"
[ "$(json "d['webhook']['delivered_at']" <<<"$details")" = "None" ] || fail "webhook must not be delivered"
set -a; . ./.env; set +a
for _ in $(seq 1 30); do
  dlq=$(curl -fsS -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" "http://localhost:15672/api/queues/%2F/payments.new.dlq" | json "d.get('messages', 0)")
  [ "$dlq" = "1" ] && break
  sleep 2
done
[ "$dlq" = "1" ] || fail "expected 1 message in payments.new.dlq, got $dlq"

step "docker compose restart keeps data"
docker compose restart api outbox-relay consumer
for _ in $(seq 1 60); do
  curl -fsS "$API/health/ready" >/dev/null 2>&1 && break
  sleep 2
done
after=$(curl -fsS "$API/api/v1/payments/$id" -H "X-API-Key: $KEY")
[ "$(json "d['status']" <<<"$after")" = "$status" ] || fail "payment changed after restart"

step "README quickstart OK"
