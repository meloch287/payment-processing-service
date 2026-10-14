import argparse
import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.e2e.receiver import WebhookReceiver


@dataclass(frozen=True, slots=True)
class Options:
    base_url: str
    api_key: str
    database_url: str
    webhook_host: str
    receiver_bind: str
    api_requests: int
    api_concurrency: int
    relay_events: int
    e2e_payments: int


def percentile(values: list[float], share: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(share * (len(ordered) - 1)))]


def body(webhook_url: str) -> dict[str, Any]:
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "bench",
        "metadata": {"bench": True},
        "webhook_url": webhook_url,
    }


async def drain(engine: AsyncEngine, seconds: float = 600) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        async with engine.connect() as connection:
            busy = await connection.scalar(
                text(
                    "SELECT count(*) FROM payments "
                    "WHERE status = 'pending' OR webhook_delivered_at IS NULL"
                )
            )
        if not busy:
            break
        await asyncio.sleep(1)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))


async def bench_api(options: Options, receiver: WebhookReceiver) -> dict[str, float]:
    latencies: list[float] = []
    url = f"http://{options.webhook_host}:{receiver.port}/ok/api"
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(options.api_requests):
        queue.put_nowait(index)

    errors = 0

    async def worker(client: httpx.AsyncClient) -> None:
        nonlocal errors
        while not queue.empty():
            queue.get_nowait()
            started = time.perf_counter()
            try:
                response = await client.post(
                    "/api/v1/payments",
                    json=body(url),
                    headers={"Idempotency-Key": str(uuid.uuid4())},
                )
            except httpx.HTTPError:
                errors += 1
                continue
            latencies.append(time.perf_counter() - started)
            if response.status_code != 202:
                errors += 1

    limits = httpx.Limits(max_connections=options.api_concurrency)
    async with httpx.AsyncClient(
        base_url=options.base_url,
        headers={"X-API-Key": options.api_key},
        limits=limits,
        timeout=60,
    ) as client:
        started = time.perf_counter()
        await asyncio.gather(*(worker(client) for _ in range(options.api_concurrency)))
        elapsed = time.perf_counter() - started
    return {
        "rps": len(latencies) / elapsed,
        "p50_ms": percentile(latencies, 0.5) * 1000,
        "p95_ms": percentile(latencies, 0.95) * 1000,
        "p99_ms": percentile(latencies, 0.99) * 1000,
        "errors": float(errors),
    }


async def bench_relay(options: Options, engine: AsyncEngine) -> dict[str, float]:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO outbox (id, aggregate_id, event_type, routing_key, payload) "
                "SELECT event_id, payment_id, 'payment.created', 'payments.new', "
                "jsonb_build_object('event_id', event_id, 'event_type', 'payment.created', "
                "'payment_id', payment_id) FROM (SELECT gen_random_uuid() AS event_id, "
                "gen_random_uuid() AS payment_id FROM generate_series(1, :count)) AS rows"
            ),
            {"count": options.relay_events},
        )
    started = time.perf_counter()
    while True:
        async with engine.connect() as connection:
            left = await connection.scalar(
                text("SELECT count(*) FROM outbox WHERE published_at IS NULL")
            )
        if not left:
            break
        await asyncio.sleep(0.05)
    elapsed = time.perf_counter() - started
    return {"events_per_s": options.relay_events / elapsed, "seconds": elapsed}


async def bench_e2e(
    options: Options, engine: AsyncEngine, receiver: WebhookReceiver
) -> dict[str, float]:
    paths = [f"/ok/e2e-{uuid.uuid4()}" for _ in range(options.e2e_payments)]
    sent: dict[str, float] = {}
    limits = httpx.Limits(max_connections=50)
    async with httpx.AsyncClient(
        base_url=options.base_url,
        headers={"X-API-Key": options.api_key},
        limits=limits,
        timeout=30,
    ) as client:
        semaphore = asyncio.Semaphore(50)

        async def create(path: str) -> None:
            async with semaphore:
                sent[path] = time.monotonic()
                response = await client.post(
                    "/api/v1/payments",
                    json=body(f"http://{options.webhook_host}:{receiver.port}{path}"),
                    headers={"Idempotency-Key": str(uuid.uuid4())},
                )
                response.raise_for_status()

        started = time.monotonic()
        await asyncio.gather(*(create(path) for path in paths))
    finished: dict[str, float] = {}
    deadline = time.monotonic() + 900
    while len(finished) < len(paths) and time.monotonic() < deadline:
        for path in paths:
            if path not in finished:
                received = receiver.received(path)
                if received:
                    finished[path] = received[0].received_at
        await asyncio.sleep(0.2)
    elapsed = max(finished.values()) - started
    latencies = [finished[path] - sent[path] for path in finished]
    async with engine.connect() as connection:
        outbox_ms = await connection.scalar(
            text(
                "SELECT avg(extract(epoch FROM published_at - created_at)) * 1000 "
                "FROM outbox WHERE published_at IS NOT NULL"
            )
        )
    return {
        "payments_per_s": len(finished) / elapsed,
        "seconds": elapsed,
        "delivered": float(len(finished)),
        "p50_s": percentile(latencies, 0.5),
        "p95_s": percentile(latencies, 0.95),
        "outbox_publish_ms": float(outbox_ms or 0),
    }


async def run(options: Options, phases: list[str]) -> dict[str, dict[str, float]]:
    engine = create_async_engine(options.database_url)
    receiver = WebhookReceiver(host=options.receiver_bind)
    receiver.start()
    results: dict[str, dict[str, float]] = {}
    try:
        for phase in phases:
            await drain(engine)
            if phase == "api":
                results[phase] = await bench_api(options, receiver)
            elif phase == "relay":
                results[phase] = await bench_relay(options, engine)
            elif phase == "e2e":
                results[phase] = await bench_e2e(options, engine, receiver)
            print(
                phase, json.dumps({k: round(v, 2) for k, v in results[phase].items()}), flush=True
            )
    finally:
        receiver.stop()
        await engine.dispose()
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--webhook-host", default="127.0.0.1")
    parser.add_argument("--receiver-bind", default="127.0.0.1")
    parser.add_argument("--api-requests", type=int, default=1000)
    parser.add_argument("--api-concurrency", type=int, default=20)
    parser.add_argument("--relay-events", type=int, default=5000)
    parser.add_argument("--e2e-payments", type=int, default=300)
    parser.add_argument("--phases", default="relay,e2e,api")
    parser.add_argument("--output")
    args = parser.parse_args()
    options = Options(
        base_url=args.base_url,
        api_key=args.api_key,
        database_url=args.database_url,
        webhook_host=args.webhook_host,
        receiver_bind=args.receiver_bind,
        api_requests=args.api_requests,
        api_concurrency=args.api_concurrency,
        relay_events=args.relay_events,
        e2e_payments=args.e2e_payments,
    )
    results = asyncio.run(run(options, args.phases.split(",")))
    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
