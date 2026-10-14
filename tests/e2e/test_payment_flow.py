import asyncio
import time
import uuid
from typing import Any

import aio_pika
import httpx
import pytest

from app.consumer.webhook import sign
from app.messaging import ATTEMPT_HEADER, ERROR_HEADER
from tests.e2e.conftest import E2EConfig, ServiceControl, drain_dead_letters
from tests.e2e.receiver import ReceivedWebhook, WebhookReceiver

pytestmark = pytest.mark.e2e

URL = "/api/v1/payments"


def body(webhook_url: str, amount: str = "250.00") -> dict[str, Any]:
    return {
        "amount": amount,
        "currency": "USD",
        "description": "e2e",
        "metadata": {"source": "e2e"},
        "webhook_url": webhook_url,
    }


def idempotency() -> dict[str, str]:
    return {"Idempotency-Key": f"e2e-{uuid.uuid4()}"}


def timeout(config: E2EConfig, attempts: int = 1) -> float:
    retries = sum(config.retry_base_delay * 2.0**step for step in range(attempts - 1))
    return config.gateway_max_delay + retries + 40


def assert_signed(webhook: ReceivedWebhook, secret: str) -> None:
    timestamp = webhook.headers["x-webhook-timestamp"]
    assert webhook.headers["x-webhook-signature"] == sign(secret, timestamp, webhook.body)
    assert webhook.headers["x-webhook-id"] == webhook.payload["event_id"]


async def create(api: httpx.AsyncClient, webhook_url: str) -> str:
    response = await api.post(URL, json=body(webhook_url), headers=idempotency())
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "pending"
    payment_id: str = response.json()["payment_id"]
    return payment_id


async def wait_webhooks(
    receiver: WebhookReceiver, path: str, count: int, seconds: float
) -> list[ReceivedWebhook]:
    received = await asyncio.to_thread(receiver.wait_for, path, count, seconds)
    assert len(received) >= count, f"expected {count} webhook(s) on {path}, got {len(received)}"
    return received


async def wait_payment(
    api: httpx.AsyncClient, payment_id: str, predicate: Any, seconds: float = 30
) -> dict[str, Any]:
    deadline = time.monotonic() + seconds
    while True:
        data: dict[str, Any] = (await api.get(f"{URL}/{payment_id}")).json()
        if predicate(data) or time.monotonic() > deadline:
            return data
        await asyncio.sleep(0.2)


async def test_payment_is_processed_and_webhook_is_signed(
    api: httpx.AsyncClient, receiver: WebhookReceiver, hook_url: Any, e2e_config: E2EConfig
) -> None:
    path, url = hook_url()
    payment_id = await create(api, url)

    [webhook] = await wait_webhooks(receiver, path, 1, timeout(e2e_config))
    payload = webhook.payload
    assert payload["event_type"] in {"payment.succeeded", "payment.failed"}
    assert payload["data"]["payment_id"] == payment_id
    assert payload["data"]["amount"] == "250.00"
    assert payload["data"]["metadata"] == {"source": "e2e"}
    assert_signed(webhook, e2e_config.webhook_secret)

    payment = await wait_payment(api, payment_id, lambda data: data["webhook"]["delivered_at"])
    assert payment["status"] == payload["data"]["status"]
    assert payment["processed_at"] is not None
    assert payment["webhook"]["attempts"] == 1
    if payment["status"] == "failed":
        assert payment["failure_reason"] == "declined_by_gateway"


async def test_duplicate_requests_create_one_payment_and_one_webhook(
    api: httpx.AsyncClient, receiver: WebhookReceiver, hook_url: Any, e2e_config: E2EConfig
) -> None:
    path, url = hook_url()
    headers = idempotency()
    responses = await asyncio.gather(
        *(api.post(URL, json=body(url), headers=headers) for _ in range(5))
    )
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1

    conflict = await api.post(URL, json=body(url, amount="1.00"), headers=headers)
    assert conflict.status_code == 409

    await wait_webhooks(receiver, path, 1, timeout(e2e_config))
    await asyncio.sleep(e2e_config.retry_base_delay + 1)
    assert len(receiver.received(path)) == 1


async def test_webhook_is_retried_with_exponential_backoff(
    api: httpx.AsyncClient, receiver: WebhookReceiver, hook_url: Any, e2e_config: E2EConfig
) -> None:
    path, url = hook_url("flaky-2")
    payment_id = await create(api, url)

    attempts = await wait_webhooks(receiver, path, 3, timeout(e2e_config, attempts=3))
    assert [webhook.status for webhook in attempts] == [503, 503, 200]
    assert len({webhook.payload["event_id"] for webhook in attempts}) == 1
    first_gap = attempts[1].received_at - attempts[0].received_at
    second_gap = attempts[2].received_at - attempts[1].received_at
    assert first_gap >= e2e_config.retry_base_delay * 0.9
    assert second_gap >= e2e_config.retry_base_delay * 2 * 0.9

    payment = await wait_payment(api, payment_id, lambda data: data["webhook"]["delivered_at"])
    assert payment["webhook"]["attempts"] == 3
    assert payment["webhook"]["delivered_at"] is not None


async def test_message_is_dead_lettered_after_three_failed_attempts(
    api: httpx.AsyncClient,
    receiver: WebhookReceiver,
    hook_url: Any,
    e2e_config: E2EConfig,
    amqp: aio_pika.abc.AbstractRobustConnection,
) -> None:
    path, url = hook_url("fail")
    payment_id = await create(api, url)

    attempts = await wait_webhooks(receiver, path, 3, timeout(e2e_config, attempts=3))
    assert [webhook.status for webhook in attempts] == [500, 500, 500]

    dead: list[Any] = []
    deadline = time.monotonic() + 30
    while not dead and time.monotonic() < deadline:
        dead = [
            message
            for message in await drain_dead_letters(amqp)
            if payment_id.encode() in message.body
        ]
        await asyncio.sleep(0.3)
    [message] = dead
    assert message.headers[ATTEMPT_HEADER] == 3
    assert "HTTP 500" in str(message.headers[ERROR_HEADER])

    payment = await wait_payment(api, payment_id, lambda data: data["webhook"]["attempts"] == 3)
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["webhook"] == {
        "attempts": 3,
        "delivered_at": None,
        "last_error": "HTTP 500",
    }
    await asyncio.sleep(e2e_config.retry_base_delay * 2)
    assert len(receiver.received(path)) == 3


async def test_payments_are_processed_concurrently(
    api: httpx.AsyncClient, receiver: WebhookReceiver, hook_url: Any, e2e_config: E2EConfig
) -> None:
    count = 20
    hooks = [hook_url() for _ in range(count)]
    started = time.monotonic()
    await asyncio.gather(*(create(api, url) for _, url in hooks))
    for path, _ in hooks:
        await wait_webhooks(receiver, path, 1, timeout(e2e_config))
    elapsed = time.monotonic() - started
    assert elapsed < count * e2e_config.gateway_min_delay


async def test_events_created_while_relay_is_down_are_published_later(
    api: httpx.AsyncClient,
    receiver: WebhookReceiver,
    hook_url: Any,
    e2e_config: E2EConfig,
    stack: ServiceControl | None,
) -> None:
    if stack is None:
        pytest.skip("requires service control: local processes or E2E_COMPOSE")
    stack.stop("outbox-relay")
    path, url = hook_url()
    try:
        payment_id = await create(api, url)
        await asyncio.sleep(2)
        assert receiver.received(path) == []
        assert (await api.get(f"{URL}/{payment_id}")).json()["status"] == "pending"
    finally:
        stack.start("outbox-relay")
    await wait_webhooks(receiver, path, 1, timeout(e2e_config))


async def test_messages_survive_consumer_restart(
    api: httpx.AsyncClient,
    receiver: WebhookReceiver,
    hook_url: Any,
    e2e_config: E2EConfig,
    stack: ServiceControl | None,
) -> None:
    if stack is None:
        pytest.skip("requires service control: local processes or E2E_COMPOSE")
    stack.stop("consumer")
    path, url = hook_url()
    try:
        await create(api, url)
        await asyncio.sleep(2)
        assert receiver.received(path) == []
    finally:
        stack.start("consumer")
    await wait_webhooks(receiver, path, 1, timeout(e2e_config) + 10)


async def test_api_requires_key(api: httpx.AsyncClient) -> None:
    response = await api.get(f"{URL}/{uuid.uuid4()}", headers={"X-API-Key": "invalid"})
    assert response.status_code == 401
