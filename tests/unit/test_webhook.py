import asyncio
import hashlib
import hmac
import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.consumer.webhook import (
    EVENT_ID_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookDeliveryError,
    WebhookSender,
    build_payload,
    sign,
    webhook_event_id,
)
from app.models import Currency, Payment, PaymentStatus


def make_payment(status: PaymentStatus = PaymentStatus.SUCCEEDED) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        idempotency_key="key-1",
        request_hash="0" * 64,
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        meta={"a": 1},
        status=status,
        failure_reason=None,
        webhook_url="https://merchant.example/hooks",
        webhook_attempts=0,
        webhook_last_error=None,
        webhook_delivered_at=None,
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


def test_payload_contains_payment_snapshot() -> None:
    payment = make_payment()
    payload = build_payload(payment)
    assert payload["event_type"] == "payment.succeeded"
    assert payload["event_id"] == str(webhook_event_id(payment))
    assert payload["data"]["payment_id"] == str(payment.id)
    assert payload["data"]["amount"] == "10.00"
    assert payload["data"]["metadata"] == {"a": 1}
    assert "webhook" not in payload["data"]


def test_event_id_is_deterministic_per_status() -> None:
    payment = make_payment()
    assert webhook_event_id(payment) == webhook_event_id(payment)
    failed = make_payment(PaymentStatus.FAILED)
    failed.id = payment.id
    assert webhook_event_id(failed) != webhook_event_id(payment)


async def test_sender_signs_request() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        payload = build_payload(make_payment())
        await WebhookSender(client, "secret").send("https://merchant.example/hooks", payload)

    request = captured[0]
    timestamp = request.headers[TIMESTAMP_HEADER]
    expected = hmac.new(
        b"secret", timestamp.encode() + b"." + request.content, hashlib.sha256
    ).hexdigest()
    assert request.headers[SIGNATURE_HEADER] == f"sha256={expected}"
    assert request.headers[SIGNATURE_HEADER] == sign("secret", timestamp, request.content)
    assert request.headers[EVENT_ID_HEADER] == payload["event_id"]
    assert json.loads(request.content) == payload


async def test_sender_without_secret_does_not_sign() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await WebhookSender(client, None).send("https://x.example", build_payload(make_payment()))
    assert SIGNATURE_HEADER not in captured[0].headers


@pytest.mark.parametrize("status_code", [301, 400, 404, 500, 503])
async def test_non_2xx_is_delivery_error(status_code: int) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(status_code))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(WebhookDeliveryError, match=str(status_code)):
            await WebhookSender(client, None).send(
                "https://x.example", build_payload(make_payment())
            )


async def test_network_error_is_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError, match="ConnectError"):
            await WebhookSender(client, None).send(
                "https://x.example", build_payload(make_payment())
            )


async def test_slow_receiver_hits_total_deadline() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = WebhookSender(client, None, timeout=0.2)
        with pytest.raises(WebhookDeliveryError, match="Timeout"):
            await sender.send("https://x.example", build_payload(make_payment()))


async def test_response_body_is_not_read() -> None:
    async def endless() -> AsyncIterator[bytes]:
        while True:
            await asyncio.sleep(0.01)
            yield b"x" * 1024

    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=endless()))
    async with httpx.AsyncClient(transport=transport) as client:
        await asyncio.wait_for(
            WebhookSender(client, None, timeout=1).send(
                "https://x.example", build_payload(make_payment())
            ),
            timeout=2,
        )
