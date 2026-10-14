import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import ChargeRequest, ChargeResult
from app.consumer.processor import (
    PaymentInProgressError,
    PaymentNotFoundError,
    PaymentProcessor,
)
from app.consumer.webhook import WebhookDeliveryError
from app.models import Currency, Payment, PaymentStatus
from app.schemas import PaymentCreate
from app.services import create_payment

pytestmark = pytest.mark.integration


class FakeGateway:
    def __init__(self, result: ChargeResult) -> None:
        self.result = result
        self.calls: list[ChargeRequest] = []

    async def charge(self, request: ChargeRequest) -> ChargeResult:
        self.calls.append(request)
        return self.result


class FakeSender:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.sent: list[tuple[str, dict[str, Any]]] = []

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        if self.failures:
            self.failures -= 1
            raise WebhookDeliveryError("HTTP 503")
        self.sent.append((url, payload))


SUCCESS = ChargeResult(status=PaymentStatus.SUCCEEDED)
DECLINE = ChargeResult(status=PaymentStatus.FAILED, failure_reason="declined_by_gateway")


async def new_payment(session_factory: async_sessionmaker[AsyncSession]) -> Payment:
    data = PaymentCreate(
        amount=Decimal("99.90"),
        currency=Currency.USD,
        description="processor test",
        webhook_url=cast(Any, "https://merchant.example/hook"),
    )
    async with session_factory() as session:
        return (await create_payment(session, data, str(uuid.uuid4()))).payment


def make_processor(
    session_factory: async_sessionmaker[AsyncSession], gateway: FakeGateway, sender: FakeSender
) -> PaymentProcessor:
    return PaymentProcessor(session_factory, gateway, cast(Any, sender))


@pytest.mark.usefixtures("clean_db")
@pytest.mark.parametrize("result", [SUCCESS, DECLINE])
async def test_payment_is_settled_and_webhook_delivered(
    session_factory: async_sessionmaker[AsyncSession], result: ChargeResult
) -> None:
    payment = await new_payment(session_factory)
    gateway, sender = FakeGateway(result), FakeSender()

    processed = await make_processor(session_factory, gateway, sender).process(payment.id)

    assert processed.status is result.status
    assert processed.failure_reason == result.failure_reason
    assert processed.processed_at is not None
    assert processed.webhook_attempts == 1
    assert processed.webhook_delivered_at is not None
    assert gateway.calls[0].payment_id == payment.id
    [(url, payload)] = sender.sent
    assert url == "https://merchant.example/hook"
    assert payload["event_type"] == f"payment.{result.status}"
    assert payload["data"]["status"] == result.status


@pytest.mark.usefixtures("clean_db")
async def test_webhook_failure_is_recorded_and_raised(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    processor = make_processor(session_factory, FakeGateway(SUCCESS), FakeSender(failures=1))

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_attempts == 1
    assert stored.webhook_last_error == "HTTP 503"
    assert stored.webhook_delivered_at is None


@pytest.mark.usefixtures("clean_db")
async def test_redelivery_does_not_charge_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    gateway, sender = FakeGateway(SUCCESS), FakeSender(failures=1)
    processor = make_processor(session_factory, gateway, sender)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)
    processed = await processor.process(payment.id)

    assert len(gateway.calls) == 1
    assert processed.webhook_attempts == 2
    assert processed.webhook_delivered_at is not None


@pytest.mark.usefixtures("clean_db")
async def test_delivered_payment_is_not_notified_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    gateway, sender = FakeGateway(SUCCESS), FakeSender()
    processor = make_processor(session_factory, gateway, sender)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert len(gateway.calls) == 1
    assert len(sender.sent) == 1


class GatedGateway(FakeGateway):
    def __init__(self, result: ChargeResult) -> None:
        super().__init__(result)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def charge(self, request: ChargeRequest) -> ChargeResult:
        self.entered.set()
        await self.release.wait()
        return await super().charge(request)


@pytest.mark.usefixtures("clean_db")
async def test_concurrent_delivery_does_not_charge_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    gated, other = GatedGateway(SUCCESS), FakeGateway(DECLINE)
    first_sender, second_sender = FakeSender(), FakeSender()
    second = make_processor(session_factory, other, second_sender)
    first = asyncio.create_task(
        make_processor(session_factory, gated, first_sender).process(payment.id)
    )
    await asyncio.wait_for(gated.entered.wait(), 5)

    with pytest.raises(PaymentInProgressError):
        await second.process(payment.id)
    gated.release.set()
    first_result = await first
    second_result = await second.process(payment.id)

    assert first_result.status is PaymentStatus.SUCCEEDED
    assert first_result.processing_lease_until is None
    assert second_result.status is PaymentStatus.SUCCEEDED
    assert other.calls == []
    assert len(first_sender.sent) == 1
    assert second_sender.sent == []


@pytest.mark.usefixtures("clean_db")
async def test_expired_lease_is_taken_over(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    async with session_factory() as session, session.begin():
        stored = await session.get(Payment, payment.id)
        assert stored is not None
        stored.processing_lease_until = datetime.now(UTC) - timedelta(seconds=1)
    gateway, sender = FakeGateway(SUCCESS), FakeSender()

    processed = await make_processor(session_factory, gateway, sender).process(payment.id)

    assert processed.status is PaymentStatus.SUCCEEDED
    assert len(gateway.calls) == 1


@pytest.mark.usefixtures("clean_db")
async def test_webhook_in_flight_elsewhere_is_not_sent_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await new_payment(session_factory)
    async with session_factory() as session, session.begin():
        stored = await session.get(Payment, payment.id)
        assert stored is not None
        stored.status = PaymentStatus.SUCCEEDED
        stored.webhook_lease_until = datetime.now(UTC) + timedelta(seconds=30)
    gateway, sender = FakeGateway(SUCCESS), FakeSender()

    with pytest.raises(PaymentInProgressError):
        await make_processor(session_factory, gateway, sender).process(payment.id)

    assert gateway.calls == []
    assert sender.sent == []


@pytest.mark.usefixtures("clean_db")
async def test_unknown_payment_raises(session_factory: async_sessionmaker[AsyncSession]) -> None:
    processor = make_processor(session_factory, FakeGateway(SUCCESS), FakeSender())
    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())
