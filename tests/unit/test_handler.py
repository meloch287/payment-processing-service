import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.consumer.handler import PaymentMessageHandler, read_attempt
from app.consumer.processor import PaymentInProgressError, PaymentNotFoundError
from app.consumer.webhook import WebhookDeliveryError
from app.events import PaymentCreatedEvent
from app.messaging import ATTEMPT_HEADER, ERROR_HEADER, build_topology

TOPOLOGY = build_topology(3, 2.0)


@dataclass
class FakeMessage:
    body: bytes
    headers: dict[str, Any]
    message_id: str = "msg-1"
    correlation_id: str = "corr-1"
    content_type: str = "application/json"
    raw_message: SimpleNamespace = field(
        default_factory=lambda: SimpleNamespace(type="payment.created", redelivered=False)
    )
    acked: bool = False
    nacked: bool = False
    rejected: bool = False

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.nacked = True

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = True


@dataclass
class FakeBroker:
    published: list[dict[str, Any]] = field(default_factory=list)
    fail: bool = False

    async def publish(self, body: bytes, **kwargs: Any) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.published.append({"body": body, **kwargs})


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> SimpleNamespace:
        self.calls.append(payment_id)
        if self.error:
            raise self.error
        return SimpleNamespace(id=payment_id, status="succeeded")


def make_message(attempt: int | None = None, body: bytes | None = None) -> FakeMessage:
    event = PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=uuid.uuid4())
    headers = {} if attempt is None else {ATTEMPT_HEADER: attempt}
    return FakeMessage(body=body or event.model_dump_json().encode(), headers=headers)


def make_handler(processor: FakeProcessor, broker: FakeBroker) -> PaymentMessageHandler:
    return PaymentMessageHandler(
        processor=cast(Any, processor),
        broker=cast(Any, broker),
        topology=TOPOLOGY,
        max_attempts=3,
        requeue_delay=0,
    )


@pytest.mark.parametrize(
    ("headers", "expected"),
    [({}, 1), ({ATTEMPT_HEADER: 2}, 2), ({ATTEMPT_HEADER: "3"}, 3), ({ATTEMPT_HEADER: "x"}, 1)],
)
def test_read_attempt(headers: dict[str, Any], expected: int) -> None:
    assert read_attempt(headers) == expected


async def test_success_acks_without_republish() -> None:
    broker, message = FakeBroker(), make_message()
    await make_handler(FakeProcessor(), broker).handle(cast(Any, message))
    assert message.acked
    assert broker.published == []


@pytest.mark.parametrize(("attempt", "retry_queue"), [(1, 0), (2, 1)])
async def test_failure_goes_to_retry_queue(attempt: int, retry_queue: int) -> None:
    broker, message = FakeBroker(), make_message(attempt)
    processor = FakeProcessor(WebhookDeliveryError("HTTP 500"))
    await make_handler(processor, broker).handle(cast(Any, message))
    assert message.acked
    [published] = broker.published
    assert published["exchange"] == TOPOLOGY.retry_exchange.name
    assert published["routing_key"] == TOPOLOGY.retry_queues[retry_queue].routing()
    assert published["headers"][ATTEMPT_HEADER] == attempt + 1
    assert "HTTP 500" in published["headers"][ERROR_HEADER]
    assert published["persist"] is True
    assert published["body"] == message.body
    assert published["message_id"] == message.message_id


async def test_last_attempt_goes_to_dead_letter_queue() -> None:
    broker, message = FakeBroker(), make_message(3)
    await make_handler(FakeProcessor(WebhookDeliveryError("HTTP 500")), broker).handle(
        cast(Any, message)
    )
    assert message.acked
    [published] = broker.published
    assert published["exchange"] == TOPOLOGY.dead_letter_exchange.name
    assert published["routing_key"] == TOPOLOGY.dead_letter_queue.routing()
    assert published["headers"][ATTEMPT_HEADER] == 3


async def test_malformed_message_goes_straight_to_dlq() -> None:
    broker, message = FakeBroker(), make_message(body=b"{not json")
    processor = FakeProcessor()
    await make_handler(processor, broker).handle(cast(Any, message))
    assert processor.calls == []
    assert broker.published[0]["exchange"] == TOPOLOGY.dead_letter_exchange.name


async def test_missing_payment_goes_straight_to_dlq() -> None:
    broker, message = FakeBroker(), make_message(1)
    await make_handler(FakeProcessor(PaymentNotFoundError("x")), broker).handle(cast(Any, message))
    assert broker.published[0]["exchange"] == TOPOLOGY.dead_letter_exchange.name


async def test_broker_failure_requeues_message() -> None:
    broker, message = FakeBroker(fail=True), make_message(1)
    await make_handler(FakeProcessor(RuntimeError("db down")), broker).handle(cast(Any, message))
    assert message.nacked
    assert not message.acked


async def test_redelivered_message_is_rejected_when_routing_keeps_failing() -> None:
    broker, message = FakeBroker(fail=True), make_message(1)
    message.raw_message.redelivered = True
    await make_handler(FakeProcessor(RuntimeError("db down")), broker).handle(cast(Any, message))
    assert message.rejected
    assert not message.nacked
    assert not message.acked


async def test_payment_in_progress_is_deferred_without_consuming_attempt() -> None:
    broker, message = FakeBroker(), make_message(2)
    await make_handler(FakeProcessor(PaymentInProgressError("x")), broker).handle(
        cast(Any, message)
    )
    assert message.acked
    [published] = broker.published
    assert published["exchange"] == TOPOLOGY.retry_exchange.name
    assert published["routing_key"] == TOPOLOGY.retry_queues[0].routing()
    assert published["headers"][ATTEMPT_HEADER] == 2
