import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.events import PaymentCreatedEvent
from app.messaging import (
    ATTEMPT_HEADER,
    Topology,
    create_broker,
    declare_topology,
    topology_from_settings,
)
from app.models import OutboxEvent
from app.outbox.relay import OutboxRelay, RabbitEventPublisher

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]


class FailingPublisher:
    async def publish(self, event: OutboxEvent) -> None:
        raise ConnectionError("broker unavailable")


@pytest.fixture
def topology(settings: Settings) -> Topology:
    return topology_from_settings(settings)


@pytest.fixture
async def broker(settings: Settings, topology: Topology) -> AsyncIterator[RabbitBroker]:
    broker = create_broker(settings)
    await broker.start()
    await declare_topology(broker, topology)
    queue = await broker.declare_queue(topology.queue)
    await queue.purge()
    yield broker
    await queue.purge()
    await broker.stop()


async def add_events(
    session_factory: async_sessionmaker[AsyncSession], count: int
) -> list[PaymentCreatedEvent]:
    events = [
        PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=uuid.uuid4()) for _ in range(count)
    ]
    async with session_factory() as session, session.begin():
        session.add_all(
            OutboxEvent(
                id=event.event_id,
                aggregate_id=event.payment_id,
                event_type=event.event_type,
                routing_key="payments.new",
                payload=event.model_dump(mode="json"),
            )
            for event in events
        )
    return events


async def load(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


async def test_relay_publishes_pending_events(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    topology: Topology,
) -> None:
    events = await add_events(session_factory, 3)
    relay = OutboxRelay(
        session_factory, RabbitEventPublisher(broker, topology), batch_size=10, poll_interval=0.1
    )

    assert await relay.relay_batch() == 3
    assert await relay.relay_batch() == 0

    stored = await load(session_factory)
    assert all(event.published_at is not None and event.attempts == 1 for event in stored)

    queue = await broker.declare_queue(topology.queue)
    received = []
    for _ in events:
        message = await queue.get(timeout=5)
        assert message is not None
        await message.ack()
        assert message.delivery_mode == 2
        assert message.headers[ATTEMPT_HEADER] == 1
        received.append(PaymentCreatedEvent.model_validate_json(message.body))
    assert {event.event_id for event in received} == {event.event_id for event in events}


async def test_relay_respects_batch_size(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    topology: Topology,
) -> None:
    await add_events(session_factory, 5)
    relay = OutboxRelay(
        session_factory, RabbitEventPublisher(broker, topology), batch_size=2, poll_interval=0.1
    )
    assert [await relay.relay_batch() for _ in range(4)] == [2, 2, 1, 0]


async def test_failed_publish_keeps_event_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_events(session_factory, 2)
    relay = OutboxRelay(session_factory, FailingPublisher(), batch_size=10, poll_interval=0.1)

    assert await relay.relay_batch() == 0

    stored = sorted(await load(session_factory), key=lambda event: event.attempts, reverse=True)
    assert stored[0].published_at is None
    assert stored[0].attempts == 1
    assert "broker unavailable" in (stored[0].last_error or "")
    assert stored[1].attempts == 0


async def test_published_events_are_purged_after_retention(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_events(session_factory, 3)
    now = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        old, recent, pending = (await session.scalars(select(OutboxEvent))).all()
        old.published_at = now - timedelta(hours=100)
        recent.published_at = now - timedelta(hours=1)
    relay = OutboxRelay(
        session_factory,
        FailingPublisher(),
        batch_size=10,
        poll_interval=0.1,
        retention=timedelta(hours=72),
    )

    assert await relay.purge_published() == 1
    remaining = {event.id for event in await load(session_factory)}
    assert remaining == {recent.id, pending.id}
