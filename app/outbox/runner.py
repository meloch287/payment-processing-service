import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.messaging import create_broker, declare_topology, topology_from_settings
from app.outbox.relay import OutboxRelay, RabbitEventPublisher


@asynccontextmanager
async def running_relay(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[OutboxRelay]:
    broker = create_broker(settings)
    topology = topology_from_settings(settings)
    stop = asyncio.Event()
    await broker.start()
    try:
        await declare_topology(broker, topology)
        relay = OutboxRelay(
            session_factory,
            RabbitEventPublisher(broker, topology, settings.publish_timeout),
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
            retention=timedelta(hours=settings.outbox_retention_hours),
            cleanup_interval=settings.outbox_cleanup_interval,
        )
        task = asyncio.create_task(relay.run(stop))
        try:
            yield relay
        finally:
            stop.set()
            relay.wake()
            await task
    finally:
        await broker.stop()
