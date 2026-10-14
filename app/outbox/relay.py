import asyncio
import contextlib
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy import CursorResult, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import ATTEMPT_HEADER, Topology
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, topology: Topology, timeout: float = 10.0) -> None:
        self._broker = broker
        self._topology = topology
        self._timeout = timeout

    async def publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=self._topology.exchange,
            routing_key=event.routing_key,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            message_type=event.event_type,
            correlation_id=str(event.aggregate_id),
            headers={ATTEMPT_HEADER: 1},
            timeout=self._timeout,
        )


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        batch_size: int,
        poll_interval: float,
        retention: timedelta = timedelta(hours=72),
        cleanup_interval: float = 300,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._retention = retention
        self._cleanup_interval = cleanup_interval

    async def relay_batch(self) -> int:
        published = 0
        while published < self._batch_size:
            async with self._session_factory() as session, session.begin():
                event = await session.scalar(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if event is None:
                    return published
                event.attempts += 1
                try:
                    await self._publisher.publish(event)
                except Exception as exc:
                    event.last_error = repr(exc)[:2000]
                    logger.warning("outbox publish failed event_id=%s: %r", event.id, exc)
                    return published
                event.published_at = datetime.now(UTC)
            published += 1
        return published

    async def purge_published(self) -> int:
        cutoff = datetime.now(UTC) - self._retention
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                delete(OutboxEvent).where(OutboxEvent.published_at < cutoff)
            )
        return result.rowcount if isinstance(result, CursorResult) else 0

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("outbox relay started")
        next_cleanup = time.monotonic()
        while not stop.is_set():
            if time.monotonic() >= next_cleanup:
                next_cleanup = time.monotonic() + self._cleanup_interval
                try:
                    purged = await self.purge_published()
                except Exception:
                    logger.exception("outbox cleanup failed")
                else:
                    if purged:
                        logger.info("outbox purged %d published event(s)", purged)
            try:
                published = await self.relay_batch()
            except Exception:
                logger.exception("outbox relay iteration failed")
                published = 0
            if published:
                logger.info("outbox published %d event(s)", published)
            if published < self._batch_size:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
        logger.info("outbox relay stopped")
