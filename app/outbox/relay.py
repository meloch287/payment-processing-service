import asyncio
import contextlib
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy import CursorResult, delete, func, select, update
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
        purge_batch_size: int = 1000,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._retention = retention
        self._cleanup_interval = cleanup_interval
        self._purge_batch_size = purge_batch_size
        self._wakeup = asyncio.Event()

    def wake(self) -> None:
        self._wakeup.set()

    async def relay_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not events:
                return 0
            results = await asyncio.gather(
                *(self._publisher.publish(event) for event in events), return_exceptions=True
            )
            outcomes = list(zip(events, results, strict=True))
            published = [event.id for event, result in outcomes if result is None]
            if published:
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id.in_(published))
                    .values(published_at=func.now(), attempts=OutboxEvent.attempts + 1)
                    .execution_options(synchronize_session=False)
                )
            failures = [
                (event, result) for event, result in outcomes if isinstance(result, BaseException)
            ]
            for event, error in failures:
                event.attempts += 1
                event.last_error = repr(error)[:2000]
            if failures:
                logger.warning(
                    "outbox publish failed for %d event(s): %r", len(failures), failures[0][1]
                )
        return len(published)

    async def purge_published(self) -> int:
        cutoff = datetime.now(UTC) - self._retention
        total = 0
        while True:
            async with self._session_factory() as session, session.begin():
                expired = (
                    select(OutboxEvent.id)
                    .where(OutboxEvent.published_at < cutoff)
                    .limit(self._purge_batch_size)
                )
                result = await session.execute(
                    delete(OutboxEvent)
                    .where(OutboxEvent.id.in_(expired))
                    .execution_options(synchronize_session=False)
                )
            deleted = result.rowcount if isinstance(result, CursorResult) else 0
            total += deleted
            if deleted < self._purge_batch_size:
                return total

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
            if published < self._batch_size and not stop.is_set():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wakeup.wait(), timeout=self._poll_interval)
                self._wakeup.clear()
        logger.info("outbox relay stopped")
