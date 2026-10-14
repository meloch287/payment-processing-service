import asyncio
from datetime import timedelta

from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.logging import configure_logging
from app.messaging import create_broker, declare_topology, topology_from_settings
from app.outbox.relay import OutboxRelay, RabbitEventPublisher
from app.runtime import install_stop_signals


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    broker = create_broker(settings)
    topology = topology_from_settings(settings)
    stop = asyncio.Event()
    install_stop_signals(stop)
    try:
        await broker.start()
        await declare_topology(broker, topology)
        relay = OutboxRelay(
            create_session_factory(engine),
            RabbitEventPublisher(broker, topology, settings.publish_timeout),
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
            retention=timedelta(hours=settings.outbox_retention_hours),
            cleanup_interval=settings.outbox_cleanup_interval,
        )
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
