import asyncio

import httpx
from faststream import AckPolicy
from faststream.rabbit import RabbitMessage

from app.config import get_settings
from app.consumer.gateway import EmulatedGateway
from app.consumer.handler import PaymentMessageHandler
from app.consumer.processor import PaymentProcessor
from app.consumer.webhook import WebhookSender
from app.db import create_engine, create_session_factory
from app.logging import configure_logging
from app.messaging import create_broker, declare_topology, topology_from_settings
from app.runtime import install_stop_signals


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    broker = create_broker(settings, prefetch_count=settings.consumer_prefetch)
    topology = topology_from_settings(settings)
    secret = settings.webhook_secret.get_secret_value() if settings.webhook_secret else None
    stop = asyncio.Event()
    install_stop_signals(stop)
    async with httpx.AsyncClient(
        timeout=settings.webhook_timeout,
        follow_redirects=False,
        limits=httpx.Limits(
            max_connections=settings.consumer_prefetch,
            max_keepalive_connections=settings.consumer_prefetch,
        ),
    ) as client:
        handler = PaymentMessageHandler(
            processor=PaymentProcessor(
                create_session_factory(engine),
                EmulatedGateway(
                    settings.gateway_min_delay,
                    settings.gateway_max_delay,
                    settings.gateway_success_rate,
                ),
                WebhookSender(client, secret, settings.webhook_timeout),
                lease_seconds=settings.processing_lease,
            ),
            broker=broker,
            topology=topology,
            max_attempts=settings.max_attempts,
            publish_timeout=settings.publish_timeout,
        )

        @broker.subscriber(topology.queue, topology.exchange, ack_policy=AckPolicy.MANUAL)
        async def on_payment_created(message: RabbitMessage) -> None:
            await handler.handle(message)

        try:
            await broker.connect()
            await declare_topology(broker, topology)
            await broker.start()
            await stop.wait()
        finally:
            await broker.stop()
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
