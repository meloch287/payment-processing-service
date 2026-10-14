import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from app.consumer.processor import (
    PaymentInProgressError,
    PaymentNotFoundError,
    PaymentProcessor,
)
from app.events import PaymentCreatedEvent
from app.messaging import ATTEMPT_HEADER, ERROR_HEADER, Topology

logger = logging.getLogger(__name__)

NON_RETRYABLE: tuple[type[Exception], ...] = (ValidationError, PaymentNotFoundError)


def read_attempt(headers: Mapping[str, Any]) -> int:
    try:
        return max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


class PaymentMessageHandler:
    def __init__(
        self,
        processor: PaymentProcessor,
        broker: RabbitBroker,
        topology: Topology,
        max_attempts: int,
        requeue_delay: float = 1.0,
        publish_timeout: float = 10.0,
    ) -> None:
        self._processor = processor
        self._broker = broker
        self._topology = topology
        self._max_attempts = max_attempts
        self._requeue_delay = requeue_delay
        self._publish_timeout = publish_timeout

    async def handle(self, message: RabbitMessage) -> None:
        try:
            await self._dispatch(message)
        except Exception:
            await asyncio.sleep(self._requeue_delay)
            if message.raw_message.redelivered:
                logger.exception("failed to route redelivered message_id=%s", message.message_id)
                await message.reject()
            else:
                logger.exception("failed to route message_id=%s", message.message_id)
                await message.nack(requeue=True)
            return
        await message.ack()

    async def _dispatch(self, message: RabbitMessage) -> None:
        attempt = read_attempt(message.headers)
        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
            payment = await self._processor.process(event.payment_id)
        except NON_RETRYABLE as exc:
            await self._dead_letter(message, attempt, exc)
        except PaymentInProgressError as exc:
            await self._defer(message, attempt, exc)
        except Exception as exc:
            if attempt >= self._max_attempts:
                await self._dead_letter(message, attempt, exc)
            else:
                await self._retry(message, attempt, exc)
        else:
            logger.info(
                "payment processed payment_id=%s status=%s attempt=%d",
                payment.id,
                payment.status,
                attempt,
            )

    async def _retry(self, message: RabbitMessage, attempt: int, exc: Exception) -> None:
        queue = self._topology.retry_queue(attempt)
        logger.warning(
            "attempt %d/%d failed message_id=%s: %s; retry via %s",
            attempt,
            self._max_attempts,
            message.message_id,
            exc,
            queue.name,
        )
        await self._republish(
            message,
            exchange=self._topology.retry_exchange.name,
            routing_key=queue.routing(),
            headers={ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: _describe(exc)},
        )

    async def _defer(self, message: RabbitMessage, attempt: int, exc: Exception) -> None:
        if not self._topology.retry_queues:
            raise exc
        queue = self._topology.retry_queues[0]
        logger.info("payment is being processed elsewhere message_id=%s", message.message_id)
        await self._republish(
            message,
            exchange=self._topology.retry_exchange.name,
            routing_key=queue.routing(),
            headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: _describe(exc)},
        )

    async def _dead_letter(self, message: RabbitMessage, attempt: int, exc: Exception) -> None:
        logger.error(
            "message_id=%s dead-lettered after attempt %d: %s",
            message.message_id,
            attempt,
            exc,
        )
        await self._republish(
            message,
            exchange=self._topology.dead_letter_exchange.name,
            routing_key=self._topology.dead_letter_queue.routing(),
            headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: _describe(exc)},
        )

    async def _republish(
        self,
        message: RabbitMessage,
        exchange: str,
        routing_key: str,
        headers: dict[str, Any],
    ) -> None:
        await self._broker.publish(
            message.body,
            exchange=exchange,
            routing_key=routing_key,
            persist=True,
            mandatory=True,
            headers={**message.headers, **headers},
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type or "application/json",
            message_type=message.raw_message.type,
            timeout=self._publish_timeout,
        )


def _describe(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:1000]
