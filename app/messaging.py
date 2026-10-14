import logging
from dataclasses import dataclass

from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_EXCHANGE = "payments"
RETRY_EXCHANGE = "payments.retry"
DEAD_LETTER_EXCHANGE = "payments.dlx"
NEW_PAYMENTS_QUEUE = "payments.new"
DEAD_LETTER_QUEUE = "payments.new.dlq"
ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-error"


@dataclass(frozen=True, slots=True)
class Topology:
    exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    queue: RabbitQueue
    dead_letter_queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]

    def retry_queue(self, failed_attempt: int) -> RabbitQueue:
        return self.retry_queues[failed_attempt - 1]


def retry_delays(max_attempts: int, base_delay: float) -> tuple[int, ...]:
    return tuple(int(base_delay * 1000 * 2**step) for step in range(max_attempts - 1))


def build_topology(max_attempts: int, base_delay: float) -> Topology:
    exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
    retry_exchange = RabbitExchange(RETRY_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
    dead_letter_exchange = RabbitExchange(
        DEAD_LETTER_EXCHANGE, type=ExchangeType.DIRECT, durable=True
    )
    queue = RabbitQueue(
        NEW_PAYMENTS_QUEUE,
        durable=True,
        routing_key=NEW_PAYMENTS_QUEUE,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
        },
    )
    dead_letter_queue = RabbitQueue(DEAD_LETTER_QUEUE, durable=True, routing_key=DEAD_LETTER_QUEUE)
    retry_queues = tuple(
        RabbitQueue(
            f"{NEW_PAYMENTS_QUEUE}.retry.{delay}ms",
            durable=True,
            routing_key=f"{NEW_PAYMENTS_QUEUE}.retry.{delay}ms",
            arguments={
                "x-message-ttl": delay,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": NEW_PAYMENTS_QUEUE,
            },
        )
        for delay in retry_delays(max_attempts, base_delay)
    )
    return Topology(
        exchange=exchange,
        retry_exchange=retry_exchange,
        dead_letter_exchange=dead_letter_exchange,
        queue=queue,
        dead_letter_queue=dead_letter_queue,
        retry_queues=retry_queues,
    )


def topology_from_settings(settings: Settings) -> Topology:
    return build_topology(settings.max_attempts, settings.retry_base_delay)


def create_broker(settings: Settings, prefetch_count: int | None = None) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=prefetch_count, publisher_confirms=True, on_return_raises=True
        ),
        graceful_timeout=30.0,
        log_level=logging.DEBUG,
    )


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    bindings = [
        (topology.exchange, topology.queue),
        (topology.dead_letter_exchange, topology.dead_letter_queue),
        *((topology.retry_exchange, queue) for queue in topology.retry_queues),
    ]
    for exchange, queue in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing())
