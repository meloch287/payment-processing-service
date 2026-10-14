import pytest

from app.messaging import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    build_topology,
    retry_delays,
)


@pytest.mark.parametrize(
    ("attempts", "base", "expected"),
    [(3, 2.0, (2000, 4000)), (4, 0.5, (500, 1000, 2000)), (1, 1.0, ())],
)
def test_retry_delays_grow_exponentially(
    attempts: int, base: float, expected: tuple[int, ...]
) -> None:
    assert retry_delays(attempts, base) == expected


def test_main_queue_dead_letters_into_dlq() -> None:
    topology = build_topology(3, 2.0)
    assert topology.queue.name == NEW_PAYMENTS_QUEUE
    assert topology.queue.arguments is not None
    assert (
        topology.queue.arguments.items()
        >= {
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
        }.items()
    )
    assert topology.dead_letter_queue.name == DEAD_LETTER_QUEUE


def test_retry_queues_return_messages_to_main_exchange() -> None:
    topology = build_topology(3, 2.0)
    assert [queue.name for queue in topology.retry_queues] == [
        "payments.new.retry.2000ms",
        "payments.new.retry.4000ms",
    ]
    for queue, ttl in zip(topology.retry_queues, (2000, 4000), strict=True):
        assert queue.arguments is not None
        assert (
            queue.arguments.items()
            >= {
                "x-message-ttl": ttl,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": NEW_PAYMENTS_QUEUE,
            }.items()
        )
    assert topology.retry_queue(1).name == "payments.new.retry.2000ms"
    assert topology.retry_queue(2).name == "payments.new.retry.4000ms"
