import random
import uuid
from decimal import Decimal

import pytest

from app.consumer.gateway import ChargeRequest, EmulatedGateway
from app.models import Currency, PaymentStatus

REQUEST = ChargeRequest(payment_id=uuid.uuid4(), amount=Decimal("10.00"), currency=Currency.USD)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded.append(delay)

    monkeypatch.setattr("app.consumer.gateway.asyncio.sleep", fake_sleep)
    return recorded


async def test_delay_is_within_bounds(sleeps: list[float]) -> None:
    gateway = EmulatedGateway(2.0, 5.0, 0.9, rng=random.Random(1))
    for _ in range(200):
        await gateway.charge(REQUEST)
    assert all(2.0 <= delay <= 5.0 for delay in sleeps)


async def test_success_rate_is_respected(sleeps: list[float]) -> None:
    gateway = EmulatedGateway(0, 0, 0.9, rng=random.Random(42))
    results = [await gateway.charge(REQUEST) for _ in range(5000)]
    succeeded = sum(result.status is PaymentStatus.SUCCEEDED for result in results)
    assert 0.88 < succeeded / len(results) < 0.92


async def test_failure_has_reason(sleeps: list[float]) -> None:
    result = await EmulatedGateway(0, 0, 0.0).charge(REQUEST)
    assert result.status is PaymentStatus.FAILED
    assert result.failure_reason == "declined_by_gateway"


def test_invalid_delay_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="min_delay"):
        EmulatedGateway(5.0, 2.0, 0.9)
