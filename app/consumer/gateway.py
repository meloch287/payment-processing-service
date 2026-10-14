import asyncio
import random
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from app.models import Currency, PaymentStatus


@dataclass(frozen=True, slots=True)
class ChargeRequest:
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency


@dataclass(frozen=True, slots=True)
class ChargeResult:
    status: PaymentStatus
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, request: ChargeRequest) -> ChargeResult: ...


class EmulatedGateway:
    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        if min_delay > max_delay:
            raise ValueError("min_delay must not exceed max_delay")
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, request: ChargeRequest) -> ChargeResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return ChargeResult(status=PaymentStatus.SUCCEEDED)
        return ChargeResult(status=PaymentStatus.FAILED, failure_reason="declined_by_gateway")
