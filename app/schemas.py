import hashlib
import json
import math
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, UrlConstraints, field_validator

from app.models import Currency, PaymentStatus

MAX_AMOUNT = Decimal("9999999999999999.99")
CENT = Decimal("0.01")

WebhookUrl = Annotated[
    AnyHttpUrl, UrlConstraints(max_length=2048, allowed_schemes=["http", "https"])
]


def _ensure_storable(value: Any) -> None:
    if isinstance(value, str) and "\x00" in value:
        raise ValueError("must not contain NUL characters")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("must not contain NaN or Infinity")
    if isinstance(value, dict):
        for key, item in value.items():
            _ensure_storable(key)
            _ensure_storable(item)
    elif isinstance(value, list):
        for item in value:
            _ensure_storable(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, le=MAX_AMOUNT, decimal_places=2)]
    currency: Currency
    description: Annotated[str, Field(min_length=1, max_length=1024)]
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: WebhookUrl

    @field_validator("description")
    @classmethod
    def reject_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def require_storable_json(cls, value: dict[str, Any]) -> dict[str, Any]:
        _ensure_storable(value)
        return value

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(CENT)

    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class WebhookDelivery(BaseModel):
    attempts: int
    delivered_at: datetime | None
    last_error: str | None


class PaymentRead(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    failure_reason: str | None
    idempotency_key: str
    webhook_url: str
    webhook: WebhookDelivery
    created_at: datetime
    processed_at: datetime | None


class ErrorResponse(BaseModel):
    detail: str
