from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.models import Currency
from app.schemas import PaymentCreate

VALID = {
    "amount": "100.5",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://merchant.example/hooks",
}


def test_amount_is_quantized_to_cents() -> None:
    payment = PaymentCreate.model_validate(VALID)
    assert payment.amount == Decimal("100.50")
    assert str(payment.amount) == "100.50"
    assert payment.currency is Currency.RUB


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000"),
        ("currency", "GBP"),
        ("description", ""),
        ("webhook_url", "ftp://merchant.example/hooks"),
        ("webhook_url", "not-a-url"),
        ("metadata", ["list"]),
        ("description", "a\x00b"),
        ("metadata", {"k": float("nan")}),
        ("metadata", {"k": [1, float("inf")]}),
        ("metadata", {"k": {"nested": "a\x00"}}),
        ("metadata", {"a\x00": 1}),
    ],
)
def test_invalid_fields_are_rejected(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, field: value})


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "status": "succeeded"})


def test_metadata_defaults_to_empty_dict() -> None:
    payload = {key: value for key, value in VALID.items() if key != "metadata"}
    assert PaymentCreate.model_validate(payload).metadata == {}


def test_fingerprint_is_stable_for_equivalent_requests() -> None:
    first = PaymentCreate.model_validate(VALID)
    second = PaymentCreate.model_validate(
        {**VALID, "amount": "100.50", "metadata": {"order_id": 1}}
    )
    assert first.fingerprint() == second.fingerprint()


def test_fingerprint_differs_for_different_requests() -> None:
    first = PaymentCreate.model_validate(VALID)
    second = PaymentCreate.model_validate({**VALID, "amount": "100.51"})
    assert first.fingerprint() != second.fingerprint()


def test_max_attempts_requires_retry_queue() -> None:
    with pytest.raises(ValidationError):
        Settings(api_key="k", max_attempts=1, _env_file=None)
