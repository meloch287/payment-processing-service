import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.events import PAYMENT_CREATED
from app.messaging import NEW_PAYMENTS_QUEUE
from app.models import OutboxEvent, Payment

pytestmark = pytest.mark.integration

URL = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Subscription",
    "metadata": {"user_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_payment_returns_202_and_writes_outbox(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(URL, json=BODY, headers=key())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"{URL}/{data['payment_id']}"
    assert "Idempotent-Replayed" not in response.headers

    async with session_factory() as session:
        [event] = (await session.scalars(select(OutboxEvent))).all()
    assert event.aggregate_id == uuid.UUID(data["payment_id"])
    assert event.event_type == PAYMENT_CREATED
    assert event.routing_key == NEW_PAYMENTS_QUEUE
    assert event.payload["payment_id"] == data["payment_id"]
    assert event.published_at is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    headers = key()
    created = (await client.post(URL, json=BODY, headers=headers)).json()

    response = await client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 200
    data = response.json()
    assert data["payment_id"] == created["payment_id"]
    assert data["amount"] == "1500.00"
    assert data["currency"] == "RUB"
    assert data["description"] == "Subscription"
    assert data["metadata"] == {"user_id": 42}
    assert data["status"] == "pending"
    assert data["idempotency_key"] == headers["Idempotency-Key"]
    assert data["webhook_url"] == BODY["webhook_url"]
    assert data["webhook"] == {"attempts": 0, "delivered_at": None, "last_error": None}
    assert data["processed_at"] is None


async def test_replay_with_same_key_returns_same_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    first = await client.post(URL, json=BODY, headers=headers)
    second = await client.post(URL, json={**BODY, "amount": "1500"}, headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_same_key_with_different_body_conflicts(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    await client.post(URL, json=BODY, headers=headers)
    response = await client.post(URL, json={**BODY, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409
    assert "Idempotency-Key" in response.json()["detail"]
    assert await count(session_factory, Payment) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    responses = await asyncio.gather(
        *(client.post(URL, json=BODY, headers=headers) for _ in range(20))
    )

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sum("Idempotent-Replayed" not in response.headers for response in responses) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


@pytest.mark.parametrize("api_key", [None, "wrong"])
@pytest.mark.parametrize("method", ["post", "get"])
async def test_requests_without_valid_api_key_are_rejected(
    client: httpx.AsyncClient, api_key: str | None, method: str
) -> None:
    headers = {**key()}
    client.headers.pop("X-API-Key")
    if api_key:
        headers["X-API-Key"] = api_key
    if method == "post":
        response = await client.post(URL, json=BODY, headers=headers)
    else:
        response = await client.get(f"{URL}/{uuid.uuid4()}", headers=headers)
    assert response.status_code == 401


async def test_missing_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(URL, json=BODY)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "patch",
    [{"amount": "-1"}, {"currency": "BTC"}, {"webhook_url": "nope"}, {"amount": "0.001"}],
)
async def test_invalid_body_is_rejected(client: httpx.AsyncClient, patch: dict[str, Any]) -> None:
    response = await client.post(URL, json={**BODY, **patch}, headers=key())
    assert response.status_code == 422


async def test_unknown_payment_returns_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")
    assert response.status_code == 404


@pytest.mark.parametrize("path", ["/health/live", "/health/ready", "/docs", "/openapi.json"])
async def test_service_endpoints_require_api_key(client: httpx.AsyncClient, path: str) -> None:
    assert (await client.get(path)).status_code == 200
    client.headers.pop("X-API-Key")
    assert (await client.get(path)).status_code == 401


async def test_openapi_documents_api_key_scheme(client: httpx.AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()
    assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
    assert "/api/v1/payments" in schema["paths"]


@pytest.mark.parametrize(
    "raw",
    [
        b'"metadata": {"k": NaN}',
        b'"metadata": {"k": Infinity}',
        b'"description": "a\\u0000b"',
    ],
)
async def test_values_postgres_cannot_store_are_rejected(
    client: httpx.AsyncClient, raw: bytes
) -> None:
    payload = b'{"amount": "1.00", "currency": "RUB", "description": "x", ' + raw
    payload += b', "webhook_url": "https://merchant.example/h"}'
    if raw.startswith(b'"description"'):
        payload = payload.replace(b'"description": "x", ', b"")
    response = await client.post(
        URL, content=payload, headers={**key(), "Content-Type": "application/json"}
    )
    assert response.status_code == 422
