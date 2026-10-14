import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.main import create_app
from app.models import OutboxEvent
from tests.conftest import API_KEY

pytestmark = pytest.mark.integration


@pytest.mark.usefixtures("migrated", "clean_db")
async def test_api_wakes_relay_right_after_commit(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    app = create_app(
        settings.model_copy(update={"outbox_relay_enabled": True, "outbox_poll_interval": 30})
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "10.00",
                "currency": "EUR",
                "description": "relay",
                "webhook_url": "https://merchant.example/h",
            },
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )
        assert response.status_code == 202
        published = None
        for _ in range(30):
            async with session_factory() as session:
                published = await session.scalar(select(OutboxEvent.published_at))
            if published is not None:
                break
            await asyncio.sleep(0.1)
        assert published is not None
