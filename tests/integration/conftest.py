from collections.abc import AsyncIterator

import httpx
import pytest

from app.config import Settings
from app.main import create_app
from tests.conftest import API_KEY


@pytest.fixture
async def client(
    settings: Settings, migrated: None, clean_db: None
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client
