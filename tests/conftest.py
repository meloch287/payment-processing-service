import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.db import create_engine, create_session_factory

ROOT = Path(__file__).resolve().parent.parent
API_KEY = "test-api-key"
WEBHOOK_SECRET = "test-webhook-secret"


@pytest.fixture(scope="session")
def settings() -> Settings:
    database_url = os.environ.get("DATABASE_URL")
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    if not database_url or not rabbitmq_url:
        pytest.skip("DATABASE_URL and RABBITMQ_URL are required")
    return Settings(
        api_key=API_KEY,
        webhook_secret=WEBHOOK_SECRET,
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        outbox_relay_enabled=False,
        _env_file=None,
    )


async def upgrade_database(database_url: str) -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    await asyncio.to_thread(command.upgrade, config, "head")


@pytest.fixture(scope="session")
async def migrated(settings: Settings) -> None:
    await upgrade_database(settings.database_url)


@pytest.fixture(scope="session")
async def engine(settings: Settings, migrated: None) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
async def clean_db(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
