import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


async def verify_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    expected = settings.api_key.get_secret_value().encode()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


SessionDep = Annotated[AsyncSession, Depends(get_session)]
