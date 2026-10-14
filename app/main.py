from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse

from app.api import health, payments
from app.api.deps import verify_api_key
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.logging import configure_logging
from app.outbox.runner import running_relay
from app.schemas import ErrorResponse


async def validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    errors = exc.errors() if isinstance(exc, RequestValidationError) else []
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "detail": [
                {"type": error["type"], "loc": list(error["loc"]), "msg": error["msg"]}
                for error in errors
            ]
        },
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        app.state.settings = settings
        app.state.session_factory = session_factory
        async with AsyncExitStack() as stack:
            stack.push_async_callback(engine.dispose)
            if settings.outbox_relay_enabled:
                await stack.enter_async_context(running_relay(settings, session_factory))
            yield

    app = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        lifespan=lifespan,
        dependencies=[Depends(verify_api_key)],
        responses={status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse}},
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.include_router(payments.router)
    app.include_router(health.router)

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi() -> dict[str, Any]:
        return app.openapi()

    @app.get("/docs", include_in_schema=False)
    async def docs() -> HTMLResponse:
        return get_swagger_ui_html(openapi_url="/openapi.json", title=app.title)

    return app
