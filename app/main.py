from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import health, payments
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.logging import configure_logging


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
        app.state.settings = settings
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    app = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.include_router(payments.router)
    app.include_router(health.router)
    return app
