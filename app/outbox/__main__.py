import asyncio

from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.logging import configure_logging
from app.outbox.runner import running_relay
from app.runtime import install_stop_signals


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    try:
        async with running_relay(settings, create_session_factory(engine)) as stop:
            install_stop_signals(stop)
            await stop.wait()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
