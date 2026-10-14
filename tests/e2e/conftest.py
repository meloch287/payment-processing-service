import asyncio
import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, ClassVar, Protocol

import aio_pika
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.messaging import DEAD_LETTER_QUEUE, NEW_PAYMENTS_QUEUE
from tests.conftest import ROOT, upgrade_database
from tests.e2e.receiver import WebhookReceiver

LOCAL_ENV = {
    "API_KEY": "e2e-api-key",
    "WEBHOOK_SECRET": "e2e-webhook-secret",
    "GATEWAY_MIN_DELAY": "0.5",
    "GATEWAY_MAX_DELAY": "1.0",
    "GATEWAY_SUCCESS_RATE": "0.9",
    "RETRY_BASE_DELAY": "1",
    "MAX_ATTEMPTS": "3",
    "OUTBOX_POLL_INTERVAL": "0.2",
    "WEBHOOK_TIMEOUT": "3",
    "LOG_LEVEL": "INFO",
}


@dataclass
class E2EConfig:
    base_url: str
    api_key: str
    webhook_secret: str
    rabbitmq_url: str
    webhook_host: str
    receiver_bind: str
    retry_base_delay: float
    gateway_min_delay: float
    gateway_max_delay: float
    database_url: str | None
    managed: bool
    compose_command: list[str] = field(default_factory=list)


class ServiceControl(Protocol):
    def stop(self, name: str) -> None: ...

    def start(self, name: str) -> None: ...


@dataclass
class ComposeServices:
    command: list[str]

    def _run(self, action: str, name: str) -> None:
        subprocess.run([*self.command, action, name], cwd=ROOT, check=True, timeout=120)

    def stop(self, name: str) -> None:
        self._run("stop", name)

    def start(self, name: str) -> None:
        self._run("start", name)


@dataclass
class ManagedProcesses:
    env: dict[str, str]
    log_dir: Path
    processes: dict[str, subprocess.Popen[bytes]] = field(default_factory=dict)
    logs: dict[str, IO[bytes]] = field(default_factory=dict)

    commands: ClassVar[dict[str, list[str]]] = {
        "api": ["-m", "uvicorn", "app.main:create_app", "--factory", "--host", "127.0.0.1"],
        "outbox-relay": ["-m", "app.outbox"],
        "consumer": ["-m", "app.consumer"],
    }

    def start(self, name: str, *extra: str) -> None:
        log = self.logs.get(name) or (self.log_dir / f"{name}.log").open("ab")
        self.logs[name] = log
        self.processes[name] = subprocess.Popen(
            [sys.executable, *self.commands[name], *extra],
            cwd=ROOT,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def stop(self, name: str) -> None:
        process = self.processes.pop(name)
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def stop_all(self) -> None:
        for name in list(self.processes):
            self.stop(name)
        for log in self.logs.values():
            log.close()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until_ready(url: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    raise TimeoutError(url)


@pytest.fixture(scope="package")
def e2e_config() -> E2EConfig:
    base_url = os.environ.get("E2E_BASE_URL")
    if base_url:
        return E2EConfig(
            base_url=base_url.rstrip("/"),
            api_key=os.environ["E2E_API_KEY"],
            webhook_secret=os.environ["E2E_WEBHOOK_SECRET"],
            rabbitmq_url=os.environ["E2E_RABBITMQ_URL"],
            webhook_host=os.environ.get("E2E_WEBHOOK_HOST", "host.docker.internal"),
            receiver_bind="0.0.0.0",
            retry_base_delay=float(os.environ.get("E2E_RETRY_BASE_DELAY", "2")),
            gateway_min_delay=float(os.environ.get("E2E_GATEWAY_MIN_DELAY", "2")),
            gateway_max_delay=float(os.environ.get("E2E_GATEWAY_MAX_DELAY", "5")),
            database_url=None,
            managed=False,
            compose_command=os.environ.get("E2E_COMPOSE", "").split(),
        )
    database_url = os.environ.get("DATABASE_URL")
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    if not database_url or not rabbitmq_url:
        pytest.skip("DATABASE_URL and RABBITMQ_URL are required for local e2e")
    return E2EConfig(
        base_url=f"http://127.0.0.1:{free_port()}",
        api_key=LOCAL_ENV["API_KEY"],
        webhook_secret=LOCAL_ENV["WEBHOOK_SECRET"],
        rabbitmq_url=rabbitmq_url,
        webhook_host="127.0.0.1",
        receiver_bind="127.0.0.1",
        retry_base_delay=float(LOCAL_ENV["RETRY_BASE_DELAY"]),
        gateway_min_delay=float(LOCAL_ENV["GATEWAY_MIN_DELAY"]),
        gateway_max_delay=float(LOCAL_ENV["GATEWAY_MAX_DELAY"]),
        database_url=database_url,
        managed=True,
    )


@pytest.fixture(scope="package")
async def reset_state(e2e_config: E2EConfig) -> None:
    if e2e_config.database_url:
        await upgrade_database(e2e_config.database_url)
        engine = create_async_engine(e2e_config.database_url)
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE payments, outbox"))
        await engine.dispose()


@pytest.fixture(scope="package")
def services(
    e2e_config: E2EConfig, reset_state: None, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[ServiceControl | None]:
    if not e2e_config.managed:
        yield ComposeServices(e2e_config.compose_command) if e2e_config.compose_command else None
        return
    port = e2e_config.base_url.rsplit(":", 1)[1]
    env = {
        **os.environ,
        **LOCAL_ENV,
        "DATABASE_URL": str(e2e_config.database_url),
        "RABBITMQ_URL": e2e_config.rabbitmq_url,
    }
    managed = ManagedProcesses(env=env, log_dir=tmp_path_factory.mktemp("e2e-logs"))
    try:
        managed.start("outbox-relay")
        managed.start("consumer")
        managed.start("api", "--port", port)
        wait_until_ready(f"{e2e_config.base_url}/health/ready")
        yield managed
    finally:
        managed.stop_all()
        for log in sorted(managed.log_dir.iterdir()):
            sys.stdout.write(f"\n===== {log.name} =====\n{log.read_text(errors='replace')[-6000:]}")


@pytest.fixture(scope="package")
async def amqp(e2e_config: E2EConfig) -> AsyncIterator[aio_pika.abc.AbstractRobustConnection]:
    connection = await aio_pika.connect_robust(e2e_config.rabbitmq_url)
    yield connection
    await connection.close()


async def queue_state(
    connection: aio_pika.abc.AbstractConnection, name: str
) -> tuple[int, int] | None:
    channel = await connection.channel()
    try:
        queue = await channel.declare_queue(name, passive=True)
    except aio_pika.exceptions.ChannelClosed:
        return None
    result = queue.declaration_result
    await channel.close()
    return result.message_count or 0, result.consumer_count or 0


async def wait_for_consumer(
    connection: aio_pika.abc.AbstractConnection, seconds: float = 60
) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = await queue_state(connection, NEW_PAYMENTS_QUEUE)
        if state and state[1]:
            return
        await asyncio.sleep(0.3)
    raise TimeoutError("consumer did not subscribe to payments.new")


async def drain_dead_letters(connection: aio_pika.abc.AbstractConnection) -> list[Any]:
    channel = await connection.channel()
    queue = await channel.declare_queue(DEAD_LETTER_QUEUE, passive=True)
    messages = []
    while (message := await queue.get(fail=False)) is not None:
        await message.ack()
        messages.append(message)
    await channel.close()
    return messages


@pytest.fixture(scope="package")
async def stack(
    services: ServiceControl | None,
    amqp: aio_pika.abc.AbstractRobustConnection,
) -> ServiceControl | None:
    await wait_for_consumer(amqp)
    await drain_dead_letters(amqp)
    return services


@pytest.fixture(scope="package")
def receiver(e2e_config: E2EConfig) -> Iterator[WebhookReceiver]:
    server = WebhookReceiver(host=e2e_config.receiver_bind)
    server.start()
    yield server
    server.stop()


@pytest.fixture
async def api(e2e_config: E2EConfig, stack: Any) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=e2e_config.base_url,
        headers={"X-API-Key": e2e_config.api_key},
        timeout=10,
    ) as client:
        yield client


@pytest.fixture
def hook_url(e2e_config: E2EConfig, receiver: WebhookReceiver) -> Any:
    def build(mode: str = "ok") -> tuple[str, str]:
        path = f"/{mode}/{uuid.uuid4()}"
        return path, f"http://{e2e_config.webhook_host}:{receiver.port}{path}"

    return build
