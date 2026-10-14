import pytest
from sqlalchemy.engine import make_url
from yarl import URL

from app.config import DatabaseSettings, Settings

PASSWORD = "p@ss:w/rd+=#?1"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("DATABASE_URL", "RABBITMQ_URL", "API_KEY"):
        monkeypatch.delenv(name, raising=False)
    for prefix in ("POSTGRES_", "RABBITMQ_"):
        for suffix in ("HOST", "PORT", "USER", "PASSWORD", "DB", "VHOST"):
            monkeypatch.delenv(prefix + suffix, raising=False)


def test_defaults_point_to_localhost() -> None:
    settings = Settings(api_key="k", _env_file=None)
    assert settings.database_url == "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    assert settings.rabbitmq_url == "amqp://guest:guest@localhost:5672/"


def test_database_url_escapes_credentials() -> None:
    settings = DatabaseSettings(
        postgres_host="postgres",
        postgres_user="svc@team",
        postgres_password=PASSWORD,
        _env_file=None,
    )
    url = make_url(settings.database_url)
    assert url.username == "svc@team"
    assert url.password == PASSWORD
    assert url.host == "postgres"
    assert url.database == "payments"


def test_rabbitmq_url_escapes_credentials_and_vhost() -> None:
    settings = Settings(
        api_key="k",
        rabbitmq_host="rabbitmq",
        rabbitmq_user="svc",
        rabbitmq_password=PASSWORD,
        rabbitmq_vhost="pay/ments",
        _env_file=None,
    )
    url = URL(settings.rabbitmq_url)
    assert url.user == "svc"
    assert url.password == PASSWORD
    assert url.host == "rabbitmq"
    assert url.path == "/pay/ments"


def test_explicit_urls_take_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://a:b@db/x")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://a:b@mq/")
    monkeypatch.setenv("POSTGRES_PASSWORD", PASSWORD)
    settings = Settings(api_key="k", _env_file=None)
    assert settings.database_url == "postgresql+asyncpg://a:b@db/x"
    assert settings.rabbitmq_url == "amqp://a:b@mq/"
