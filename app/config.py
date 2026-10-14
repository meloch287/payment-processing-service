from functools import lru_cache
from typing import Self
from urllib.parse import quote

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = ""
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "payments"
    postgres_password: SecretStr = SecretStr("payments")
    postgres_db: str = "payments"

    @model_validator(mode="after")
    def assemble_database_url(self) -> Self:
        if not self.database_url:
            self.database_url = URL.create(
                "postgresql+asyncpg",
                username=self.postgres_user,
                password=self.postgres_password.get_secret_value(),
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
            ).render_as_string(hide_password=False)
        return self


class Settings(DatabaseSettings):
    api_key: SecretStr
    rabbitmq_url: str = ""
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: SecretStr = SecretStr("guest")
    rabbitmq_vhost: str = "/"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    log_level: str = "INFO"

    outbox_relay_enabled: bool = True
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_retention_hours: float = Field(default=72, gt=0)
    outbox_cleanup_interval: float = Field(default=300, gt=0)

    consumer_prefetch: int = Field(default=128, ge=1)
    max_attempts: int = Field(default=3, ge=2)
    publish_timeout: float = Field(default=10.0, gt=0)
    processing_lease: float = Field(default=30.0, gt=0)
    retry_base_delay: float = Field(default=2.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout: float = Field(default=5.0, gt=0)
    webhook_secret: SecretStr | None = None

    @model_validator(mode="after")
    def assemble_rabbitmq_url(self) -> Self:
        if not self.rabbitmq_url:
            credentials = (
                f"{quote(self.rabbitmq_user, safe='')}:"
                f"{quote(self.rabbitmq_password.get_secret_value(), safe='')}"
            )
            vhost = "" if self.rabbitmq_vhost == "/" else quote(self.rabbitmq_vhost, safe="")
            self.rabbitmq_url = (
                f"amqp://{credentials}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
