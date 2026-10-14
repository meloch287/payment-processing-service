from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"


class Settings(DatabaseSettings):
    api_key: SecretStr
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    log_level: str = "INFO"

    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_retention_hours: float = Field(default=72, gt=0)
    outbox_cleanup_interval: float = Field(default=300, gt=0)

    consumer_prefetch: int = Field(default=16, ge=1)
    max_attempts: int = Field(default=3, ge=2)
    publish_timeout: float = Field(default=10.0, gt=0)
    processing_lease: float = Field(default=30.0, gt=0)
    retry_base_delay: float = Field(default=2.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout: float = Field(default=5.0, gt=0)
    webhook_secret: SecretStr | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
