from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="COORDINATOR_", env_file=".env", extra="ignore"
    )
    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://coordinator:local_only@localhost:5432/coordinator_db"
    )
    log_level: str = "INFO"
    dependency_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    seller_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    max_seller_concurrency: int = Field(default=5, ge=1, le=20)
    seller_urls: dict[str, str] = Field(default_factory=lambda: {
        f"seller-{i}": f"http://localhost:{8100+i}" for i in range(1, 6)
    })
    kafka_bootstrap_servers: str = "localhost:9092"
    # PROPOSED values until D publishes contracts/events/topics.md.
    incoming_topic: str = "order.created"
    workflow_topic: str = "workflow.events"
    consumer_group: str = "coordinator-v1"
    reconciliation_max_cycles: int = Field(default=3, ge=1, le=10)
    reconciliation_backoff_seconds: float = Field(default=1.0, gt=0, le=60)
    outbox_batch_size: int = Field(default=50, ge=1, le=1000)
    outbox_poll_seconds: float = Field(default=1.0, gt=0, le=60)
    kafka_operation_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    delivery_poll_seconds: float = Field(default=10.0, gt=0, le=300)
    delivery_poll_concurrency: int = Field(default=5, ge=1, le=20)

    @field_validator("log_level")
    @classmethod
    def valid_level(cls, value: str) -> str:
        value = value.upper()
        if value not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("unsupported log level")
        return value

    @field_validator("database_url")
    @classmethod
    def async_database(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(
            ("postgresql+asyncpg://", "sqlite+aiosqlite://")
        ):
            raise ValueError("use PostgreSQL/asyncpg; SQLite/aiosqlite is for tests only")
        return value
