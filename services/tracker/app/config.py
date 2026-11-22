from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRACKER_", env_file=".env", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://tracker:local_only@localhost:5432/tracker_db")
    log_level: str = "INFO"
    port: int = Field(default=8004, ge=1, le=65535)
    run_migrations: bool = True
    dependency_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    not_visible_retry_after_seconds: int = Field(default=2, ge=1, le=60)

    # Topic names/keys are defined in contracts/events/topics.md.
    kafka_bootstrap_servers: str = "localhost:9092"
    order_topic: str = "order.created"
    workflow_topic: str = "workflow.events"
    consumer_group: str = "tracker-v1"
    consumer_enabled: bool = True
    consumer_poll_timeout_ms: int = Field(default=1000, ge=10, le=60_000)
    consumer_max_records: int = Field(default=100, ge=1, le=1000)
    consumer_restart_backoff_seconds: float = Field(default=1.0, gt=0, le=60)
    consumer_restart_backoff_max_seconds: float = Field(default=30.0, gt=0, le=600)
    skip_invalid_events: bool = False

    @field_validator("log_level")
    @classmethod
    def valid_level(cls, value: str) -> str:
        value = value.upper()
        if value not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("unsupported log level")
        return value

    @field_validator("database_url")
    @classmethod
    def async_driver(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(("postgresql+asyncpg://", "sqlite+aiosqlite://")):
            raise ValueError("use postgresql+asyncpg (SQLite/aiosqlite is for tests only)")
        return value

    @model_validator(mode="after")
    def distinct_topics(self):
        if not self.order_topic.strip() or not self.workflow_topic.strip():
            raise ValueError("topic names must not be empty")
        if self.order_topic == self.workflow_topic:
            raise ValueError("order and workflow topics must differ (producer authority is per topic)")
        return self
