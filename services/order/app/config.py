from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ORDER_", env_file=".env", extra="ignore")
    database_url: SecretStr = SecretStr("sqlite+aiosqlite:///./order.db")
    kafka_bootstrap_servers: str = "localhost:9092"
    created_topic: str = "order.created"
    kafka_enabled: bool = False
    status_base_url: str = "http://localhost:8004"
    log_level: str = Field(default="INFO")

    @field_validator("log_level")
    @classmethod
    def valid_level(cls, value: str) -> str:
        value = value.upper()
        if value not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("unsupported log level")
        return value


