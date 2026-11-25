"""Environment configuration. One codebase, five sellers: only env differs."""
from __future__ import annotations

from functools import lru_cache
from urllib.parse import quote

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Identity / business behaviour (differs per instance, see config/seller-N.env)
    seller_id: str = "seller-1"
    seller_name: str = "Seller One"
    delivery_hours: float = Field(48, gt=0, le=24 * 60, description="Hours from reservation to estimated delivery")
    price_multiplier: float = Field(1.0, gt=0)

    # Seed data
    seed_on_startup: bool = True
    seed_base: int = 20261124  # fixed -> reproducible inventory

    # Database. Either DATABASE_URL, or the parts (password comes from infra/.env via compose).
    database_url: str | None = None
    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "seller1_db"
    db_user: str = "seller1"
    db_password: str = ""
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_statement_timeout_ms: int = 5000

    # Admin / fault injection. Unset = admin endpoints disabled (403).
    admin_token: str | None = None

    log_level: str = "INFO"

    @property
    def sqlalchemy_url(self) -> str:
        if self.database_url:
            url = self.database_url
            # Accept plain postgres URLs; force the psycopg 3 driver.
            for prefix in ("postgresql+psycopg://", "postgresql://", "postgres://"):
                if url.startswith(prefix):
                    return "postgresql+psycopg://" + url[len(prefix):]
            return url
        return (
            f"postgresql+psycopg://{quote(self.db_user, safe='')}:{quote(self.db_password, safe='')}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def seller_number(self) -> int:
        """seller-3 -> 3 (used to derive per-seller seed data)."""
        digits = "".join(ch for ch in self.seller_id if ch.isdigit())
        return int(digits) if digits else 1


@lru_cache
def get_settings() -> Settings:
    return Settings()
