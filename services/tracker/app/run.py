"""Container entrypoint: apply migrations (optional), then serve API + consumer.

python -m app.run
"""
from pathlib import Path

import uvicorn
from alembic import command
from alembic.config import Config

from app.config import Settings
from app.logging_config import configure_logging
from app.main import create_app

SERVICE_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    if settings.run_migrations:
        # Safe for the single tracker replica; run as a separate step if you ever scale out.
        command.upgrade(Config(str(SERVICE_ROOT / "alembic.ini")), "head")
    uvicorn.run(create_app(settings), host="0.0.0.0", port=settings.port,
                log_config=None, access_log=False)


if __name__ == "__main__":
    main()
