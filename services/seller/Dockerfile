FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ app/
COPY seed/ seed/
COPY migrations/ migrations/

RUN useradd --system --uid 10001 seller && chown -R seller /app
USER seller

# Each instance listens on its own port (seller-N -> 810N), set by PORT in config/seller-N.env.
ENV PORT=8000
EXPOSE 8101 8102 8103 8104 8105
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)"

# Migrations + seed run in the app's startup (idempotent; advisory-locked).
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port \"$PORT\" --workers 1 --no-access-log"]
