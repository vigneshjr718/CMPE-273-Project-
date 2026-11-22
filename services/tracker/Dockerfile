FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /service
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home tracker
COPY --chown=tracker:tracker app ./app
COPY --chown=tracker:tracker migrations ./migrations
COPY --chown=tracker:tracker alembic.ini .
USER tracker
EXPOSE 8004
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8004/health', timeout=2)"]
# Applies migrations (TRACKER_RUN_MIGRATIONS=true), then serves the API and runs the consumer.
CMD ["python", "-m", "app.run"]
