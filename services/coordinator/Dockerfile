FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /service
COPY requirements.txt constraints.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home coordinator
COPY --chown=coordinator:coordinator app ./app
COPY --chown=coordinator:coordinator migrations ./migrations
COPY --chown=coordinator:coordinator alembic.ini .
USER coordinator
EXPOSE 8002
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002", "--no-access-log"]
