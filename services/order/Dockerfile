FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /service
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home order
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini .
COPY web ./web
USER order
EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--no-access-log"]

