FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd -u 10001 -m appuser
USER 10001

CMD ["sh", "-c", "gunicorn -k uvicorn.workers.UvicornWorker app.main:app \
    --bind 0.0.0.0:${PORT} \
    --workers ${WEB_CONCURRENCY:-2} \
    --threads ${WEB_THREADS:-4} \
    --timeout ${GUNICORN_TIMEOUT:-120}"]
