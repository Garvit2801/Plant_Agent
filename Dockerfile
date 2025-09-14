# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt openpyxl

COPY . /app

ENV PORT=8080 USE_MOCK=1 MOCK_TICK_SEC=5 APPLY_ENABLED=1
EXPOSE 8080
CMD ["uvicorn", "agent.service:app", "--host", "0.0.0.0", "--port", "8080"]
