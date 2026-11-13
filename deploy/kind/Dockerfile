# Образ воркера для kind. Контекст сборки — корень репозитория.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY bench /app/bench

# PID 1 задаёт манифест (exec-форма). sh -c здесь нет.
