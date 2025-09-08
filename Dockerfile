FROM mcr.microsoft.com/playwright/python:v1.54.0-noble
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY . .
RUN apt-get update && apt-get install -y nano
RUN  pip install --upgrade pip \
    && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi --no-root

RUN playwright install chrome