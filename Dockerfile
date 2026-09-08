# Финальный этап: минимальный runtime
FROM python:3.12-slim

# Установка системных зависимостей + локали
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl \
        libasound2 \
        libnss3 \
        libatk-bridge2.0-0 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxrandr2 \
        libxtst6 \
        libgtk-3-0 \
        libpango-1.0-0 \
        libcairo2 \
        locales \
        xvfb \
        xauth \
    # Настройка русской локали
    && sed -i 's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen ru_RU.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Экспорт локали
ENV LANG=ru_RU.UTF-8
ENV LANGUAGE=ru_RU:ru
ENV LC_ALL=ru_RU.UTF-8

# Установка Poetry
RUN pip install poetry

# Копируем pyproject.toml и poetry.lock для кэширования
COPY pyproject.toml poetry.lock* ./

# Устанавливаем зависимости (без dev)
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi --no-root

# Копируем приложение
COPY . /app
WORKDIR /app

# Устанавливаем Chrome через переменную окружения Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Запуск через Xvfb
ENTRYPOINT playwright install chrome && xvfb-run --auto-servernum --server-arg="-screen 0 1920x1080x24" python main.py