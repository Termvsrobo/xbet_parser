FROM mcr.microsoft.com/playwright/python:v1.54.0-noble
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY **.py .
COPY older.json .
COPY poetry.lock .
COPY pyproject.toml .
COPY saved_url.yaml .
COPY parsers parsers
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y xvfb
RUN apt-get install -qqy x11-apps
RUN apt-get install -y libnss3 \
                       libxss1 \
                       libasound2t64 \
                       fonts-noto-color-emoji \
                       python3-full
RUN python -m pip install --break-system-packages pipx
RUN pipx ensurepath --global --prepend
RUN pipx install --global poetry
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi --no-root

RUN playwright install chrome

ENTRYPOINT xvfb-run python main.py