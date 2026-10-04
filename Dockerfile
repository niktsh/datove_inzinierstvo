# Obraz aplikácie Kraków DI (plánovač, API, data lake, migrácie, nástroje).
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /app

# závislosti (vrstva sa prebuduje len pri zmene pyproject.toml / uv.lock)
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
COPY config ./config
COPY schemas ./schemas
COPY tools ./tools
COPY docs/asyncapi.yaml ./docs/asyncapi.yaml
RUN uv sync --frozen --no-dev

# neprivilegovaný používateľ; /app/data je pre heartbeat a pod.
RUN useradd --create-home --uid 10001 app && mkdir -p /app/data && chown -R app /app/data
USER app

CMD ["python", "-m", "krakow_di.scheduler"]
