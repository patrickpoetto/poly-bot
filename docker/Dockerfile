# syntax=docker/dockerfile:1
# Multi-stage build — production image with Postgres + dashboard support.
# Build: docker build -f docker/Dockerfile -t poly-bot:latest .
# Dev:   docker compose -f docker/docker-compose.yml up
# Prod:  docker compose -f docker/docker-compose.prod.yml up -d

# -----------------------------------------------------------------------------
# Stage 1: builder
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md alembic.ini ./
COPY alembic ./alembic
COPY src ./src

# Dashboard globe view (no CDN required at runtime)
RUN mkdir -p src/poly_bot/catalog/static/vendor \
    && curl -fsSL -o src/poly_bot/catalog/static/vendor/globe.gl.min.js \
         "https://cdn.jsdelivr.net/npm/globe.gl@2.46.1/dist/globe.gl.min.js" \
    && curl -fsSL -o src/poly_bot/catalog/static/vendor/earth-night.jpg \
         "https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-night.jpg"

RUN pip install --no-cache-dir hatchling \
    && pip wheel --no-cache-dir --wheel-dir /wheels ".[production]"

# -----------------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 botuser

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

COPY alembic.ini ./
COPY alembic ./alembic
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER botuser

ENV POLY_BOT_ENV=production \
    POLY_BOT_LOG_JSON=true \
    POLY_BOT_DATA_DIR=/app/data

VOLUME ["/app/data"]

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["poly-bot", "run"]
