# syntax=docker/dockerfile:1
# Build from the repository root:
#   docker build -f services/collector/Dockerfile .
#   docker build -f services/collector/Dockerfile --target test .
FROM python:3.12-slim-bookworm AS base

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

FROM base AS test
RUN uv sync --frozen
COPY tests ./tests
RUN chown -R 1000:1000 /app
USER 1000:1000
RUN pytest -q -p no:cacheprovider && ruff check --no-cache src tests

FROM base AS runtime
RUN uv sync --frozen --no-dev --no-editable && rm -rf /root/.cache
RUN mkdir -p /var/lib/kmb-collector/spool && chown -R 1000:1000 /var/lib/kmb-collector
USER 1000:1000
EXPOSE 8710
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8710/health', timeout=3)"
CMD ["python", "-m", "kmb.collector", "run"]
