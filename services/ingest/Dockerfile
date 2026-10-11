# syntax=docker/dockerfile:1
# Build from the repo root:
#   docker build -f services/ingest/Dockerfile .                 -> runtime image
#   docker build -f services/ingest/Dockerfile --target test .   -> runs tests + lint (used by Jenkins)
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
RUN uv sync --frozen --extra ingest
COPY tests ./tests
# Run as the same non-root uid as production so permission behaviour matches.
RUN chown -R 1000:1000 /app
USER 1000:1000
RUN pytest -q -p no:cacheprovider && ruff check --no-cache src tests

FROM base AS runtime
RUN uv sync --frozen --no-dev --extra ingest --no-editable && rm -rf /root/.cache

# uid/gid 1000 matches the exFAT mount options (uid=1000,gid=1000) on node001.
USER 1000:1000
EXPOSE 8701
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8701/health', timeout=3)"
ENTRYPOINT ["python", "-m", "kmb.ingest"]
CMD ["serve"]