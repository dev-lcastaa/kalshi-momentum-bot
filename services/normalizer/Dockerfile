# syntax=docker/dockerfile:1
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
RUN chown -R 1000:1000 /app
USER 1000:1000
RUN pytest -q -p no:cacheprovider && ruff check --no-cache src tests

FROM base AS runtime
# StorageGuard is shared with ingest; explicitly retain that optional dependency contract.
RUN uv sync --frozen --no-dev --extra ingest --no-editable && rm -rf /root/.cache
USER 1000:1000
EXPOSE 8702
ENTRYPOINT ["python"]
CMD ["-m", "kmb.normalizer", "loop", "--interval", "900"]
