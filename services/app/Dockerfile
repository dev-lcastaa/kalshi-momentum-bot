# syntax=docker/dockerfile:1
# Backend (FastAPI + built UI) and replay/paper/live workers share this image.
# Build from the repository root:
#   docker build -f services/app/Dockerfile .                 # runtime
#   docker build -f services/app/Dockerfile --target test .   # python tests + lint
FROM node:22-bookworm-slim AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run lint && npm test && npm run build

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
COPY --from=ui /ui/dist /app/static
ENV KMB_BACKEND_STATIC_DIR=/app/static
USER 1000:1000
EXPOSE 8800
ENTRYPOINT ["python"]
CMD ["-m", "kmb.backend", "serve"]
