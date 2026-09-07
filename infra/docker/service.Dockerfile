# One parametrized image for every Python service in the workspace.
#   docker build -f infra/docker/service.Dockerfile --build-arg PACKAGE=ov-producer --build-arg SERVICE_DIR=producer .
# Layering: lockfile + all member pyproject files first (dependency layer is cached until a
# dependency changes), then the source of common + this service.
FROM python:3.12-slim AS runtime
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

ARG PACKAGE
ARG SERVICE_DIR
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY common/pyproject.toml common/
COPY producer/pyproject.toml producer/
COPY processor/pyproject.toml processor/
COPY archiver/pyproject.toml archiver/
COPY gtfs-static/pyproject.toml gtfs-static/
COPY dashboard/pyproject.toml dashboard/
RUN uv sync --frozen --no-dev --no-install-workspace --package ${PACKAGE}

# common, producer and archiver are also libraries: the processor's replay tool reuses the
# GTFS-RT parser and the archive reader. All three are tiny.
COPY common common
COPY producer producer
COPY archiver archiver
COPY ${SERVICE_DIR} ${SERVICE_DIR}
RUN uv sync --frozen --no-dev --package ${PACKAGE}

RUN useradd --system --uid 1001 app && mkdir -p /cache && chown -R app:app /app /cache
USER app
