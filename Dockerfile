# Keep both stages aligned with .python-version and the dev scripts.
FROM python:3.14-slim AS builder

WORKDIR /build

# setup.py builds the native compression codec; keep the compiler out of runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml setup.py README.md LICENSE ./
COPY src/ ./src/
RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels ".[server]"

FROM python:3.14-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/app \
    TZ=Asia/Shanghai

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir --no-index --find-links=/wheels "thspypc[server]" \
    && rm -rf /wheels \
    && useradd --create-home --uid 10001 app

USER app

# Fail the build if installed package data or runtime imports are missing; no login.
RUN python -c "from thspypc.server.app import create_app; import thspypc.codecs._compression_native; create_app().openapi()"

# Persist all home-based caches with: --mount source=thspypc-cache,target=/home/app
# Bind mounts must be writable by app (UID 10001).
EXPOSE 8765

# One process owns the shared THSClient and its HTTP/WebSocket connections.
CMD ["python", "-m", "thspypc.server", "--host", "0.0.0.0", "--port", "8765"]
