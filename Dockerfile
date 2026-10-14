# Production image for the sync platform API + background processor.
#
#   docker build -t sync-platform .
#   docker run -p 8000:8000 -e DATABASE_URL=... -e KAFKA_BOOTSTRAP_SERVERS=... sync-platform
#
# The same image runs the API (default CMD) or a standalone processor:
#   docker run ... sync-platform python -m src.workers.sync_processor

FROM python:3.14-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, so code changes don't invalidate this layer.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src
COPY scripts ./scripts
COPY docker/entrypoint.sh /entrypoint.sh

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 app \
    && chmod +x /entrypoint.sh \
    && chown -R app:app /app
USER app

# Platforms like Render inject PORT; default to 8000 locally.
ENV PORT=8000
EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=4)"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["sh", "-c", "exec uvicorn src.main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]
