"""FastAPI application entry point.

Run with:  uvicorn src.main:app --reload
"""

import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse

from src.config import get_settings
from src.database import check_db_connection, engine
from src.kafka.producer import KafkaPublishError, get_producer
from src.observability.metrics import render
from src.observability.tracing import init_tracing, shutdown_tracing
from src.routes import health, sync
from src.workers.sync_processor import get_processor

settings = get_settings()

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("sync_platform")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Check the database, start the Kafka producer and the background processor.

    Shutdown runs in reverse: the processor finishes its batch and commits its
    offsets first, then the producer flushes, then the DB pool is released.
    """
    logger.info(
        "Starting %s v%s (%s)", settings.app_name, settings.app_version, settings.environment
    )
    init_tracing()
    if await check_db_connection():
        logger.info("Database connection OK")
    else:
        # Start anyway so /health works; /health/ready will report the problem.
        logger.warning("Database unreachable at startup - is `docker compose up -d` running?")

    producer = get_producer()
    try:
        await producer.start()
    except KafkaPublishError:
        # Start anyway: the producer reconnects on the next publish, and until
        # then POST /api/v1/sync saves the sync and returns 500.
        logger.warning("Kafka unreachable at startup - syncs will not be published until it is")

    processor = get_processor()
    if settings.run_sync_processor:
        # Runs independently of requests; retries on its own if Kafka is down.
        processor.start()
        logger.info("Background sync processor started")
    else:
        logger.info("Background sync processor disabled (RUN_SYNC_PROCESSOR=false)")
    yield
    await processor.stop()
    await producer.stop()
    await engine.dispose()
    shutdown_tracing()
    logger.info("Shutdown complete")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Synchronises entity changes between enterprise systems "
        "(Salesforce, HubSpot, Stripe) with idempotent, low-latency writes."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def request_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Attach a request ID and log method, path, status and duration for every request."""
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started) * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time-Ms"] = f"{duration_ms:.2f}"
    logger.info(
        "request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a JSON 500 instead of leaking a stack trace to the client."""
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus scrape endpoint (text exposition format). Unauthenticated: put it
    behind the network or a reverse proxy in a real deployment."""
    body, content_type = render()
    return Response(content=body, media_type=content_type)


app.include_router(health.router)
app.include_router(sync.router)
