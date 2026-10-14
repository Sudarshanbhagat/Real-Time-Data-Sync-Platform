"""Liveness and readiness endpoints."""

from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.workers.sync_processor import SyncProcessor, get_processor

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness probe")
async def health() -> dict[str, str]:
    """Return 200 while the process is up. Does not touch dependencies."""
    return {"status": "healthy"}


@router.get(
    "/health/ready",
    summary="Readiness probe",
    responses={503: {"description": "A dependency is unavailable"}},
)
async def readiness(db: AsyncSession = Depends(get_db)) -> JSONResponse:
    """Return 200 only if the database answers; 503 otherwise.

    Kept separate from `/health` so an orchestrator doesn't restart a healthy
    process just because the database is briefly unavailable.
    """
    try:
        await db.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable", "database": "unreachable"},
        )
    return JSONResponse(content={"status": "ready", "database": "ok"})


@router.get("/health/processor", summary="Background processor status and metrics")
async def processor_status(
    processor: SyncProcessor = Depends(get_processor),
) -> dict[str, Any]:
    """Whether the Kafka -> database processor is running, plus its counters.

    Counts and latency percentiles cover this process since startup; Phase 4
    exports the same numbers to Prometheus.
    """
    return {"running": processor.is_running, **processor.stats.snapshot()}
