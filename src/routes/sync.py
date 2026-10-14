"""Sync API: submit entity changes and query their sync status."""

import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Path, Response, status
from opentelemetry.trace import Status, StatusCode
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.kafka.producer import EventPublisher, get_producer
from src.observability.metrics import API_LATENCY, ErrorType, record_error
from src.observability.tracing import get_tracer
from src.schemas.event import ErrorResponse, SyncRequest, SyncResponse, SyncStatusResponse
from src.services import sync_service
from src.services.sync_service import (
    EventPublishError,
    IdempotencyConflictError,
    SyncInProgressError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["sync"])


@router.post(
    "/sync",
    response_model=SyncResponse,
    status_code=status.HTTP_200_OK,
    summary="Sync an entity between two systems",
    responses={
        409: {"model": ErrorResponse, "description": "Same idempotency key still processing"},
        422: {"model": ErrorResponse, "description": "Invalid body, or idempotency key reused"},
        500: {"model": ErrorResponse, "description": "Saved, but not published to Kafka"},
        503: {"model": ErrorResponse, "description": "Database unavailable"},
    },
)
async def create_sync(
    request: SyncRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
    publisher: EventPublisher = Depends(get_producer),
) -> SyncResponse:
    """Record a sync of `entity_id` from `source_system` to `destination_system`
    and publish it to the `sync_events` Kafka topic.

    Supplying `idempotency_key` makes retries safe: a repeat of the same request
    returns the original result (with `idempotent_replay: true` and an
    `Idempotent-Replayed: true` header) instead of creating a second sync, and
    publishes no second Kafka message.

    If Kafka is unavailable the sync is still saved but the response is a 500;
    retrying with the same `idempotency_key` publishes the saved event.
    """
    started = time.perf_counter()
    outcome_label = "error"
    # FastAPI's built-in OpenTelemetry support opens the `POST /api/v1/sync` server
    # span; `sync.accept` (inside it) carries the sync's attributes and parents the
    # db/kafka spans.
    with get_tracer().start_as_current_span(
        "sync.accept",
        attributes={
            "sync.entity_id": request.entity_id,
            "sync.source_system": request.source_system.value,
            "sync.destination_system": request.destination_system.value,
        },
    ) as span:
        try:
            result = await _create_sync(request, response, db, publisher)
            outcome_label = "replayed" if result.idempotent_replay else "accepted"
            span.set_attribute("sync.id", result.sync_id)
            span.set_attribute("sync.idempotent_replay", result.idempotent_replay)
            return result
        except HTTPException as exc:
            if exc.status_code >= 500:
                span.set_status(Status(StatusCode.ERROR, str(exc.detail)))
            raise
        finally:
            API_LATENCY.labels(status=outcome_label).observe(time.perf_counter() - started)


async def _create_sync(
    request: SyncRequest, response: Response, db: AsyncSession, publisher: EventPublisher
) -> SyncResponse:
    """`create_sync` body: call the service and map its errors to HTTP statuses."""
    try:
        outcome = await sync_service.create_sync(db, request, publisher)
    except IdempotencyConflictError as exc:
        raise HTTPException(422, detail=str(exc)) from exc
    except SyncInProgressError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except EventPublishError as exc:
        logger.error(
            "Sync saved but not published sync_id=%s entity_id=%s source=%s destination=%s",
            exc.sync_id,
            exc.entity_id,
            request.source_system.value,
            request.destination_system.value,
        )
        hint = "retry with the same idempotency_key" if request.idempotency_key else "retry later"
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{exc}; event streaming is unavailable, {hint}",
        ) from exc
    except SQLAlchemyError as exc:
        logger.exception("Database error while syncing entity_id=%s", request.entity_id)
        record_error(ErrorType.DATABASE)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable, please retry"
        ) from exc

    if outcome.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return outcome.response


@router.get(
    "/status/{entity_id}",
    response_model=SyncStatusResponse,
    summary="Get the latest sync status for an entity",
    responses={
        404: {"model": ErrorResponse, "description": "No sync recorded for this entity"},
        503: {"model": ErrorResponse, "description": "Database unavailable"},
    },
)
async def get_sync_status(
    entity_id: str = Path(..., min_length=1, max_length=255),
    db: AsyncSession = Depends(get_db),
) -> SyncStatusResponse:
    """Return the most recent sync event recorded for `entity_id`."""
    try:
        event = await sync_service.get_latest_sync(db, entity_id)
    except SQLAlchemyError as exc:
        logger.exception("Database error while reading status for entity_id=%s", entity_id)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable, please retry"
        ) from exc

    if event is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail=f"No sync records found for entity '{entity_id}'"
        )
    return SyncStatusResponse.model_validate(event)
