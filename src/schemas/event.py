"""Pydantic request/response schemas for the sync API."""

from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.models.sync_event import IntegrationSystem, SyncStatus


class SyncRequest(BaseModel):
    """Body of `POST /api/v1/sync`."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "entity_id": "contact_12345",
                "source_system": "salesforce",
                "destination_system": "hubspot",
                "data": {"FirstName": "Ada", "LastName": "Lovelace", "Email": "ada@example.com"},
                "idempotency_key": "sf-contact_12345-evt-0001",
            }
        },
    )

    entity_id: str = Field(
        ..., min_length=1, max_length=255, description="ID of the entity being synced."
    )
    source_system: IntegrationSystem = Field(..., description="System the change originated in.")
    destination_system: IntegrationSystem = Field(
        ..., description="System to propagate the change to."
    )
    data: dict[str, Any] = Field(
        ..., min_length=1, description="Raw record payload from the source."
    )
    idempotency_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Client-supplied key; retries with the same key return the original result.",
    )

    @model_validator(mode="after")
    def _source_differs_from_destination(self) -> Self:
        """Reject syncing a system to itself, which would create a feedback loop."""
        if self.source_system == self.destination_system:
            raise ValueError("source_system and destination_system must be different")
        return self


class SyncResponse(BaseModel):
    """Result of `POST /api/v1/sync`."""

    sync_id: int
    entity_id: str
    status: SyncStatus
    latency_ms: float = Field(description="API latency: persistence + publish.")
    publish_latency_ms: float | None = Field(
        default=None, description="Time spent publishing the event to Kafka."
    )
    message: str
    idempotent_replay: bool = Field(
        default=False,
        description="True if this response was served from a previous identical request.",
    )


class SyncStatusResponse(BaseModel):
    """Latest sync state for an entity, returned by `GET /api/v1/status/{entity_id}`."""

    model_config = ConfigDict(from_attributes=True)

    sync_id: int = Field(validation_alias="id")
    entity_id: str
    source_system: str
    destination_system: str
    status: SyncStatus
    latency_ms: float | None
    error_message: str | None = Field(description="Why processing failed (status `failed`).")
    canonical_data: dict[str, Any] | None = Field(
        description="Canonical record; null until the background processor has run."
    )
    published_at: datetime | None
    processed_at: datetime | None
    processing_latency_ms: float | None
    created_at: datetime
    updated_at: datetime


class ErrorResponse(BaseModel):
    """Uniform error body (matches FastAPI's `HTTPException` shape)."""

    detail: str
