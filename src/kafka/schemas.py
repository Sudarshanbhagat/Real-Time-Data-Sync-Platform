"""Wire format of messages on the `sync_events` topic.

Producer and consumer share this model, so a schema change is made in one place.
Unknown fields are ignored on read, which lets producers add fields before
every consumer has been upgraded.
"""

import uuid
from datetime import UTC, datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field

from src.models.sync_event import SyncEvent

# Namespace for deterministic event IDs (uuid5 of the sync_id).
_EVENT_ID_NAMESPACE = uuid.UUID("5f1d7a52-3c1e-4b0a-9a51-6c2f0e8d4b17")


def event_id_for(sync_id: int) -> str:
    """Stable message ID for a sync: re-publishing the same sync reuses it."""
    return uuid.uuid5(_EVENT_ID_NAMESPACE, str(sync_id)).hex


class SyncEventMessage(BaseModel):
    """A sync that was recorded by the API, published for downstream processing."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(
        default_factory=lambda: uuid.uuid4().hex,
        description="Same for every publish of one sync; consumers de-duplicate on it.",
    )
    event_type: Literal["sync.created"] = "sync.created"
    # v2: carries raw `source_data`; `canonical_data` is filled in by the processor
    # in the database, so it is null on the wire for newly created syncs.
    schema_version: int = 2
    sync_id: int
    entity_id: str
    source_system: str
    destination_system: str
    source_data: dict[str, Any] = Field(default_factory=dict)
    canonical_data: dict[str, Any] | None = None
    idempotency_key: str | None = None
    timestamp: datetime = Field(description="When the sync was recorded (UTC).")

    @classmethod
    def from_sync_event(cls, event: SyncEvent, idempotency_key: str | None = None) -> Self:
        """Build the message for a persisted sync event."""
        created = event.created_at
        # SQLite (tests) drops tzinfo; every timestamp in this app is UTC.
        timestamp = created if created.tzinfo else created.replace(tzinfo=UTC)
        return cls(
            event_id=event_id_for(event.id),
            sync_id=event.id,
            entity_id=event.entity_id,
            source_system=event.source_system,
            destination_system=event.destination_system,
            source_data=event.source_data,
            canonical_data=event.canonical_data,
            idempotency_key=idempotency_key,
            timestamp=timestamp,
        )
