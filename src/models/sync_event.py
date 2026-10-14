"""ORM models for sync events, idempotency records and conflict records."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, CreatedAtMixin, JSONType, TimestampMixin


class IntegrationSystem(StrEnum):
    """External systems the platform can sync between."""

    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    STRIPE = "stripe"
    CUSTOM = "custom"  # in-house systems that already send canonical records


class SyncStatus(StrEnum):
    """Lifecycle of a single sync event."""

    PENDING = "pending"
    SYNCED = "synced"
    FAILED = "failed"


class IdempotencyStatus(StrEnum):
    """Lifecycle of an idempotency key."""

    PROCESSING = "processing"
    COMPLETED = "completed"


class ConflictStatus(StrEnum):
    """Lifecycle of a detected field-level conflict."""

    PENDING = "pending"
    RESOLVED = "resolved"
    MANUAL_REVIEW = "manual_review"


def _in_values(column: str, enum: type[StrEnum]) -> str:
    """Build a SQL CHECK expression restricting `column` to the enum's values."""
    values = ", ".join(f"'{member.value}'" for member in enum)
    return f"{column} IN ({values})"


class SyncEvent(TimestampMixin, Base):
    """One attempt to propagate an entity change from a source to a destination."""

    __tablename__ = "sync_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    source_system: Mapped[str] = mapped_column(String(50), nullable=False)
    destination_system: Mapped[str] = mapped_column(String(50), nullable=False)
    source_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    canonical_data: Mapped[dict[str, Any] | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=SyncStatus.PENDING.value, index=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    # When the event was acknowledged by Kafka; NULL = not (yet) published.
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Set by the background processor when the event leaves `pending`.
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Kafka consume -> database update, in milliseconds.
    processing_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)

    # lazy="raise" turns accidental implicit loads (illegal in async) into clear errors;
    # load explicitly with selectinload() when needed.
    conflicts: Mapped[list["ConflictRecord"]] = relationship(
        back_populates="sync_event", lazy="raise"
    )

    __table_args__ = (
        CheckConstraint(_in_values("status", SyncStatus), name="status_valid"),
        # Serves GET /status/{entity_id}: latest event for an entity is an index scan.
        Index("ix_sync_events_entity_id_created_at", "entity_id", "created_at"),
        # Serves per-integration dashboards / failure analysis in later phases.
        Index("ix_sync_events_source_destination", "source_system", "destination_system"),
        # Small partial index: lets a relay job find events that still need publishing.
        Index(
            "ix_sync_events_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
            sqlite_where=text("published_at IS NULL"),
        ),
    )

    def __repr__(self) -> str:
        """Return a concise debug representation."""
        return f"<SyncEvent id={self.id} entity_id={self.entity_id!r} status={self.status}>"


class IdempotencyRecord(CreatedAtMixin, Base):
    """Remembers the result of a request so retries return it instead of re-syncing."""

    __tablename__ = "idempotency_records"

    idempotency_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    # SHA-256 of the request body: detects a key being reused for a *different* request.
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=IdempotencyStatus.PROCESSING.value
    )
    sync_event_id: Mapped[int | None] = mapped_column(
        ForeignKey("sync_events.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        CheckConstraint(_in_values("status", IdempotencyStatus), name="status_valid"),
        # Supports a periodic TTL cleanup job (DELETE ... WHERE created_at < now() - interval).
        Index("ix_idempotency_records_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        """Return a concise debug representation."""
        return f"<IdempotencyRecord key={self.idempotency_key!r} status={self.status}>"


class ConflictRecord(CreatedAtMixin, Base):
    """A field changed concurrently in two systems, and how it was resolved."""

    __tablename__ = "conflict_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    sync_event_id: Mapped[int | None] = mapped_column(
        ForeignKey("sync_events.id", ondelete="SET NULL"), nullable=True, index=True
    )
    field_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value_from_system_a: Mapped[Any] = mapped_column(JSONType, nullable=True)
    value_from_system_b: Mapped[Any] = mapped_column(JSONType, nullable=True)
    resolution_strategy: Mapped[str | None] = mapped_column(String(50), nullable=True)
    resolution_result: Mapped[Any] = mapped_column(JSONType, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=ConflictStatus.PENDING.value, index=True
    )

    sync_event: Mapped[SyncEvent | None] = relationship(back_populates="conflicts", lazy="raise")

    __table_args__ = (CheckConstraint(_in_values("status", ConflictStatus), name="status_valid"),)

    def __repr__(self) -> str:
        """Return a concise debug representation."""
        return (
            f"<ConflictRecord id={self.id} entity_id={self.entity_id!r} "
            f"field={self.field_name!r} status={self.status}>"
        )
