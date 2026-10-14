"""API tests for health, sync creation, status lookup, idempotency and Kafka publishing."""

import asyncio
import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.kafka.consumer import SyncEventConsumer
from src.kafka.producer import EventPublisher, SyncEventProducer, get_producer
from src.kafka.schemas import SyncEventMessage
from src.main import app
from src.models import IdempotencyRecord, SyncEvent
from tests.conftest import FakePublisher


async def test_health_returns_healthy(client: AsyncClient) -> None:
    """GET /health is a dependency-free liveness check."""
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


async def test_create_sync_persists_event(
    client: AsyncClient, db_session: AsyncSession, sync_payload: dict[str, Any]
) -> None:
    """POST /api/v1/sync stores the raw data as `pending` and returns a sync_id.

    Transformation to canonical data is the background processor's job (Phase 3).
    """
    response = await client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 200
    body = response.json()
    assert body["sync_id"] > 0
    assert body["entity_id"] == "contact_12345"
    assert body["status"] == "pending"
    assert body["latency_ms"] >= 0
    assert body["idempotent_replay"] is False

    event = await db_session.get(SyncEvent, body["sync_id"])
    assert event is not None
    assert event.source_data == sync_payload["data"]
    assert event.status == "pending"
    assert event.canonical_data is None


async def test_get_status_returns_latest_sync(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """GET /api/v1/status/{entity_id} returns the most recent event for the entity."""
    await client.post("/api/v1/sync", json=sync_payload)
    second = await client.post(
        "/api/v1/sync",
        json={**sync_payload, "source_system": "hubspot", "destination_system": "stripe"},
    )

    response = await client.get("/api/v1/status/contact_12345")

    assert response.status_code == 200
    body = response.json()
    assert body["sync_id"] == second.json()["sync_id"]
    assert body["source_system"] == "hubspot"
    assert body["status"] == "pending"
    assert "created_at" in body


async def test_get_status_unknown_entity_returns_404(client: AsyncClient) -> None:
    """Unknown entities produce a 404 with a helpful detail message."""
    response = await client.get("/api/v1/status/does_not_exist")

    assert response.status_code == 404
    assert "does_not_exist" in response.json()["detail"]


async def test_idempotency_key_prevents_duplicate_sync(
    client: AsyncClient, db_session: AsyncSession, sync_payload: dict[str, Any]
) -> None:
    """Retrying with the same key replays the original result and writes nothing new."""
    payload = {**sync_payload, "idempotency_key": "evt-0001"}

    first = await client.post("/api/v1/sync", json=payload)
    retry = await client.post("/api/v1/sync", json=payload)

    assert retry.status_code == 200
    assert retry.json()["sync_id"] == first.json()["sync_id"]
    assert retry.json()["idempotent_replay"] is True
    assert retry.headers["Idempotent-Replayed"] == "true"

    count = await db_session.scalar(select(func.count()).select_from(SyncEvent))
    assert count == 1
    assert await db_session.get(IdempotencyRecord, "evt-0001") is not None


async def test_idempotency_key_reused_with_different_body_is_rejected(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """Reusing a key for a different payload is a client bug and returns 422."""
    await client.post("/api/v1/sync", json={**sync_payload, "idempotency_key": "evt-0002"})

    response = await client.post(
        "/api/v1/sync",
        json={
            **sync_payload,
            "data": {"Email": "other@example.com"},
            "idempotency_key": "evt-0002",
        },
    )

    assert response.status_code == 422
    assert "different request body" in response.json()["detail"]


async def test_invalid_requests_return_422(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """Unknown systems, same source/destination and empty data are all rejected."""
    bad_bodies = [
        {**sync_payload, "source_system": "oracle"},
        {**sync_payload, "destination_system": "salesforce"},
        {**sync_payload, "data": {}},
        {key: value for key, value in sync_payload.items() if key != "entity_id"},
    ]
    for body in bad_bodies:
        response = await client.post("/api/v1/sync", json=body)
        assert response.status_code == 422, body


# --- Kafka publishing (fake publisher) ------------------------------------------


async def test_sync_publishes_one_event(
    client: AsyncClient,
    db_session: AsyncSession,
    fake_publisher: FakePublisher,
    sync_payload: dict[str, Any],
) -> None:
    """A successful sync publishes exactly one event and reports its latency."""
    response = await client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 200
    body = response.json()
    [message] = fake_publisher.messages
    assert message.sync_id == body["sync_id"]
    assert message.entity_id == "contact_12345"
    assert message.source_data == sync_payload["data"]
    assert body["publish_latency_ms"] == 0.1
    assert body["latency_ms"] >= body["publish_latency_ms"]

    event = await db_session.get(SyncEvent, body["sync_id"])
    assert event is not None and event.published_at is not None


async def test_duplicate_idempotency_key_publishes_once(
    client: AsyncClient, fake_publisher: FakePublisher, sync_payload: dict[str, Any]
) -> None:
    """Retries with the same idempotency key produce no second Kafka message."""
    payload = {**sync_payload, "idempotency_key": "evt-kafka-1"}

    for _ in range(3):
        assert (await client.post("/api/v1/sync", json=payload)).status_code == 200

    assert len(fake_publisher.messages) == 1


async def test_kafka_outage_saves_sync_and_returns_500(
    client: AsyncClient,
    db_session: AsyncSession,
    fake_publisher: FakePublisher,
    sync_payload: dict[str, Any],
) -> None:
    """Kafka down: the API answers (500, JSON), the sync is kept, the app keeps working."""
    fake_publisher.fail = True

    response = await client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 500
    assert "not published" in response.json()["detail"]
    event = await db_session.scalar(select(SyncEvent))
    assert event is not None and event.published_at is None
    assert (await client.get("/health")).status_code == 200
    assert (await client.get("/api/v1/status/contact_12345")).status_code == 200


async def test_retry_after_outage_publishes_saved_sync_once(
    client: AsyncClient,
    db_session: AsyncSession,
    fake_publisher: FakePublisher,
    sync_payload: dict[str, Any],
) -> None:
    """After an outage, a retry with the same key publishes the saved sync - once."""
    payload = {**sync_payload, "idempotency_key": "evt-outage-1"}
    fake_publisher.fail = True
    failed = await client.post("/api/v1/sync", json=payload)
    fake_publisher.fail = False

    retry = await client.post("/api/v1/sync", json=payload)
    again = await client.post("/api/v1/sync", json=payload)

    assert failed.status_code == 500
    assert retry.status_code == 200 and retry.json()["idempotent_replay"] is True
    assert again.status_code == 200
    assert len(fake_publisher.messages) == 1
    assert await db_session.scalar(select(func.count()).select_from(SyncEvent)) == 1


# --- End-to-end through real Kafka ---------------------------------------------


@pytest.fixture
def e2e_client(client: AsyncClient, kafka_producer: SyncEventProducer) -> AsyncClient:
    """HTTP client whose POST /api/v1/sync publishes to the per-test Kafka topic."""
    real: EventPublisher = kafka_producer
    app.dependency_overrides[get_producer] = lambda: real
    return client


async def read_topic(topic: str, expected: int) -> list[SyncEventMessage]:
    """Read `expected` messages from `topic` as a fresh consumer group."""
    consumer = SyncEventConsumer(topic=topic, group_id=f"test_{uuid.uuid4().hex}")
    await consumer.start()
    received: list[SyncEventMessage] = []
    try:

        async def collect() -> None:
            while len(received) < expected:
                received.extend(await consumer.poll(timeout_ms=500))

        await asyncio.wait_for(collect(), timeout=20)
        # Catch any extra (duplicate) messages beyond what we expected.
        received.extend(await consumer.poll(timeout_ms=1000))
    finally:
        await consumer.stop()
    return received


async def test_e2e_sync_event_reaches_kafka_with_schema(
    e2e_client: AsyncClient, kafka_topic: str, sync_payload: dict[str, Any]
) -> None:
    """Test C: POST /sync -> one Kafka message with the documented JSON schema."""
    response = await e2e_client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 200
    [message] = await read_topic(kafka_topic, 1)
    payload = message.model_dump(mode="json")
    assert {
        "event_id",
        "sync_id",
        "entity_id",
        "source_system",
        "destination_system",
        "source_data",
        "canonical_data",
        "timestamp",
    } <= payload.keys()
    assert payload["sync_id"] == response.json()["sync_id"]
    assert payload["source_system"] == "salesforce"
    assert payload["destination_system"] == "hubspot"
    assert payload["source_data"]["Email"] == "ada@example.com"
    assert payload["canonical_data"] is None  # filled in by the background processor
    assert payload["timestamp"].endswith("Z")
    assert response.json()["publish_latency_ms"] > 0


async def test_e2e_multiple_syncs_produce_multiple_messages(
    e2e_client: AsyncClient, kafka_topic: str, sync_payload: dict[str, Any]
) -> None:
    """Test B: N distinct syncs -> N messages, in order."""
    entity_ids = [f"contact_{n}" for n in range(5)]
    for entity_id in entity_ids:
        response = await e2e_client.post(
            "/api/v1/sync", json={**sync_payload, "entity_id": entity_id}
        )
        assert response.status_code == 200

    messages = await read_topic(kafka_topic, len(entity_ids))

    assert [m.entity_id for m in messages] == entity_ids


async def test_e2e_duplicate_idempotency_key_yields_one_message(
    e2e_client: AsyncClient, kafka_topic: str, sync_payload: dict[str, Any]
) -> None:
    """Duplicate requests with one idempotency key leave exactly one message in Kafka."""
    payload = {**sync_payload, "idempotency_key": f"evt-{uuid.uuid4().hex}"}
    for _ in range(3):
        assert (await e2e_client.post("/api/v1/sync", json=payload)).status_code == 200

    messages = await read_topic(kafka_topic, 1)

    assert len(messages) == 1
    assert messages[0].idempotency_key == payload["idempotency_key"]
