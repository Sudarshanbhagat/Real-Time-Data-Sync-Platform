"""Producer tests: serialization and error handling (mocked), plus real delivery."""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError

from src.kafka.producer import KafkaPublishError, SyncEventProducer, serialize
from src.kafka.schemas import SyncEventMessage, event_id_for
from tests.conftest import KAFKA_BOOTSTRAP


def make_message(**overrides: Any) -> SyncEventMessage:
    """A representative message; override any field."""
    fields: dict[str, Any] = {
        "event_id": event_id_for(42),
        "sync_id": 42,
        "entity_id": "contact_12345",
        "source_system": "salesforce",
        "destination_system": "hubspot",
        "canonical_data": {"fields": {"first_name": "Ada"}},
        "idempotency_key": "evt-1",
        "timestamp": datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
    }
    return SyncEventMessage(**{**fields, **overrides})


def test_serialize_produces_json_with_required_fields() -> None:
    """Messages are UTF-8 JSON containing everything downstream consumers need."""
    payload = json.loads(serialize(make_message()))

    assert payload["entity_id"] == "contact_12345"
    assert payload["source_system"] == "salesforce"
    assert payload["destination_system"] == "hubspot"
    assert payload["canonical_data"] == {"fields": {"first_name": "Ada"}}
    assert payload["timestamp"] == "2026-10-12T09:30:00Z"
    assert payload["event_type"] == "sync.created"
    assert SyncEventMessage.model_validate(payload) == make_message()


def test_event_id_is_stable_per_sync() -> None:
    """Re-publishing a sync reuses its event_id, so consumers can de-duplicate."""
    assert event_id_for(42) == event_id_for(42)
    assert event_id_for(42) != event_id_for(43)


async def test_publish_sends_keyed_json_and_reports_metadata() -> None:
    """publish_event sends JSON keyed by entity_id and returns where it landed."""
    mock = AsyncMock()
    mock.send_and_wait.return_value = SimpleNamespace(topic="sync_events", partition=0, offset=7)
    producer = SyncEventProducer(bootstrap_servers="unused:9092", topic="sync_events")
    producer._producer = mock  # simulate a started producer

    result = await producer.publish_event(make_message())

    mock.send_and_wait.assert_awaited_once()
    topic = mock.send_and_wait.call_args.args[0]
    kwargs = mock.send_and_wait.call_args.kwargs
    assert topic == "sync_events"
    assert kwargs["key"] == b"contact_12345"
    assert json.loads(kwargs["value"])["sync_id"] == 42
    assert (result.topic, result.partition, result.offset) == ("sync_events", 0, 7)
    assert result.latency_ms >= 0


async def test_publish_failure_is_logged_and_reraised(caplog: pytest.LogCaptureFixture) -> None:
    """Broker errors surface as KafkaPublishError, logged with entity context."""
    mock = AsyncMock()
    mock.send_and_wait.side_effect = KafkaTimeoutError()
    producer = SyncEventProducer(bootstrap_servers="unused:9092")
    producer._producer = mock

    with pytest.raises(KafkaPublishError):
        await producer.publish_event(make_message())

    assert "kafka_publish_failed" in caplog.text
    assert "entity_id=contact_12345" in caplog.text
    # The broken client is discarded so the next publish reconnects cleanly.
    mock.stop.assert_awaited_once()
    assert producer.is_connected is False


async def test_start_failure_raises_publish_error() -> None:
    """An unreachable broker on start() raises KafkaPublishError and leaves no client."""
    with patch("src.kafka.producer.AIOKafkaProducer") as cls:
        cls.return_value.start = AsyncMock(side_effect=KafkaConnectionError())
        cls.return_value.stop = AsyncMock()
        producer = SyncEventProducer(bootstrap_servers="unused:9092")

        with pytest.raises(KafkaPublishError):
            await producer.start()

    assert producer.is_connected is False
    cls.return_value.stop.assert_awaited_once()


async def test_publish_reconnects_lazily_after_failed_start() -> None:
    """If Kafka was down at startup, the next publish connects and succeeds."""
    with patch("src.kafka.producer.AIOKafkaProducer") as cls:
        client = cls.return_value
        client.start = AsyncMock()
        client.send_and_wait = AsyncMock(
            return_value=SimpleNamespace(topic="sync_events", partition=0, offset=0)
        )
        producer = SyncEventProducer(bootstrap_servers="unused:9092")

        await producer.publish_event(make_message())

    client.start.assert_awaited_once()
    assert producer.is_connected is True


async def test_real_publish_lands_in_topic(
    kafka_producer: SyncEventProducer, kafka_topic: str
) -> None:
    """Integration: a published event can be read back from the real broker."""
    message = make_message()

    result = await kafka_producer.publish_event(message)

    consumer = AIOKafkaConsumer(
        kafka_topic, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        record = await asyncio.wait_for(consumer.getone(), timeout=15)
    finally:
        await consumer.stop()
    assert result.topic == kafka_topic
    assert record.key == b"contact_12345"
    assert SyncEventMessage.model_validate_json(record.value) == message
