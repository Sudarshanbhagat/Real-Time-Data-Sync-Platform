"""Consumer tests: initialisation and deserialization (no broker), plus real consumption."""

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from aiokafka import AIOKafkaProducer

from src.kafka.consumer import SyncEventConsumer, deserialize
from src.kafka.producer import SyncEventProducer, serialize
from src.kafka.schemas import SyncEventMessage, event_id_for
from tests.conftest import KAFKA_BOOTSTRAP


def make_message(sync_id: int) -> SyncEventMessage:
    """A message whose identity is derived from `sync_id`."""
    return SyncEventMessage(
        event_id=event_id_for(sync_id),
        sync_id=sync_id,
        entity_id=f"contact_{sync_id}",
        source_system="salesforce",
        destination_system="hubspot",
        canonical_data={"fields": {"n": sync_id}},
        timestamp=datetime.now(UTC),
    )


async def poll_until(consumer: SyncEventConsumer, count: int) -> list[SyncEventMessage]:
    """Poll (committing each batch) until `count` messages arrive or 20 s pass."""
    received: list[SyncEventMessage] = []

    async def collect() -> None:
        while len(received) < count:
            received.extend(await consumer.poll(timeout_ms=500))
            await consumer.commit()

    await asyncio.wait_for(collect(), timeout=20)
    return received


def test_consumer_initialises_from_settings() -> None:
    """Defaults come from settings: sync_events topic, sync_group, start from earliest."""
    consumer = SyncEventConsumer()

    assert consumer.topic == "sync_events"
    assert consumer.group_id == "sync_group"
    assert consumer.bootstrap_servers == KAFKA_BOOTSTRAP
    assert consumer.auto_offset_reset == "earliest"
    assert consumer.is_running is False


async def test_poll_before_start_is_an_error() -> None:
    """Using the consumer before start() fails loudly instead of returning nothing."""
    with pytest.raises(RuntimeError, match="not started"):
        await SyncEventConsumer().poll()


def test_deserialize_skips_malformed_messages(caplog: pytest.LogCaptureFixture) -> None:
    """Bad JSON, wrong schema and empty values are skipped and logged, not raised."""
    assert deserialize(b"not json") is None
    assert deserialize(b'{"entity_id": "x"}') is None
    assert deserialize(None) is None
    assert caplog.text.count("kafka_message_skipped") == 3

    message = make_message(1)
    assert deserialize(serialize(message)) == message


async def test_producer_to_consumer_round_trip(
    kafka_producer: SyncEventProducer, kafka_topic: str
) -> None:
    """Test A: what the producer publishes is exactly what the consumer reads."""
    sent = make_message(1)
    await kafka_producer.publish_event(sent)

    consumer = SyncEventConsumer(topic=kafka_topic, group_id=f"test_{uuid.uuid4().hex}")
    await consumer.start()
    try:
        [received] = await poll_until(consumer, 1)
    finally:
        await consumer.stop()
    assert received == sent


async def test_run_handles_messages_and_skips_malformed(
    kafka_producer: SyncEventProducer, kafka_topic: str
) -> None:
    """run() passes valid events to the handler and survives a poison message."""
    raw = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await raw.start()
    try:
        await raw.send_and_wait(kafka_topic, b"{broken json")
    finally:
        await raw.stop()
    await kafka_producer.publish_event(make_message(1))
    await kafka_producer.publish_event(make_message(2))

    handled: list[int] = []

    async def handler(message: SyncEventMessage) -> None:
        handled.append(message.sync_id)

    consumer = SyncEventConsumer(topic=kafka_topic, group_id=f"test_{uuid.uuid4().hex}")
    try:
        count = await asyncio.wait_for(consumer.run(handler, max_messages=2), timeout=20)
    finally:
        await consumer.stop()
    assert count == 2
    assert handled == [1, 2]


async def test_consumer_group_resumes_from_committed_offset(
    kafka_producer: SyncEventProducer, kafka_topic: str
) -> None:
    """Test D: a restarted group member gets only new messages - no duplicates."""
    group_id = f"test_{uuid.uuid4().hex}"
    for sync_id in (1, 2):
        await kafka_producer.publish_event(make_message(sync_id))

    first = SyncEventConsumer(topic=kafka_topic, group_id=group_id)
    await first.start()
    try:
        first_batch = await poll_until(first, 2)
    finally:
        await first.stop()

    await kafka_producer.publish_event(make_message(3))

    second = SyncEventConsumer(topic=kafka_topic, group_id=group_id)
    await second.start()
    try:
        second_batch = await poll_until(second, 1)
        # Give any wrongly redelivered messages a chance to show up.
        second_batch.extend(await second.poll(timeout_ms=1000))
    finally:
        await second.stop()

    assert [m.sync_id for m in first_batch] == [1, 2]
    assert [m.sync_id for m in second_batch] == [3]
