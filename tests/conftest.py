"""Shared pytest fixtures.

- Unit/API tests use an in-memory SQLite DB and a fake publisher (no Docker).
- Tests marked `@pytest.mark.kafka` use the real broker at KAFKA_BOOTSTRAP_SERVERS,
  each on its own throwaway topic; they are skipped if the broker is unreachable.
"""

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

# Must be set before `src` is imported so the app never points at a real database.
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["ENVIRONMENT"] = "test"

import pytest  # noqa: E402
from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: E402
from aiokafka.errors import KafkaError  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.config import get_settings  # noqa: E402
from src.database import get_db  # noqa: E402
from src.kafka.producer import (  # noqa: E402
    EventPublisher,
    KafkaPublishError,
    PublishResult,
    SyncEventProducer,
    get_producer,
)
from src.kafka.schemas import SyncEventMessage  # noqa: E402
from src.main import app  # noqa: E402
from src.models import Base  # noqa: E402

KAFKA_BOOTSTRAP = get_settings().kafka_bootstrap_servers


class FakePublisher:
    """In-memory stand-in for `SyncEventProducer`; set `fail=True` to simulate an outage."""

    def __init__(self) -> None:
        """Start with no published messages."""
        self.messages: list[SyncEventMessage] = []
        self.fail = False

    async def publish_event(self, message: SyncEventMessage) -> PublishResult:
        """Record the message, or raise like the real producer does when Kafka is down."""
        if self.fail:
            raise KafkaPublishError("simulated Kafka outage")
        self.messages.append(message)
        return PublishResult(
            topic="sync_events", partition=0, offset=len(self.messages) - 1, latency_ms=0.1
        )


@pytest.fixture
async def test_engine() -> AsyncIterator[AsyncEngine]:
    """Fresh in-memory database per test.

    StaticPool reuses one connection: each new connection to `:memory:` would
    otherwise see a separate, empty database.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(test_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory bound to the test database."""
    return async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """A session for tests that assert directly against the database."""
    async with session_factory() as session:
        yield session


@pytest.fixture
def fake_publisher() -> FakePublisher:
    """Captures published events instead of sending them to Kafka."""
    return FakePublisher()


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession], fake_publisher: FakePublisher
) -> AsyncIterator[AsyncClient]:
    """Async HTTP client wired to the app with the test DB and the fake publisher.

    The ASGI transport does not run the app lifespan, so the real producer is
    never started here. Real-Kafka tests swap in a real producer (`e2e_client`).
    """
    publisher: EventPublisher = fake_publisher

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_producer] = lambda: publisher
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def sync_payload() -> dict[str, Any]:
    """A valid Salesforce -> HubSpot sync request body."""
    return {
        "entity_id": "contact_12345",
        "source_system": "salesforce",
        "destination_system": "hubspot",
        "data": {"FirstName": "Ada", "LastName": "Lovelace", "Email": "ada@example.com"},
    }


# --- Real-Kafka fixtures -------------------------------------------------------


@pytest.fixture
async def kafka_topic() -> AsyncIterator[str]:
    """Create a unique, empty topic for one test and delete it afterwards.

    A fresh topic per test is the cleanup strategy: no test can see another
    test's messages, and nothing leaks into the real `sync_events` topic.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
    try:
        await admin.start()
    except KafkaError as exc:
        await admin.close()
        pytest.skip(f"Kafka not reachable at {KAFKA_BOOTSTRAP} ({exc!r}); run docker compose up -d")

    topic = f"test_sync_events_{uuid.uuid4().hex[:12]}"
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        yield topic
    finally:
        try:
            await admin.delete_topics([topic])
        finally:
            await admin.close()


@pytest.fixture
async def kafka_producer(kafka_topic: str) -> AsyncIterator[SyncEventProducer]:
    """A started real producer publishing to the per-test topic."""
    producer = SyncEventProducer(bootstrap_servers=KAFKA_BOOTSTRAP, topic=kafka_topic)
    await producer.start()
    yield producer
    await producer.stop()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test that uses a real-Kafka fixture with `kafka` (deselect with -m)."""
    for item in items:
        if {"kafka_topic", "kafka_producer"} & set(getattr(item, "fixturenames", ())):
            item.add_marker(pytest.mark.kafka)
