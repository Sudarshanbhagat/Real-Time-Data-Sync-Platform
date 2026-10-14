"""Create the `sync_events` topic if it doesn't exist (safe to run repeatedly).

Uses the same connection/security settings as the app, so it works against the
local broker and managed clusters (SSL/SASL) alike.

Usage:
    python scripts/create_topic.py                    # defaults: 3 partitions, RF 1
    python scripts/create_topic.py --partitions 2     # e.g. Aiven free tier (max 2)
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Allow `python scripts/create_topic.py` from the project root without installing.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: E402
from aiokafka.errors import TopicAlreadyExistsError  # noqa: E402

from src.config import get_settings  # noqa: E402


async def main(partitions: int, replication: int) -> int:
    """Ensure the topic exists. Returns a process exit code."""
    settings = get_settings()
    topic = settings.kafka_sync_events_topic
    admin = AIOKafkaAdminClient(**settings.kafka_client_options(), request_timeout_ms=10000)
    try:
        await asyncio.wait_for(admin.start(), timeout=20)
    except Exception as exc:  # noqa: BLE001 - report any connection failure plainly
        print(f"ERROR: cannot reach Kafka at {settings.kafka_bootstrap_servers}: {exc!r}")
        await admin.close()
        return 1
    try:
        if topic in await admin.list_topics():
            print(f"Topic '{topic}' already exists")
            return 0
        try:
            await admin.create_topics(
                [NewTopic(topic, num_partitions=partitions, replication_factor=replication)]
            )
        except TopicAlreadyExistsError:
            print(f"Topic '{topic}' already exists")
            return 0
        print(f"Created topic '{topic}' ({partitions} partitions, replication {replication})")
        return 0
    finally:
        await admin.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--partitions", type=int, default=int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "3"))
    )
    parser.add_argument(
        "--replication",
        type=int,
        default=int(os.environ.get("KAFKA_TOPIC_REPLICATION", "1")),
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.partitions, args.replication)))
