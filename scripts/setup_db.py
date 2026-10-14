"""Create the database schema (tables + indexes). Safe to run repeatedly.

Usage:
    poetry run python scripts/setup_db.py           # create missing tables
    poetry run python scripts/setup_db.py --reset   # DROP and recreate (destroys data)
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Allow `python scripts/setup_db.py` from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect, text  # noqa: E402
from sqlalchemy.engine import Connection  # noqa: E402

from src.database import check_db_connection, engine, init_db  # noqa: E402
from src.models import Base  # noqa: E402

# Columns added after Phase 1. create_all() never alters existing tables, so
# databases created earlier get them here. (Alembic replaces this later.)
UPGRADES = [
    "ALTER TABLE sync_events ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_sync_events_unpublished "
    "ON sync_events (id) WHERE published_at IS NULL",
    # Phase 3: background processing.
    "ALTER TABLE sync_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ",
    "ALTER TABLE sync_events ADD COLUMN IF NOT EXISTS processing_latency_ms DOUBLE PRECISION",
]


def _describe(conn: Connection) -> list[str]:
    """Return 'table (N indexes)' lines for every table in the database."""
    inspector = inspect(conn)
    return [
        f"  - {name} ({len(inspector.get_indexes(name))} indexes)"
        for name in sorted(inspector.get_table_names())
    ]


async def main(reset: bool) -> int:
    """Connect, optionally drop, create the schema and print a summary."""
    print(f"Connecting to {engine.url.render_as_string(hide_password=True)} ...")
    if not await check_db_connection():
        print("ERROR: cannot connect. Is PostgreSQL running? Try: docker compose up -d postgres")
        return 1

    if reset:
        print("Dropping all tables (--reset) ...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    print("Creating tables ...")
    await init_db()

    if engine.dialect.name == "postgresql":
        print("Applying column upgrades ...")
        async with engine.begin() as conn:
            for statement in UPGRADES:
                await conn.execute(text(statement))

    async with engine.connect() as conn:
        lines = await conn.run_sync(_describe)
    print("Database ready. Tables:")
    print("\n".join(lines))

    await engine.dispose()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--reset", action="store_true", help="drop all tables first (destroys data)"
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.reset)))
