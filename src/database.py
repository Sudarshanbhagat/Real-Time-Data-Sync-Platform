"""Async database engine, session factory and FastAPI dependency."""

import logging
from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config import get_settings
from src.models.base import Base

logger = logging.getLogger(__name__)


def build_engine(database_url: str) -> AsyncEngine:
    """Create an async engine with pool settings appropriate for the backend.

    SQLite (used in tests) does not support connection-pool sizing arguments,
    so those are only applied to server databases such as PostgreSQL.
    """
    settings = get_settings()
    kwargs: dict[str, Any] = {"echo": settings.db_echo, "pool_pre_ping": True}
    if not database_url.startswith("sqlite"):
        kwargs["pool_size"] = settings.db_pool_size
        kwargs["max_overflow"] = settings.db_max_overflow
    return create_async_engine(database_url, **kwargs)


# Creating the engine does not open a connection; that happens on first use.
engine: AsyncEngine = build_engine(get_settings().database_url)

# expire_on_commit=False keeps ORM attributes readable after commit, which
# avoids implicit (blocking-style) lazy reloads that async sessions forbid.
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields one session per request.

    The context manager guarantees the session is closed (and any open
    transaction rolled back) even if the request handler raises.
    """
    async with SessionLocal() as session:
        yield session


async def init_db(target_engine: AsyncEngine = engine) -> None:
    """Create all tables and indexes that do not exist yet (idempotent)."""
    # Import models so they register themselves on Base.metadata.
    import src.models  # noqa: F401

    async with target_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def check_db_connection(target_engine: AsyncEngine = engine) -> bool:
    """Return True if a trivial query succeeds against the database."""
    try:
        async with target_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001 - any failure means "not reachable"
        logger.exception("Database connectivity check failed")
        return False
