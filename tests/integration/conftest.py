"""Shared fixtures for integration tests.

Provides PostgreSQL and Redis containers via testcontainers,
plus pre-configured SQLAlchemy session factories and DedupService instances.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.common.db import Base, create_engine, create_session_factory


@pytest.fixture(scope="session")
def event_loop():
    """Create a single event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture()
async def pg_engine():
    """Spin up a PostgreSQL container and return an async engine.

    Uses testcontainers to start PostgreSQL 16, applies the DDL from
    ``migrations/001_init.sql``, and tears down after the test.
    """
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url().replace("psycopg2", "asyncpg")
        engine = create_engine(url)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        yield engine

        await engine.dispose()


@pytest_asyncio.fixture()
async def session_factory(pg_engine) -> async_sessionmaker[AsyncSession]:
    """Return a session factory bound to the test PostgreSQL engine."""
    return create_session_factory(pg_engine)
