"""Integration test: Worker_TG → DedupService → PostgreSQL.

Validates Requirement 1.1: end-to-end pipeline from a Telegram message
through keyword filtering and deduplication to lead storage in PostgreSQL.
Telethon is mocked — the test exercises the real DedupService and database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select, func

from src.common.db import Lead
from src.dedup.service import DedupService
from src.worker_tg.config_loader import ChannelsConfig
from src.worker_tg.worker import WorkerTG


@pytest_asyncio.fixture()
async def dedup(session_factory):
    """DedupService wired to the real test database, no Redis."""
    return DedupService(session_factory=session_factory, redis=None)


@pytest_asyncio.fixture()
async def worker(dedup):
    """WorkerTG with mocked Telethon client and real DedupService."""
    client = AsyncMock()
    config_loader = MagicMock()
    config_loader.config = ChannelsConfig(
        channels=["@test_channel"],
        keywords=["python", "backend"],
    )
    config_loader.load = MagicMock()
    return WorkerTG(
        client=client,
        dedup_service=dedup,
        config_loader=config_loader,
        notifier=None,
    )


def _make_event(text: str, message_id: int = 1) -> AsyncMock:
    """Create a fake Telethon NewMessage event."""
    event = AsyncMock()
    event.message.text = text
    event.message.date = datetime(2024, 7, 1, 10, 0, 0, tzinfo=timezone.utc)
    event.message.id = message_id
    event.message.sender = True

    chat = MagicMock()
    chat.username = "test_channel"
    event.get_chat = AsyncMock(return_value=chat)

    sender = MagicMock()
    sender.username = "author1"
    event.message.get_sender = AsyncMock(return_value=sender)
    return event


class TestWorkerTGPipeline:
    """Full cycle: message → filter → dedup → PostgreSQL."""

    async def test_matching_message_creates_lead(self, worker, session_factory):
        """A message containing a keyword should result in a new Lead row."""
        event = _make_event("Looking for a python backend developer", message_id=100)
        await worker._on_new_message(event)

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 1

            lead = (await session.execute(select(Lead))).scalar_one()
            assert lead.source == "telegram"
            assert "python" in lead.title.lower() or "backend" in lead.title.lower()

    async def test_non_matching_message_creates_no_lead(self, worker, session_factory):
        """A message without keywords should not create any Lead."""
        event = _make_event("Weather is nice today", message_id=200)
        await worker._on_new_message(event)

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 0

    async def test_duplicate_message_rejected(self, worker, session_factory):
        """Sending the same message twice should only create one Lead."""
        event1 = _make_event("Need python developer urgently", message_id=300)
        event2 = _make_event("Need python developer urgently", message_id=300)

        await worker._on_new_message(event1)
        await worker._on_new_message(event2)

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 1

    async def test_different_messages_both_stored(self, worker, session_factory):
        """Two distinct matching messages should create two Leads."""
        event1 = _make_event("Senior python engineer wanted", message_id=400)
        event2 = _make_event("Junior backend developer position", message_id=401)

        await worker._on_new_message(event1)
        await worker._on_new_message(event2)

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 2
