"""Unit tests for DedupService.check_and_store pipeline.

Validates Requirements 4.1, 4.2, 4.3:
- Exact duplicate rejection with original hash logged
- Fuzzy duplicate rejection with original lead ID and similarity score
- Unique lead storage in PostgreSQL + Redis
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import LeadCandidate
from src.dedup.service import DedupService, DeduplicationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candidate(**overrides) -> LeadCandidate:
    """Build a LeadCandidate with sensible defaults."""
    defaults = {
        "source": "telegram",
        "title": "Python backend developer needed",
        "description": "Looking for a senior Python dev",
        "url": "https://example.com/job/123",
        "discovered_at": datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return LeadCandidate(**defaults)


def _make_session_factory(session: AsyncMock) -> MagicMock:
    """Create a mock async_sessionmaker that yields *session*.

    The factory returns an async context manager (``async with factory() as s``),
    and the session itself supports ``async with session.begin()``.
    """
    # session.begin() → async context manager (no-op)
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=None)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    session.begin = MagicMock(return_value=begin_cm)

    # session.flush() → coroutine
    session.flush = AsyncMock()
    # session.add → regular method (synchronous in SQLAlchemy)
    session.add = MagicMock()

    # factory() → async context manager yielding session
    factory = MagicMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    factory.return_value = cm

    return factory


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRejectExactDuplicate:
    """Validates: Requirements 4.2, 4.3

    When a candidate's hash already exists in Redis, check_and_store must
    return is_duplicate=True with match_type='exact_hash' and log a message
    containing the hash prefix.
    """

    async def test_reject_exact_duplicate_with_original_id_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Exact hash hit in Redis → duplicate rejected, log contains hash."""
        candidate = _make_candidate()

        # Redis mock: hash already exists
        redis_mock = AsyncMock()
        redis_mock.exists = AsyncMock(return_value=1)

        session = AsyncMock()
        factory = _make_session_factory(session)

        service = DedupService(session_factory=factory, redis=redis_mock)

        with caplog.at_level(logging.INFO, logger="src.dedup.service"):
            result = await service.check_and_store(candidate)

        assert result.is_duplicate is True
        assert result.match_type == "exact_hash"

        # The log must mention the rejection
        assert any("Exact duplicate rejected" in msg for msg in caplog.messages), (
            f"Expected 'Exact duplicate rejected' in logs, got: {caplog.messages}"
        )

        # The log must contain the hash prefix so operators can trace it
        expected_hash = DedupService.compute_hash(
            source=candidate.source, url=candidate.url
        )
        assert any(expected_hash[:12] in msg for msg in caplog.messages)


class TestRejectFuzzyDuplicate:
    """Validates: Requirements 4.1, 4.2

    When a candidate passes the exact hash check but has a title similar
    (≥85%) to an existing lead in the same source, check_and_store must
    return is_duplicate=True with match_type='fuzzy_match', the original
    lead's ID, and the similarity score.
    """

    async def test_reject_fuzzy_duplicate(self) -> None:
        """Similar title in same source → fuzzy duplicate rejected."""
        original_id = uuid.uuid4()
        existing_title = "Python backend developer needed urgently"
        candidate = _make_candidate(title="Python backend developer needed")

        # Redis mock: hash does NOT exist
        redis_mock = AsyncMock()
        redis_mock.exists = AsyncMock(return_value=0)

        # Session mock: return one existing lead with a similar title
        session = AsyncMock()
        factory = _make_session_factory(session)

        # Mock the execute() call inside check_fuzzy_duplicate
        mock_row = MagicMock()
        mock_row.__iter__ = MagicMock(return_value=iter((original_id, existing_title)))
        # Unpack support: lead_id, title = row
        mock_row.__getitem__ = MagicMock(
            side_effect=lambda i: (original_id, existing_title)[i]
        )

        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=[(original_id, existing_title)])
        session.execute = AsyncMock(return_value=mock_result)

        service = DedupService(session_factory=factory, redis=redis_mock)
        result = await service.check_and_store(candidate)

        assert result.is_duplicate is True
        assert result.match_type == "fuzzy_match"
        assert result.original_lead_id is not None
        assert result.similarity_score is not None
        assert result.similarity_score >= 85


class TestStoreUniqueLead:
    """Validates: Requirements 4.1, 4.2, 4.3

    When a candidate passes both exact and fuzzy checks, check_and_store
    must return is_duplicate=False, INSERT the lead into PostgreSQL, store
    the hash in Redis and PostgreSQL, and log 'New lead stored'.
    """

    async def test_store_unique_lead(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unique candidate → stored in DB + Redis, log confirms storage."""
        candidate = _make_candidate(
            title="Completely unique project title XYZ-999",
            url="https://example.com/unique/999",
        )

        # Redis mock: hash does NOT exist, set succeeds
        redis_mock = AsyncMock()
        redis_mock.exists = AsyncMock(return_value=0)
        redis_mock.set = AsyncMock(return_value=True)

        # Session mock: no existing leads for fuzzy check
        session = AsyncMock()
        factory = _make_session_factory(session)

        # First call (check_exact_duplicate fallback or fuzzy) returns empty
        # We need two separate session contexts:
        #   1. check_exact_duplicate — Redis succeeds, so no DB call
        #   2. check_fuzzy_duplicate — returns no rows
        #   3. check_and_store final block — INSERT lead + hash
        mock_result_empty = MagicMock()
        mock_result_empty.all = MagicMock(return_value=[])
        mock_result_empty.scalar_one_or_none = MagicMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result_empty)

        # After flush(), lead.id should be populated
        lead_id = uuid.uuid4()

        original_add = session.add

        def side_effect_add(obj):
            """Simulate SQLAlchemy populating lead.id after flush."""
            if hasattr(obj, "source"):  # It's a Lead
                obj.id = str(lead_id)

        session.add = MagicMock(side_effect=side_effect_add)

        service = DedupService(session_factory=factory, redis=redis_mock)

        with caplog.at_level(logging.INFO, logger="src.dedup.service"):
            result = await service.check_and_store(candidate)

        assert result.is_duplicate is False

        # Verify lead and hash were added to the session
        add_calls = session.add.call_args_list
        assert len(add_calls) >= 2, (
            f"Expected at least 2 session.add() calls (Lead + LeadHash), got {len(add_calls)}"
        )

        # Verify Redis.set was called to store the hash
        redis_mock.set.assert_called_once()

        # Verify log message
        assert any("New lead stored" in msg for msg in caplog.messages), (
            f"Expected 'New lead stored' in logs, got: {caplog.messages}"
        )
