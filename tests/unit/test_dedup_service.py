"""Unit tests for DedupService.check_and_store pipeline.

Validates Requirements 4.1, 4.2, 4.3:
- Exact duplicate rejection via PostgreSQL hash lookup
- Fuzzy duplicate rejection with original lead ID and similarity score
- Unique lead storage in PostgreSQL
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

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
    """Create a mock async_sessionmaker that yields *session*."""
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=None)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    session.begin = MagicMock(return_value=begin_cm)

    session.flush = AsyncMock()
    session.add = MagicMock()

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

    When a candidate's hash already exists in PostgreSQL, check_and_store
    must return is_duplicate=True with match_type='exact_hash'.
    """

    async def test_reject_exact_duplicate_with_original_id_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Exact hash hit in PostgreSQL → duplicate rejected, log contains hash."""
        candidate = _make_candidate()

        session = AsyncMock()
        factory = _make_session_factory(session)

        # Mock: hash exists in PostgreSQL
        mock_result = MagicMock()
        mock_result.scalar_one_or_none = MagicMock(return_value="some_hash")
        session.execute = AsyncMock(return_value=mock_result)

        service = DedupService(session_factory=factory)

        with caplog.at_level(logging.INFO, logger="src.dedup.service"):
            result = await service.check_and_store(candidate)

        assert result.is_duplicate is True
        assert result.match_type == "exact_hash"

        assert any("Exact duplicate rejected" in msg for msg in caplog.messages), (
            f"Expected 'Exact duplicate rejected' in logs, got: {caplog.messages}"
        )

        expected_hash = DedupService.compute_hash(
            source=candidate.source, url=candidate.url
        )
        assert any(expected_hash[:12] in msg for msg in caplog.messages)


class TestRejectFuzzyDuplicate:
    """Validates: Requirements 4.1, 4.2

    When a candidate passes the exact hash check but has a title similar
    (≥85%) to an existing lead, check_and_store returns is_duplicate=True.
    """

    async def test_reject_fuzzy_duplicate(self) -> None:
        """Similar title in same source → fuzzy duplicate rejected."""
        original_id = uuid.uuid4()
        existing_title = "Python backend developer needed urgently"
        candidate = _make_candidate(title="Python backend developer needed")

        session = AsyncMock()
        factory = _make_session_factory(session)

        # First call: check_exact_duplicate → hash not found
        mock_result_no_hash = MagicMock()
        mock_result_no_hash.scalar_one_or_none = MagicMock(return_value=None)

        # Second call: check_fuzzy_duplicate → return existing lead
        mock_result_fuzzy = MagicMock()
        mock_result_fuzzy.all = MagicMock(return_value=[(original_id, existing_title)])

        session.execute = AsyncMock(
            side_effect=[mock_result_no_hash, mock_result_fuzzy]
        )

        service = DedupService(session_factory=factory)
        result = await service.check_and_store(candidate)

        assert result.is_duplicate is True
        assert result.match_type == "fuzzy_match"
        assert result.original_lead_id is not None
        assert result.similarity_score is not None
        assert result.similarity_score >= 85


class TestStoreUniqueLead:
    """Validates: Requirements 4.1, 4.2, 4.3

    When a candidate passes both checks, check_and_store stores it.
    """

    async def test_store_unique_lead(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unique candidate → stored in DB, log confirms storage."""
        candidate = _make_candidate(
            title="Completely unique project title XYZ-999",
            url="https://example.com/unique/999",
        )

        session = AsyncMock()
        factory = _make_session_factory(session)

        # First call: check_exact_duplicate → not found
        mock_result_no_hash = MagicMock()
        mock_result_no_hash.scalar_one_or_none = MagicMock(return_value=None)

        # Second call: check_fuzzy_duplicate → no matches
        mock_result_empty = MagicMock()
        mock_result_empty.all = MagicMock(return_value=[])

        session.execute = AsyncMock(
            side_effect=[mock_result_no_hash, mock_result_empty]
        )

        lead_id = uuid.uuid4()

        def side_effect_add(obj):
            if hasattr(obj, "source"):
                obj.id = str(lead_id)

        session.add = MagicMock(side_effect=side_effect_add)

        service = DedupService(session_factory=factory)

        with caplog.at_level(logging.INFO, logger="src.dedup.service"):
            result = await service.check_and_store(candidate)

        assert result.is_duplicate is False

        add_calls = session.add.call_args_list
        assert len(add_calls) >= 2, (
            f"Expected at least 2 session.add() calls (Lead + LeadHash), got {len(add_calls)}"
        )

        assert any("New lead stored" in msg for msg in caplog.messages), (
            f"Expected 'New lead stored' in logs, got: {caplog.messages}"
        )
