"""Integration test: Worker_Web → DedupService → PostgreSQL.

Validates Requirement 2.1: end-to-end pipeline from a scraped web order
through category filtering and deduplication to lead storage in PostgreSQL.
HTTP/Playwright is mocked — the test exercises the real DedupService and database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select, func

from src.common.db import Lead
from src.dedup.service import DedupService
from src.worker_web.parsers.base import ScrapedOrder
from src.worker_web.worker import WorkerWeb, filter_by_category, _to_lead_candidate


@pytest_asyncio.fixture()
async def dedup(session_factory):
    """DedupService wired to the real test database."""
    return DedupService(session_factory=session_factory)


def _make_order(
    *,
    title: str = "Build a web app",
    source: str = "fl.ru",
    url: str = "https://fl.ru/projects/123",
    category: str | None = "web",
) -> ScrapedOrder:
    return ScrapedOrder(
        source=source,
        title=title,
        description="Full description here",
        url=url,
        budget=Decimal("50000"),
        category=category,
        published_at=datetime(2024, 7, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


class TestWorkerWebPipeline:
    """Full cycle: scraped order → category filter → dedup → PostgreSQL."""

    async def test_matching_order_creates_lead(self, dedup, session_factory):
        """An order passing the category filter should be stored as a Lead."""
        order = _make_order(category="web")
        assert filter_by_category(order, {"web", "mobile"})

        candidate = _to_lead_candidate(order)
        result = await dedup.check_and_store(candidate)

        assert result.is_duplicate is False

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 1

            lead = (await session.execute(select(Lead))).scalar_one()
            assert lead.source == "fl.ru"
            assert lead.title == "Build a web app"
            assert lead.url == "https://fl.ru/projects/123"

    async def test_non_matching_category_skipped(self, dedup, session_factory):
        """An order whose category is not in the filter should not be stored."""
        order = _make_order(category="design")
        assert not filter_by_category(order, {"web", "mobile"})

        # No lead should be created since the filter rejects it
        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 0

    async def test_duplicate_order_rejected(self, dedup, session_factory):
        """Submitting the same order twice should only create one Lead."""
        order = _make_order(url="https://fl.ru/projects/999")
        candidate = _to_lead_candidate(order)

        r1 = await dedup.check_and_store(candidate)
        r2 = await dedup.check_and_store(candidate)

        assert r1.is_duplicate is False
        assert r2.is_duplicate is True

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 1

    async def test_empty_category_filter_passes_all(self, dedup, session_factory):
        """When category_filters is empty, all orders pass through."""
        order = _make_order(category="anything")
        assert filter_by_category(order, set())

        candidate = _to_lead_candidate(order)
        result = await dedup.check_and_store(candidate)
        assert result.is_duplicate is False

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 1

    async def test_scrape_source_with_mocked_parser(self, dedup, session_factory):
        """WorkerWeb._scrape_source stores leads from a mocked parser."""
        orders = [
            _make_order(title="Build a Django REST API backend", url="https://fl.ru/a"),
            _make_order(title="Mobile iOS application for delivery service", url="https://fl.ru/b"),
        ]

        page = AsyncMock()
        page.close = AsyncMock()
        page.set_default_timeout = MagicMock()

        context = AsyncMock()
        context.new_page = AsyncMock(return_value=page)
        context.close = AsyncMock()

        browser = AsyncMock()
        browser.new_context = AsyncMock(return_value=context)

        proxy_pool = AsyncMock()
        proxy_pool.get_next = AsyncMock(return_value="http://proxy:8080")

        worker = WorkerWeb(
            dedup_service=dedup,
            proxy_pool=proxy_pool,
            browser_factory=AsyncMock(return_value=browser),
            category_filters={"web"},
        )
        worker._browser = browser
        worker._fl_parser.parse = AsyncMock(return_value=orders)

        result = await worker.scrape_fl_ru()

        assert len(result) == 2

        async with session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(Lead))).scalar_one()
            assert count == 2
