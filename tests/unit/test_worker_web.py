"""Unit tests for WorkerWeb — scraping, category filtering, error handling, and lifecycle."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import LeadCandidate
from src.dedup.service import DeduplicationResult
from src.worker_web.parsers.base import ScrapedOrder
from src.worker_web.proxy_pool import NoAvailableProxiesError
from src.worker_web.worker import (
    WorkerWeb,
    _to_lead_candidate,
    filter_by_category,
    _Http403Error,
    _Http429Error,
)


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def _make_order(
    source: str = "fl.ru",
    title: str = "Test Order",
    category: str | None = "Веб-разработка",
    is_urgent: bool = False,
    url: str = "https://example.com/1",
) -> ScrapedOrder:
    return ScrapedOrder(
        source=source,
        title=title,
        description="desc",
        url=url,
        budget=Decimal("10000"),
        category=category,
        published_at=datetime(2024, 6, 10, tzinfo=timezone.utc),
        is_urgent=is_urgent,
    )


def _make_mock_page(orders: list[ScrapedOrder] | None = None) -> AsyncMock:
    """Create a mock Playwright page."""
    page = AsyncMock()
    page.close = AsyncMock()
    page.set_default_timeout = MagicMock()
    return page


def _make_mock_context(page: AsyncMock) -> AsyncMock:
    context = AsyncMock()
    context.new_page = AsyncMock(return_value=page)
    context.close = AsyncMock()
    return context


def _make_mock_browser(context: AsyncMock) -> AsyncMock:
    browser = AsyncMock()
    browser.new_context = AsyncMock(return_value=context)
    browser.close = AsyncMock()
    return browser


def _make_dedup(is_duplicate: bool = False) -> AsyncMock:
    dedup = AsyncMock()
    dedup.check_and_store = AsyncMock(
        return_value=DeduplicationResult(is_duplicate=is_duplicate)
    )
    return dedup


def _make_proxy_pool(proxy: str = "http://proxy1:8080") -> AsyncMock:
    pool = AsyncMock()
    pool.get_next = AsyncMock(return_value=proxy)
    pool.mark_blocked = AsyncMock()
    return pool


# ---------------------------------------------------------------------------
# filter_by_category tests
# ---------------------------------------------------------------------------


class TestFilterByCategory:
    def test_empty_filter_passes_all(self) -> None:
        order = _make_order(category="anything")
        assert filter_by_category(order, set()) is True

    def test_matching_category_passes(self) -> None:
        order = _make_order(category="Веб-разработка")
        assert filter_by_category(order, {"Веб-разработка", "Дизайн"}) is True

    def test_non_matching_category_rejected(self) -> None:
        order = _make_order(category="Маркетинг")
        assert filter_by_category(order, {"Веб-разработка"}) is False

    def test_none_category_rejected_when_filter_set(self) -> None:
        order = _make_order(category=None)
        assert filter_by_category(order, {"Веб-разработка"}) is False

    def test_none_category_passes_when_no_filter(self) -> None:
        order = _make_order(category=None)
        assert filter_by_category(order, set()) is True


# ---------------------------------------------------------------------------
# _to_lead_candidate tests
# ---------------------------------------------------------------------------


class TestToLeadCandidate:
    def test_basic_conversion(self) -> None:
        order = _make_order()
        candidate = _to_lead_candidate(order)
        assert candidate.source == "fl.ru"
        assert candidate.title == "Test Order"
        assert candidate.description == "desc"
        assert candidate.url == "https://example.com/1"
        assert candidate.budget == Decimal("10000")
        assert candidate.category == "Веб-разработка"
        assert candidate.tags == []

    def test_urgent_order_gets_tag(self) -> None:
        order = _make_order(is_urgent=True)
        candidate = _to_lead_candidate(order)
        assert "urgent" in candidate.tags

    def test_non_urgent_order_no_tag(self) -> None:
        order = _make_order(is_urgent=False)
        candidate = _to_lead_candidate(order)
        assert "urgent" not in candidate.tags

    def test_zakupki_fields_preserved(self) -> None:
        order = ScrapedOrder(
            source="zakupki_gov",
            title="Procurement",
            description="desc",
            url="https://zakupki.gov.ru/1",
            okpd2_codes=["62.01.11.000"],
            max_contract_price=Decimal("5000000"),
            submission_deadline=datetime(2024, 7, 1, tzinfo=timezone.utc),
            is_urgent=True,
            published_at=datetime(2024, 6, 10, tzinfo=timezone.utc),
        )
        candidate = _to_lead_candidate(order)
        assert candidate.okpd2_codes == ["62.01.11.000"]
        assert candidate.max_contract_price == Decimal("5000000")
        assert candidate.submission_deadline == datetime(2024, 7, 1, tzinfo=timezone.utc)
        assert "urgent" in candidate.tags


# ---------------------------------------------------------------------------
# WorkerWeb lifecycle tests
# ---------------------------------------------------------------------------


class TestWorkerWebLifecycle:
    @pytest.mark.asyncio
    async def test_start_creates_three_tasks(self) -> None:
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        browser = _make_mock_browser(_make_mock_context(_make_mock_page()))
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        await worker.start()

        assert len(worker._tasks) == 5
        assert all(isinstance(t, asyncio.Task) for t in worker._tasks)
        assert worker._running is True

        await worker.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks_and_closes_browser(self) -> None:
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        browser = _make_mock_browser(_make_mock_context(_make_mock_page()))
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        await worker.start()
        await worker.stop()

        assert worker._running is False
        assert len(worker._tasks) == 0
        browser.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_custom_intervals(self) -> None:
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        browser = _make_mock_browser(_make_mock_context(_make_mock_page()))
        factory = AsyncMock(return_value=browser)

        intervals = {"fl_ru": 100, "kwork": 200, "zakupki": 300}
        worker = WorkerWeb(dedup, pool, factory, intervals=intervals)

        assert worker._intervals["fl_ru"] == 100
        assert worker._intervals["kwork"] == 200
        assert worker._intervals["zakupki"] == 300

        # Cleanup (no start needed)


# ---------------------------------------------------------------------------
# WorkerWeb scraping tests
# ---------------------------------------------------------------------------


class TestWorkerWebScraping:
    @pytest.mark.asyncio
    async def test_scrape_fl_ru_filters_and_stores(self) -> None:
        """Orders matching category filter are sent to dedup; others are dropped."""
        orders = [
            _make_order(source="fl.ru", category="Веб-разработка", url="https://fl.ru/1"),
            _make_order(source="fl.ru", category="Маркетинг", url="https://fl.ru/2"),
        ]
        dedup = _make_dedup(is_duplicate=False)
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(
            dedup, pool, factory, category_filters={"Веб-разработка"}
        )
        worker._browser = browser

        # Patch the parser to return our orders
        worker._fl_parser.parse = AsyncMock(return_value=orders)

        result = await worker.scrape_fl_ru()

        assert len(result) == 1
        assert result[0].category == "Веб-разработка"
        assert dedup.check_and_store.await_count == 1

    @pytest.mark.asyncio
    async def test_scrape_no_filter_passes_all(self) -> None:
        """When category_filters is empty, all orders pass."""
        orders = [
            _make_order(source="fl.ru", category="A", url="https://fl.ru/1"),
            _make_order(source="fl.ru", category="B", url="https://fl.ru/2"),
        ]
        dedup = _make_dedup(is_duplicate=False)
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, category_filters=set())
        worker._browser = browser
        worker._fl_parser.parse = AsyncMock(return_value=orders)

        result = await worker.scrape_fl_ru()

        assert len(result) == 2
        assert dedup.check_and_store.await_count == 2

    @pytest.mark.asyncio
    async def test_scrape_zakupki_urgent_tag(self) -> None:
        """Urgent zakupki orders get the 'urgent' tag in LeadCandidate."""
        order = ScrapedOrder(
            source="zakupki_gov",
            title="Urgent Procurement",
            description="desc",
            url="https://zakupki.gov.ru/1",
            is_urgent=True,
            published_at=datetime(2024, 6, 10, tzinfo=timezone.utc),
        )
        dedup = _make_dedup(is_duplicate=False)
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        worker._browser = browser
        worker._zakupki_parser.parse = AsyncMock(return_value=[order])

        await worker.scrape_zakupki()

        call_args = dedup.check_and_store.call_args[0][0]
        assert isinstance(call_args, LeadCandidate)
        assert "urgent" in call_args.tags

    @pytest.mark.asyncio
    async def test_scrape_duplicate_not_counted_as_created(self) -> None:
        """Duplicate leads are not counted in created_count."""
        orders = [_make_order(url="https://fl.ru/dup")]
        dedup = _make_dedup(is_duplicate=True)
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        worker._browser = browser
        worker._fl_parser.parse = AsyncMock(return_value=orders)

        result = await worker.scrape_fl_ru()

        assert len(result) == 1  # filtered orders returned
        assert dedup.check_and_store.await_count == 1


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestWorkerWebErrorHandling:
    @pytest.mark.asyncio
    async def test_timeout_triggers_retry(self) -> None:
        """Playwright timeout triggers a retry via _retry_with_new_proxy."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser

        call_count = 0

        async def side_effect(p):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TimeoutError("Playwright timeout")
            return []

        worker._fl_parser.parse = AsyncMock(side_effect=side_effect)

        result = await worker.scrape_fl_ru()

        assert result == []
        # Retry should have been attempted (parser called twice)
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_general_exception_returns_empty(self) -> None:
        """General exceptions are caught and return empty list."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        worker._browser = browser
        worker._profi_ru_parser.parse = AsyncMock(side_effect=RuntimeError("network error"))

        result = await worker.scrape_profi_ru()

        assert result == []

    @pytest.mark.asyncio
    async def test_no_proxies_on_retry_returns_empty(self) -> None:
        """When no proxies available for retry, returns empty list."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser

        # Parse raises timeout; retry with pool also fails
        worker._fl_parser.parse = AsyncMock(side_effect=TimeoutError("timeout"))
        pool.get_next = AsyncMock(
            side_effect=NoAvailableProxiesError("all blocked"),
        )

        result = await worker.scrape_fl_ru()
        assert result == []


# ---------------------------------------------------------------------------
# is_urgent_deadline delegation test
# ---------------------------------------------------------------------------


class TestIsUrgentDeadline:
    def test_delegates_to_base_function(self) -> None:
        from src.worker_web.parsers.base import is_urgent_deadline

        # A deadline far in the future should not be urgent
        far_future = datetime(2099, 12, 31, tzinfo=timezone.utc)
        assert is_urgent_deadline(far_future) is False

        # A deadline tomorrow should be urgent
        from datetime import timedelta
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        assert is_urgent_deadline(tomorrow) is True


# ---------------------------------------------------------------------------
# HTTP 429 and HTTP 403 error handling tests
# ---------------------------------------------------------------------------


class TestWorkerWebHttp429Handling:
    """Tests for HTTP 429 (Too Many Requests) handling.

    Validates: Requirements 2.4 — HTTP 429 causes 300s pause.
    """

    @pytest.mark.asyncio
    async def test_http_429_pauses(self) -> None:
        """HTTP 429 triggers a 300s pause and returns empty list."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser

        worker._fl_parser.parse = AsyncMock(side_effect=_Http429Error("429"))

        with patch("src.worker_web.worker.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await worker.scrape_fl_ru()

        assert result == []
        mock_sleep.assert_awaited_once_with(300)

    @pytest.mark.asyncio
    async def test_http_429_returns_empty_list(self) -> None:
        """After HTTP 429 handling, an empty list is returned (no retry)."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory)
        worker._browser = browser
        worker._profi_ru_parser.parse = AsyncMock(side_effect=_Http429Error("rate limited"))

        with patch("src.worker_web.worker.asyncio.sleep", new_callable=AsyncMock):
            result = await worker.scrape_profi_ru()

        assert result == []
        # Dedup should never be called since no orders were parsed
        dedup.check_and_store.assert_not_awaited()


class TestWorkerWebHttp403Handling:
    """Tests for HTTP 403 (Forbidden) handling.

    Validates: Requirements 2.5 — HTTP 403 triggers retry.
    """

    @pytest.mark.asyncio
    async def test_http_403_retries(self) -> None:
        """HTTP 403 triggers a retry that can succeed."""
        orders_on_retry = [_make_order(source="fl.ru", url="https://fl.ru/retry")]
        dedup = _make_dedup(is_duplicate=False)
        pool = _make_proxy_pool()
        pool.get_next = AsyncMock(return_value="http://proxy2:8080")
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser

        call_count = 0

        async def parse_side_effect(p):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _Http403Error("forbidden")
            return orders_on_retry

        worker._fl_parser.parse = AsyncMock(side_effect=parse_side_effect)

        result = await worker.scrape_fl_ru()

        # Retry should have succeeded with orders
        assert len(result) == 1
        assert result[0].url == "https://fl.ru/retry"

    @pytest.mark.asyncio
    async def test_http_403_retry_with_no_proxies_returns_empty(self) -> None:
        """When no proxies available for retry after 403, still retries with VPN proxy."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser
        # Both attempts fail with 403
        worker._fl_parser.parse = AsyncMock(side_effect=_Http403Error("forbidden"))

        pool.get_next = AsyncMock(
            side_effect=NoAvailableProxiesError("all blocked"),
        )

        result = await worker.scrape_fl_ru()
        assert result == []

    @pytest.mark.asyncio
    async def test_http_403_retry_failure_returns_empty(self) -> None:
        """When retry after 403 also fails, returns empty list."""
        dedup = _make_dedup()
        pool = _make_proxy_pool()
        pool.get_next = AsyncMock(return_value="http://proxy2:8080")
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, vpn_proxy_url="http://squid:3128")
        worker._browser = browser

        # Both attempts fail
        worker._profi_ru_parser.parse = AsyncMock(
            side_effect=[_Http403Error("forbidden"), RuntimeError("retry also failed")]
        )

        result = await worker.scrape_profi_ru()
        assert result == []


# ---------------------------------------------------------------------------
# Proxy routing (split traffic) tests
# ---------------------------------------------------------------------------


class TestProxyRouting:
    """Tests for split-traffic proxy routing.

    Russian sites (profi.ru, zakupki_gov) use direct_proxy dict,
    other sites use vpn_proxy_url.
    """

    def test_vpn_source_gets_vpn_proxy(self) -> None:
        """fl.ru (non-direct source) should use the VPN proxy."""
        worker = WorkerWeb(
            _make_dedup(), _make_proxy_pool(), AsyncMock(),
            vpn_proxy_url="http://squid:3128",
            direct_proxy={"server": "http://ru-proxy:8080", "username": "u", "password": "p"},
        )
        result = worker._proxy_for_source("fl.ru")
        assert result == {"server": "http://squid:3128"}

    def test_direct_source_gets_direct_proxy_with_auth(self) -> None:
        """zakupki_gov (direct source) should use the direct proxy with auth."""
        worker = WorkerWeb(
            _make_dedup(), _make_proxy_pool(), AsyncMock(),
            vpn_proxy_url="http://squid:3128",
            direct_proxy={"server": "http://ru-proxy:8080", "username": "u", "password": "p"},
        )
        result = worker._proxy_for_source("zakupki_gov")
        assert result == {"server": "http://ru-proxy:8080", "username": "u", "password": "p"}

    def test_profi_ru_no_longer_direct_source(self) -> None:
        """profi.ru is accessible from Europe — should NOT use direct proxy."""
        worker = WorkerWeb(
            _make_dedup(), _make_proxy_pool(), AsyncMock(),
            vpn_proxy_url="http://vpn:3128",
            direct_proxy={"server": "http://ru-proxy:8080"},
        )
        result = worker._proxy_for_source("profi.ru")
        assert result == {"server": "http://vpn:3128"}

    def test_direct_source_no_proxy_when_not_set(self) -> None:
        """When direct_proxy is None, direct sources get no proxy."""
        worker = WorkerWeb(
            _make_dedup(), _make_proxy_pool(), AsyncMock(),
            vpn_proxy_url="http://squid:3128",
            direct_proxy=None,
        )
        result = worker._proxy_for_source("zakupki_gov")
        assert result is None

    def test_vpn_source_no_proxy_when_not_set(self) -> None:
        """When vpn_proxy_url is None, VPN sources get no proxy."""
        worker = WorkerWeb(
            _make_dedup(), _make_proxy_pool(), AsyncMock(),
            vpn_proxy_url=None,
        )
        result = worker._proxy_for_source("fl.ru")
        assert result is None

    @pytest.mark.asyncio
    async def test_scrape_uses_correct_proxy_per_source(self) -> None:
        """Verify browser.new_context is called with the right proxy for each source."""
        dedup = _make_dedup(is_duplicate=False)
        pool = _make_proxy_pool()
        page = _make_mock_page()
        context = _make_mock_context(page)
        browser = _make_mock_browser(context)
        factory = AsyncMock(return_value=browser)

        direct = {"server": "http://ru-proxy:8080", "username": "u", "password": "p"}
        worker = WorkerWeb(
            dedup, pool, factory,
            vpn_proxy_url="http://squid:3128",
            direct_proxy=direct,
        )
        worker._browser = browser

        # Scrape fl.ru (VPN source)
        worker._fl_parser.parse = AsyncMock(return_value=[])
        await worker.scrape_fl_ru()
        browser.new_context.assert_awaited_with(proxy={"server": "http://squid:3128"})

        browser.new_context.reset_mock()

        # Scrape zakupki (direct source)
        worker._zakupki_parser.parse = AsyncMock(return_value=[])
        await worker.scrape_zakupki()
        browser.new_context.assert_awaited_with(proxy=direct)
