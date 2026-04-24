"""Worker_Web — periodic web scraper for freelance marketplaces and procurement.

Scrapes FL.ru, Kwork, Weblancer, Profi.ru, and zakupki.gov.ru on configurable
intervals, filters by category/keywords, handles errors with proxy rotation,
and sends unique leads to DedupService.
"""

from __future__ import annotations

import asyncio
import logging
import re

from src.common.constants import HTTP_429_PAUSE_SECONDS, PLAYWRIGHT_TIMEOUT_MS
from src.common.models import LeadCandidate
from src.dedup.service import DedupService
from src.worker_web.parsers.base import ScrapedOrder
from src.worker_web.parsers.fl_ru import FlRuParser
from src.worker_web.parsers.kwork import KworkParser
from src.worker_web.parsers.profi_ru import ProfiRuParser
from src.worker_web.parsers.weblancer import WeblancerParser
from src.worker_web.parsers.zakupki_gov import ZakupkiGovParser
from src.worker_web.proxy_pool import NoAvailableProxiesError, ProxyPool

logger = logging.getLogger(__name__)

_DEFAULT_INTERVALS: dict[str, int] = {
    "fl_ru": 900,
    "zakupki": 3600,
}
_DEFAULT_EXCLUDE = ["wordpress", "вордпресс", "wp "]

# Sources that require a Russian IP — only zakupki.gov.ru blocks foreign traffic.
# profi.ru is accessible directly from Europe.
_DIRECT_SOURCES: frozenset[str] = frozenset({"zakupki_gov"})


class _Http429Error(Exception):
    """Raised when a source returns HTTP 429."""


class _Http403Error(Exception):
    """Raised when a source returns HTTP 403."""


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------


def _to_lead_candidate(order: ScrapedOrder, matched_kw: list[str] | None = None) -> LeadCandidate:
    """Convert a ScrapedOrder to a LeadCandidate for dedup processing."""
    tags: list[str] = ["urgent"] if order.is_urgent else []
    return LeadCandidate(
        source=order.source,
        title=order.title,
        description=order.description,
        url=order.url,
        budget=order.budget,
        budget_max=order.budget_max,
        category=order.category,
        matched_keywords=matched_kw or [],
        tags=tags,
        okpd2_codes=order.okpd2_codes,
        max_contract_price=order.max_contract_price,
        submission_deadline=order.submission_deadline,
        discovered_at=order.published_at,
    )


def filter_by_category(order: ScrapedOrder, allowed: set[str]) -> bool:
    """Return True if order passes the category filter.

    Empty *allowed* set means no filtering — all orders pass.
    """
    if not allowed:
        return True
    return order.category is not None and order.category in allowed


def filter_by_keywords(order: ScrapedOrder, keywords: list[str]) -> list[str]:
    """Return matched keywords from order title + description.

    Short keywords (≤4 chars) use word-boundary matching to avoid
    false positives like 'бот' matching 'работа'.
    """
    if not keywords:
        return []
    text = f"{order.title} {order.description}".lower()
    matched: list[str] = []
    for kw in keywords:
        kw_lower = kw.lower()
        if len(kw_lower) <= 4:
            if re.search(r"(?<!\w)" + re.escape(kw_lower) + r"(?!\w)", text):
                matched.append(kw)
        elif kw_lower in text:
            matched.append(kw)
    return matched


def matches_exclude_keywords(order: ScrapedOrder, exclude: list[str]) -> bool:
    """Return True if order matches any exclusion keyword."""
    if not exclude:
        return False
    text = f"{order.title} {order.description}".lower()
    return any(kw.lower() in text for kw in exclude)


# ---------------------------------------------------------------------------
# WorkerWeb
# ---------------------------------------------------------------------------


class WorkerWeb:
    """Periodic web scraper for freelance marketplaces and government procurement.

    Args:
        dedup_service: Service for deduplication and lead storage.
        proxy_pool: Pool of rotating proxies.
        browser_factory: Async callable returning a Playwright Browser.
        category_filters: Allowed categories (empty = no filtering).
        intervals: Scrape intervals per source key (seconds).
        web_keywords: Keywords for relevance filtering.
        exclude_keywords: Keywords that disqualify an order.
        vpn_proxy_url: Proxy URL for sites that need it (optional, not needed from Germany).
        direct_proxy: Playwright proxy dict for Russian-only sites (profi.ru, zakupki.gov.ru).
            Must include ``server`` key, and optionally ``username``/``password``.
            Should route through a Russian IP. When None, these sites are accessed directly.
    """

    def __init__(
        self,
        dedup_service: DedupService,
        proxy_pool: ProxyPool,
        browser_factory,  # noqa: ANN001
        category_filters: set[str] | None = None,
        intervals: dict[str, int] | None = None,
        web_keywords: list[str] | None = None,
        exclude_keywords: list[str] | None = None,
        vpn_proxy_url: str | None = None,
        direct_proxy: dict[str, str] | None = None,
    ) -> None:
        self._dedup = dedup_service
        self._proxy_pool = proxy_pool
        self._browser_factory = browser_factory
        self._category_filters: set[str] = category_filters or set()
        self._intervals = {**_DEFAULT_INTERVALS, **(intervals or {})}
        self._web_keywords: list[str] = web_keywords or []
        self._exclude_keywords: list[str] = exclude_keywords or list(_DEFAULT_EXCLUDE)
        self._vpn_proxy_url = vpn_proxy_url
        self._direct_proxy = direct_proxy
        self._browser = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

        # Parsers — exposed as attributes for testability
        self._fl_parser = FlRuParser()
        self._kwork_parser = KworkParser()
        self._weblancer_parser = WeblancerParser()
        self._zakupki_parser = ZakupkiGovParser()
        self._profi_ru_parser = ProfiRuParser()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Launch periodic scraping tasks for all sources."""
        self._browser = await self._browser_factory()
        self._running = True

        fl_interval = self._intervals["fl_ru"]
        self._tasks = [
            asyncio.create_task(self._periodic_scrape("fl_ru", self.scrape_fl_ru, fl_interval)),
            asyncio.create_task(self._periodic_scrape("kwork", self.scrape_kwork, fl_interval)),
            asyncio.create_task(self._periodic_scrape("weblancer", self.scrape_weblancer, fl_interval)),
            asyncio.create_task(self._periodic_scrape("profi_ru", self.scrape_profi_ru, fl_interval)),
            asyncio.create_task(self._periodic_scrape("zakupki", self.scrape_zakupki, self._intervals["zakupki"])),
        ]
        logger.info(
            "WorkerWeb started: fl_ru/kwork/weblancer/profi_ru=%ds, zakupki=%ds",
            fl_interval, self._intervals["zakupki"],
        )
        logger.info(
            "Proxy routing: direct (RU sites)=%s, other=%s",
            self._direct_proxy.get("server", "none") if self._direct_proxy else "none (no proxy)",
            self._vpn_proxy_url or "direct",
        )

    async def stop(self) -> None:
        """Cancel all scraping tasks and close the browser."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception:
                logger.exception("Error closing Playwright browser")
            self._browser = None
        logger.info("WorkerWeb stopped")

    # ------------------------------------------------------------------
    # Public scrape methods (one per source)
    # ------------------------------------------------------------------

    async def scrape_fl_ru(self) -> list[ScrapedOrder]:
        """Scrape FL.ru and send leads to DedupService."""
        return await self._scrape_source("fl.ru", self._fl_parser)

    async def scrape_kwork(self) -> list[ScrapedOrder]:
        """Scrape Kwork.ru and send leads to DedupService."""
        return await self._scrape_source("kwork", self._kwork_parser)

    async def scrape_weblancer(self) -> list[ScrapedOrder]:
        """Scrape Weblancer.net and send leads to DedupService."""
        return await self._scrape_source("weblancer", self._weblancer_parser)

    async def scrape_profi_ru(self) -> list[ScrapedOrder]:
        """Scrape Profi.ru and send leads to DedupService."""
        return await self._scrape_source("profi.ru", self._profi_ru_parser)

    async def scrape_zakupki(self) -> list[ScrapedOrder]:
        """Scrape zakupki.gov.ru and send leads to DedupService."""
        return await self._scrape_source("zakupki_gov", self._zakupki_parser)

    # ------------------------------------------------------------------
    # Proxy selection
    # ------------------------------------------------------------------

    def _proxy_for_source(self, source_name: str) -> dict | None:
        """Return Playwright proxy dict for the given source.

        Russian-only sites (profi.ru, zakupki_gov) use ``_direct_proxy``
        (or no proxy when it is not set).  All other sites go through the
        VPN proxy (``_vpn_proxy_url``).
        """
        if source_name in _DIRECT_SOURCES:
            return dict(self._direct_proxy) if self._direct_proxy else None
        if self._vpn_proxy_url:
            return {"server": self._vpn_proxy_url}
        return None

    # ------------------------------------------------------------------
    # Core scraping logic
    # ------------------------------------------------------------------

    async def _scrape_source(self, source_name: str, parser) -> list[ScrapedOrder]:  # noqa: ANN001
        """Run a single scrape cycle for *source_name*.

        Handles proxy routing (VPN vs direct), proxy-pool rotation for
        fallback, HTTP 429/403, and Playwright timeouts.
        """
        context = None
        page = None
        try:
            ctx_kwargs: dict = {}
            proxy_cfg = self._proxy_for_source(source_name)
            if proxy_cfg:
                ctx_kwargs["proxy"] = proxy_cfg
            context = await self._browser.new_context(**ctx_kwargs)
            page = await context.new_page()
            page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            orders = await parser.parse(page)
        except _Http429Error:
            logger.warning("HTTP 429 from %s — pausing %ds", source_name, HTTP_429_PAUSE_SECONDS)
            await asyncio.sleep(HTTP_429_PAUSE_SECONDS)
            return []
        except _Http403Error:
            logger.warning("HTTP 403 from %s — retrying", source_name)
            return await self._retry_with_new_proxy(source_name, parser)
        except TimeoutError:
            logger.warning("Timeout for %s — retrying", source_name)
            return await self._retry_with_new_proxy(source_name, parser)
        except Exception:
            logger.exception("Error scraping %s", source_name)
            return []
        finally:
            await self._close_resources(page, context)

        return await self._filter_and_store(source_name, orders)

    async def _retry_with_new_proxy(self, source_name: str, parser) -> list[ScrapedOrder]:  # noqa: ANN001
        """Single retry of a scrape, respecting the source proxy routing."""
        context = None
        page = None
        try:
            ctx_kwargs: dict = {}
            # For direct sources, retry with the same direct route.
            # For VPN sources, try a proxy from the pool as fallback.
            if source_name in _DIRECT_SOURCES:
                proxy_cfg = self._proxy_for_source(source_name)
                if proxy_cfg:
                    ctx_kwargs["proxy"] = proxy_cfg
            else:
                try:
                    pool_proxy = await self._proxy_pool.get_next()
                    ctx_kwargs["proxy"] = {"server": pool_proxy}
                except NoAvailableProxiesError:
                    proxy_cfg = self._proxy_for_source(source_name)
                    if proxy_cfg:
                        ctx_kwargs["proxy"] = proxy_cfg

            context = await self._browser.new_context(**ctx_kwargs)
            page = await context.new_page()
            page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            orders = await parser.parse(page)
        except Exception:
            logger.exception("Retry failed for %s", source_name)
            return []
        finally:
            await self._close_resources(page, context)

        return await self._filter_and_store(source_name, orders)

    # ------------------------------------------------------------------
    # Filtering & storage
    # ------------------------------------------------------------------

    async def _filter_and_store(
        self, source_name: str, orders: list[ScrapedOrder],
    ) -> list[ScrapedOrder]:
        """Apply keyword/category filters and store unique leads."""
        filtered: list[tuple[ScrapedOrder, list[str]]] = []
        for o in orders:
            if matches_exclude_keywords(o, self._exclude_keywords):
                continue
            if not filter_by_category(o, self._category_filters):
                continue
            matched_kw = filter_by_keywords(o, self._web_keywords)
            if self._web_keywords and not matched_kw:
                continue
            filtered.append((o, matched_kw))

        created = 0
        for order, kw in filtered:
            try:
                result = await self._dedup.check_and_store(_to_lead_candidate(order, kw))
                if not result.is_duplicate:
                    created += 1
            except Exception:
                logger.exception("Error storing lead from %s: %s", source_name, order.title[:80])

        logger.info(
            "Scrape %s: total=%d relevant=%d created=%d",
            source_name, len(orders), len(filtered), created,
        )
        return [o for o, _ in filtered]

    # ------------------------------------------------------------------
    # Periodic loop
    # ------------------------------------------------------------------

    async def _periodic_scrape(self, source_key: str, scrape_fn, interval: int) -> None:  # noqa: ANN001
        """Run *scrape_fn* in a loop with *interval* seconds between cycles."""
        while self._running:
            try:
                await scrape_fn()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unhandled error in periodic scrape for %s", source_key)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _close_resources(page, context) -> None:  # noqa: ANN001
        """Safely close a Playwright page and browser context."""
        for resource in (page, context):
            if resource is not None:
                try:
                    await resource.close()
                except Exception:
                    logger.debug("Error closing Playwright resource", exc_info=True)
