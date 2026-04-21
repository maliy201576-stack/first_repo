"""Worker_Web — periodic web scraper for freelance marketplaces and government procurement.

Scrapes FL.ru, Kwork, Weblancer, Profi.ru, and zakupki.gov.ru on configurable
intervals, filters by category, handles HTTP errors with proxy rotation, and sends
unique leads to DedupService.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from src.common.models import LeadCandidate
from src.dedup.service import DedupService
from src.notifier.service import Notifier
from src.worker_web.parsers.base import ScrapedOrder
from src.worker_web.parsers.fl_ru import FlRuParser
from src.worker_web.parsers.kwork import KworkParser
from src.worker_web.parsers.profi_ru import ProfiRuParser
from src.worker_web.parsers.weblancer import WeblancerParser
from src.worker_web.parsers.zakupki_gov import ZakupkiGovParser
from src.worker_web.proxy_pool import ProxyPool, NoAvailableProxiesError

logger = logging.getLogger(__name__)

# Default scrape intervals in seconds
_DEFAULT_INTERVALS = {
    "fl_ru": 900,
    "habr": 900,
    "zakupki": 3600,
}

# Error handling constants
_HTTP_429_PAUSE_SECONDS = 300
_PLAYWRIGHT_TIMEOUT_MS = 30_000


class _Http429Error(Exception):
    """Raised when a source returns HTTP 429."""


class _Http403Error(Exception):
    """Raised when a source returns HTTP 403."""


def _to_lead_candidate(order: ScrapedOrder) -> LeadCandidate:
    """Convert a ScrapedOrder to a LeadCandidate for dedup processing."""
    tags: list[str] = []
    if order.is_urgent:
        tags.append("urgent")
    return LeadCandidate(
        source=order.source,
        title=order.title,
        description=order.description,
        url=order.url,
        budget=order.budget,
        category=order.category,
        tags=tags,
        okpd2_codes=order.okpd2_codes,
        max_contract_price=order.max_contract_price,
        submission_deadline=order.submission_deadline,
        discovered_at=order.published_at,
    )


def filter_by_category(order: ScrapedOrder, allowed_categories: set[str]) -> bool:
    """Return True if order passes the category filter.

    If allowed_categories is empty, all orders pass (no filter applied).
    Otherwise the order's category must be in the allowed set.
    """
    if not allowed_categories:
        return True
    return order.category is not None and order.category in allowed_categories


def filter_by_keywords(order: ScrapedOrder, keywords: list[str]) -> list[str]:
    """Return matched keywords from order title + description.

    Uses word-boundary matching for short keywords (<=4 chars) to avoid
    false positives like 'бот' matching 'работа'.
    """
    if not keywords:
        return []
    import re
    text = f"{order.title} {order.description}".lower()
    matched = []
    for kw in keywords:
        kw_lower = kw.lower()
        if len(kw_lower) <= 4:
            # Short keywords need word boundary check
            pattern = r'(?<!\w)' + re.escape(kw_lower) + r'(?!\w)'
            if re.search(pattern, text):
                matched.append(kw)
        else:
            if kw_lower in text:
                matched.append(kw)
    return matched


def matches_exclude_keywords(order: ScrapedOrder, exclude: list[str]) -> bool:
    """Return True if order matches any exclusion keyword.

    Args:
        order: The scraped order to check.
        exclude: List of keywords to exclude (case-insensitive).

    Returns:
        True if the order should be excluded.
    """
    if not exclude:
        return False
    text = f"{order.title} {order.description}".lower()
    for kw in exclude:
        if kw.lower() in text:
            return True
    return False


class WorkerWeb:
    """Periodic web scraper for FL.ru, Habr Freelance, and zakupki.gov.ru.

    Args:
        dedup_service: Service for deduplication and lead storage.
        proxy_pool: Pool of rotating proxies.
        browser_factory: Async callable that returns a Playwright Browser instance.
        category_filters: Set of allowed categories. Empty set means no filtering.
        intervals: Dict with scrape intervals per source (fl_ru, habr, zakupki).
    """

    def __init__(
        self,
        dedup_service: DedupService,
        proxy_pool: ProxyPool,
        browser_factory,  # noqa: ANN001 — async callable returning Browser
        category_filters: set[str] | None = None,
        intervals: dict[str, int] | None = None,
        notifier: Notifier | None = None,
        web_keywords: list[str] | None = None,
        exclude_keywords: list[str] | None = None,
    ) -> None:
        self._dedup = dedup_service
        self._proxy_pool = proxy_pool
        self._browser_factory = browser_factory
        self._category_filters: set[str] = category_filters or set()
        self._intervals = {**_DEFAULT_INTERVALS, **(intervals or {})}
        self._notifier = notifier
        self._web_keywords: list[str] = web_keywords or []
        self._exclude_keywords: list[str] = exclude_keywords or ["wordpress", "вордпресс", "wp "]
        self._browser = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

        # Parsers
        self._fl_parser = FlRuParser()
        self._kwork_parser = KworkParser()
        self._weblancer_parser = WeblancerParser()
        self._zakupki_parser = ZakupkiGovParser()
        self._profi_ru_parser = ProfiRuParser()

    async def start(self) -> None:
        """Launch periodic scraping tasks for all sources."""
        self._browser = await self._browser_factory()
        self._running = True

        self._tasks = [
            asyncio.create_task(
                self._periodic_scrape("fl_ru", self.scrape_fl_ru, self._intervals["fl_ru"])
            ),
            asyncio.create_task(
                self._periodic_scrape("kwork", self.scrape_kwork, self._intervals["fl_ru"])
            ),
            asyncio.create_task(
                self._periodic_scrape("weblancer", self.scrape_weblancer, self._intervals["fl_ru"])
            ),
            asyncio.create_task(
                self._periodic_scrape("profi_ru", self.scrape_profi_ru, self._intervals["fl_ru"])
            ),
            asyncio.create_task(
                self._periodic_scrape("zakupki", self.scrape_zakupki, self._intervals["zakupki"])
            ),
        ]
        logger.info(
            "WorkerWeb started: fl_ru=%ds, kwork=%ds, weblancer=%ds, profi_ru=%ds, zakupki=%ds",
            self._intervals["fl_ru"],
            self._intervals["fl_ru"],
            self._intervals["fl_ru"],
            self._intervals["fl_ru"],
            self._intervals["zakupki"],
        )

    async def stop(self) -> None:
        """Cancel all scraping tasks and close the browser."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception:
                logger.exception("Error closing Playwright browser")
            self._browser = None
        logger.info("WorkerWeb stopped")

    async def scrape_fl_ru(self) -> list[ScrapedOrder]:
        """Scrape FL.ru, filter by category, and send leads to DedupService."""
        return await self._scrape_source("fl.ru", self._fl_parser)

    async def scrape_kwork(self) -> list[ScrapedOrder]:
        """Scrape Kwork.ru, filter by keywords, and send leads to DedupService."""
        return await self._scrape_source("kwork", self._kwork_parser)

    async def scrape_weblancer(self) -> list[ScrapedOrder]:
        """Scrape Weblancer.net, filter by keywords, and send leads to DedupService."""
        return await self._scrape_source("weblancer", self._weblancer_parser)

    async def scrape_profi_ru(self) -> list[ScrapedOrder]:
        """Scrape Profi.ru IT freelance orders, filter, and send leads to DedupService."""
        return await self._scrape_source("profi.ru", self._profi_ru_parser)

    async def scrape_zakupki(self) -> list[ScrapedOrder]:
        """Scrape zakupki.gov.ru, filter by category, and send leads to DedupService."""
        return await self._scrape_source("zakupki_gov", self._zakupki_parser)

    def is_urgent_deadline(self, deadline: datetime) -> bool:
        """Check if a procurement deadline is urgent (< 3 business days)."""
        from src.worker_web.parsers.base import is_urgent_deadline
        return is_urgent_deadline(deadline)

    async def _scrape_source(self, source_name: str, parser) -> list[ScrapedOrder]:  # noqa: ANN001
        """Run a single scrape cycle for a given source.

        Handles proxy rotation, HTTP error codes, and Playwright timeouts.
        Returns the list of scraped orders (post-filter).
        """
        try:
            proxy = await self._proxy_pool.get_next()
        except NoAvailableProxiesError:
            proxy = None

        context = None
        page = None
        try:
            ctx_kwargs: dict = {}
            if proxy:
                ctx_kwargs["proxy"] = {"server": proxy}
            context = await self._browser.new_context(**ctx_kwargs)
            page = await context.new_page()
            page.set_default_timeout(_PLAYWRIGHT_TIMEOUT_MS)

            orders = await parser.parse(page)

            # Check for HTTP error status in the response
            # (parsers handle navigation internally; we detect errors via page status)

        except _Http429Error:
            logger.warning(
                "HTTP 429 from %s — pausing %ds, switching proxy",
                source_name,
                _HTTP_429_PAUSE_SECONDS,
            )
            await asyncio.sleep(_HTTP_429_PAUSE_SECONDS)
            if proxy:
                await self._proxy_pool.mark_blocked(proxy)
            return []
        except _Http403Error:
            logger.warning("HTTP 403 from %s — marking proxy %s as blocked", source_name, proxy)
            if proxy:
                await self._proxy_pool.mark_blocked(proxy)
            # Retry once with a different proxy
            return await self._retry_with_new_proxy(source_name, parser)
        except TimeoutError:
            logger.warning(
                "Playwright timeout for %s — retrying with different proxy", source_name
            )
            if proxy:
                await self._proxy_pool.mark_blocked(proxy)
            return await self._retry_with_new_proxy(source_name, parser)
        except Exception as exc:
            logger.exception("Error scraping %s", source_name)
            if self._notifier is not None:
                try:
                    await self._notifier.send_error("worker_web", exc)
                except Exception:
                    logger.exception("Failed to send notifier alert")
            return []
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass

        # Filter by keywords and process leads
        return await self._filter_and_store(source_name, orders)

    async def _filter_and_store(
        self, source_name: str, orders: list[ScrapedOrder]
    ) -> list[ScrapedOrder]:
        """Apply keyword/category filters and store unique leads via DedupService."""
        filtered = []
        for o in orders:
            if matches_exclude_keywords(o, self._exclude_keywords):
                continue
            if filter_by_category(o, self._category_filters):
                matched_kw = filter_by_keywords(o, self._web_keywords)
                if self._web_keywords and not matched_kw:
                    continue
                o._matched_keywords = matched_kw  # type: ignore[attr-defined]
                filtered.append(o)

        created_count = 0
        for order in filtered:
            candidate = _to_lead_candidate(order)
            candidate.matched_keywords = getattr(order, "_matched_keywords", [])
            try:
                result = await self._dedup.check_and_store(candidate)
                if not result.is_duplicate:
                    created_count += 1
            except Exception:
                logger.exception("Error storing lead from %s: %s", source_name, order.title[:80])

        logger.info(
            "Scrape cycle %s: total=%d, relevant=%d, created=%d",
            source_name,
            len(orders),
            len(filtered),
            created_count,
        )
        return filtered

    async def _retry_with_new_proxy(self, source_name: str, parser) -> list[ScrapedOrder]:  # noqa: ANN001
        """Retry a scrape with a different proxy (single retry)."""
        try:
            proxy = await self._proxy_pool.get_next()
        except NoAvailableProxiesError:
            proxy = None

        context = None
        page = None
        try:
            ctx_kwargs: dict = {}
            if proxy:
                ctx_kwargs["proxy"] = {"server": proxy}
            context = await self._browser.new_context(**ctx_kwargs)
            page = await context.new_page()
            page.set_default_timeout(_PLAYWRIGHT_TIMEOUT_MS)
            orders = await parser.parse(page)
        except Exception:
            logger.exception("Retry failed for %s", source_name)
            return []
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass

        filtered = await self._filter_and_store(source_name, orders)

        logger.info(
            "Retry scrape %s: total=%d, relevant=%d",
            source_name,
            len(orders),
            len(filtered),
        )
        return filtered

    async def _periodic_scrape(
        self,
        source_key: str,
        scrape_fn,  # noqa: ANN001
        interval: int,
    ) -> None:
        """Run a scrape function in a periodic loop."""
        while self._running:
            try:
                await scrape_fn()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Unhandled error in periodic scrape for %s", source_key)
                if self._notifier is not None:
                    try:
                        await self._notifier.send_error("worker_web", exc)
                    except Exception:
                        logger.exception("Failed to send notifier alert")
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
