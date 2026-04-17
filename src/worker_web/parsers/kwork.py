"""Parser for Kwork.ru freelance marketplace."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder

logger = logging.getLogger(__name__)

_KWORK_PROJECTS_URL = "https://kwork.ru/projects"
_MAX_PAGES = 7


class KworkParser:
    """Scrapes project requests (wants) from Kwork.ru."""

    def __init__(self, max_pages: int = _MAX_PAGES) -> None:
        self._max_pages = max_pages

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to Kwork projects and extract orders.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        all_orders: list[ScrapedOrder] = []

        for page_num in range(1, self._max_pages + 1):
            url = _KWORK_PROJECTS_URL if page_num == 1 else f"{_KWORK_PROJECTS_URL}?page={page_num}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                # Kwork uses JS rendering, wait a bit for content
                await page.wait_for_timeout(2000)
                html = await page.content()
            except Exception:
                logger.exception("Failed to load Kwork page %d", page_num)
                break

            soup = BeautifulSoup(html, "html.parser")

            # Kwork project cards
            items = soup.select("div.want-card")
            if not items:
                # Fallback selectors
                items = soup.select("div.card__content")
            if not items:
                items = soup.select("div[class*='project']")

            if not items:
                logger.info("Kwork: no items on page %d, stopping", page_num)
                break

            page_count = 0
            for item in items:
                try:
                    order = self._parse_item(item)
                    if order is not None:
                        all_orders.append(order)
                        page_count += 1
                except Exception:
                    logger.exception("Failed to parse Kwork project item")

            logger.info("Kwork: page %d — parsed %d orders", page_num, page_count)

        logger.info("Kwork: total parsed %d orders", len(all_orders))
        return all_orders

    def _parse_item(self, item: Tag) -> ScrapedOrder | None:
        """Extract a single order from a Kwork project card."""
        # Title
        title_tag = item.select_one("a.wants-card__header-title")
        if title_tag is None:
            title_tag = item.select_one("a[class*='title']")
        if title_tag is None:
            title_tag = item.select_one("h3 a") or item.select_one("a")
        if title_tag is None:
            return None

        title = title_tag.get_text(strip=True)
        if not title:
            return None

        href = title_tag.get("href", "")
        url = f"https://kwork.ru{href}" if href and not str(href).startswith("http") else str(href)

        # Description
        desc_tag = item.select_one("div.wants-card__description-text")
        if desc_tag is None:
            desc_tag = item.select_one("div[class*='description']")
        description = desc_tag.get_text(strip=True) if desc_tag else ""

        # Budget
        budget = self._extract_budget(item)

        return ScrapedOrder(
            source="kwork.ru",
            title=title,
            description=description,
            url=url,
            budget=budget,
            published_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _extract_budget(item: Tag) -> Decimal | None:
        """Extract budget from a Kwork project card."""
        price_tag = item.select_one("div.wants-card__header-price")
        if price_tag is None:
            price_tag = item.select_one("span[class*='price']")
        if price_tag is None:
            return None
        text = price_tag.get_text(strip=True).replace("\xa0", "").replace(" ", "")
        digits = "".join(ch for ch in text if ch.isdigit() or ch == ".")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except InvalidOperation:
            return None
