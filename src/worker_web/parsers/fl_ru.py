"""Parser for FL.ru freelance marketplace."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder, clean_description

logger = logging.getLogger(__name__)

_FL_RU_URL = "https://www.fl.ru/projects/"
_MAX_PAGES = 5


class FlRuParser:
    """Scrapes project listings from FL.ru using Playwright + BeautifulSoup."""

    def __init__(self, max_pages: int = _MAX_PAGES) -> None:
        self._max_pages = max_pages

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to FL.ru projects pages and extract orders.

        Iterates through multiple pages to collect more results.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        all_orders: list[ScrapedOrder] = []

        for page_num in range(1, self._max_pages + 1):
            url = _FL_RU_URL if page_num == 1 else f"{_FL_RU_URL}?page={page_num}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                html = await page.content()
            except Exception:
                logger.exception("Failed to load FL.ru page %d", page_num)
                break

            soup = BeautifulSoup(html, "html.parser")
            items = soup.select("div.b-post")

            if not items:
                logger.info("FL.ru: no items on page %d, stopping", page_num)
                break

            page_count = 0
            for item in items:
                try:
                    order = self._parse_item(item)
                    if order is not None:
                        all_orders.append(order)
                        page_count += 1
                except Exception:
                    logger.exception("Failed to parse FL.ru project item")

            logger.info("FL.ru: page %d — parsed %d orders", page_num, page_count)

        logger.info("FL.ru: total parsed %d orders across %d pages", len(all_orders), min(self._max_pages, 5))
        return all_orders

    def _parse_item(self, item: Tag) -> ScrapedOrder | None:
        """Extract a single order from an HTML element."""
        # Current FL.ru layout: title link is inside h2.b-post__title > a
        title_tag = item.select_one("h2.b-post__title a")
        if title_tag is None:
            # Fallback to legacy selector
            title_tag = item.select_one("a.b-post__link")
        if title_tag is None:
            return None

        title = title_tag.get_text(strip=True)
        href = title_tag.get("href", "")
        url = f"https://www.fl.ru{href}" if href and not str(href).startswith("http") else str(href)

        desc_tag = item.select_one("div.b-post__txt")
        description = desc_tag.get_text(strip=True) if desc_tag else ""
        description = clean_description(description)

        budget = self._extract_budget(item)
        category = self._extract_category(item)
        published_at = self._extract_date(item)

        return ScrapedOrder(
            source="fl.ru",
            title=title,
            description=description,
            url=url,
            budget=budget,
            category=category,
            published_at=published_at,
        )

    @staticmethod
    def _extract_budget(item: Tag) -> Decimal | None:
        budget_tag = item.select_one("div.b-post__price")
        if budget_tag is None:
            return None
        text = budget_tag.get_text(strip=True).replace("\xa0", "").replace(" ", "")
        # Remove currency symbols and non-numeric suffixes
        digits = "".join(ch for ch in text if ch.isdigit() or ch == ".")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except InvalidOperation:
            return None

    @staticmethod
    def _extract_category(item: Tag) -> str | None:
        cat_tag = item.select_one("span.b-post__spec")
        if cat_tag is None:
            cat_tag = item.select_one("div.b-post__categs a")
        return cat_tag.get_text(strip=True) if cat_tag else None

    @staticmethod
    def _extract_date(item: Tag) -> datetime:
        date_tag = item.select_one("span.b-post__time")
        if date_tag and date_tag.get("title"):
            try:
                return datetime.fromisoformat(str(date_tag["title"]))
            except (ValueError, TypeError):
                pass
        return datetime.now(timezone.utc)
