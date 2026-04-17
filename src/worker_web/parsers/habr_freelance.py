"""Parser for Habr Freelance marketplace."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder

logger = logging.getLogger(__name__)

_HABR_FREELANCE_URL = "https://freelance.habr.com/tasks"


class HabrFreelanceParser:
    """Scrapes task listings from Habr Freelance using Playwright + BeautifulSoup."""

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to Habr Freelance tasks page and extract orders.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        orders: list[ScrapedOrder] = []
        try:
            await page.goto(
                _HABR_FREELANCE_URL,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            html = await page.content()
        except Exception:
            logger.exception("Failed to load Habr Freelance tasks page")
            return orders

        soup = BeautifulSoup(html, "html.parser")

        for item in soup.select("li.content-list__item"):
            try:
                order = self._parse_item(item)
                if order is not None:
                    orders.append(order)
            except Exception:
                logger.exception("Failed to parse Habr Freelance task item")

        logger.info("Habr Freelance: parsed %d orders", len(orders))
        return orders

    def _parse_item(self, item: Tag) -> ScrapedOrder | None:
        """Extract a single order from an HTML element."""
        title_tag = item.select_one("div.task__title a")
        if title_tag is None:
            return None

        title = title_tag.get_text(strip=True)
        href = title_tag.get("href", "")
        url = (
            f"https://freelance.habr.com{href}"
            if href and not str(href).startswith("http")
            else str(href)
        )

        desc_tag = item.select_one("div.task__description")
        description = desc_tag.get_text(strip=True) if desc_tag else ""

        budget = self._extract_budget(item)
        category = self._extract_category(item)
        published_at = self._extract_date(item)

        return ScrapedOrder(
            source="habr_freelance",
            title=title,
            description=description,
            url=url,
            budget=budget,
            category=category,
            published_at=published_at,
        )

    @staticmethod
    def _extract_budget(item: Tag) -> Decimal | None:
        budget_tag = item.select_one("span.count")
        if budget_tag is None:
            return None
        text = budget_tag.get_text(strip=True).replace("\xa0", "").replace(" ", "")
        digits = "".join(ch for ch in text if ch.isdigit() or ch == ".")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except InvalidOperation:
            return None

    @staticmethod
    def _extract_category(item: Tag) -> str | None:
        cat_tag = item.select_one("span.tags__item_link")
        if cat_tag is None:
            cat_tag = item.select_one("a.tags__item_link")
        return cat_tag.get_text(strip=True) if cat_tag else None

    @staticmethod
    def _extract_date(item: Tag) -> datetime:
        date_tag = item.select_one("span.params__published-at")
        if date_tag:
            time_tag = date_tag.select_one("time")
            if time_tag and time_tag.get("datetime"):
                try:
                    return datetime.fromisoformat(str(time_tag["datetime"]))
                except (ValueError, TypeError):
                    pass
        return datetime.now(timezone.utc)
