"""Parser for Kwork.ru freelance marketplace."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder, clean_description

from src.common.budget import parse_price_text

logger = logging.getLogger(__name__)

_KWORK_PROJECTS_URL = "https://kwork.ru/projects"
_MAX_PAGES = 7

# Regex patterns for budget extraction from card text.
# "Желаемый бюджет: до 25 000 ₽", "Цена 500 ₽", "Цена до: 1 500 ₽"
_RE_BUDGET = re.compile(
    r"(?:"
    r"(?:Желаемый\s+бюджет|бюджет)\s*[:]\s*(?:до\s+)?"
    r"|Цена\s+(?:до\s*:?\s*)?"
    r")"
    r"([\d\s\xa0]+)"
    r"\s*₽",
    re.IGNORECASE,
)
# "Допустимый: до 150 000 ₽"
_RE_BUDGET_MAX = re.compile(
    r"Допустимый\s*[:]\s*(?:до\s+)?([\d\s\xa0]+)\s*₽",
    re.IGNORECASE,
)
# Fallback: any number followed by ₽
_RE_PRICE_SIMPLE = re.compile(r"([\d\s\xa0]+)\s*₽")


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
                # Kwork uses Vue.js rendering, wait for content
                await page.wait_for_timeout(3000)
                html = await page.content()
            except Exception:
                logger.exception("Failed to load Kwork page %d", page_num)
                break

            soup = BeautifulSoup(html, "html.parser")

            # Kwork project cards — try multiple strategies
            items = soup.select("div.want-card")
            if not items:
                items = soup.select("div.card__content")

            if items:
                page_count = 0
                for item in items:
                    try:
                        order = self._parse_item(item)
                        if order is not None:
                            all_orders.append(order)
                            page_count += 1
                    except Exception:
                        logger.exception("Failed to parse Kwork project item")
            else:
                # Fallback: find project links and extract from their containers
                page_count = 0
                project_links = soup.find_all(
                    "a", href=re.compile(r"/projects/\d+")
                )
                seen_hrefs: set[str] = set()
                for link in project_links:
                    href = str(link.get("href", ""))
                    if href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)
                    try:
                        order = self._parse_from_link(link)
                        if order is not None:
                            all_orders.append(order)
                            page_count += 1
                    except Exception:
                        logger.exception("Failed to parse Kwork project from link")

            if page_count == 0:
                logger.info("Kwork: no items on page %d, stopping", page_num)
                break

            logger.info("Kwork: page %d — parsed %d orders", page_num, page_count)

        logger.info("Kwork: total parsed %d orders", len(all_orders))
        return all_orders

    def _parse_from_link(self, link: Tag) -> ScrapedOrder | None:
        """Extract an order starting from a project link element.

        Walks up the DOM to find the containing card, then extracts
        title, description, and budget from the card text.

        Args:
            link: An <a> tag linking to /projects/NNN.

        Returns:
            A ScrapedOrder if extraction succeeds, None otherwise.
        """
        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        href = str(link.get("href", ""))
        url = f"https://kwork.ru{href}" if not href.startswith("http") else href

        # Walk up to find the card container
        container = link
        for _ in range(8):
            parent = container.parent
            if parent is None:
                break
            container = parent
            # Stop at a reasonable container level
            children = list(container.children)
            if len(children) > 3:
                break

        # Extract description from nearby text
        full_text = container.get_text(separator="\n")
        lines = [ln.strip() for ln in full_text.split("\n") if ln.strip()]
        desc_lines = [
            ln for ln in lines
            if ln != title and len(ln) > 10
            and "Покупатель" not in ln
            and "Размещено" not in ln
            and "Осталось" not in ln
            and "Предложений" not in ln
            and "Нанято" not in ln
        ]
        description = " ".join(desc_lines[:3]) if desc_lines else ""
        description = clean_description(description)

        budget, budget_max = self._extract_budget_pair(container)

        return ScrapedOrder(
            source="kwork.ru",
            title=title,
            description=description,
            url=url,
            budget=budget,
            budget_max=budget_max,
            published_at=datetime.now(timezone.utc),
        )

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
        description = clean_description(description)

        budget, budget_max = self._extract_budget_pair(item)

        return ScrapedOrder(
            source="kwork.ru",
            title=title,
            description=description,
            url=url,
            budget=budget,
            budget_max=budget_max,
            published_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _extract_budget_pair(item: Tag) -> tuple[Decimal | None, Decimal | None]:
        """Extract budget and budget_max from a Kwork project card.

        Returns a tuple of (desired_budget, max_budget).
        Kwork shows: "Желаемый бюджет: до 25 000 ₽ Допустимый: до 75 000 ₽"
        or just "Цена 500 ₽".

        Args:
            item: The card DOM element.

        Returns:
            Tuple of (budget, budget_max). Either or both may be None.
        """
        budget: Decimal | None = None
        budget_max: Decimal | None = None

        # Strategy 1: Try dedicated price element (legacy selectors)
        price_tag = item.select_one("div.wants-card__header-price")
        if price_tag is None:
            price_tag = item.select_one("span[class*='price']")
        if price_tag is not None:
            result = parse_price_text(price_tag.get_text(strip=True))
            if result is not None:
                return result, None

        # Strategy 2: Search full card text for budget/price patterns
        full_text = item.get_text()

        match = _RE_BUDGET.search(full_text)
        if match:
            budget = parse_price_text(match.group(1))

        match_max = _RE_BUDGET_MAX.search(full_text)
        if match_max:
            budget_max = parse_price_text(match_max.group(1))

        if budget is not None:
            return budget, budget_max

        # Strategy 3: Fallback — first occurrence of "N ₽"
        match = _RE_PRICE_SIMPLE.search(full_text)
        if match:
            return parse_price_text(match.group(1)), None

        return None, None
