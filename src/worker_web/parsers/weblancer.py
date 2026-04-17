"""Parser for Weblancer.net freelance marketplace."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder

logger = logging.getLogger(__name__)

_WEBLANCER_URL = "https://www.weblancer.net/freelance/"
_MAX_PAGES = 5


class WeblancerParser:
    """Scrapes project listings from Weblancer.net."""

    def __init__(self, max_pages: int = _MAX_PAGES) -> None:
        self._max_pages = max_pages

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to Weblancer jobs and extract orders.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        all_orders: list[ScrapedOrder] = []

        for page_num in range(1, self._max_pages + 1):
            url = _WEBLANCER_URL if page_num == 1 else f"{_WEBLANCER_URL}?page={page_num}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(2000)
                html = await page.content()
            except Exception:
                logger.exception("Failed to load Weblancer page %d", page_num)
                break

            soup = BeautifulSoup(html, "html.parser")

            # Current Weblancer layout: each job is an h2 with a link,
            # followed by price and description text in sibling elements.
            # We find all job title links and walk up to their container.
            title_links = soup.select("h2 a[href*='/freelance/']")
            if not title_links:
                # Fallback: legacy selectors
                title_links = soup.select("div.cols_table div.row a.title")
            if not title_links:
                title_links = soup.select("a.title")

            if not title_links:
                logger.info("Weblancer: no items on page %d, stopping", page_num)
                break

            page_count = 0
            for link in title_links:
                try:
                    order = self._parse_from_link(link)
                    if order is not None:
                        all_orders.append(order)
                        page_count += 1
                except Exception:
                    logger.exception("Failed to parse Weblancer item")

            logger.info("Weblancer: page %d — parsed %d orders", page_num, page_count)

        logger.info("Weblancer: total parsed %d orders across %d pages", len(all_orders), min(len(all_orders), self._max_pages))
        return all_orders

    def _parse_from_link(self, link: Tag) -> ScrapedOrder | None:
        """Extract a single order starting from its title link element."""
        title = link.get_text(strip=True)
        if not title:
            return None

        href = link.get("href", "")
        url = (
            f"https://www.weblancer.net{href}"
            if href and not str(href).startswith("http")
            else str(href)
        )

        # Walk up to the container that holds price and description
        container = link.find_parent("div") or link.parent
        if container is None:
            container = link

        # Try to find description — usually a <p> or text block near the title
        description = ""
        # Look for sibling or parent-sibling text blocks
        parent_section = link.find_parent("div", recursive=True)
        if parent_section:
            # Get all text content after the title, excluding nested links
            texts = []
            for sibling in parent_section.find_all(string=True, recursive=True):
                text = sibling.strip()
                if text and text != title and len(text) > 20:
                    texts.append(text)
            if texts:
                description = " ".join(texts[:3])

        budget = self._extract_budget_from_container(parent_section or container)

        return ScrapedOrder(
            source="weblancer.net",
            title=title,
            description=description,
            url=url,
            budget=budget,
            published_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _extract_budget_from_container(container: Tag) -> Decimal | None:
        """Extract budget from the container surrounding a job listing."""
        # Look for dollar/ruble amounts in the container text
        for tag in container.find_all(string=True):
            text = str(tag).strip()
            if "$" in text or "₽" in text or "руб" in text.lower() or "грн" in text.lower():
                digits = "".join(ch for ch in text if ch.isdigit() or ch == ".")
                if digits:
                    try:
                        return Decimal(digits)
                    except InvalidOperation:
                        continue
        return None
