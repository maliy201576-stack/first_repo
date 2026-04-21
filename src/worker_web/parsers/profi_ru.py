"""Parser for Profi.ru freelance marketplace.

Profi.ru is a Russian platform connecting clients with specialists.
The IT freelance section (https://profi.ru/catalog/it_freelance/) contains
orders for web development, programming, design, and other IT services.

The parser scrapes multiple IT-related subcategory pages and extracts
order cards from the "Прямо сейчас ищут" (Currently looking for) and
"Задачи, которые доверили Профи.ру" (Tasks entrusted to Profi.ru) sections.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder

logger = logging.getLogger(__name__)

# IT-related subcategory pages on profi.ru
_PROFI_RU_PAGES: list[str] = [
    "https://profi.ru/it_freelance/programmer/",
    "https://profi.ru/it_freelance/sozdanie-saita/",
    "https://profi.ru/it_freelance/razrabotka-mobilnogo-prilozheniya/",
    "https://profi.ru/it_freelance/web-design/",
    "https://profi.ru/it_freelance/seo/",
    "https://profi.ru/it_freelance/1c/",
    "https://profi.ru/it_freelance/sistemnaya-integraciya/",
    "https://profi.ru/it_freelance/testirovschiki/",
    "https://profi.ru/it_freelance/kontekstnaya-reklama/",
    "https://profi.ru/it_freelance/smm/",
]

# Profi.ru is a JS-heavy Next.js app, needs extra wait time
_JS_RENDER_WAIT_MS = 5000


class ProfiRuParser:
    """Scrapes IT freelance orders from Profi.ru using Playwright + BeautifulSoup.

    Args:
        pages: List of profi.ru subcategory URLs to scrape.
            Defaults to a curated set of IT-related pages.
    """

    def __init__(self, pages: list[str] | None = None) -> None:
        self._pages = pages or _PROFI_RU_PAGES

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to Profi.ru IT freelance pages and extract orders.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        all_orders: list[ScrapedOrder] = []
        seen_titles: set[str] = set()

        for url in self._pages:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                # Scroll down to trigger lazy-loading of order sections
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await page.wait_for_timeout(500)
                await page.wait_for_timeout(_JS_RENDER_WAIT_MS)
                html = await page.content()
            except Exception:
                logger.exception("Failed to load Profi.ru page: %s", url)
                continue

            soup = BeautifulSoup(html, "html.parser")
            page_orders = self._extract_orders(soup)

            for order in page_orders:
                dedup_key = order.title.strip().lower()
                if dedup_key not in seen_titles:
                    seen_titles.add(dedup_key)
                    all_orders.append(order)

            logger.info(
                "Profi.ru: %s — parsed %d orders", url, len(page_orders)
            )

        logger.info("Profi.ru: total parsed %d unique orders", len(all_orders))
        return all_orders

    def _extract_orders(self, soup: BeautifulSoup) -> list[ScrapedOrder]:
        """Extract order cards from the page HTML.

        Profi.ru renders order cards in sections like "Прямо сейчас ищут"
        and "Задачи, которые доверили Профи.ру". Each card typically contains
        a title, task details, location info, date, and optionally a price.

        Args:
            soup: Parsed HTML of a profi.ru subcategory page.

        Returns:
            List of scraped orders found on the page.
        """
        orders: list[ScrapedOrder] = []

        # Strategy 1: Find order cards by looking for elements that contain
        # task titles followed by "Детали задачи" (Task details) sections.
        # These are the primary order listing blocks on profi.ru.
        detail_markers = soup.find_all(string=re.compile(r"Детали задачи"))
        for marker in detail_markers:
            try:
                order = self._parse_order_card(marker)
                if order is not None:
                    orders.append(order)
            except Exception:
                logger.exception("Failed to parse Profi.ru order card")

        # Strategy 2: If no detail markers found, try finding cards by
        # looking for price patterns (e.g., "8000 ₽") near task descriptions.
        if not orders:
            orders = self._fallback_extract(soup)

        return orders

    def _parse_order_card(self, detail_marker: Tag) -> ScrapedOrder | None:
        """Parse a single order card starting from its 'Детали задачи' marker.

        Walks up the DOM to find the containing card element, then extracts
        title, description, budget, and date.

        Args:
            detail_marker: A NavigableString or Tag containing "Детали задачи".

        Returns:
            A ScrapedOrder if extraction succeeds, None otherwise.
        """
        # Walk up to find the card container (usually a few levels up)
        container = detail_marker
        for _ in range(10):
            parent = getattr(container, "parent", None)
            if parent is None:
                break
            container = parent
            # Stop at a reasonable container level (div with siblings)
            if container.name == "div" and len(list(container.children)) > 2:
                break

        # Extract title — typically a bold/heading text before "Детали задачи"
        title = self._extract_title(container)
        if not title:
            return None

        # Extract description from the details section
        description = self._extract_description(container)

        # Extract budget
        budget = self._extract_budget(container)

        # Extract date
        published_at = self._extract_date(container)

        # Build URL — profi.ru order cards don't have direct links to orders,
        # so we use the page URL as the source reference
        url = "https://profi.ru/catalog/it_freelance/"

        return ScrapedOrder(
            source="profi.ru",
            title=title,
            description=description,
            url=url,
            budget=budget,
            category="IT",
            published_at=published_at,
        )

    def _fallback_extract(self, soup: BeautifulSoup) -> list[ScrapedOrder]:
        """Fallback extraction when primary strategy finds no orders.

        Looks for text blocks containing price patterns and task descriptions.

        Args:
            soup: Parsed HTML of a profi.ru page.

        Returns:
            List of scraped orders found via fallback method.
        """
        orders: list[ScrapedOrder] = []

        # Look for elements containing "Стоимость" (Cost) followed by a price
        cost_markers = soup.find_all(string=re.compile(r"Стоимость"))
        for marker in cost_markers:
            try:
                container = marker
                for _ in range(8):
                    parent = getattr(container, "parent", None)
                    if parent is None:
                        break
                    container = parent
                    if container.name == "div" and len(list(container.children)) > 2:
                        break

                title = self._extract_title(container)
                if not title:
                    continue

                description = self._extract_description(container)
                budget = self._extract_budget(container)
                published_at = self._extract_date(container)

                orders.append(
                    ScrapedOrder(
                        source="profi.ru",
                        title=title,
                        description=description,
                        url="https://profi.ru/catalog/it_freelance/",
                        budget=budget,
                        category="IT",
                        published_at=published_at,
                    )
                )
            except Exception:
                logger.exception("Failed to parse Profi.ru fallback order card")

        return orders

    @staticmethod
    def _extract_title(container: Tag) -> str | None:
        """Extract order title from a card container.

        Profi.ru order titles are typically rendered as bold text or headings
        within the card. Common titles include service names like
        "Landing page", "Корпоративный сайт", "Создание интернет-магазина".

        Args:
            container: The DOM element containing the order card.

        Returns:
            The title string, or None if not found.
        """
        # Try heading tags first
        for tag_name in ("h3", "h4", "h2", "strong", "b"):
            tag = container.find(tag_name)
            if tag:
                text = tag.get_text(strip=True)
                if text and len(text) > 3 and "Детали" not in text:
                    return text

        # Try finding the first substantial text block that looks like a title
        for el in container.find_all(string=True, recursive=True):
            text = str(el).strip()
            if (
                text
                and 4 < len(text) < 200
                and "Детали" not in text
                and "Стоимость" not in text
                and "₽" not in text
                and "Дистанционно" not in text
            ):
                return text

        return None

    @staticmethod
    def _extract_description(container: Tag) -> str:
        """Extract order description from a card container.

        Collects all meaningful text from the details section.

        Args:
            container: The DOM element containing the order card.

        Returns:
            Description text, or empty string if not found.
        """
        texts: list[str] = []
        skip_words = {"Детали задачи", "Стоимость", "Дистанционно"}

        for el in container.find_all(string=True, recursive=True):
            text = str(el).strip()
            if text and len(text) > 5 and text not in skip_words:
                texts.append(text)

        # Join and deduplicate consecutive identical fragments
        if not texts:
            return ""

        result_parts: list[str] = []
        for t in texts:
            if not result_parts or result_parts[-1] != t:
                result_parts.append(t)

        return " ".join(result_parts[:10])  # Limit to avoid overly long descriptions

    @staticmethod
    def _extract_budget(container: Tag) -> Decimal | None:
        """Extract budget/price from a card container.

        Looks for patterns like "8000 ₽", "40 000 ₽", "от 5980 ₽".

        Args:
            container: The DOM element containing the order card.

        Returns:
            Budget as Decimal, or None if not found.
        """
        text = container.get_text()
        # Match patterns like "8000 ₽", "40 000 ₽", "56000 ₽"
        price_match = re.search(r"(\d[\d\s]*)\s*₽", text)
        if price_match:
            digits = price_match.group(1).replace(" ", "").replace("\xa0", "")
            try:
                return Decimal(digits)
            except InvalidOperation:
                pass
        return None

    @staticmethod
    def _extract_date(container: Tag) -> datetime:
        """Extract publication date from a card container.

        Profi.ru shows relative dates like "вчера", "12 апреля 2026",
        "Неделю назад", "Три недели назад".

        Args:
            container: The DOM element containing the order card.

        Returns:
            Parsed datetime, or current UTC time as fallback.
        """
        text = container.get_text()

        # Try to find a date pattern like "12 апреля 2026"
        months_ru = {
            "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
            "мая": 5, "июня": 6, "июля": 7, "августа": 8,
            "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
        }
        date_match = re.search(
            r"(\d{1,2})\s+("
            + "|".join(months_ru.keys())
            + r")\s+(\d{4})",
            text,
        )
        if date_match:
            day = int(date_match.group(1))
            month = months_ru[date_match.group(2)]
            year = int(date_match.group(3))
            try:
                return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                pass

        return datetime.now(timezone.utc)
