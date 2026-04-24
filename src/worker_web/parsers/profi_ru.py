"""Parser for Profi.ru freelance marketplace.

Profi.ru is a Russian platform connecting clients with specialists.
The IT freelance section contains orders for web development, programming,
design, and other IT services.

The parser uses Playwright's inner_text() to get the rendered page text
and extracts orders via regex patterns, since profi.ru is a Next.js SPA
where BeautifulSoup cannot reliably find elements by CSS selectors.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from src.worker_web.parsers.base import ScrapedOrder, clean_description

logger = logging.getLogger(__name__)

# IT-related subcategory pages on profi.ru — pages with most orders
_PROFI_RU_PAGES: list[str] = [
    "https://profi.ru/it_freelance/razrabotka-ii/",
    "https://profi.ru/it_freelance/podklyuchenie-platezhnyh-sistem/",
    "https://profi.ru/it_freelance/nastroika-api/",
    "https://profi.ru/it_freelance/programmer/",
    "https://profi.ru/it_freelance/sozdanie-saita/",
    "https://profi.ru/it_freelance/razrabotka-mobilnogo-prilozheniya/",
    "https://profi.ru/it_freelance/web-design/",
    "https://profi.ru/it_freelance/seo/",
    "https://profi.ru/it_freelance/sistemnaya-integraciya/",
    "https://profi.ru/it_freelance/testirovschiki/",
    "https://profi.ru/it_freelance/kontekstnaya-reklama/",
    "https://profi.ru/it_freelance/smm/",
]

_JS_RENDER_WAIT_MS = 5000
_GOTO_TIMEOUT_MS = 60_000
_MAX_PAGE_RETRIES = 2

# Months for date parsing
_MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

_RE_DATE = re.compile(
    r"(\d{1,2})\s+(" + "|".join(_MONTHS_RU.keys()) + r")\s+(\d{4})"
)
_RE_PRICE = re.compile(r"Стоимость\s*\n\s*([\d\s\xa0]+)\s*₽")
_RE_HOURS_AGO = re.compile(r"(\d+)\s+час", re.IGNORECASE)


class ProfiRuParser:
    """Scrapes IT freelance orders from Profi.ru using Playwright text extraction.

    Instead of parsing HTML with BeautifulSoup, this parser uses
    Playwright's inner_text() to get the fully rendered page text,
    then extracts orders via regex. This is more reliable for Next.js SPAs.

    Args:
        pages: List of profi.ru subcategory URLs to scrape.
    """

    def __init__(self, pages: list[str] | None = None) -> None:
        self._pages = pages or _PROFI_RU_PAGES

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to Profi.ru pages and extract orders from rendered text.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders.
        """
        all_orders: list[ScrapedOrder] = []
        seen_titles: set[str] = set()

        for url in self._pages:
            text: str | None = None
            for attempt in range(1, _MAX_PAGE_RETRIES + 1):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=_GOTO_TIMEOUT_MS)
                    # Scroll to trigger lazy-loading of order sections
                    for _ in range(8):
                        await page.evaluate("window.scrollBy(0, 800)")
                        await page.wait_for_timeout(400)
                    await page.wait_for_timeout(_JS_RENDER_WAIT_MS)
                    # Get rendered text — much more reliable than HTML parsing
                    text = await page.inner_text("body")
                    break
                except Exception:
                    if attempt == _MAX_PAGE_RETRIES:
                        logger.exception(
                            "Failed to load Profi.ru page after %d attempts: %s",
                            _MAX_PAGE_RETRIES, url,
                        )
                    else:
                        logger.warning(
                            "Profi.ru page load attempt %d/%d timed out: %s",
                            attempt, _MAX_PAGE_RETRIES, url,
                        )
            if text is None:
                continue

            page_orders = self._extract_from_text(text, url)

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

    def _extract_from_text(self, text: str, page_url: str) -> list[ScrapedOrder]:
        """Extract orders from the rendered page text using regex.

        Args:
            text: Full rendered text of the page (from inner_text).
            page_url: The URL of the page being parsed.

        Returns:
            List of scraped orders.
        """
        orders: list[ScrapedOrder] = []

        # Find the sections with orders
        # Orders appear after "Прямо сейчас ищут" and "Задачи, которые доверили"
        for section_marker in [
            "Прямо сейчас ищут",
            "Задачи, которые доверили",
        ]:
            idx = text.find(section_marker)
            if idx == -1:
                continue

            section_text = text[idx:]
            # Cut off at next major section
            for end_marker in [
                "Заказы за последние",
                "Попробуйте новый",
                "Вы профи?",
                "Похожие страницы",
                "Нужен репетитор",
            ]:
                end_idx = section_text.find(end_marker)
                if end_idx > 0:
                    section_text = section_text[:end_idx]
                    break

            section_orders = self._parse_section(section_text, page_url)
            orders.extend(section_orders)

        return orders

    def _parse_section(self, text: str, page_url: str) -> list[ScrapedOrder]:
        """Parse individual orders from a section of text.

        Args:
            text: Text of a section containing order listings.
            page_url: Source page URL.

        Returns:
            List of scraped orders.
        """
        orders: list[ScrapedOrder] = []

        # Split by "Детали задачи" marker — each occurrence = one order
        parts = text.split("Детали задачи")

        for i in range(1, len(parts)):
            # The title/date is at the end of the previous part
            header = parts[i - 1]
            # The description/price is at the start of the current part
            body = parts[i]

            try:
                order = self._parse_single(header, body)
                if order is not None:
                    orders.append(order)
            except Exception:
                logger.debug("Failed to parse Profi.ru order block")

        return orders

    def _parse_single(
        self, header: str, body: str,
    ) -> ScrapedOrder | None:
        """Parse a single order from header (before 'Детали задачи') and body (after).

        Args:
            header: Text before the 'Детали задачи' marker.
            body: Text after the 'Детали задачи' marker.

        Returns:
            A ScrapedOrder, or None if parsing fails.
        """
        # Extract title — last meaningful line before "Детали задачи"
        header_lines = [
            ln.strip() for ln in header.strip().split("\n")
            if ln.strip() and len(ln.strip()) > 2
        ]
        if not header_lines:
            return None

        title = None
        date_str = None
        # Walk backwards to find title (skip date/location lines)
        for ln in reversed(header_lines):
            if not title:
                # Skip lines that are clearly not titles
                if ln.startswith("·") or "Дистанционно" in ln:
                    continue
                if _RE_DATE.search(ln) or _RE_HOURS_AGO.search(ln):
                    date_str = ln
                    continue
                if len(ln) > 3 and "Оценка клиента" not in ln:
                    title = ln
            elif not date_str:
                if "·" in ln or _RE_DATE.search(ln) or _RE_HOURS_AGO.search(ln):
                    date_str = ln

        if not title or len(title) < 3:
            return None

        # Extract description from body — take lines until price or next order
        body_lines = [ln.strip() for ln in body.strip().split("\n") if ln.strip()]
        desc_lines: list[str] = []
        for ln in body_lines:
            if ln == "Стоимость" or "Оценка клиента" in ln:
                break
            if len(ln) > 3:
                desc_lines.append(ln)

        description = clean_description(" ".join(desc_lines[:15]))

        # Extract budget
        budget = self._extract_budget(body)

        # Extract date
        published_at = self._parse_date(date_str or header)

        return ScrapedOrder(
            source="profi.ru",
            title=title,
            description=description,
            url=None,
            budget=budget,
            category="IT",
            published_at=published_at,
        )

    @staticmethod
    def _extract_budget(text: str) -> Decimal | None:
        """Extract budget from order body text.

        Args:
            text: The body text after 'Детали задачи'.

        Returns:
            Budget as Decimal, or None.
        """
        match = _RE_PRICE.search(text)
        if match:
            digits = match.group(1).replace(" ", "").replace("\xa0", "")
            try:
                return Decimal(digits)
            except InvalidOperation:
                pass
        return None

    @staticmethod
    def _parse_date(text: str) -> datetime:
        """Parse a Russian date string into a datetime.

        Args:
            text: Text potentially containing a date.

        Returns:
            Parsed datetime, or current UTC time as fallback.
        """
        match = _RE_DATE.search(text)
        if match:
            day = int(match.group(1))
            month = _MONTHS_RU.get(match.group(2), 0)
            year = int(match.group(3))
            if month:
                try:
                    return datetime(year, month, day, tzinfo=timezone.utc)
                except ValueError:
                    pass
        return datetime.now(timezone.utc)
