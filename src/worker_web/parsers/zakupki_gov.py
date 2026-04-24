"""Parser for ЕИС (zakupki.gov.ru) government procurement portal."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal

from bs4 import BeautifulSoup, Tag

from src.worker_web.parsers.base import ScrapedOrder, is_urgent_deadline

from src.common.budget import parse_price_text

logger = logging.getLogger(__name__)

_GOTO_TIMEOUT_MS = 60_000
_MAX_PAGE_RETRIES = 2

# Target OKPD2 codes for IT services
TARGET_OKPD2_CODES = [
    "62.01.11.000",
    "62.01.12.000",
    "62.02.20.120",
]

_ZAKUPKI_SEARCH_URL = (
    "https://zakupki.gov.ru/epz/order/extendedsearch/results.html"
)


class ZakupkiGovParser:
    """Scrapes procurement listings from zakupki.gov.ru filtered by OKPD2 codes."""

    def __init__(self, okpd2_codes: list[str] | None = None) -> None:
        self.okpd2_codes = okpd2_codes or list(TARGET_OKPD2_CODES)

    async def parse(self, page) -> list[ScrapedOrder]:  # noqa: ANN001
        """Navigate to ЕИС search results and extract procurement orders.

        Args:
            page: A Playwright page object.

        Returns:
            List of scraped orders matching target OKPD2 codes.
        """
        orders: list[ScrapedOrder] = []
        search_url = self._build_search_url()
        html: str | None = None
        for attempt in range(1, _MAX_PAGE_RETRIES + 1):
            try:
                await page.goto(search_url, wait_until="load", timeout=_GOTO_TIMEOUT_MS)
                html = await page.content()
                break
            except Exception:
                if attempt == _MAX_PAGE_RETRIES:
                    logger.exception(
                        "Failed to load zakupki.gov.ru search results after %d attempts",
                        _MAX_PAGE_RETRIES,
                    )
                else:
                    logger.warning(
                        "zakupki.gov.ru load attempt %d/%d timed out",
                        attempt, _MAX_PAGE_RETRIES,
                    )
        if html is None:
            return orders

        soup = BeautifulSoup(html, "html.parser")

        for item in soup.select("div.search-registry-entry-block"):
            try:
                order = self._parse_item(item)
                if order is not None:
                    orders.append(order)
            except Exception:
                logger.exception("Failed to parse zakupki.gov.ru entry")

        logger.info("zakupki.gov.ru: parsed %d orders", len(orders))
        return orders

    def _build_search_url(self) -> str:
        """Build the search URL with OKPD2 code filters."""
        codes_param = "%2C+".join(self.okpd2_codes)
        return f"{_ZAKUPKI_SEARCH_URL}?searchString=&morphology=on&okpd2Ids={codes_param}"

    def _parse_item(self, item: Tag) -> ScrapedOrder | None:
        """Extract a single procurement order from an HTML element."""
        title_tag = item.select_one("div.registry-entry__header-mid__number a")
        if title_tag is None:
            return None

        href = str(title_tag.get("href", ""))
        url = (
            f"https://zakupki.gov.ru{href}"
            if href and not href.startswith("http")
            else href
        )

        # Procurement name / title
        name_tag = item.select_one("div.registry-entry__body-value")
        title = name_tag.get_text(strip=True) if name_tag else title_tag.get_text(strip=True)

        description = self._extract_description(item)
        max_price = self._extract_max_price(item)
        deadline = self._extract_deadline(item)
        okpd2 = self._extract_okpd2_codes(item)
        published_at = self._extract_published_date(item)

        urgent = is_urgent_deadline(deadline) if deadline else False

        return ScrapedOrder(
            source="zakupki_gov",
            title=title,
            description=description,
            url=url,
            max_contract_price=max_price,
            submission_deadline=deadline,
            okpd2_codes=okpd2 if okpd2 else self.okpd2_codes,
            is_urgent=urgent,
            published_at=published_at,
        )

    @staticmethod
    def _extract_description(item: Tag) -> str:
        parts: list[str] = []
        for val in item.select("div.registry-entry__body-value"):
            text = val.get_text(strip=True)
            if text:
                parts.append(text)
        return " | ".join(parts) if parts else ""

    @staticmethod
    def _extract_max_price(item: Tag) -> Decimal | None:
        price_tag = item.select_one("div.price-block__value")
        if price_tag is None:
            return None
        return parse_price_text(price_tag.get_text(strip=True))

    @staticmethod
    def _extract_deadline(item: Tag) -> datetime | None:
        for dt_block in item.select("div.data-block__value"):
            text = dt_block.get_text(strip=True)
            # Try common Russian date formats
            for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M"):
                try:
                    return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _extract_okpd2_codes(item: Tag) -> list[str] | None:
        codes: list[str] = []
        for tag in item.select("span.registry-entry__body-val"):
            text = tag.get_text(strip=True)
            # OKPD2 codes match pattern like 62.01.11.000
            if len(text) > 5 and "." in text and text.replace(".", "").isdigit():
                codes.append(text)
        return codes if codes else None

    @staticmethod
    def _extract_published_date(item: Tag) -> datetime:
        date_tag = item.select_one("div.data-block__title + div.data-block__value")
        if date_tag:
            text = date_tag.get_text(strip=True)
            for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M"):
                try:
                    return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return datetime.now(timezone.utc)
