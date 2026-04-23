"""Base data structures and utilities for web parsers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

# UI noise phrases scraped from source sites that should be stripped
_NOISE_PHRASES = [
    "Показать полностью",
    "Показать ещё",
    "Показать еще",
    "Свернуть",
    "Скрыть",
    "Читать далее",
    "Подробнее",
    "ещё",
]
_NOISE_RE = re.compile(
    r"\s*(?:" + "|".join(re.escape(p) for p in _NOISE_PHRASES) + r")\s*",
    re.IGNORECASE,
)


@dataclass
class ScrapedOrder:
    """Unified data structure for orders scraped from web sources."""

    source: str  # "fl.ru" | "habr_freelance" | "zakupki_gov"
    title: str
    description: str
    url: str
    budget: Decimal | None = None
    budget_max: Decimal | None = None
    category: str | None = None
    published_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Fields specific to government procurement
    okpd2_codes: list[str] | None = None
    max_contract_price: Decimal | None = None
    submission_deadline: datetime | None = None
    is_urgent: bool = False


def is_urgent_deadline(
    deadline: datetime | date,
    now: datetime | date | None = None,
) -> bool:
    """Determine if a deadline is urgent (fewer than 3 business days away).

    Business days exclude Saturday (weekday 5) and Sunday (weekday 6).
    If the deadline is in the past or today, it is considered urgent.

    Args:
        deadline: The submission deadline.
        now: Current date/time for testability. Defaults to UTC now.

    Returns:
        True if fewer than 3 business days remain before the deadline.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    deadline_date = deadline.date() if isinstance(deadline, datetime) else deadline
    now_date = now.date() if isinstance(now, datetime) else now

    if deadline_date <= now_date:
        return True

    # Count business days strictly between now_date and deadline_date
    # (excluding both endpoints).
    business_days = 0
    current = now_date + timedelta(days=1)
    while current < deadline_date:
        if current.weekday() < 5:  # Monday=0 .. Friday=4
            business_days += 1
        current += timedelta(days=1)

    return business_days < 3


def clean_description(text: str) -> str:
    """Remove UI noise phrases from scraped description text.

    Strips phrases like "Показать полностью", "Свернуть", etc.
    that are part of the source site UI, not the actual content.

    Args:
        text: Raw description text from a parser.

    Returns:
        Cleaned description string.
    """
    cleaned = _NOISE_RE.sub(" ", text)
    # Collapse multiple spaces
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()
