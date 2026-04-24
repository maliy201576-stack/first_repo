"""Shared budget/price parsing utilities for web parsers.

Centralises the repeated pattern of extracting Decimal values from
price strings containing spaces, non-breaking spaces, and currency symbols.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


def parse_price_text(text: str) -> Decimal | None:
    """Parse a price string like '25 000' or '500' into a Decimal.

    Handles non-breaking spaces (``\\xa0``), regular spaces, and comma
    decimal separators commonly found on Russian freelance sites.

    Args:
        text: Raw price text, possibly with spaces and non-breaking spaces.

    Returns:
        Decimal value, or ``None`` if parsing fails.
    """
    cleaned = text.replace("\xa0", "").replace(" ", "").replace(",", ".")
    digits = "".join(ch for ch in cleaned if ch.isdigit() or ch == ".")
    if not digits:
        return None
    try:
        return Decimal(digits)
    except InvalidOperation:
        return None
