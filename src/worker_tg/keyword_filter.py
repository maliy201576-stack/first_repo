"""Keyword filtering for Telegram messages.

Provides case-insensitive keyword matching against message text.
"""


def filter_message(text: str, keywords: list[str]) -> list[str]:
    """Return all keywords found in *text* (case-insensitive).

    Args:
        text: The message text to search.
        keywords: List of keywords to look for.

    Returns:
        List of matched keywords (preserving original casing from *keywords*).
    """
    if not text or not keywords:
        return []

    text_lower = text.lower()
    return [kw for kw in keywords if kw.lower() in text_lower]
