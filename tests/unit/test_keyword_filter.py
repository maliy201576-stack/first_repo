# Feature: glukhov-sales-engine, Property 1: Фильтрация по ключевым словам возвращает все совпадения
# Validates: Requirement 1.4
"""Property-based tests for filter_message keyword matching.

Property 1 states:
  For any message text and for any set of keywords, filter_message must
  return a list containing all and only those keywords that are present
  in the text (case-insensitive).
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from src.worker_tg.keyword_filter import filter_message

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_keyword = st.text(min_size=1)
_keywords = st.lists(_keyword, min_size=0, max_size=20)
_text = st.text()


# ---------------------------------------------------------------------------
# Property 1a: Every returned keyword is actually present in the text
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(text=_text, keywords=_keywords)
def test_returned_keywords_are_present_in_text(text: str, keywords: list[str]) -> None:
    """**Validates: Requirements 1.4**

    Every keyword in the result must actually occur in the text
    (case-insensitive).
    """
    result = filter_message(text, keywords)
    text_lower = text.lower()
    for kw in result:
        assert kw.lower() in text_lower, (
            f"Keyword {kw!r} was returned but is not present in text"
        )


# ---------------------------------------------------------------------------
# Property 1b: Every keyword present in the text is returned
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(text=_text, keywords=_keywords)
def test_all_present_keywords_are_returned(text: str, keywords: list[str]) -> None:
    """**Validates: Requirements 1.4**

    If a keyword from the input list is present in the text
    (case-insensitive), it must appear in the result.
    """
    result = filter_message(text, keywords)
    text_lower = text.lower()
    for kw in keywords:
        if kw.lower() in text_lower:
            assert kw in result, (
                f"Keyword {kw!r} is present in text but was not returned"
            )


# ---------------------------------------------------------------------------
# Property 1c: Result is a subset of the input keywords list
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(text=_text, keywords=_keywords)
def test_result_is_subset_of_keywords(text: str, keywords: list[str]) -> None:
    """**Validates: Requirements 1.4**

    The result must only contain elements from the original keywords list.
    No fabricated keywords should appear.
    """
    result = filter_message(text, keywords)
    for kw in result:
        assert kw in keywords, (
            f"Keyword {kw!r} in result is not from the input keywords list"
        )
