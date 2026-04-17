# Feature: glukhov-sales-engine, Property 4: Фильтрация по категориям создаёт Lead тогда и только тогда, когда категория входит в фильтр
# Validates: Requirement 2.6
"""Property-based tests for filter_by_category.

Property 4 states:
  For any order with category C and for any set of category filters F,
  a Lead is created if and only if C ∈ F.

The function filter_by_category(order, allowed_categories) returns:
  - True  if allowed_categories is empty (no filter applied — all pass)
  - True  if order.category is not None and order.category ∈ allowed_categories
  - False otherwise
"""

from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.worker_web.parsers.base import ScrapedOrder
from src.worker_web.worker import filter_by_category


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_order(category: str | None) -> ScrapedOrder:
    return ScrapedOrder(
        source="fl.ru",
        title="Test",
        description="desc",
        url="https://example.com",
        category=category,
        published_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_category = st.text()
_filter_set = st.sets(st.text(min_size=1))


# ---------------------------------------------------------------------------
# Property 4a: Category in filter → returns True
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(category=st.text(min_size=1), extra=_filter_set)
def test_category_in_filter_passes(category: str, extra: set[str]) -> None:
    """**Validates: Requirements 2.6**

    If the order's category is in the allowed set, filter_by_category
    must return True.
    """
    allowed = extra | {category}
    order = _make_order(category)
    assert filter_by_category(order, allowed) is True


# ---------------------------------------------------------------------------
# Property 4b: Category NOT in non-empty filter → returns False
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(category=st.text(min_size=1), allowed=st.sets(st.text(min_size=1), min_size=1))
def test_category_not_in_filter_rejected(category: str, allowed: set[str]) -> None:
    """**Validates: Requirements 2.6**

    If the filter set is non-empty and the order's category is NOT in it,
    filter_by_category must return False.
    """
    allowed.discard(category)
    if not allowed:
        return  # degenerate case — skip
    order = _make_order(category)
    assert filter_by_category(order, allowed) is False


# ---------------------------------------------------------------------------
# Property 4c: Empty filter → always returns True
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(category=_category)
def test_empty_filter_passes_all(category: str) -> None:
    """**Validates: Requirements 2.6**

    When the allowed_categories set is empty (no filter), every order
    passes regardless of its category.
    """
    order = _make_order(category if category else None)
    assert filter_by_category(order, set()) is True


# ---------------------------------------------------------------------------
# Property 4d: None category with non-empty filter → returns False
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(allowed=st.sets(st.text(min_size=1), min_size=1))
def test_none_category_rejected_with_filter(allowed: set[str]) -> None:
    """**Validates: Requirements 2.6**

    If the order has no category (None) and the filter set is non-empty,
    filter_by_category must return False.
    """
    order = _make_order(None)
    assert filter_by_category(order, allowed) is False


# ---------------------------------------------------------------------------
# Property 4e: Biconditional — filter_by_category ↔ (empty filter OR category ∈ filter)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    category=st.one_of(st.none(), st.text()),
    allowed=st.sets(st.text(min_size=1)),
)
def test_biconditional(category: str | None, allowed: set[str]) -> None:
    """**Validates: Requirements 2.6**

    filter_by_category returns True if and only if:
      - allowed_categories is empty, OR
      - category is not None and category ∈ allowed_categories
    """
    order = _make_order(category)
    result = filter_by_category(order, allowed)
    expected = (not allowed) or (category is not None and category in allowed)
    assert result == expected, (
        f"category={category!r}, allowed={allowed!r}: "
        f"got {result}, expected {expected}"
    )
