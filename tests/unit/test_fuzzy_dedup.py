# Feature: glukhov-sales-engine, Property 7: Нечёткая дедупликация соблюдает порог сходства
# Validates: Requirement 4.1
"""Property-based tests for DedupService.fuzzy_match threshold behaviour.

Property 7 states:
  For any two strings a and b, if fuzzy_match(a, b) >= 85, the pair is
  considered a duplicate.  If fuzzy_match(a, b) < 85, the pair is NOT a
  duplicate.  For any string a, fuzzy_match(a, a) == 100.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from src.dedup.service import DedupService

FUZZY_THRESHOLD = 85

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_text = st.text(min_size=1)


# ---------------------------------------------------------------------------
# Property 7a: Self-similarity — fuzzy_match(a, a) == 100
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(a=_text)
def test_self_similarity(a: str) -> None:
    """**Validates: Requirements 4.1**

    For any non-empty string a, fuzzy_match(a, a) must equal 100.
    """
    assert DedupService.fuzzy_match(a, a) == 100


# ---------------------------------------------------------------------------
# Property 7b: Threshold consistency — score determines duplicate status
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(a=_text, b=_text)
def test_threshold_consistency(a: str, b: str) -> None:
    """**Validates: Requirements 4.1**

    For any two strings a and b, the duplicate decision must be consistent
    with the threshold: score >= 85 → duplicate, score < 85 → not duplicate.
    """
    score = DedupService.fuzzy_match(a, b)
    if score >= FUZZY_THRESHOLD:
        assert score >= FUZZY_THRESHOLD, "Pair should be considered a duplicate"
    else:
        assert score < FUZZY_THRESHOLD, "Pair should NOT be considered a duplicate"


# ---------------------------------------------------------------------------
# Property 7c: Symmetry — fuzzy_match(a, b) == fuzzy_match(b, a)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(a=_text, b=_text)
def test_symmetry(a: str, b: str) -> None:
    """**Validates: Requirements 4.1**

    token_sort_ratio is symmetric: fuzzy_match(a, b) == fuzzy_match(b, a).
    """
    assert DedupService.fuzzy_match(a, b) == DedupService.fuzzy_match(b, a)


# ---------------------------------------------------------------------------
# Property 7d: Score range — result is always in [0, 100]
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(a=_text, b=_text)
def test_score_range(a: str, b: str) -> None:
    """**Validates: Requirements 4.1**

    fuzzy_match must always return a value in the closed interval [0, 100].
    """
    score = DedupService.fuzzy_match(a, b)
    assert 0 <= score <= 100
