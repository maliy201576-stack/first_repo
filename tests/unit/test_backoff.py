# Feature: glukhov-sales-engine, Property 2: Экспоненциальная задержка ограничена сверху
# Validates: Requirement 1.5
"""Property-based tests for compute_backoff exponential delay.

Property 2 states:
  For any retry attempt number n ≥ 0, the computed delay must equal
  min(2^n, 60) seconds — monotonically non-decreasing up to a maximum
  of 60 seconds and never exceeding it.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from src.worker_tg.worker import compute_backoff

# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

_attempt = st.integers(min_value=0, max_value=100)


# ---------------------------------------------------------------------------
# Property 2a: Delay equals min(2^n, 60)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(attempt=_attempt)
def test_backoff_equals_expected_formula(attempt: int) -> None:
    """**Validates: Requirements 1.5**

    The backoff delay for attempt n must equal min(2**n, 60).
    """
    result = compute_backoff(attempt)
    expected = min(2 ** attempt, 60)
    assert result == expected, (
        f"compute_backoff({attempt}) = {result}, expected {expected}"
    )


# ---------------------------------------------------------------------------
# Property 2b: Delay never exceeds 60 seconds (upper bound)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(attempt=_attempt)
def test_backoff_never_exceeds_max(attempt: int) -> None:
    """**Validates: Requirements 1.5**

    The backoff delay must never exceed 60 seconds.
    """
    result = compute_backoff(attempt)
    assert result <= 60, (
        f"compute_backoff({attempt}) = {result}, exceeds max of 60"
    )


# ---------------------------------------------------------------------------
# Property 2c: Delay is monotonically non-decreasing
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(attempt=_attempt)
def test_backoff_is_monotonically_non_decreasing(attempt: int) -> None:
    """**Validates: Requirements 1.5**

    The delay for attempt n+1 must be >= the delay for attempt n.
    """
    current = compute_backoff(attempt)
    next_val = compute_backoff(attempt + 1)
    assert next_val >= current, (
        f"compute_backoff({attempt + 1}) = {next_val} < "
        f"compute_backoff({attempt}) = {current}"
    )


# ---------------------------------------------------------------------------
# Property 2d: Delay is always positive
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(attempt=_attempt)
def test_backoff_is_always_positive(attempt: int) -> None:
    """**Validates: Requirements 1.5**

    The backoff delay must always be > 0.
    """
    result = compute_backoff(attempt)
    assert result > 0, (
        f"compute_backoff({attempt}) = {result}, expected > 0"
    )
