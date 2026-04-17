# Feature: glukhov-sales-engine, Property 5: Определение срочности закупки
# Validates: Requirement 3.3
"""Property-based tests for is_urgent_deadline.

Property 5 states:
  For any submission deadline date and for any current date, the function
  is_urgent_deadline returns True if and only if there are fewer than 3
  business days between the current date and the deadline (excluding weekends).

Business days are counted strictly between now_date and deadline_date,
excluding both endpoints.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from src.worker_web.parsers.base import is_urgent_deadline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_business_days_between(start: date, end: date) -> int:
    """Count business days strictly between *start* and *end* (exclusive)."""
    if end <= start:
        return 0
    count = 0
    current = start + timedelta(days=1)
    while current < end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


def _add_business_days(d: date, n: int) -> date:
    """Advance *d* by *n* business days (skipping weekends)."""
    added = 0
    current = d
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_dates = st.dates(min_value=date(2000, 1, 1), max_value=date(2100, 12, 31))
_datetimes = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 12, 31),
    timezones=st.just(timezone.utc),
)


# ---------------------------------------------------------------------------
# Property 5a: Deadline in the past or today → always urgent
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_dates, delta=st.integers(min_value=0, max_value=3650))
def test_past_or_today_deadline_is_always_urgent(now: date, delta: int) -> None:
    """**Validates: Requirements 3.3**

    If the deadline is today or in the past, is_urgent_deadline must return True.
    """
    deadline = now - timedelta(days=delta)
    assert is_urgent_deadline(deadline, now) is True


# ---------------------------------------------------------------------------
# Property 5b: Deadline far in the future → never urgent
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_dates)
def test_far_future_deadline_is_not_urgent(now: date) -> None:
    """**Validates: Requirements 3.3**

    If the deadline is 30+ business days away, is_urgent_deadline must return False.
    """
    deadline = _add_business_days(now, 30)
    assert is_urgent_deadline(deadline, now) is False


# ---------------------------------------------------------------------------
# Property 5c: Consistency with business-day count (oracle test)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_dates, deadline=_dates)
def test_urgency_matches_business_day_count(now: date, deadline: date) -> None:
    """**Validates: Requirements 3.3**

    is_urgent_deadline(deadline, now) must be True iff deadline <= now or
    the number of business days strictly between now and deadline is < 3.
    """
    if deadline <= now:
        assert is_urgent_deadline(deadline, now) is True
    else:
        bdays = _count_business_days_between(now, deadline)
        expected = bdays < 3
        result = is_urgent_deadline(deadline, now)
        assert result == expected, (
            f"now={now}, deadline={deadline}, bdays_between={bdays}, "
            f"expected={expected}, got={result}"
        )


# ---------------------------------------------------------------------------
# Property 5d: Weekend invariance — inserting weekend days doesn't change result
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_dates)
def test_weekend_does_not_affect_urgency(now: date) -> None:
    """**Validates: Requirements 3.3**

    Moving a deadline across a weekend (adding Sat+Sun) should not change
    the business-day count.  We verify by comparing a Friday deadline with
    the following Monday deadline — both have the same business days between
    now and the deadline when now is before the Friday.
    """
    # Find the next Friday from now (or now itself if it's Friday)
    days_until_friday = (4 - now.weekday()) % 7
    friday = now + timedelta(days=days_until_friday)
    monday = friday + timedelta(days=3)

    # Only meaningful when now is strictly before friday
    assume(now < friday)

    bdays_friday = _count_business_days_between(now, friday)
    bdays_monday = _count_business_days_between(now, monday)

    # Monday adds exactly 1 more business day (Monday itself is excluded as endpoint,
    # but Friday is included as a day between now and Monday)
    # The key insight: Sat and Sun between Friday and Monday add 0 business days
    assert bdays_monday == bdays_friday + 1, (
        f"now={now}, friday={friday}, monday={monday}, "
        f"bdays_friday={bdays_friday}, bdays_monday={bdays_monday}"
    )


# ---------------------------------------------------------------------------
# Property 5e: Determinism — same inputs always produce same output
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_dates, deadline=_dates)
def test_deterministic_output(now: date, deadline: date) -> None:
    """**Validates: Requirements 3.3**

    Calling is_urgent_deadline with the same arguments must always return
    the same result.
    """
    r1 = is_urgent_deadline(deadline, now)
    r2 = is_urgent_deadline(deadline, now)
    assert r1 == r2


# ---------------------------------------------------------------------------
# Property 5f: datetime inputs produce same result as date inputs
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(now=_datetimes, deadline=_datetimes)
def test_datetime_and_date_inputs_agree(now: datetime, deadline: datetime) -> None:
    """**Validates: Requirements 3.3**

    is_urgent_deadline must produce the same result whether given datetime
    or date objects (only the date part matters).
    """
    result_dt = is_urgent_deadline(deadline, now)
    result_d = is_urgent_deadline(deadline.date(), now.date())
    assert result_dt == result_d, (
        f"datetime result={result_dt}, date result={result_d} "
        f"for now={now}, deadline={deadline}"
    )
