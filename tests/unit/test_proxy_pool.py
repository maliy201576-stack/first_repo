"""Unit tests for ProxyPool.

Validates Requirements 2.3, 2.5:
- Round-robin rotation of available proxies
- Exclusion of blocked proxies (in-memory with TTL)
"""

from __future__ import annotations

import pytest

from src.worker_web.proxy_pool import ProxyPool, NoAvailableProxiesError


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetNext:
    """Validates: Requirements 2.3"""

    async def test_round_robin_returns_all_proxies(self) -> None:
        """N calls to get_next() return N distinct proxies in order."""
        proxies = ["proxy1:8080", "proxy2:8080", "proxy3:8080"]
        pool = ProxyPool(proxies=proxies)

        results = [await pool.get_next() for _ in range(3)]
        assert results == proxies

    async def test_round_robin_wraps_around(self) -> None:
        """After exhausting all proxies, rotation starts over."""
        proxies = ["proxy1:8080", "proxy2:8080"]
        pool = ProxyPool(proxies=proxies)

        results = [await pool.get_next() for _ in range(4)]
        assert results == ["proxy1:8080", "proxy2:8080", "proxy1:8080", "proxy2:8080"]

    async def test_skips_blocked_proxy(self) -> None:
        """Blocked proxies are skipped during rotation."""
        proxies = ["proxy1:8080", "proxy2:8080", "proxy3:8080"]
        pool = ProxyPool(proxies=proxies)
        await pool.mark_blocked("proxy2:8080")

        result = await pool.get_next()
        assert result == "proxy1:8080"

        result = await pool.get_next()
        assert result == "proxy3:8080"

    async def test_raises_when_all_blocked(self) -> None:
        """NoAvailableProxiesError raised when every proxy is blocked."""
        proxies = ["proxy1:8080", "proxy2:8080"]
        pool = ProxyPool(proxies=proxies)
        await pool.mark_blocked("proxy1:8080")
        await pool.mark_blocked("proxy2:8080")

        with pytest.raises(NoAvailableProxiesError, match="All proxies are blocked"):
            await pool.get_next()

    async def test_raises_when_pool_empty(self) -> None:
        """NoAvailableProxiesError raised when pool has no proxies."""
        pool = ProxyPool(proxies=[])

        with pytest.raises(NoAvailableProxiesError, match="empty"):
            await pool.get_next()


class TestMarkBlocked:
    """Validates: Requirements 2.5"""

    async def test_blocked_proxy_excluded_from_rotation(self) -> None:
        """After mark_blocked(), the proxy is no longer returned by get_next()."""
        proxies = ["proxy1:8080", "proxy2:8080"]
        pool = ProxyPool(proxies=proxies)

        await pool.mark_blocked("proxy1:8080")

        result = await pool.get_next()
        assert result == "proxy2:8080"


class TestGetAvailableCount:
    """Validates: Requirements 2.3, 2.5"""

    async def test_all_available(self) -> None:
        """All proxies available when none are blocked."""
        proxies = ["proxy1:8080", "proxy2:8080", "proxy3:8080"]
        pool = ProxyPool(proxies=proxies)

        count = await pool.get_available_count()
        assert count == 3

    async def test_some_blocked(self) -> None:
        """Count excludes blocked proxies."""
        proxies = ["proxy1:8080", "proxy2:8080", "proxy3:8080"]
        pool = ProxyPool(proxies=proxies)
        await pool.mark_blocked("proxy2:8080")

        count = await pool.get_available_count()
        assert count == 2

    async def test_all_blocked(self) -> None:
        """Count is 0 when all proxies are blocked."""
        proxies = ["proxy1:8080", "proxy2:8080"]
        pool = ProxyPool(proxies=proxies)
        await pool.mark_blocked("proxy1:8080")
        await pool.mark_blocked("proxy2:8080")

        count = await pool.get_available_count()
        assert count == 0


class TestFromFile:
    """Validates proxy loading from file."""

    async def test_loads_proxies_from_file(self, tmp_path) -> None:
        """from_file() reads one proxy per line, skipping blanks."""
        proxy_file = tmp_path / "proxies.txt"
        proxy_file.write_text("proxy1:8080\nproxy2:8080\n\nproxy3:8080\n")

        pool = ProxyPool.from_file(str(proxy_file))

        assert pool._proxies == ["proxy1:8080", "proxy2:8080", "proxy3:8080"]


# ---------------------------------------------------------------------------
# Feature: glukhov-sales-engine, Property 3: ProxyPool исключает заблокированные прокси и ротирует доступные
# Validates: Requirements 2.3, 2.5
# ---------------------------------------------------------------------------
"""
Property 3 states:
  For any proxy pool, after calling mark_blocked(proxy), get_next() never
  returns the blocked proxy. Given N available proxies, N consecutive calls
  to get_next() must return N distinct proxies.
"""

from hypothesis import given, settings, assume
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Property 3a: After mark_blocked(proxy), get_next() never returns it
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    proxies=st.lists(st.text(min_size=5), min_size=2, unique=True),
)
async def test_blocked_proxy_never_returned(proxies: list[str]) -> None:
    """**Validates: Requirements 2.3, 2.5**

    For any proxy pool, after calling mark_blocked(proxy), get_next()
    never returns the blocked proxy.
    """
    pool = ProxyPool(proxies=proxies)

    blocked_proxy = proxies[0]
    await pool.mark_blocked(blocked_proxy)

    available_count = len(proxies) - 1
    for _ in range(available_count):
        result = await pool.get_next()
        assert result != blocked_proxy, (
            f"Blocked proxy {blocked_proxy!r} was returned by get_next()"
        )


# ---------------------------------------------------------------------------
# Property 3b: N available proxies → N consecutive get_next() return N distinct
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    proxies=st.lists(st.text(min_size=5), min_size=2, unique=True),
)
async def test_n_calls_return_n_distinct_proxies(proxies: list[str]) -> None:
    """**Validates: Requirements 2.3, 2.5**

    Given N available proxies, N consecutive calls to get_next() must
    return N distinct proxies.
    """
    pool = ProxyPool(proxies=proxies)

    n = len(proxies)
    results = []
    for _ in range(n):
        results.append(await pool.get_next())

    assert len(set(results)) == n, (
        f"Expected {n} distinct proxies, got {len(set(results))}: {results}"
    )


# ---------------------------------------------------------------------------
# Property 3c: get_available_count() correct after blocking
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    proxies=st.lists(st.text(min_size=5), min_size=2, unique=True),
    block_indices=st.lists(st.integers(min_value=0), min_size=0, max_size=5),
)
async def test_available_count_after_blocking(
    proxies: list[str], block_indices: list[int]
) -> None:
    """**Validates: Requirements 2.3, 2.5**

    get_available_count() returns the correct count after blocking proxies.
    """
    pool = ProxyPool(proxies=proxies)

    blocked = set()
    for idx in block_indices:
        proxy = proxies[idx % len(proxies)]
        await pool.mark_blocked(proxy)
        blocked.add(proxy)

    expected_available = len(proxies) - len(blocked)
    actual_available = await pool.get_available_count()
    assert actual_available == expected_available, (
        f"Expected {expected_available} available, got {actual_available}"
    )


# ---------------------------------------------------------------------------
# Property 3d: Round-robin wraps correctly — 2N calls on N proxies
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    proxies=st.lists(st.text(min_size=5), min_size=2, unique=True),
)
async def test_round_robin_wraps_correctly(proxies: list[str]) -> None:
    """**Validates: Requirements 2.3, 2.5**

    After N calls exhaust all proxies, the next N calls return the same
    proxies in the same order (round-robin wrap).
    """
    pool = ProxyPool(proxies=proxies)

    n = len(proxies)
    first_cycle = [await pool.get_next() for _ in range(n)]
    second_cycle = [await pool.get_next() for _ in range(n)]

    assert first_cycle == second_cycle, (
        f"Round-robin did not wrap: first={first_cycle}, second={second_cycle}"
    )
