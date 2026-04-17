"""Proxy pool with round-robin rotation and Redis-backed blocking.

Provides rotating proxy selection for web scraping workers, with automatic
exclusion of blocked proxies. Blocked status is stored in Redis with a 1-hour
TTL. When Redis is unavailable, all proxies are treated as available.
"""

from __future__ import annotations

import logging
from pathlib import Path

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

_BLOCKED_TTL_SECONDS = 60 * 60  # 1 hour


class NoAvailableProxiesError(Exception):
    """Raised when all proxies in the pool are blocked."""


class ProxyPool:
    """Round-robin proxy pool with Redis-backed block list."""

    def __init__(self, proxies: list[str], redis: Redis | None = None) -> None:
        self._proxies = list(proxies)
        self._redis = redis
        self._index = 0

    @classmethod
    def from_file(cls, path: str, redis: Redis | None = None) -> ProxyPool:
        """Load proxies from a text file (one proxy per line)."""
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        proxies = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        logger.info("Loaded %d proxies from %s", len(proxies), path)
        return cls(proxies=proxies, redis=redis)

    async def _is_blocked(self, proxy: str) -> bool:
        """Check whether *proxy* is marked as blocked in Redis."""
        if self._redis is None:
            return False
        try:
            return bool(await self._redis.exists(f"proxy:blocked:{proxy}"))
        except RedisError:
            logger.warning("Redis unavailable, treating proxy as available: %s", proxy)
            return False

    async def get_next(self) -> str:
        """Return the next available (non-blocked) proxy using round-robin.

        Raises :class:`NoAvailableProxiesError` when every proxy is blocked.
        """
        total = len(self._proxies)
        if total == 0:
            raise NoAvailableProxiesError("Proxy pool is empty")

        for _ in range(total):
            proxy = self._proxies[self._index % total]
            self._index = (self._index + 1) % total
            if not await self._is_blocked(proxy):
                return proxy

        raise NoAvailableProxiesError("All proxies are blocked")

    async def mark_blocked(self, proxy: str) -> None:
        """Mark *proxy* as blocked in Redis with a 1-hour TTL."""
        if self._redis is None:
            logger.warning("Redis not configured, cannot mark proxy as blocked: %s", proxy)
            return
        try:
            await self._redis.set(
                f"proxy:blocked:{proxy}",
                "1",
                ex=_BLOCKED_TTL_SECONDS,
            )
            logger.info("Proxy marked as blocked: %s (TTL=%ds)", proxy, _BLOCKED_TTL_SECONDS)
        except RedisError:
            logger.warning("Redis unavailable, failed to mark proxy as blocked: %s", proxy)

    async def get_available_count(self) -> int:
        """Return the number of proxies that are not currently blocked."""
        count = 0
        for proxy in self._proxies:
            if not await self._is_blocked(proxy):
                count += 1
        return count
