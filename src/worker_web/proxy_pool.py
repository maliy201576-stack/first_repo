"""Proxy pool with round-robin rotation and in-memory blocking.

Provides rotating proxy selection for web scraping workers, with automatic
exclusion of blocked proxies. Blocked status is stored in-memory with a
configurable TTL (default 1 hour). Block list resets on container restart.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from src.common.constants import PROXY_BLOCKED_TTL_SECONDS

logger = logging.getLogger(__name__)


class NoAvailableProxiesError(Exception):
    """Raised when all proxies in the pool are blocked."""


class ProxyPool:
    """Round-robin proxy pool with in-memory block list."""

    def __init__(self, proxies: list[str]) -> None:
        self._proxies = list(proxies)
        self._index = 0
        self._blocked: dict[str, float] = {}  # proxy -> expiry timestamp

    @classmethod
    def from_file(cls, path: str) -> ProxyPool:
        """Load proxies from a text file (one proxy per line).

        Returns an empty pool if the file does not exist.
        """
        file_path = Path(path)
        if not file_path.exists():
            logger.warning("Proxy file not found: %s — running without proxies", path)
            return cls(proxies=[])
        lines = file_path.read_text(encoding="utf-8").splitlines()
        proxies = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        logger.info("Loaded %d proxies from %s", len(proxies), path)
        return cls(proxies=proxies)

    def _is_blocked(self, proxy: str) -> bool:
        """Check whether *proxy* is currently blocked."""
        expiry = self._blocked.get(proxy)
        if expiry is None:
            return False
        if time.monotonic() >= expiry:
            del self._blocked[proxy]
            return False
        return True

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
            if not self._is_blocked(proxy):
                return proxy

        raise NoAvailableProxiesError("All proxies are blocked")

    async def mark_blocked(self, proxy: str) -> None:
        """Mark *proxy* as blocked for the configured TTL."""
        self._blocked[proxy] = time.monotonic() + PROXY_BLOCKED_TTL_SECONDS
        logger.info("Proxy marked as blocked: %s (TTL=%ds)", proxy, PROXY_BLOCKED_TTL_SECONDS)

    async def get_available_count(self) -> int:
        """Return the number of proxies that are not currently blocked."""
        return sum(1 for proxy in self._proxies if not self._is_blocked(proxy))
