"""Shared constants used across multiple modules."""

from __future__ import annotations

# Deduplication
DEDUP_WINDOW_DAYS: int = 30

# Proxy
PROXY_BLOCKED_TTL_SECONDS: int = 60 * 60  # 1 hour

# Playwright
PLAYWRIGHT_TIMEOUT_MS: int = 30_000

# HTTP error handling
HTTP_429_PAUSE_SECONDS: int = 300
