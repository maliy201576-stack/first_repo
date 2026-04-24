"""Application configuration loaded from environment variables via Pydantic Settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration for all Glukhov Sales Engine services.

    Service-specific fields (TG_API_ID, TG_API_HASH) default to
    empty/zero so that services which don't need them can start
    without providing values.  Each service's ``__main__`` validates
    the fields it actually requires.
    """

    # Database
    DATABASE_URL: str = ""

    # Telegram (Telethon) — required only by worker_tg
    TG_API_ID: int = 0
    TG_API_HASH: str = ""
    TG_SESSION_NAME: str = "worker_tg"
    TG_SESSION_STRING: str = ""  # Telethon StringSession — portable, no file needed
    TG_CHANNELS_CONFIG: str = "/config/channels.yaml"

    # Web scraping intervals (seconds)
    WEB_SCRAPE_INTERVAL_FL: int = 900
    WEB_SCRAPE_INTERVAL_ZAKUPKI: int = 3600

    # Proxy
    PROXY_LIST_PATH: str = "/config/proxies.txt"
    # VPN proxy — not needed when server has direct access to target sites.
    # Set only if Telegram or other non-Russian sites require a proxy.
    SCRAPER_PROXY_URL: str = ""
    # Proxy with Russian IP for zakupki.gov.ru (blocks foreign traffic).
    # profi.ru works fine without a proxy from Europe.
    SCRAPER_DIRECT_PROXY_URL: str = ""
    SCRAPER_DIRECT_PROXY_USER: str = ""
    SCRAPER_DIRECT_PROXY_PASS: str = ""

    # Deduplication
    DEDUP_FUZZY_THRESHOLD: int = 85

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance loaded from environment variables."""
    return Settings()  # type: ignore[call-arg]
