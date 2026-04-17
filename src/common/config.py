"""Application configuration loaded from environment variables via Pydantic Settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration for all Glukhov Sales Engine services.

    Fields that are only needed by specific services (TG_API_ID, TG_API_HASH,
    NOTIFIER_BOT_TOKEN, NOTIFIER_CHAT_ID) default to empty/zero so that
    services which don't use them can start without providing values.
    Each service's __main__ validates the fields it actually requires.
    """

    # Database
    DATABASE_URL: str = ""

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # Telegram (Telethon) — required only by worker_tg
    TG_API_ID: int = 0
    TG_API_HASH: str = ""
    TG_SESSION_NAME: str = "worker_tg"
    TG_CHANNELS_CONFIG: str = "/config/channels.yaml"

    # Web scraping intervals (seconds)
    WEB_SCRAPE_INTERVAL_FL: int = 900
    WEB_SCRAPE_INTERVAL_HABR: int = 900
    WEB_SCRAPE_INTERVAL_ZAKUPKI: int = 3600

    # Proxy
    PROXY_LIST_PATH: str = "/config/proxies.txt"
    SCRAPER_PROXY_URL: str = ""

    # Deduplication
    DEDUP_FUZZY_THRESHOLD: int = 85

    # Notifier — uses Telethon (same TG_API_ID/TG_API_HASH as worker_tg)
    # No separate bot token needed; alerts go to Saved Messages.

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


_settings: Settings | None = None


def get_settings() -> Settings:
    """Create and return a cached Settings instance from environment variables."""
    global _settings  # noqa: PLW0603
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]
    return _settings
