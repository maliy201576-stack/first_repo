"""Entry point for Worker_Web — web scraping worker."""

from __future__ import annotations

import asyncio
import logging
import signal

import yaml

from src.common.config import get_settings
from src.common.db import create_engine, create_session_factory
from src.common.logging import setup_logging
from src.dedup.service import DedupService
from src.worker_web.proxy_pool import ProxyPool
from src.worker_web.worker import WorkerWeb

logger = logging.getLogger(__name__)


def _load_web_keywords(config_path: str) -> list[str]:
    """Load web_keywords from channels.yaml config."""
    try:
        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("web_keywords", [])
    except Exception:
        logger.warning("Could not load web_keywords from %s", config_path)
        return []


async def _create_browser():
    """Launch a Playwright Chromium browser instance with proxy support."""
    from playwright.async_api import async_playwright

    settings = get_settings()
    pw = await async_playwright().start()

    launch_kwargs: dict = {"headless": True}
    if settings.SCRAPER_PROXY_URL:
        launch_kwargs["proxy"] = {"server": settings.SCRAPER_PROXY_URL}
        logger.info("Playwright browser using proxy: %s", settings.SCRAPER_PROXY_URL)

    return await pw.chromium.launch(**launch_kwargs)


async def main() -> None:
    """Bootstrap and run the web scraping worker."""
    settings = get_settings()
    setup_logging("worker_web", settings.LOG_LEVEL)

    if not settings.DATABASE_URL:
        raise RuntimeError("Worker_Web missing required env var: DATABASE_URL")

    engine = create_engine(settings.DATABASE_URL)
    session_factory = create_session_factory(engine)

    try:
        from redis.asyncio import Redis

        redis = Redis.from_url(settings.REDIS_URL)
    except Exception:
        logger.warning("Redis unavailable, running without cache")
        redis = None

    dedup = DedupService(session_factory, redis=redis, fuzzy_threshold=settings.DEDUP_FUZZY_THRESHOLD)
    proxy_pool = ProxyPool.from_file(settings.PROXY_LIST_PATH, redis=redis)

    web_keywords = _load_web_keywords(settings.TG_CHANNELS_CONFIG)
    logger.info("Loaded %d web keywords for filtering", len(web_keywords))

    worker = WorkerWeb(
        dedup_service=dedup,
        proxy_pool=proxy_pool,
        browser_factory=_create_browser,
        intervals={
            "fl_ru": settings.WEB_SCRAPE_INTERVAL_FL,
            "habr": settings.WEB_SCRAPE_INTERVAL_HABR,
            "zakupki": settings.WEB_SCRAPE_INTERVAL_ZAKUPKI,
        },
        notifier=None,
        web_keywords=web_keywords,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await worker.start()
        logger.info("Worker_Web running — waiting for stop signal")
        await stop_event.wait()
    finally:
        await worker.stop()
        await engine.dispose()
        if redis is not None:
            await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
