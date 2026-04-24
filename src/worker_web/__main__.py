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
    """Launch a Playwright Chromium browser without a browser-level proxy.

    Proxy routing is handled per-context inside WorkerWeb so that
    Russian sites (profi.ru, zakupki.gov.ru) can bypass the VPN proxy.
    """
    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    return await pw.chromium.launch(headless=True)


async def main() -> None:
    """Bootstrap and run the web scraping worker."""
    settings = get_settings()
    setup_logging("worker_web", settings.LOG_LEVEL)

    if not settings.DATABASE_URL:
        raise RuntimeError("Worker_Web missing required env var: DATABASE_URL")

    engine = create_engine(settings.DATABASE_URL)
    session_factory = create_session_factory(engine)

    dedup = DedupService(session_factory, fuzzy_threshold=settings.DEDUP_FUZZY_THRESHOLD)
    proxy_pool = ProxyPool.from_file(settings.PROXY_LIST_PATH)

    web_keywords = _load_web_keywords(settings.TG_CHANNELS_CONFIG)
    logger.info("Loaded %d web keywords for filtering", len(web_keywords))

    # Build direct proxy config (separate fields for Playwright compatibility)
    direct_proxy: dict[str, str] | None = None
    if settings.SCRAPER_DIRECT_PROXY_URL:
        direct_proxy = {"server": settings.SCRAPER_DIRECT_PROXY_URL}
        if settings.SCRAPER_DIRECT_PROXY_USER:
            direct_proxy["username"] = settings.SCRAPER_DIRECT_PROXY_USER
            direct_proxy["password"] = settings.SCRAPER_DIRECT_PROXY_PASS

    worker = WorkerWeb(
        dedup_service=dedup,
        proxy_pool=proxy_pool,
        browser_factory=_create_browser,
        intervals={
            "fl_ru": settings.WEB_SCRAPE_INTERVAL_FL,
            "zakupki": settings.WEB_SCRAPE_INTERVAL_ZAKUPKI,
        },
        web_keywords=web_keywords,
        vpn_proxy_url=settings.SCRAPER_PROXY_URL or None,
        direct_proxy=direct_proxy,
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


if __name__ == "__main__":
    asyncio.run(main())
