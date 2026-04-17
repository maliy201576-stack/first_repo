"""Entry point for Worker_TG — Telegram channel monitor."""

from __future__ import annotations

import asyncio
import logging

from telethon import TelegramClient

from src.common.config import get_settings
from src.common.db import create_engine, create_session_factory
from src.common.logging import setup_logging
from src.dedup.service import DedupService
from src.notifier.service import Notifier
from src.worker_tg.config_loader import ConfigLoader
from src.worker_tg.worker import WorkerTG

logger = logging.getLogger(__name__)


async def main() -> None:
    """Bootstrap and run the Telegram monitoring worker."""
    settings = get_settings()
    setup_logging("worker_tg", settings.LOG_LEVEL)

    missing = []
    if not settings.DATABASE_URL:
        missing.append("DATABASE_URL")
    if not settings.TG_API_ID:
        missing.append("TG_API_ID")
    if not settings.TG_API_HASH:
        missing.append("TG_API_HASH")
    if missing:
        raise RuntimeError(f"Worker_TG missing required env vars: {', '.join(missing)}")

    engine = create_engine(settings.DATABASE_URL)
    session_factory = create_session_factory(engine)

    try:
        from redis.asyncio import Redis

        redis = Redis.from_url(settings.REDIS_URL)
    except Exception:
        logger.warning("Redis unavailable, running without cache")
        redis = None

    dedup = DedupService(session_factory, redis=redis, fuzzy_threshold=settings.DEDUP_FUZZY_THRESHOLD)
    config_loader = ConfigLoader(settings.TG_CHANNELS_CONFIG)

    client = TelegramClient(
        settings.TG_SESSION_NAME,
        settings.TG_API_ID,
        settings.TG_API_HASH,
    )
    notifier = Notifier(client)

    worker = WorkerTG(
        client=client,
        dedup_service=dedup,
        config_loader=config_loader,
        notifier=notifier,
    )

    try:
        await worker.start()
        logger.info("Worker_TG running — press Ctrl+C to stop")
        await client.run_until_disconnected()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await worker.stop()
        await engine.dispose()
        if redis is not None:
            await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
