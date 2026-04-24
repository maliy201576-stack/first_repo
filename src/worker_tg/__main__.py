"""Entry point for Worker_TG — Telegram channel monitor."""

from __future__ import annotations

import asyncio
import logging

from src.common.config import get_settings
from src.common.db import create_engine, create_session_factory
from src.common.logging import setup_logging
from src.common.telegram import create_telegram_client
from src.dedup.service import DedupService
from src.worker_tg.config_loader import ConfigLoader
from src.worker_tg.worker import WorkerTG

logger = logging.getLogger(__name__)


async def main() -> None:
    """Bootstrap and run the Telegram monitoring worker."""
    settings = get_settings()
    setup_logging("worker_tg", settings.LOG_LEVEL)

    if not settings.DATABASE_URL:
        raise RuntimeError("Worker_TG missing required env var: DATABASE_URL")

    if not settings.TG_SESSION_STRING:
        raise RuntimeError(
            "TG_SESSION_STRING is empty. "
            "Run 'docker compose run --rm tg-auth' to authorize first."
        )

    engine = create_engine(settings.DATABASE_URL)
    session_factory = create_session_factory(engine)

    dedup = DedupService(session_factory, fuzzy_threshold=settings.DEDUP_FUZZY_THRESHOLD)
    config_loader = ConfigLoader(settings.TG_CHANNELS_CONFIG)

    client = create_telegram_client(
        session_name=settings.TG_SESSION_NAME,
        api_id=settings.TG_API_ID,
        api_hash=settings.TG_API_HASH,
        session_string=settings.TG_SESSION_STRING,
    )

    worker = WorkerTG(
        client=client,
        dedup_service=dedup,
        config_loader=config_loader,
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


if __name__ == "__main__":
    asyncio.run(main())
