"""Entry point for the Notifier service.

The notifier runs as a lightweight standby process. It creates a Telethon
client that other services can use for sending alerts to Saved Messages.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from src.common.config import get_settings
from src.common.logging import setup_logging

logger = logging.getLogger(__name__)


async def main() -> None:
    """Run the notifier service (standby mode)."""
    settings = get_settings()
    setup_logging("notifier", settings.LOG_LEVEL)

    missing = []
    if not settings.TG_API_ID:
        missing.append("TG_API_ID")
    if not settings.TG_API_HASH:
        missing.append("TG_API_HASH")
    if missing:
        raise RuntimeError(f"Notifier missing required env vars: {', '.join(missing)}")

    logger.info("Notifier service started — standing by for alerts")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    logger.info("Notifier service stopped")


if __name__ == "__main__":
    asyncio.run(main())
