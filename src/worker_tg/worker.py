"""Worker_TG — real-time Telegram channel monitor.

Connects to Telegram via Telethon, subscribes to configured channels,
filters messages by keywords, and sends matching leads to DedupService.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, events

from src.common.models import LeadCandidate
from src.dedup.service import DedupService
from src.worker_tg.config_loader import ConfigLoader
from src.worker_tg.keyword_filter import filter_message

logger = logging.getLogger(__name__)


def compute_backoff(attempt: int) -> float:
    """Compute exponential backoff delay capped at 60 seconds.

    Args:
        attempt: Zero-based retry attempt number.

    Returns:
        Delay in seconds: ``min(2 ** attempt, 60)``.
    """
    return min(2 ** attempt, 60)


class WorkerTG:
    """Telegram channel monitor that filters messages and creates leads.

    Args:
        client: A configured :class:`TelegramClient` instance.
        dedup_service: The deduplication service for storing leads.
        config_loader: Loader for channels/keywords configuration.
        config_reload_interval: Seconds between config reload checks.
    """

    def __init__(
        self,
        client: TelegramClient,
        dedup_service: DedupService,
        config_loader: ConfigLoader,
        config_reload_interval: int = 60,
    ) -> None:
        self._client = client
        self._dedup = dedup_service
        self._config_loader = config_loader
        self._config_reload_interval = config_reload_interval
        self._running = False
        self._reload_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Connect to Telegram and start listening for messages."""
        self._config_loader.load()
        config = self._config_loader.config

        attempt = 0
        while True:
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    raise RuntimeError(
                        "Telegram session not authorized. "
                        "Run 'python scripts/tg_auth.py' locally first to create a session file."
                    )
                logger.info("Connected to Telegram")
                attempt = 0
                break
            except RuntimeError:
                raise
            except Exception as exc:
                delay = compute_backoff(attempt)
                logger.warning(
                    "Telegram connection failed (attempt %d): %s — retrying in %.0fs",
                    attempt,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                attempt += 1

        self._running = True

        # Register handler for configured channels
        channels = config.channels
        if channels:
            self._client.add_event_handler(
                self._on_new_message,
                events.NewMessage(chats=channels),
            )
            logger.info("Subscribed to %d channels", len(channels))
        else:
            logger.warning("No channels configured — nothing to monitor")

        # Start periodic config reload
        self._reload_task = asyncio.create_task(self._periodic_reload())

    async def stop(self) -> None:
        """Disconnect from Telegram and stop background tasks."""
        self._running = False
        if self._reload_task is not None:
            self._reload_task.cancel()
            try:
                await self._reload_task
            except asyncio.CancelledError:
                pass
        await self._client.disconnect()
        logger.info("Worker_TG stopped")

    async def reload_config(self) -> None:
        """Hot-reload channels and keywords from the config file."""
        if not self._config_loader.reload_if_changed():
            return

        config = self._config_loader.config

        # Remove existing handlers and re-register with new channels
        self._client.remove_event_handler(self._on_new_message)
        if config.channels:
            self._client.add_event_handler(
                self._on_new_message,
                events.NewMessage(chats=config.channels),
            )
            logger.info(
                "Reloaded config: %d channels, %d keywords",
                len(config.channels),
                len(config.keywords),
            )

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        """Handle an incoming Telegram message.

        Extracts fields, filters by keywords, and sends matching
        messages to the dedup service.
        """
        message = event.message
        text = message.text or ""
        config = self._config_loader.config

        # Extract source info
        chat = await event.get_chat()
        channel_name = getattr(chat, "username", None) or str(chat.id)

        # Filter by keywords
        matched = filter_message(text, config.keywords)

        logger.info(
            "Message processed: source=%s timestamp=%s relevant=%s",
            channel_name,
            message.date.isoformat() if message.date else "unknown",
            bool(matched),
        )

        if not matched:
            return

        # Build lead candidate
        candidate = LeadCandidate(
            source="telegram",
            title=text[:500],
            description=text if len(text) > 500 else None,
            url=None,
            matched_keywords=matched,
            tags=[],
            discovered_at=message.date or datetime.now(timezone.utc),
            message_id=message.id,
        )

        try:
            result = await self._dedup.check_and_store(candidate)
            if result.is_duplicate:
                logger.info(
                    "Lead rejected (duplicate): channel=%s match_type=%s",
                    channel_name,
                    result.match_type,
                )
            else:
                logger.info("Lead created from channel=%s", channel_name)
        except Exception:
            logger.exception("Error storing lead from channel=%s", channel_name)

    async def _periodic_reload(self) -> None:
        """Periodically check for config file changes."""
        while self._running:
            try:
                await asyncio.sleep(self._config_reload_interval)
                await self.reload_config()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error during config reload")
