"""Notifier service — sends error alerts to Telegram via Telethon (user account).

Messages are sent to the user's own Saved Messages (chat_id='me'),
so no separate bot is required.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from telethon import TelegramClient

logger = logging.getLogger(__name__)


class Notifier:
    """Sends alert notifications to Telegram Saved Messages via Telethon.

    Args:
        client: An authenticated TelegramClient instance.
    """

    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    async def send_alert(self, service_name: str, level: str, message: str) -> None:
        """Send a generic alert message to Saved Messages.

        Args:
            service_name: Name of the service raising the alert.
            level: Alert level (e.g. ERROR, CRITICAL).
            message: Alert message text.
        """
        text = f"⚠️ {level} в {service_name}\n{message}"
        try:
            await self._client.send_message("me", text)
        except Exception:
            logger.exception("Failed to send alert for %s", service_name)

    async def send_error(self, service_name: str, error: Exception) -> None:
        """Send a formatted error notification to Saved Messages.

        Args:
            service_name: Name of the service where the error occurred.
            error: The exception instance.
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        text = (
            f"🚨 ERROR в {service_name}\n"
            f"Время: {timestamp}\n"
            f"Сообщение: {error!s}"
        )
        try:
            await self._client.send_message("me", text)
        except Exception:
            logger.exception("Failed to send error notification for %s", service_name)
