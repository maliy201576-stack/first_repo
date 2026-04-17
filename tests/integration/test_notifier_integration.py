"""Integration test: Notifier → Telegram via Telethon (mocked).

Validates that when a worker generates an ERROR-level event,
the Notifier sends a correctly formatted alert to Saved Messages.
The Telethon client is mocked via AsyncMock.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.notifier.service import Notifier


class TestNotifierTelegramIntegration:
    """Notifier sends correctly formatted messages via Telethon client."""

    async def test_send_error_format(self):
        """send_error produces the expected 🚨 format with service, time, message."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        error = RuntimeError("Database connection timeout")
        await notifier.send_error("worker_tg", error)

        client.send_message.assert_awaited_once()
        args = client.send_message.call_args
        assert args[0][0] == "me"
        text = args[0][1]

        assert "🚨 ERROR в worker_tg" in text
        assert "Время:" in text
        assert "Database connection timeout" in text

    async def test_send_alert_format(self):
        """send_alert produces a ⚠️ format with level and service name."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        await notifier.send_alert("worker_web", "CRITICAL", "Proxy pool exhausted")

        client.send_message.assert_awaited_once()
        args = client.send_message.call_args
        text = args[0][1]

        assert "CRITICAL" in text
        assert "worker_web" in text
        assert "Proxy pool exhausted" in text

    async def test_send_error_does_not_raise_on_failure(self):
        """If the Telethon call fails, send_error swallows the exception."""
        client = AsyncMock()
        client.send_message = AsyncMock(
            side_effect=ConnectionError("Telegram API unreachable")
        )
        notifier = Notifier(client)

        # Must not raise
        await notifier.send_error("api", ValueError("bad request"))

    async def test_multiple_errors_sent_independently(self):
        """Each send_error call produces a separate message."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        await notifier.send_error("worker_tg", RuntimeError("error 1"))
        await notifier.send_error("worker_web", RuntimeError("error 2"))

        assert client.send_message.await_count == 2

        calls = client.send_message.call_args_list
        assert "worker_tg" in calls[0][0][1]
        assert "worker_web" in calls[1][0][1]
