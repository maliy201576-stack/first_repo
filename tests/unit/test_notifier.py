"""Unit tests for the Notifier service and its integration with workers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.notifier.service import Notifier


# ---------------------------------------------------------------------------
# Notifier service tests
# ---------------------------------------------------------------------------


class TestNotifierSendAlert:
    @pytest.mark.asyncio
    async def test_send_alert_calls_client(self) -> None:
        """send_alert sends a formatted message via Telethon client."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        await notifier.send_alert("worker_tg", "ERROR", "Something broke")

        client.send_message.assert_awaited_once()
        args = client.send_message.call_args
        assert args[0][0] == "me"
        text = args[0][1]
        assert "ERROR" in text
        assert "worker_tg" in text

    @pytest.mark.asyncio
    async def test_send_alert_handles_failure_gracefully(self) -> None:
        """If Telethon fails, send_alert logs but does not raise."""
        client = AsyncMock()
        client.send_message = AsyncMock(side_effect=RuntimeError("network error"))
        notifier = Notifier(client)

        # Should not raise
        await notifier.send_alert("worker_web", "CRITICAL", "Proxy pool empty")


class TestNotifierSendError:
    @pytest.mark.asyncio
    async def test_send_error_formats_correctly(self) -> None:
        """send_error formats the error with emoji, service name, timestamp, and message."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        error = ValueError("invalid input data")
        await notifier.send_error("worker_tg", error)

        client.send_message.assert_awaited_once()
        args = client.send_message.call_args
        text = args[0][1]
        assert "🚨 ERROR в worker_tg" in text
        assert "Время:" in text
        assert "invalid input data" in text

    @pytest.mark.asyncio
    async def test_send_error_handles_failure_gracefully(self) -> None:
        """If Telethon fails, send_error logs but does not raise."""
        client = AsyncMock()
        client.send_message = AsyncMock(side_effect=RuntimeError("timeout"))
        notifier = Notifier(client)

        # Should not raise
        await notifier.send_error("worker_web", RuntimeError("scrape failed"))

    @pytest.mark.asyncio
    async def test_send_error_sends_to_saved_messages(self) -> None:
        """send_error sends to 'me' (Saved Messages)."""
        client = AsyncMock()
        client.send_message = AsyncMock()
        notifier = Notifier(client)

        await notifier.send_error("api", Exception("db down"))

        args = client.send_message.call_args
        assert args[0][0] == "me"


# ---------------------------------------------------------------------------
# Worker_TG notifier integration tests
# ---------------------------------------------------------------------------


class TestWorkerTGNotifierIntegration:
    @pytest.mark.asyncio
    async def test_notifier_called_on_lead_store_error(self) -> None:
        """When dedup.check_and_store raises, Worker_TG calls notifier.send_error."""
        from src.worker_tg.worker import WorkerTG
        from src.worker_tg.config_loader import ConfigLoader, ChannelsConfig

        client = AsyncMock()
        dedup = AsyncMock()
        dedup.check_and_store = AsyncMock(side_effect=RuntimeError("db connection lost"))

        config_loader = MagicMock(spec=ConfigLoader)
        config_loader.config = ChannelsConfig(
            channels=["@test_channel"],
            keywords=["python"],
        )

        notifier = AsyncMock()
        notifier.send_error = AsyncMock()

        worker = WorkerTG(
            client=client,
            dedup_service=dedup,
            config_loader=config_loader,
            notifier=notifier,
        )

        event = AsyncMock()
        event.message.text = "Looking for python developer"
        event.message.date = datetime(2024, 6, 10, tzinfo=timezone.utc)
        event.message.id = 123
        event.message.sender = True

        chat = MagicMock()
        chat.username = "test_channel"
        event.get_chat = AsyncMock(return_value=chat)

        sender = MagicMock()
        sender.username = "user1"
        event.message.get_sender = AsyncMock(return_value=sender)

        await worker._on_new_message(event)

        notifier.send_error.assert_awaited_once()
        args = notifier.send_error.call_args[0]
        assert args[0] == "worker_tg"
        assert isinstance(args[1], RuntimeError)

    @pytest.mark.asyncio
    async def test_no_notifier_no_crash(self) -> None:
        """When notifier is None, Worker_TG still handles errors without crashing."""
        from src.worker_tg.worker import WorkerTG
        from src.worker_tg.config_loader import ConfigLoader, ChannelsConfig

        client = AsyncMock()
        dedup = AsyncMock()
        dedup.check_and_store = AsyncMock(side_effect=RuntimeError("db error"))

        config_loader = MagicMock(spec=ConfigLoader)
        config_loader.config = ChannelsConfig(
            channels=["@test_channel"],
            keywords=["python"],
        )

        worker = WorkerTG(
            client=client,
            dedup_service=dedup,
            config_loader=config_loader,
            notifier=None,
        )

        event = AsyncMock()
        event.message.text = "Need python dev"
        event.message.date = datetime(2024, 6, 10, tzinfo=timezone.utc)
        event.message.id = 456
        event.message.sender = True

        chat = MagicMock()
        chat.username = "test_channel"
        event.get_chat = AsyncMock(return_value=chat)

        sender = MagicMock()
        sender.username = "user2"
        event.message.get_sender = AsyncMock(return_value=sender)

        await worker._on_new_message(event)


# ---------------------------------------------------------------------------
# Worker_Web notifier integration tests
# ---------------------------------------------------------------------------


class TestWorkerWebNotifierIntegration:
    @pytest.mark.asyncio
    async def test_notifier_called_on_scrape_error(self) -> None:
        """When _scrape_source hits a general exception, notifier.send_error is called."""
        from src.worker_web.worker import WorkerWeb

        dedup = AsyncMock()
        pool = AsyncMock()
        pool.get_next = AsyncMock(return_value="http://proxy1:8080")
        pool.mark_blocked = AsyncMock()

        page = AsyncMock()
        page.close = AsyncMock()
        page.set_default_timeout = MagicMock()

        context = AsyncMock()
        context.new_page = AsyncMock(return_value=page)
        context.close = AsyncMock()

        browser = AsyncMock()
        browser.new_context = AsyncMock(return_value=context)
        browser.close = AsyncMock()

        factory = AsyncMock(return_value=browser)

        notifier = AsyncMock()
        notifier.send_error = AsyncMock()

        worker = WorkerWeb(dedup, pool, factory, notifier=notifier)
        worker._browser = browser

        worker._fl_parser.parse = AsyncMock(side_effect=RuntimeError("unexpected error"))

        result = await worker.scrape_fl_ru()

        assert result == []
        notifier.send_error.assert_awaited_once()
        args = notifier.send_error.call_args[0]
        assert args[0] == "worker_web"
        assert isinstance(args[1], RuntimeError)

    @pytest.mark.asyncio
    async def test_no_notifier_no_crash_on_scrape_error(self) -> None:
        """When notifier is None, Worker_Web still handles errors without crashing."""
        from src.worker_web.worker import WorkerWeb

        dedup = AsyncMock()
        pool = AsyncMock()
        pool.get_next = AsyncMock(return_value="http://proxy1:8080")

        page = AsyncMock()
        page.close = AsyncMock()
        page.set_default_timeout = MagicMock()

        context = AsyncMock()
        context.new_page = AsyncMock(return_value=page)
        context.close = AsyncMock()

        browser = AsyncMock()
        browser.new_context = AsyncMock(return_value=context)
        browser.close = AsyncMock()

        factory = AsyncMock(return_value=browser)

        worker = WorkerWeb(dedup, pool, factory, notifier=None)
        worker._browser = browser
        worker._fl_parser.parse = AsyncMock(side_effect=RuntimeError("boom"))

        result = await worker.scrape_fl_ru()
        assert result == []
