"""Telegram client factory with built-in API credential fallbacks.

Supports two session modes:
1. StringSession (preferred) — session stored as env var TG_SESSION_STRING,
   no file needed, survives container rebuilds.
2. File session (legacy) — session stored as a file on disk.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelegramAPI:
    """API credentials and device fingerprint for a Telegram client."""

    api_id: int
    api_hash: str
    device_model: str
    system_version: str
    app_version: str
    lang_code: str = "en"
    system_lang_code: str = "en-US"


# Official open-source client credentials (publicly available in source code).
# Source: https://github.com/telegramdesktop/tdesktop (GPLv3)
TDESKTOP_API = TelegramAPI(
    api_id=2040,
    api_hash="b18441a1ff607e10a989891a5462e627",
    device_model="Desktop",
    system_version="Windows 10",
    app_version="5.8.3 x64",
)

# Default fallback — TDesktop is the safest choice for server-side usage.
DEFAULT_API = TDESKTOP_API


def get_api_credentials(api_id: int, api_hash: str) -> TelegramAPI:
    """Resolve API credentials, falling back to built-in official ones.

    If the user has not provided their own TG_API_ID / TG_API_HASH
    (i.e. they are 0 / empty), returns credentials from the official
    open-source Telegram Desktop client.

    Args:
        api_id: User-provided API ID (0 means not set).
        api_hash: User-provided API hash (empty means not set).

    Returns:
        A TelegramAPI with valid credentials.
    """
    if api_id and api_hash:
        return TelegramAPI(
            api_id=api_id,
            api_hash=api_hash,
            device_model=DEFAULT_API.device_model,
            system_version=DEFAULT_API.system_version,
            app_version=DEFAULT_API.app_version,
        )

    logger.info(
        "TG_API_ID/TG_API_HASH not set — using built-in TDesktop credentials "
        "(api_id=%d)",
        DEFAULT_API.api_id,
    )
    return DEFAULT_API


def create_telegram_client(
    session_name: str,
    api_id: int,
    api_hash: str,
    session_string: str = "",
) -> TelegramClient:
    """Create a TelegramClient with API credential fallback.

    When *session_string* is provided, uses Telethon's StringSession
    (no file on disk). Otherwise falls back to file-based session.

    Args:
        session_name: Telethon session file name (used when session_string is empty).
        api_id: Telegram API ID (0 = use built-in).
        api_hash: Telegram API hash (empty = use built-in).
        session_string: Telethon StringSession string (preferred over file).

    Returns:
        A configured TelegramClient instance (not yet connected).
    """
    api = get_api_credentials(api_id, api_hash)

    if session_string:
        logger.info("Using StringSession from TG_SESSION_STRING env var")
        session = StringSession(session_string)
    else:
        logger.info("Using file-based session: %s", session_name)
        session = session_name

    return TelegramClient(
        session,
        api.api_id,
        api.api_hash,
        device_model=api.device_model,
        system_version=api.system_version,
        app_version=api.app_version,
        lang_code=api.lang_code,
        system_lang_code=api.system_lang_code,
    )
