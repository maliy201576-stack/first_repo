"""One-time interactive Telegram authorization script.

Authorizes with Telegram, saves the StringSession directly into .env,
so that worker_tg picks it up automatically on next start.

Usage (via Docker Compose):
    docker compose run --rm tg-auth

Usage (locally):
    python scripts/tg_auth.py
"""

from __future__ import annotations

import asyncio
import os
import re
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telethon import TelegramClient
from telethon.sessions import StringSession

from src.common.telegram import get_api_credentials


def _resolve_env_path() -> str:
    """Return the path to .env — works both locally and inside a container."""
    # Inside container, .env is mounted at /app/.env
    if os.path.exists("/app/.env"):
        return "/app/.env"
    # Locally — project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, ".env")


def _update_env_file(env_path: str, session_string: str) -> None:
    """Write TG_SESSION_STRING into the .env file.

    If the key already exists (even if empty), replaces the line.
    Otherwise appends it.
    """
    if not os.path.exists(env_path):
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(f"TG_SESSION_STRING={session_string}\n")
        return

    with open(env_path, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = r"^TG_SESSION_STRING=.*$"
    replacement = f"TG_SESSION_STRING={session_string}"

    if re.search(pattern, content, flags=re.MULTILINE):
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        content = content.rstrip("\n") + f"\n{replacement}\n"

    with open(env_path, "w", encoding="utf-8") as f:
        f.write(content)


async def main() -> None:
    """Interactively authorize with Telegram and save session to .env."""
    api_id = int(os.getenv("TG_API_ID", "0"))
    api_hash = os.getenv("TG_API_HASH", "")
    api = get_api_credentials(api_id, api_hash)

    print(f"Using API ID: {api.api_id}")
    print("Connecting to Telegram...\n")

    client = TelegramClient(
        StringSession(),
        api.api_id,
        api.api_hash,
        device_model=api.device_model,
        system_version=api.system_version,
        app_version=api.app_version,
    )

    await client.start()
    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()

    print(f"\n✓ Authorized as: {me.first_name} (@{me.username or 'N/A'})")

    # Save to .env
    env_path = _resolve_env_path()
    _update_env_file(env_path, session_string)
    print(f"✓ TG_SESSION_STRING saved to {env_path}")
    print("\nТеперь запусти: docker compose up -d")


if __name__ == "__main__":
    asyncio.run(main())
