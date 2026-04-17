"""Hot-reloadable YAML configuration loader for Worker_TG.

Loads channels and keywords from a YAML file and supports
reloading without restarting the service.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ChannelsConfig:
    """Parsed channels configuration."""

    channels: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)


class ConfigLoader:
    """Loads and hot-reloads ``channels.yaml``.

    Args:
        config_path: Path to the YAML configuration file.
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._config_path = config_path or os.getenv(
            "TG_CHANNELS_CONFIG", "/config/channels.yaml"
        )
        self._config = ChannelsConfig()
        self._mtime: float = 0.0

    @property
    def config(self) -> ChannelsConfig:
        """Current configuration snapshot."""
        return self._config

    def load(self) -> ChannelsConfig:
        """Load (or reload) the configuration from disk.

        Returns:
            The parsed :class:`ChannelsConfig`.

        Raises:
            FileNotFoundError: If the config file does not exist.
        """
        path = Path(self._config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {self._config_path}")

        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

        channels = raw.get("channels", [])
        keywords = raw.get("keywords", [])

        if not isinstance(channels, list):
            channels = []
        if not isinstance(keywords, list):
            keywords = []

        self._config = ChannelsConfig(
            channels=[str(ch) for ch in channels],
            keywords=[str(kw) for kw in keywords],
        )
        self._mtime = path.stat().st_mtime

        logger.info(
            "Config loaded: %d channels, %d keywords from %s",
            len(self._config.channels),
            len(self._config.keywords),
            self._config_path,
        )
        return self._config

    def reload_if_changed(self) -> bool:
        """Reload configuration only if the file has been modified.

        Returns:
            ``True`` if the config was reloaded, ``False`` otherwise.
        """
        path = Path(self._config_path)
        if not path.exists():
            return False

        current_mtime = path.stat().st_mtime
        if current_mtime > self._mtime:
            self.load()
            logger.info("Config hot-reloaded from %s", self._config_path)
            return True
        return False
