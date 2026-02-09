"""Compatibility wrapper for strategy config loader."""

from typing import Any


class ConfigLoader:  # pragma: no cover - thin delegation wrapper
    """Delegate to `src.strategy_config.loader.ConfigLoader` at runtime."""

    def __new__(cls, *args: Any, **kwargs: Any):
        from src.strategy_config.loader import ConfigLoader as LegacyConfigLoader

        return LegacyConfigLoader(*args, **kwargs)


__all__ = ["ConfigLoader"]
