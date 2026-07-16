"""Stable project resource paths for relocated cutover modules."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from src.infrastructure.db.market.managed_root import CutoverSafetyError


@lru_cache(maxsize=1)
def bt_project_root() -> Path:
    """Resolve the bt project root by repository markers, not module depth."""

    for candidate in Path(__file__).resolve().parents:
        if (
            (candidate / "pyproject.toml").is_file()
            and (candidate / "src").is_dir()
            and (candidate / "config").is_dir()
        ):
            return candidate
    raise CutoverSafetyError("Could not resolve the bt project resource root")


def repository_root() -> Path:
    """Return the repository root that owns the bt project."""

    root = bt_project_root().parent.parent
    if not (root / ".git").exists():
        raise CutoverSafetyError("Could not resolve the repository root")
    return root


def repository_default_config_path() -> Path:
    """Return the repository-owned default configuration path."""

    return bt_project_root() / "config" / "default.yaml"
