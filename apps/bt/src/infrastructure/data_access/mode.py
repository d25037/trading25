"""Data access mode control for loader paths.

This module allows switching between HTTP-based access and direct DB access
without changing call sites. Backtest jobs can set mode="direct" to avoid
FastAPI self-calls.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Literal

DataAccessMode = Literal["http", "direct"]
DATA_ACCESS_MODE_ENV = "BT_DATA_ACCESS_MODE"

_mode_ctx: ContextVar[DataAccessMode | None] = ContextVar(
    "bt_data_access_mode", default=None
)


def normalize_data_access_mode(mode: str | None) -> DataAccessMode:
    """Normalize user/input mode into supported values."""
    if mode and mode.lower() == "direct":
        return "direct"
    return "http"


def get_data_access_mode() -> DataAccessMode:
    """Resolve current access mode (contextvar first, then env, then default)."""
    ctx_mode = _mode_ctx.get()
    if ctx_mode is not None:
        return ctx_mode
    return normalize_data_access_mode(os.getenv(DATA_ACCESS_MODE_ENV))


def should_use_direct_db() -> bool:
    """True when current mode is direct DB access."""
    return get_data_access_mode() == "direct"


@contextmanager
def data_access_mode_context(mode: str | None) -> Iterator[None]:
    """Temporarily override access mode within current context."""
    if mode is None:
        yield
        return

    token = _mode_ctx.set(normalize_data_access_mode(mode))
    try:
        yield
    finally:
        _mode_ctx.reset(token)
