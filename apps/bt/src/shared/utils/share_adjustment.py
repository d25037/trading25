"""Share-based normalization helpers for per-share metrics."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any, cast

from src.shared.models.types import normalize_period_type

_QUARTERLY_PERIOD_TYPES = frozenset({"1Q", "2Q", "3Q"})


def is_valid_share_count(value: float | int | None) -> bool:
    """Return True when share count can be used for per-share adjustment."""
    if value is None:
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    if number == 0:
        return False
    return math.isfinite(number)


def resolve_latest_quarterly_baseline_shares(
    snapshots: Iterable[tuple[Any, Any, float | int | None]],
) -> float | None:
    """Resolve baseline shares from latest quarterly disclosure, then fallback to latest any."""
    latest_quarter_key: str | None = None
    latest_quarter_shares: float | None = None
    latest_any_key: str | None = None
    latest_any_shares: float | None = None

    for period_type, disclosed_date, shares in snapshots:
        if not is_valid_share_count(shares):
            continue

        normalized_period = normalize_period_type(period_type)
        disclosed_key = str(disclosed_date) if disclosed_date is not None else ""
        share_value = float(cast(float | int, shares))

        if normalized_period in _QUARTERLY_PERIOD_TYPES:
            if latest_quarter_key is None or disclosed_key > latest_quarter_key:
                latest_quarter_key = disclosed_key
                latest_quarter_shares = share_value

        if latest_any_key is None or disclosed_key > latest_any_key:
            latest_any_key = disclosed_key
            latest_any_shares = share_value

    return latest_quarter_shares if latest_quarter_shares is not None else latest_any_shares
