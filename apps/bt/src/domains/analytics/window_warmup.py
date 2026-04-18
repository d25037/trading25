"""Helpers for indicator warmup-aware research windowing."""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

_LOOKBACK_KEYWORDS = ("window", "period", "lookback")
_EXCLUDED_KEYS = {"period_type"}


def estimate_strategy_indicator_warmup_calendar_days(
    parameters: dict[str, Any],
) -> int:
    """Estimate calendar-day preload needed to stabilize indicator lookbacks."""
    candidates: list[int] = []
    for section_name in ("entry_filter_params", "exit_trigger_params"):
        _collect_indicator_period_candidates(parameters.get(section_name), candidates)

    if not candidates:
        return 0

    max_period = max(candidates)
    if max_period <= 1:
        return 0

    # Strategy periods are mostly trading-day based; convert conservatively to calendar days.
    return int(math.ceil(max_period * 1.5)) + 5


def resolve_window_load_start_date(
    *,
    dataset_start_date: str,
    window_start_date: str,
    warmup_calendar_days: int,
) -> str:
    """Clamp warmup-aware load start to dataset bounds."""
    dataset_start = date.fromisoformat(dataset_start_date)
    window_start = date.fromisoformat(window_start_date)
    if warmup_calendar_days <= 0 or window_start <= dataset_start:
        return window_start_date

    warmup_start = window_start - timedelta(days=warmup_calendar_days)
    if warmup_start < dataset_start:
        warmup_start = dataset_start
    return warmup_start.isoformat()


def _collect_indicator_period_candidates(value: Any, candidates: list[int]) -> None:
    if value is None:
        return

    if hasattr(value, "model_dump"):
        _collect_indicator_period_candidates(value.model_dump(mode="python"), candidates)
        return

    if isinstance(value, dict):
        for key, nested in value.items():
            if (
                isinstance(nested, int)
                and not isinstance(nested, bool)
                and key not in _EXCLUDED_KEYS
                and any(keyword in key for keyword in _LOOKBACK_KEYWORDS)
            ):
                candidates.append(nested)
                continue
            _collect_indicator_period_candidates(nested, candidates)

