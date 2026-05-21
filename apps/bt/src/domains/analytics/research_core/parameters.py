"""Parameter and date helpers for internal analytics research."""

from __future__ import annotations

import pandas as pd


def normalize_positive_int_sequence(
    values: tuple[int, ...] | list[int] | None,
    *,
    fallback: tuple[int, ...],
    name: str,
    non_positive: str = "raise",
) -> tuple[int, ...]:
    """Return sorted unique positive integers for horizon/window parameters."""
    raw_values = fallback if values is None else tuple(values)
    normalized: list[int] = []
    for raw_value in raw_values:
        value = int(raw_value)
        if value <= 0:
            if non_positive == "filter":
                continue
            raise ValueError(f"{name} values must be positive")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        if non_positive == "filter":
            raise ValueError(f"{name} must contain at least one positive integer")
        raise ValueError(f"at least one {name} value is required")
    return tuple(sorted(normalized))


def warmup_start_date(
    analysis_start_date: str | None,
    available_start_date: str | None,
    *,
    warmup_sessions: int,
    session_to_calendar_multiplier: float,
    padding_days: int = 30,
) -> str | None:
    """Estimate a raw-data start date that preserves rolling-window warmup rows."""
    if analysis_start_date is None:
        return available_start_date
    warmup_days = int(warmup_sessions * session_to_calendar_multiplier) + int(padding_days)
    candidate = (pd.Timestamp(analysis_start_date) - pd.Timedelta(days=warmup_days)).strftime(
        "%Y-%m-%d"
    )
    if available_start_date is None:
        return candidate
    return max(available_start_date, candidate)
