"""Share-based normalization helpers for per-share metrics."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from src.shared.models.types import normalize_period_type

_QUARTERLY_PERIOD_TYPES = frozenset({"1Q", "2Q", "3Q"})


@dataclass(frozen=True)
class ShareAdjustmentEvent:
    date: str
    adjustment_factor: float


@dataclass(frozen=True)
class ShareCountSnapshot:
    period_type: str | None
    disclosed_date: str
    shares: float


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
    snapshot = resolve_latest_quarterly_share_snapshot(snapshots)
    return snapshot.shares if snapshot is not None else None


def resolve_latest_quarterly_share_snapshot(
    snapshots: Iterable[tuple[Any, Any, float | int | None]],
) -> ShareCountSnapshot | None:
    """Resolve latest share snapshot, preferring quarterly disclosures."""
    latest_quarter_key: str | None = None
    latest_quarter_snapshot: ShareCountSnapshot | None = None
    latest_any_key: str | None = None
    latest_any_snapshot: ShareCountSnapshot | None = None

    for period_type, disclosed_date, shares in snapshots:
        if not is_valid_share_count(shares):
            continue

        normalized_period = normalize_period_type(period_type)
        disclosed_key = str(disclosed_date) if disclosed_date is not None else ""
        share_value = float(cast(float | int, shares))
        snapshot = ShareCountSnapshot(
            period_type=normalized_period,
            disclosed_date=disclosed_key,
            shares=share_value,
        )

        if normalized_period in _QUARTERLY_PERIOD_TYPES:
            if latest_quarter_key is None or disclosed_key > latest_quarter_key:
                latest_quarter_key = disclosed_key
                latest_quarter_snapshot = snapshot

        if latest_any_key is None or disclosed_key > latest_any_key:
            latest_any_key = disclosed_key
            latest_any_snapshot = snapshot

    return latest_quarter_snapshot if latest_quarter_snapshot is not None else latest_any_snapshot


def cumulative_adjustment_factor_after(
    events: Iterable[ShareAdjustmentEvent],
    *,
    from_date: str | None,
    through_date: str | None,
) -> float:
    """Return cumulative price adjustment factor in (from_date, through_date]."""
    factor = 1.0
    from_key = str(from_date) if from_date is not None else ""
    through_key = str(through_date) if through_date is not None else None
    for event in events:
        event_date = str(event.date)
        if from_key and event_date <= from_key:
            continue
        if through_key is not None and event_date > through_key:
            continue
        adjustment_factor = float(event.adjustment_factor)
        if not math.isfinite(adjustment_factor) or adjustment_factor <= 0:
            continue
        factor *= adjustment_factor
    return factor


def adjust_share_count_to_price_basis(
    shares: float | int | None,
    events: Iterable[ShareAdjustmentEvent],
    *,
    from_date: str | None,
    through_date: str | None,
    allow_zero: bool = False,
) -> float | None:
    """Adjust a disclosed share count onto the adjusted-price basis for through_date."""
    if shares is None:
        return None
    try:
        share_value = float(shares)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(share_value):
        return None
    if share_value == 0:
        return 0.0 if allow_zero else None
    if share_value < 0:
        return None

    factor = cumulative_adjustment_factor_after(
        events,
        from_date=from_date,
        through_date=through_date,
    )
    if not math.isfinite(factor) or factor <= 0:
        return share_value
    return share_value / factor
