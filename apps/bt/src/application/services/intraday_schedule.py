"""Intraday minute publish schedule helpers.

This module intentionally avoids using daily SoT tables to determine minute
freshness. The policy is a wall-clock heuristic in JST:

- minute bars are considered publish-ready at 16:45 JST
- before that cutoff, the latest expected date is the previous weekday
- after that cutoff, the latest expected date is today when today is a weekday

Exchange-holiday precision can be added later with a dedicated calendar source.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Literal
from zoneinfo import ZoneInfo

_JST = ZoneInfo("Asia/Tokyo")
_READY_TIME_JST = time(16, 45)
_READY_TIME_LABEL = "16:45"
_CALENDAR_BASIS = "weekday_cutoff"


@dataclass(frozen=True)
class IntradayFreshnessSnapshot:
    status: Literal["idle", "up_to_date", "stale"]
    expected_date: str
    latest_date: str | None
    latest_time: str | None
    last_intraday_sync: str | None
    ready_time_jst: str
    evaluated_at_jst: str
    calendar_basis: str = _CALENDAR_BASIS


def _to_jst(now: datetime | None = None) -> datetime:
    resolved = now or datetime.now(_JST)
    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=_JST)
    return resolved.astimezone(_JST)


def _previous_weekday(target_date: date) -> date:
    current = target_date
    while True:
        current = current.fromordinal(current.toordinal() - 1)
        if current.weekday() < 5:
            return current


def resolve_latest_ready_intraday_date(now: datetime | None = None) -> str:
    current = _to_jst(now)
    current_date = current.date()

    if current_date.weekday() >= 5:
        return _previous_weekday(current_date).isoformat()
    if current.time() < _READY_TIME_JST:
        return _previous_weekday(current_date).isoformat()
    return current_date.isoformat()


def build_intraday_freshness(
    *,
    latest_date: str | None,
    latest_time: str | None,
    last_intraday_sync: str | None,
    now: datetime | None = None,
) -> IntradayFreshnessSnapshot:
    current = _to_jst(now)
    expected_date = resolve_latest_ready_intraday_date(current)

    if latest_date is None and not last_intraday_sync:
        status: Literal["idle", "up_to_date", "stale"] = "idle"
    elif latest_date is not None and latest_date >= expected_date:
        status = "up_to_date"
    else:
        status = "stale"

    return IntradayFreshnessSnapshot(
        status=status,
        expected_date=expected_date,
        latest_date=latest_date,
        latest_time=latest_time,
        last_intraday_sync=last_intraday_sync,
        ready_time_jst=_READY_TIME_LABEL,
        evaluated_at_jst=current.isoformat(),
    )
