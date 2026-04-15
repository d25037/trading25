from __future__ import annotations

from datetime import datetime, timezone

from src.application.services.intraday_schedule import (
    build_intraday_freshness,
    resolve_latest_ready_intraday_date,
)


def test_resolve_latest_ready_intraday_date_uses_today_after_cutoff() -> None:
    now = datetime(2026, 4, 15, 16, 45)
    assert resolve_latest_ready_intraday_date(now) == "2026-04-15"


def test_resolve_latest_ready_intraday_date_uses_previous_weekday_before_cutoff() -> None:
    now = datetime(2026, 4, 15, 16, 44)
    assert resolve_latest_ready_intraday_date(now) == "2026-04-14"


def test_resolve_latest_ready_intraday_date_rolls_back_from_monday() -> None:
    now = datetime(2026, 4, 13, 9, 0)
    assert resolve_latest_ready_intraday_date(now) == "2026-04-10"


def test_resolve_latest_ready_intraday_date_handles_weekend() -> None:
    now = datetime(2026, 4, 12, 12, 0)
    assert resolve_latest_ready_intraday_date(now) == "2026-04-10"


def test_resolve_latest_ready_intraday_date_converts_utc_to_jst() -> None:
    now = datetime(2026, 4, 15, 7, 45, tzinfo=timezone.utc)
    assert resolve_latest_ready_intraday_date(now) == "2026-04-15"


def test_build_intraday_freshness_returns_idle_without_local_state() -> None:
    freshness = build_intraday_freshness(
        latest_date=None,
        latest_time=None,
        last_intraday_sync=None,
        now=datetime(2026, 4, 15, 12, 0),
    )

    assert freshness.status == "idle"
    assert freshness.expected_date == "2026-04-14"
    assert freshness.latest_date is None


def test_build_intraday_freshness_returns_up_to_date_when_latest_matches_expected() -> None:
    freshness = build_intraday_freshness(
        latest_date="2026-04-15",
        latest_time="15:30",
        last_intraday_sync="2026-04-15T16:50:00+09:00",
        now=datetime(2026, 4, 15, 17, 0),
    )

    assert freshness.status == "up_to_date"
    assert freshness.expected_date == "2026-04-15"
    assert freshness.latest_time == "15:30"


def test_build_intraday_freshness_returns_stale_when_expected_date_is_missing() -> None:
    freshness = build_intraday_freshness(
        latest_date="2026-04-14",
        latest_time="15:30",
        last_intraday_sync="2026-04-14T16:50:00+09:00",
        now=datetime(2026, 4, 15, 17, 0),
    )

    assert freshness.status == "stale"
    assert freshness.expected_date == "2026-04-15"
