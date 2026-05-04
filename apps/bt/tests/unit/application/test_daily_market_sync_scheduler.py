from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from src.application.services.daily_market_sync_scheduler import (
    is_jquants_trading_day,
    parse_schedule_time_jst,
    seconds_until_next_schedule,
)


class FakeJQuantsClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, object] | None, int]] = []

    @property
    def has_api_key(self) -> bool:
        return True

    async def get_paginated(
        self,
        path: str,
        params: dict[str, object] | None = None,
        max_pages: int = 10,
    ) -> list[dict[str, object]]:
        self.calls.append((path, params, max_pages))
        return self.rows


def test_parse_schedule_time_jst_accepts_hh_mm() -> None:
    assert parse_schedule_time_jst("16:30") == time(16, 30)


@pytest.mark.parametrize("value", ["1630", "24:00", "aa:bb"])
def test_parse_schedule_time_jst_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_schedule_time_jst(value)


def test_seconds_until_next_schedule_same_day() -> None:
    now = datetime(2026, 5, 1, 15, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert seconds_until_next_schedule(now=now, schedule_time=time(16, 30)) == 90 * 60


def test_seconds_until_next_schedule_rolls_to_next_day() -> None:
    now = datetime(2026, 5, 1, 17, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert seconds_until_next_schedule(now=now, schedule_time=time(16, 30)) == (23 * 60 + 30) * 60


@pytest.mark.asyncio
async def test_is_jquants_trading_day_skips_weekends_without_api_call() -> None:
    client = FakeJQuantsClient(rows=[{"Date": "20260502"}])

    assert await is_jquants_trading_day(client, target_date=date(2026, 5, 2)) is False
    assert client.calls == []


@pytest.mark.asyncio
async def test_is_jquants_trading_day_uses_topix_date_row() -> None:
    client = FakeJQuantsClient(rows=[{"Date": "20260501"}])

    assert await is_jquants_trading_day(client, target_date=date(2026, 5, 1)) is True
    assert client.calls == [
        (
            "/indices/bars/daily/topix",
            {"date": "20260501"},
            1,
        )
    ]


@pytest.mark.asyncio
async def test_is_jquants_trading_day_requires_matching_date() -> None:
    client = FakeJQuantsClient(rows=[{"Date": "20260430"}])

    assert await is_jquants_trading_day(client, target_date=date(2026, 5, 1)) is False
