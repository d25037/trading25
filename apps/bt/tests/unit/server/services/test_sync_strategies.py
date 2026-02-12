from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.lib.market_db.market_db import METADATA_KEYS
from src.server.services.sync_strategies import IncrementalSyncStrategy, SyncContext


class DummyMarketDb:
    def __init__(
        self,
        latest_trading_date: str = "20260206",
        latest_stock_data_date: str | None = None,
    ) -> None:
        self.latest_trading_date = latest_trading_date
        self.latest_stock_data_date = latest_stock_data_date or latest_trading_date
        self.stock_rows: list[dict[str, Any]] = []
        self.topix_rows: list[dict[str, Any]] = []
        self.metadata: dict[str, str] = {}

    def get_sync_metadata(self, key: str) -> str | None:
        if key == METADATA_KEYS["LAST_SYNC_DATE"]:
            return "2026-02-06T00:00:00+00:00"
        return None

    def get_latest_trading_date(self) -> str | None:
        return self.latest_trading_date

    def get_latest_stock_data_date(self) -> str | None:
        return self.latest_stock_data_date

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        self.topix_rows.extend(rows)
        return len(rows)

    def upsert_stocks(self, _rows: list[dict[str, Any]]) -> int:
        return 0

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        self.stock_rows.extend(rows)
        return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value


class DummyClient:
    def __init__(self, daily_quotes: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.daily_quotes = daily_quotes

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/indices/bars/daily/topix":
            if params and params.get("from") == "20260210":
                return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102}]
            return [
                {"Date": "2026-02-06", "O": 100, "H": 101, "L": 99, "C": 100},
                {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102},
            ]
        if path == "/equities/master":
            return []
        if path == "/equities/bars/daily":
            if self.daily_quotes is not None:
                rows: list[dict[str, Any]] = []
                for quote in self.daily_quotes:
                    row = dict(quote)
                    if "Date" not in row:
                        row["Date"] = params["date"] if params else ""
                    rows.append(row)
                return rows
            date_value = params["date"] if params else ""
            return [{"Code": "72030", "Date": date_value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}]
        return []


@pytest.mark.asyncio
async def test_incremental_sync_handles_mixed_date_formats() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient()

    progresses: list[tuple[str, int, int, str]] = []

    def on_progress(stage: str, current: int, total: int, message: str) -> None:
        progresses.append((stage, current, total, message))

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=on_progress,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 1
    assert result.stocksUpdated == 1
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-10"} for path, params in client.calls)

    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {"from": "20260206"}

    assert market_db.metadata.get(METADATA_KEYS["LAST_SYNC_DATE"])
    assert progresses[-1][0] == "complete"


@pytest.mark.asyncio
async def test_incremental_sync_uses_stock_data_anchor_when_topix_is_ahead() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260210", latest_stock_data_date="20260206")
    client = DummyClient()

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 1
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-10"} for path, params in client.calls)

    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    # topix_data ではなく stock_data の最新日（2026-02-06）を基準に差分取得する
    assert topix_calls[0][1] == {"from": "20260206"}


@pytest.mark.asyncio
async def test_incremental_sync_skips_rows_with_missing_ohlcv() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        daily_quotes=[
            {"Code": "131A0", "O": None, "H": None, "L": None, "C": None, "Vo": None, "AdjFactor": 1.0},
            {"Code": "72030", "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000, "AdjFactor": 1.0},
        ]
    )

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert result.stocksUpdated == 1
    assert len(market_db.stock_rows) == 1
    assert market_db.stock_rows[0]["code"] == "7203"
