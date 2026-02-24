from __future__ import annotations

from typing import Any

import pytest

from src.application.services.stock_refresh_service import refresh_stocks


class DummyMarketDb:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.metadata: dict[str, str] = {}

    def get_topix_date_range(self) -> dict[str, str]:
        return {"min": "2026-02-01", "max": "2026-02-28"}

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        self.rows.extend(rows)
        return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value


class DummyJQuantsClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    async def get_paginated(self, path: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        return self.rows


class DummyFailingJQuantsClient:
    async def get_paginated(self, _path: str, _params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        raise RuntimeError("network error")


@pytest.mark.asyncio
async def test_refresh_stocks_skips_incomplete_ohlcv_rows() -> None:
    market_db = DummyMarketDb()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "131A0",
                "Date": "2026-02-10",
                "O": None,
                "H": None,
                "L": None,
                "C": None,
                "Vo": None,
                "AdjFactor": 1.0,
            },
            {
                "Code": "131A0",
                "Date": "2026-02-10",
                "O": 100.0,
                "H": 102.0,
                "L": 99.0,
                "C": 101.0,
                "Vo": 12345,
                "AdjFactor": 1.0,
            },
        ]
    )

    result = await refresh_stocks(["131A"], market_db, client)  # type: ignore[arg-type]

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert len(market_db.rows) == 1
    assert market_db.rows[0]["code"] == "131A"
    assert market_db.rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_refresh_stocks_applies_topix_date_range_filter() -> None:
    market_db = DummyMarketDb()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-01-31", "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 100},
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
            {"Code": "72030", "Date": "2026-03-01", "O": 20, "H": 22, "L": 19, "C": 21, "Vo": 2000},
        ]
    )

    result = await refresh_stocks(["7203"], market_db, client)  # type: ignore[arg-type]

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert len(market_db.rows) == 1
    assert market_db.rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_refresh_stocks_handles_jquants_error() -> None:
    market_db = DummyMarketDb()

    result = await refresh_stocks(["7203"], market_db, DummyFailingJQuantsClient())  # type: ignore[arg-type]

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.totalRecordsStored == 0
    assert len(result.errors) == 1
