from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from src.lib.market_db.market_db import METADATA_KEYS
from src.server.services.sync_strategies import (
    IncrementalSyncStrategy,
    IndicesOnlySyncStrategy,
    InitialSyncStrategy,
    SyncContext,
    _build_fallback_index_master_rows,
    _convert_index_master_rows,
    _convert_indices_data_rows,
    _convert_stock_rows,
    _date_sort_key,
    _extract_list_items,
    _is_date_after,
    _parse_date,
    _to_jquants_date_param,
    get_strategy,
)


class DummyMarketDb:
    def __init__(
        self,
        latest_trading_date: str | None = "20260206",
        latest_stock_data_date: str | None = None,
        latest_indices_data_dates: dict[str, str] | None = None,
    ) -> None:
        self.latest_trading_date = latest_trading_date
        self.latest_stock_data_date = latest_stock_data_date or latest_trading_date
        if latest_indices_data_dates is not None:
            self.latest_indices_data_dates = latest_indices_data_dates
        elif latest_trading_date:
            self.latest_indices_data_dates = {"0000": latest_trading_date}
        else:
            self.latest_indices_data_dates = {}
        self.stocks_rows: list[dict[str, Any]] = []
        self.stock_rows: list[dict[str, Any]] = []
        self.topix_rows: list[dict[str, Any]] = []
        self.index_master_rows: list[dict[str, Any]] = []
        self.indices_rows: list[dict[str, Any]] = []
        self.metadata: dict[str, str] = {}

    def get_sync_metadata(self, key: str) -> str | None:
        if key == METADATA_KEYS["LAST_SYNC_DATE"]:
            return "2026-02-06T00:00:00+00:00"
        return None

    def get_latest_trading_date(self) -> str | None:
        return self.latest_trading_date

    def get_latest_stock_data_date(self) -> str | None:
        return self.latest_stock_data_date

    def get_latest_indices_data_dates(self) -> dict[str, str]:
        return dict(self.latest_indices_data_dates)

    def get_index_master_codes(self) -> set[str]:
        return {
            row.get("code")
            for row in self.index_master_rows
            if row.get("code")
        }

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        self.topix_rows.extend(rows)
        return len(rows)

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        self.stocks_rows.extend(rows)
        return len(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        self.stock_rows.extend(rows)
        return len(rows)

    def upsert_index_master(self, rows: list[dict[str, Any]]) -> int:
        self.index_master_rows.extend(rows)
        return len(rows)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        self.indices_rows.extend(rows)
        return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value


class DummyClient:
    def __init__(
        self,
        daily_quotes: list[dict[str, Any]] | None = None,
        indices_quotes: list[dict[str, Any]] | None = None,
        master_quotes: list[dict[str, Any]] | None = None,
        daily_error_dates: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.daily_quotes = daily_quotes
        self.indices_quotes = indices_quotes
        self.master_quotes = master_quotes
        self.daily_error_dates = daily_error_dates or set()

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/indices":
            return {
                "data": [
                    {
                        "code": "0000",
                        "name": "TOPIX",
                        "name_english": "TOPIX",
                        "category": "topix",
                        "data_start_date": "2008-05-07",
                    }
                ]
            }
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/indices/bars/daily/topix":
            if params and params.get("from") == "20260210":
                return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102}]
            return [
                {"Date": "2026-02-06", "O": 100, "H": 101, "L": 99, "C": 100},
                {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102},
            ]
        if path == "/indices/bars/daily":
            if self.indices_quotes is not None:
                return [dict(row) for row in self.indices_quotes]
            if params and params.get("from") == "20260210":
                return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"}]
            return [
                {"Date": "2026-02-06", "O": 100, "H": 101, "L": 99, "C": 100, "SectorName": "TOPIX"},
                {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
            ]
        if path == "/equities/master":
            return self.master_quotes or []
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
            if date_value in self.daily_error_dates:
                raise RuntimeError("daily fetch failed")
            return [{"Code": "72030", "Date": date_value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}]
        return []


class InitialSyncClient:
    def __init__(
        self,
        *,
        topix_dates: list[str],
        fail_stock_dates: set[str] | None = None,
        master_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self._topix_dates = topix_dates
        self._fail_stock_dates = fail_stock_dates or set()
        self._master_rows = master_rows

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/indices":
            return {
                "data": [
                    {
                        "code": "0000",
                        "name": "TOPIX",
                        "name_english": "TOPIX",
                        "category": "topix",
                        "data_start_date": "2008-05-07",
                    }
                ]
            }
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/indices/bars/daily/topix":
            return [{"Date": d, "O": 100, "H": 101, "L": 99, "C": 100} for d in self._topix_dates]
        if path == "/equities/master":
            if self._master_rows is not None:
                return self._master_rows
            return [
                {
                    "Code": "72030",
                    "CoName": "トヨタ自動車",
                    "Mkt": "0111",
                    "MktNm": "プライム",
                    "S17": "6",
                    "S17Nm": "輸送用機器",
                    "S33": "3700",
                    "S33Nm": "輸送用機器",
                    "Date": "1949-05-16",
                },
                {"Code": "", "CoName": "invalid"},
            ]
        if path == "/equities/bars/daily":
            date_value = (params or {}).get("date", "")
            if date_value in self._fail_stock_dates:
                raise RuntimeError("daily quotes unavailable")
            return [{"Code": "72030", "Date": date_value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}]
        if path == "/indices/bars/daily":
            return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"}]
        return []


class IndicesOnlyClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/indices":
            return {
                "data": [
                    {"code": "0000", "name": "TOPIX", "category": "topix"},
                    {"Code": "", "Name": "invalid", "Category": "other"},
                    {"code": "9999", "name": "BROKEN", "category": "other"},
                ]
            }
        raise RuntimeError(f"unexpected path: {path}")

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path != "/indices/bars/daily":
            raise RuntimeError(f"unexpected path: {path}")
        if (params or {}).get("code") == "9999":
            raise RuntimeError("boom")
        return [
            {"Date": "", "O": 1, "H": 2, "L": 1, "C": 2, "SectorName": "TOPIX"},
            {"Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "SectorName": "TOPIX"},
        ]


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
    indices_calls = [c for c in client.calls if c[0] == "/indices/bars/daily"]
    assert indices_calls
    assert indices_calls[0][1] == {"code": "0000", "from": "20260206"}
    assert len(market_db.indices_rows) == 1
    assert market_db.indices_rows[0]["date"] == "2026-02-10"

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


@pytest.mark.asyncio
async def test_incremental_sync_skips_index_rows_with_missing_date() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "", "O": 100, "H": 101, "L": 99, "C": 100, "SectorName": "TOPIX"},
            {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
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
    assert len(market_db.indices_rows) == 1
    assert market_db.indices_rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_incremental_sync_falls_back_to_date_based_indices_when_master_unavailable() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
        ]
    )

    async def _raise_on_indices(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        client.calls.append((path, params))
        if path == "/indices":
            raise RuntimeError("indices master unavailable")
        return {"data": []}

    client.get = _raise_on_indices  # type: ignore[method-assign]

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(path == "/indices/bars/daily" and params == {"date": "20260210"} for path, params in client.calls)
    assert len(market_db.indices_rows) >= 1
    assert all(row["code"] == "0040" for row in market_db.indices_rows)
    assert any(row["date"] == "2026-02-10" for row in market_db.indices_rows)


@pytest.mark.asyncio
async def test_incremental_sync_fallback_inserts_missing_master_for_fk_compatibility() -> None:
    class FkAwareMarketDb(DummyMarketDb):
        def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
            known_codes = self.get_index_master_codes()
            missing_codes = sorted(
                {
                    str(row.get("code"))
                    for row in rows
                    if row.get("code") and row.get("code") not in known_codes
                }
            )
            if missing_codes:
                raise RuntimeError(f"FOREIGN KEY constraint failed for codes: {','.join(missing_codes)}")
            return super().upsert_indices_data(rows)

    market_db = FkAwareMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102},
        ]
    )

    async def _raise_on_indices(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        client.calls.append((path, params))
        if path == "/indices":
            raise RuntimeError("indices master unavailable")
        return {"data": []}

    client.get = _raise_on_indices  # type: ignore[method-assign]

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(row["code"] == "0040" for row in market_db.index_master_rows)
    fallback_master = next(row for row in market_db.index_master_rows if row["code"] == "0040")
    assert fallback_master["category"] == "unknown"
    assert fallback_master["name"] == "0040"
    assert len(market_db.indices_rows) >= 1
    assert all(row["code"] == "0040" for row in market_db.indices_rows)


@pytest.mark.asyncio
async def test_incremental_sync_fallback_rechecks_anchor_date_when_master_unavailable() -> None:
    market_db = DummyMarketDb(
        latest_trading_date="20260210",
        latest_stock_data_date="20260210",
        latest_indices_data_dates={"0000": "20260210"},
    )
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
        ]
    )

    async def _raise_on_indices(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        client.calls.append((path, params))
        if path == "/indices":
            raise RuntimeError("indices master unavailable")
        return {"data": []}

    client.get = _raise_on_indices  # type: ignore[method-assign]

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(path == "/indices/bars/daily" and params == {"date": "20260210"} for path, params in client.calls)
    assert len(market_db.indices_rows) == 1
    assert market_db.indices_rows[0]["code"] == "0040"


@pytest.mark.asyncio
async def test_incremental_sync_requires_last_sync_metadata() -> None:
    market_db = DummyMarketDb()
    market_db.metadata = {}

    def _no_last_sync(key: str) -> str | None:
        return None if key == METADATA_KEYS["LAST_SYNC_DATE"] else market_db.metadata.get(key)

    market_db.get_sync_metadata = _no_last_sync  # type: ignore[method-assign]
    client = DummyClient()

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["No last_sync_date found. Run initial sync first."]


@pytest.mark.asyncio
async def test_incremental_sync_cancelled_before_start() -> None:
    market_db = DummyMarketDb()
    client = DummyClient()
    cancelled = asyncio.Event()
    cancelled.set()

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]


@pytest.mark.asyncio
async def test_incremental_sync_handles_unexpected_topix_exception() -> None:
    market_db = DummyMarketDb()
    client = DummyClient()

    async def _raise(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError("topix fail")

    client.get_paginated = _raise  # type: ignore[method-assign]
    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["topix fail"]


@pytest.mark.asyncio
async def test_indices_only_sync_collects_errors_and_skips_invalid_code() -> None:
    market_db = DummyMarketDb()
    client = IndicesOnlyClient()
    progresses: list[tuple[str, int, int, str]] = []

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda stage, current, total, msg: progresses.append((stage, current, total, msg)),
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert len(result.errors) == 1
    assert "Index 9999" in result.errors[0]
    assert len(market_db.index_master_rows) == 2
    assert len(market_db.indices_rows) == 1
    assert any(stage == "indices_master" for stage, *_ in progresses)
    assert any(stage == "indices_data" for stage, *_ in progresses)


@pytest.mark.asyncio
async def test_indices_only_sync_cancelled_immediately() -> None:
    market_db = DummyMarketDb()
    client = IndicesOnlyClient()
    cancelled = asyncio.Event()
    cancelled.set()
    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert result.totalApiCalls == 0


@pytest.mark.asyncio
async def test_indices_only_sync_cancelled_during_loop() -> None:
    market_db = DummyMarketDb()
    cancelled = asyncio.Event()

    class _Client:
        async def get(self, _path: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {"data": [{"code": "0000"}, {"code": "0001"}]}

        async def get_paginated(self, _path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if (params or {}).get("code") == "0000":
                cancelled.set()
            return [{"Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11}]

    ctx = SyncContext(
        client=_Client(),  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert result.totalApiCalls == 2


@pytest.mark.asyncio
async def test_indices_only_sync_handles_outer_exception() -> None:
    market_db = DummyMarketDb()

    class _Client:
        async def get(self, _path: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            raise RuntimeError("indices master failed")

    ctx = SyncContext(
        client=_Client(),  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["indices master failed"]


@pytest.mark.asyncio
async def test_initial_sync_breaks_after_consecutive_failures_and_sets_metadata() -> None:
    topix_dates = [f"2026-02-{day:02d}" for day in range(6, 11)]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates, fail_stock_dates=set(topix_dates))

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.datesProcessed == len(topix_dates)
    assert len(result.failedDates) == len(topix_dates)
    assert any("Too many consecutive failures" in err for err in result.errors)
    assert market_db.metadata[METADATA_KEYS["INIT_COMPLETED"]] == "true"
    assert METADATA_KEYS["LAST_SYNC_DATE"] in market_db.metadata
    failed_dates = json.loads(market_db.metadata[METADATA_KEYS["FAILED_DATES"]])
    assert failed_dates == topix_dates


@pytest.mark.asyncio
async def test_initial_sync_success_path_without_failed_dates() -> None:
    topix_dates = ["2026-02-10", "2026-02-12"]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.failedDates == []
    assert result.stocksUpdated == len(topix_dates)
    assert METADATA_KEYS["FAILED_DATES"] not in market_db.metadata


@pytest.mark.asyncio
async def test_initial_sync_with_empty_topix_and_empty_master() -> None:
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=[], master_rows=[])

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 0
    assert result.stocksUpdated == 0


@pytest.mark.asyncio
async def test_initial_sync_returns_cancelled_when_flag_set_during_stock_loop() -> None:
    topix_dates = ["2026-02-10", "2026-02-12"]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)
    cancelled = asyncio.Event()

    def on_progress(stage: str, *_args: Any) -> None:
        if stage == "stock_data":
            cancelled.set()

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=cancelled,
        on_progress=on_progress,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert METADATA_KEYS["LAST_SYNC_DATE"] not in market_db.metadata


@pytest.mark.asyncio
async def test_initial_sync_cancelled_before_start() -> None:
    market_db = DummyMarketDb()
    cancelled = asyncio.Event()
    cancelled.set()

    ctx = SyncContext(
        client=InitialSyncClient(topix_dates=["2026-02-10"]),  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]


@pytest.mark.asyncio
async def test_incremental_sync_without_anchor_date_and_with_stock_master_update() -> None:
    market_db = DummyMarketDb(latest_trading_date=None, latest_stock_data_date=None, latest_indices_data_dates={})
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
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
    assert len(market_db.stocks_rows) == 1
    assert any(path == "/indices/bars/daily/topix" and params == {} for path, params in client.calls)


@pytest.mark.asyncio
async def test_incremental_sync_collects_stock_daily_fetch_errors() -> None:
    market_db = DummyMarketDb(latest_trading_date=None, latest_stock_data_date=None, latest_indices_data_dates={})
    client = DummyClient(daily_error_dates={"2026-02-06", "2026-02-10"})

    ctx = SyncContext(
        client=client,  # type: ignore[arg-type]
        market_db=market_db,  # type: ignore[arg-type]
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert len(result.errors) >= 1
    assert any(err.startswith("Date 2026-02-") for err in result.errors)


def test_strategy_selection_and_api_call_estimates() -> None:
    assert isinstance(get_strategy("initial"), InitialSyncStrategy)
    assert isinstance(get_strategy("incremental"), IncrementalSyncStrategy)
    assert isinstance(get_strategy("indices-only"), IndicesOnlySyncStrategy)
    assert isinstance(get_strategy("unknown"), InitialSyncStrategy)

    assert IndicesOnlySyncStrategy().estimate_api_calls() == 52
    assert InitialSyncStrategy().estimate_api_calls() == 552
    assert IncrementalSyncStrategy().estimate_api_calls() == 60


def test_data_conversion_helpers_handle_aliases_and_invalid_rows() -> None:
    stock_rows = _convert_stock_rows(
        [
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {"Code": "", "CoName": "skip"},
        ]
    )
    assert len(stock_rows) == 1
    assert stock_rows[0]["code"] == "7203"

    master_rows = _convert_index_master_rows(
        [
            {"Code": "0001", "Name": "Electric", "nameEnglish": "Electric", "Category": "sector33", "dataStartDate": "2010-01-04"},
            {"code": "", "name": "skip"},
        ]
    )
    assert len(master_rows) == 1
    assert master_rows[0]["code"] == "0001"
    assert master_rows[0]["name"] == "Electric"

    index_rows = _convert_indices_data_rows(
        [
            {"date": "2026-02-10", "open": 1, "high": 2, "low": 1, "close": 2, "sector_name": "TOPIX"},
            {"Date": "", "O": 10, "H": 12, "L": 9, "C": 11},
        ],
        code="0001",
    )
    assert len(index_rows) == 1
    assert index_rows[0]["date"] == "2026-02-10"
    assert index_rows[0]["open"] == 1

    index_rows_from_payload_code = _convert_indices_data_rows(
        [
            {"Date": "2026-02-10", "Code": "40", "O": 1, "H": 2, "L": 1, "C": 2, "SectorName": "TOPIX"},
        ],
        code=None,
    )
    assert len(index_rows_from_payload_code) == 1
    assert index_rows_from_payload_code[0]["code"] == "0040"


def test_build_fallback_index_master_rows_deduplicates_and_keeps_inputs_immutable() -> None:
    known_codes = {"0000"}
    rows = [
        {"code": "40", "date": "2026-02-10", "sector_name": None},
        {"code": "0040", "date": "2026-02-11", "sector_name": "Electric"},
        {"code": "0000", "date": "2026-02-11", "sector_name": "TOPIX"},
    ]

    fallback_rows = _build_fallback_index_master_rows(rows, known_codes)

    assert known_codes == {"0000"}
    assert len(fallback_rows) == 1
    assert fallback_rows[0]["code"] == "0040"
    assert fallback_rows[0]["name"] == "Electric"
    assert fallback_rows[0]["category"] == "unknown"
    assert fallback_rows[0]["data_start_date"] == "2026-02-10"
    assert fallback_rows[0]["created_at"]


def test_extract_list_items_handles_key_aliases_and_fallback() -> None:
    assert _extract_list_items({"data": [{"code": "0000"}]}) == [{"code": "0000"}]
    assert _extract_list_items({"indices": [{"code": "0000"}]}, preferred_keys=("data", "indices")) == [{"code": "0000"}]
    assert _extract_list_items({"other": [{"code": "0000"}]}) == [{"code": "0000"}]
    assert _extract_list_items({"data": ["not-dict", {"code": "0001"}]}) == [{"code": "0001"}]
    assert _extract_list_items({"count": 0}) == []


def test_date_helpers_cover_parse_and_fallback_paths() -> None:
    assert _parse_date("20260210")
    assert _parse_date("2026-02-10")
    assert _parse_date("") is None
    assert _parse_date("   ") is None
    assert _parse_date("2026-02-30") is None

    assert _to_jquants_date_param("2026-02-10") == "20260210"
    assert _to_jquants_date_param("not-a-date") == "not-a-date"

    assert _is_date_after("2026-02-11", "2026-02-10")
    assert _is_date_after("zz", "aa")

    assert _date_sort_key("invalid") == (1, "invalid")
    assert _date_sort_key("2026-02-10") == (0, "2026-02-10")
