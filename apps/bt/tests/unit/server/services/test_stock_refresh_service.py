from __future__ import annotations

from typing import Any

import pytest

from src.application.services.stock_refresh_service import refresh_stocks
from src.infrastructure.db.market.market_db import (
    LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    METADATA_KEYS,
)


class DummyMarketDb:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}
        self.resolved_calls: list[list[str] | None] = []

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def mark_stock_adjustments_resolved(self, codes: list[str] | None = None) -> int:
        self.resolved_calls.append(None if codes is None else list(codes))
        return len(codes or [])


class DummyTimeSeriesStore:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.index_calls = 0

    def inspect(self, *, missing_stock_dates_limit: int = 0, statement_non_null_columns: list[str] | None = None) -> Any:
        del missing_stock_dates_limit, statement_non_null_columns
        return type("Inspection", (), {"topix_min": "2026-02-01", "topix_max": "2026-02-28"})()

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
        self.rows.extend(rows)
        return len(rows)

    def index_stock_data(self) -> None:
        self.index_calls += 1
        return None


class DummyIndexFailingTimeSeriesStore(DummyTimeSeriesStore):
    def index_stock_data(self) -> None:
        self.index_calls += 1
        raise RuntimeError("index failed")


class DummyJQuantsClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    async def get_paginated(self, path: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        return self.rows


class DummyFailingJQuantsClient:
    async def get_paginated(self, path: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        del path, params
        raise RuntimeError("network error")


class RoutingJQuantsClient:
    def __init__(self, responses: dict[str, list[dict[str, Any]] | Exception]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    async def get_paginated(
        self,
        path: str,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        del path
        code = (params or {}).get("code", "")
        self.calls.append(code)
        response = self.responses[code]
        if isinstance(response, Exception):
            raise response
        return response


@pytest.mark.asyncio
async def test_refresh_stocks_skips_incomplete_ohlcv_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
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

    result = await refresh_stocks(["131A"], market_db, store, client)

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert len(store.rows) == 1
    assert store.rows[0]["code"] == "131A"
    assert store.rows[0]["date"] == "2026-02-10"
    assert market_db.resolved_calls == []


@pytest.mark.asyncio
async def test_refresh_stocks_applies_topix_date_range_filter() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-01-31", "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 100},
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
            {"Code": "72030", "Date": "2026-03-01", "O": 20, "H": 22, "L": 19, "C": 21, "Vo": 2000},
        ]
    )

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert len(store.rows) == 1
    assert store.rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_refresh_stocks_handles_jquants_error() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        DummyFailingJQuantsClient(),
    )

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.totalRecordsStored == 0
    assert len(result.errors) == 1
    assert market_db.resolved_calls == []


@pytest.mark.asyncio
async def test_refresh_stocks_dedupes_codes_and_skips_index_when_no_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-01-31", "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 100},
            {"Code": "72030", "Date": "2026-03-01", "O": 20, "H": 22, "L": 19, "C": 21, "Vo": 2000},
        ]
    )
    progress_messages: list[tuple[int, int, str]] = []

    result = await refresh_stocks(
        ["7203", "7203"],
        market_db,
        store,
        client,
        progress_callback=lambda current, total, message: progress_messages.append((current, total, message)),
    )

    assert result.totalStocks == 1
    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.totalRecordsStored == 0
    assert store.index_calls == 0
    assert len(client.calls) == 1
    assert result.results[0].success is False
    assert result.results[0].error == "No publishable rows matched the local market snapshot date range"
    assert progress_messages[0][2].startswith("Refreshing stock 1/1")
    assert progress_messages[-1][2].startswith("Refresh failed for stock 1/1")


@pytest.mark.asyncio
async def test_refresh_stocks_marks_zero_stored_publish_as_failure() -> None:
    market_db = DummyMarketDb()

    class ZeroPublishStore(DummyTimeSeriesStore):
        def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
            self.rows.extend(rows)
            return 0

    store = ZeroPublishStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
        ]
    )

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.results[0].success is False
    assert result.results[0].error == "No rows were published to the local market snapshot"
    assert store.index_calls == 0
    assert market_db.resolved_calls == []


@pytest.mark.asyncio
async def test_refresh_stocks_reports_progress_for_failures() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    progress_messages: list[tuple[int, int, str]] = []

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        DummyFailingJQuantsClient(),
        progress_callback=lambda current, total, message: progress_messages.append((current, total, message)),
    )

    assert result.failedCount == 1
    assert store.index_calls == 0
    assert progress_messages[0][2].startswith("Refreshing stock 1/1")
    assert progress_messages[-1][2].startswith("Refresh failed for stock 1/1")


@pytest.mark.asyncio
async def test_refresh_stocks_stops_when_cancelled_between_codes() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
        ]
    )
    cancelled = False

    def _cancel_check() -> bool:
        return cancelled

    def _on_progress(_current: int, _total: int, message: str) -> None:
        nonlocal cancelled
        if message.startswith("Refreshed stock 1/2"):
            cancelled = True

    result = await refresh_stocks(
        ["7203", "6758"],
        market_db,
        store,
        client,
        progress_callback=_on_progress,
        cancel_check=_cancel_check,
    )

    assert result.totalStocks == 2
    assert result.successCount == 1
    assert len(client.calls) == 1
    assert result.errors[-1] == "Cancelled"


@pytest.mark.asyncio
async def test_refresh_stocks_reports_index_failure_without_raising() -> None:
    market_db = DummyMarketDb()
    store = DummyIndexFailingTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
        ]
    )

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert result.errors == ["stock_data index: index failed"]
    assert store.index_calls == 1


@pytest.mark.asyncio
async def test_refresh_stocks_handles_empty_code_list_without_indexing() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(rows=[])

    result = await refresh_stocks([], market_db, store, client)

    assert result.totalStocks == 0
    assert result.successCount == 0
    assert result.failedCount == 0
    assert result.totalApiCalls == 0
    assert result.totalRecordsStored == 0
    assert result.errors == []
    assert store.index_calls == 0
    assert market_db.metadata[METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]] == (
        LOCAL_STOCK_PRICE_ADJUSTMENT_MODE
    )
    assert METADATA_KEYS["LAST_STOCKS_REFRESH"] in market_db.metadata


@pytest.mark.asyncio
async def test_refresh_stocks_cancels_before_first_fetch() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(rows=[])
    progress_messages: list[str] = []

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        client,
        progress_callback=lambda _current, _total, message: progress_messages.append(message),
        cancel_check=lambda: True,
    )

    assert result.totalApiCalls == 0
    assert result.results == []
    assert result.errors == ["Cancelled"]
    assert store.index_calls == 0
    assert progress_messages == ["Cancelled stock refresh before stock 1/1"]


@pytest.mark.asyncio
async def test_refresh_stocks_cancels_after_fetch_before_processing_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
        ]
    )
    progress_messages: list[str] = []
    checks = iter([False, True])

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        client,
        progress_callback=lambda _current, _total, message: progress_messages.append(message),
        cancel_check=lambda: next(checks, True),
    )

    assert result.totalApiCalls == 1
    assert result.totalRecordsStored == 0
    assert result.results == []
    assert result.errors == ["Cancelled"]
    assert store.rows == []
    assert store.index_calls == 0
    assert progress_messages[-1] == "Cancelled stock refresh after fetching stock 1/1: 7203"


@pytest.mark.asyncio
async def test_refresh_stocks_cancels_before_publishing_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
        ]
    )
    progress_messages: list[str] = []
    checks = iter([False, False, True])

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        client,
        progress_callback=lambda _current, _total, message: progress_messages.append(message),
        cancel_check=lambda: next(checks, True),
    )

    assert result.totalApiCalls == 1
    assert result.totalRecordsStored == 0
    assert result.results == []
    assert result.errors == ["Cancelled"]
    assert store.rows == []
    assert store.index_calls == 0
    assert progress_messages[-1] == "Cancelled stock refresh before publishing stock 1/1: 7203"


@pytest.mark.asyncio
async def test_refresh_stocks_handles_empty_api_response_without_callback() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(rows=[])

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.results[0].success is False
    assert result.results[0].error == "No publishable rows matched the local market snapshot date range"
    assert result.errors == [
        "7203: No publishable rows matched the local market snapshot date range"
    ]
    assert store.index_calls == 0


@pytest.mark.asyncio
async def test_refresh_stocks_continues_after_success_and_failure_across_multiple_codes() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = RoutingJQuantsClient(
        responses={
            "72030": [
                {"Code": "72030", "Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "Vo": 1000},
            ],
            "67580": RuntimeError("network error"),
            "99840": [
                {"Code": "99840", "Date": "2026-02-10", "O": 20, "H": 22, "L": 19, "C": 21, "Vo": 2000},
            ],
        }
    )

    result = await refresh_stocks(["7203", "6758", "9984"], market_db, store, client)

    assert result.successCount == 2
    assert result.failedCount == 1
    assert result.totalApiCalls == 2
    assert result.totalRecordsStored == 2
    assert len(store.rows) == 2
    assert store.index_calls == 1
    assert client.calls == ["72030", "67580", "99840"]
    assert any(error.startswith("6758: network error") for error in result.errors)
