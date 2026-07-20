from __future__ import annotations

from typing import Any

import pytest

from src.application.services.stock_refresh_service import refresh_stocks
from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
)


class DummyMarketDb:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value


class DummyTimeSeriesStore:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.replacements: list[
            tuple[str, list[dict[str, Any]], dict[str, str], dict[str, str]]
        ] = []
        self.metadata: dict[str, str] = {}
        self.index_calls = 0

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> Any:
        del missing_stock_dates_limit, statement_non_null_columns
        return type(
            "Inspection", (), {"topix_min": "2026-02-01", "topix_max": "2026-02-28"}
        )()

    def replace_stock_provider_window(
        self,
        code: str,
        rows: list[dict[str, Any]],
        coverage: dict[str, str],
        metadata: dict[str, str],
    ) -> SemanticDeltaResult:
        self.replacements.append((code, rows, coverage, metadata))
        self.rows.extend(rows)
        self.metadata.update(metadata)
        return SemanticDeltaResult(
            stats=MarketMutationStats(
                input=len(rows), inserted=len(rows), updated=0, unchanged=0, deleted=0
            )
        )

    def index_stock_data(self) -> None:
        self.index_calls += 1
        return None


class DummyIndexFailingTimeSeriesStore(DummyTimeSeriesStore):
    def index_stock_data(self) -> None:
        self.index_calls += 1
        raise RuntimeError("index failed")


class DummyJQuantsClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = [_complete_provider_row(row) for row in rows]
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    async def get_paginated(
        self, path: str, params: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        return self.rows


class DummyFailingJQuantsClient:
    async def get_paginated(
        self, path: str, params: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        del path, params
        raise RuntimeError("network error")


class RoutingJQuantsClient:
    def __init__(self, responses: dict[str, list[dict[str, Any]] | Exception]) -> None:
        self.responses = {
            code: (
                [_complete_provider_row(row) for row in response]
                if isinstance(response, list)
                else response
            )
            for code, response in responses.items()
        }
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


def _complete_provider_row(row: dict[str, Any]) -> dict[str, Any]:
    completed = dict(row)
    completed.setdefault(
        "Va",
        completed.get("C", 0) * completed.get("Vo", 0)
        if completed.get("C") is not None and completed.get("Vo") is not None
        else None,
    )
    completed.setdefault("AdjFactor", 1.0)
    completed.setdefault("AdjO", completed.get("O"))
    completed.setdefault("AdjH", completed.get("H"))
    completed.setdefault("AdjL", completed.get("L"))
    completed.setdefault("AdjC", completed.get("C"))
    completed.setdefault("AdjVo", completed.get("Vo"))
    return completed


@pytest.mark.asyncio
async def test_refresh_stocks_rejects_incomplete_provider_window_atomically() -> None:
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

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.totalRecordsStored == 0
    assert store.rows == []
    assert store.replacements == []


@pytest.mark.asyncio
async def test_refresh_stocks_rejects_invalid_date_before_range_filtering() -> None:
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 100.0,
                "H": 102.0,
                "L": 99.0,
                "C": 101.0,
                "Vo": 12345,
            },
            {
                "Code": "72030",
                "Date": None,
                "O": 100.0,
                "H": 102.0,
                "L": 99.0,
                "C": 101.0,
                "Vo": 12345,
            },
        ]
    )

    result = await refresh_stocks(["7203"], DummyMarketDb(), store, client)

    assert result.failedCount == 1
    assert store.replacements == []


@pytest.mark.asyncio
async def test_refresh_stocks_applies_topix_date_range_filter() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-01-31",
                "O": 1,
                "H": 2,
                "L": 1,
                "C": 2,
                "Vo": 100,
            },
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
            {
                "Code": "72030",
                "Date": "2026-03-01",
                "O": 20,
                "H": 22,
                "L": 19,
                "C": 21,
                "Vo": 2000,
            },
        ]
    )

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.totalRecordsStored == 1
    assert len(store.rows) == 1
    assert store.rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_refresh_stocks_fetches_all_pages_before_one_atomic_replacement() -> None:
    events: list[str] = []

    class PagedClient:
        provider_plan = "premium"

        async def get_paginated_with_meta(
            self,
            path: str,
            params: dict[str, str] | None = None,
            max_pages: int = 10,
        ) -> tuple[list[dict[str, Any]], int]:
            del path, params
            assert max_pages > 10
            events.extend(("page-1", "page-2"))
            return [
                _complete_provider_row(
                    {
                        "Code": "72030",
                        "Date": "2026-02-10",
                        "O": 10,
                        "H": 12,
                        "L": 9,
                        "C": 11,
                        "Vo": 1000,
                    }
                ),
                _complete_provider_row(
                    {
                        "Code": "72030",
                        "Date": "2026-02-11",
                        "O": 11,
                        "H": 13,
                        "L": 10,
                        "C": 12,
                        "Vo": 1100,
                    }
                ),
            ], 2

    class EventStore(DummyTimeSeriesStore):
        def replace_stock_provider_window(
            self,
            code: str,
            rows: list[dict[str, Any]],
            coverage: dict[str, str],
            metadata: dict[str, str],
        ) -> SemanticDeltaResult:
            events.append("replace")
            return super().replace_stock_provider_window(code, rows, coverage, metadata)

    store = EventStore()
    result = await refresh_stocks(["7203"], DummyMarketDb(), store, PagedClient())

    assert result.successCount == 1
    assert result.totalApiCalls == 2
    assert events == ["page-1", "page-2", "replace"]
    assert len(store.replacements) == 1
    assert store.replacements[0][2] == {"start": "2026-02-10", "end": "2026-02-11"}


@pytest.mark.asyncio
async def test_refresh_stocks_pagination_failure_does_not_mutate_rows_or_metadata() -> (
    None
):
    class FailingPagedClient:
        async def get_paginated_with_meta(
            self,
            path: str,
            params: dict[str, str] | None = None,
            max_pages: int = 10,
        ) -> tuple[list[dict[str, Any]], int]:
            del path, params, max_pages
            raise RuntimeError("page 2 failed")

    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    result = await refresh_stocks(["7203"], market_db, store, FailingPagedClient())

    assert result.failedCount == 1
    assert store.replacements == []
    assert store.rows == []
    assert store.metadata == {}
    assert market_db.metadata == {}


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


@pytest.mark.asyncio
async def test_refresh_stocks_dedupes_codes_and_skips_index_when_no_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-01-31",
                "O": 1,
                "H": 2,
                "L": 1,
                "C": 2,
                "Vo": 100,
            },
            {
                "Code": "72030",
                "Date": "2026-03-01",
                "O": 20,
                "H": 22,
                "L": 19,
                "C": 21,
                "Vo": 2000,
            },
        ]
    )
    progress_messages: list[tuple[int, int, str]] = []

    result = await refresh_stocks(
        ["7203", "7203"],
        market_db,
        store,
        client,
        progress_callback=lambda current, total, message: progress_messages.append(
            (current, total, message)
        ),
    )

    assert result.totalStocks == 1
    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.totalRecordsStored == 0
    assert store.index_calls == 0
    assert len(client.calls) == 1
    assert result.results[0].success is False
    assert (
        result.results[0].error
        == "No publishable rows matched the local market snapshot date range"
    )
    assert progress_messages[0][2].startswith("Refreshing stock 1/1")
    assert progress_messages[-1][2].startswith("Refresh failed for stock 1/1")


@pytest.mark.asyncio
async def test_refresh_stocks_treats_identical_zero_delta_publish_as_success() -> None:
    market_db = DummyMarketDb()

    class ZeroPublishStore(DummyTimeSeriesStore):
        def replace_stock_provider_window(
            self,
            code: str,
            rows: list[dict[str, Any]],
            coverage: dict[str, str],
            metadata: dict[str, str],
        ) -> SemanticDeltaResult:
            self.replacements.append((code, rows, coverage, metadata))
            self.rows.extend(rows)
            self.metadata.update(metadata)
            return SemanticDeltaResult(
                stats=MarketMutationStats(
                    input=len(rows),
                    inserted=0,
                    updated=0,
                    unchanged=len(rows),
                    deleted=0,
                )
            )

    store = ZeroPublishStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
        ]
    )

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 1
    assert result.failedCount == 0
    assert result.results[0].success is True
    assert result.results[0].recordsStored == 0
    assert store.index_calls == 0


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
        progress_callback=lambda current, total, message: progress_messages.append(
            (current, total, message)
        ),
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
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
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
async def test_refresh_stocks_propagates_index_failure_after_commit() -> None:
    market_db = DummyMarketDb()
    store = DummyIndexFailingTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
        ]
    )

    with pytest.raises(RuntimeError, match="index failed"):
        await refresh_stocks(["7203"], market_db, store, client)

    assert len(store.replacements) == 1
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
    assert market_db.metadata == {}
    assert store.metadata == {}


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
        progress_callback=lambda _current, _total, message: progress_messages.append(
            message
        ),
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
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
        ]
    )
    progress_messages: list[str] = []
    checks = iter([False, True])

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        client,
        progress_callback=lambda _current, _total, message: progress_messages.append(
            message
        ),
        cancel_check=lambda: next(checks, True),
    )

    assert result.totalApiCalls == 1
    assert result.totalRecordsStored == 0
    assert result.results == []
    assert result.errors == ["Cancelled"]
    assert store.rows == []
    assert store.index_calls == 0
    assert (
        progress_messages[-1]
        == "Cancelled stock refresh after fetching stock 1/1: 7203"
    )


@pytest.mark.asyncio
async def test_refresh_stocks_cancels_before_publishing_rows() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(
        rows=[
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 1000,
            },
        ]
    )
    progress_messages: list[str] = []
    checks = iter([False, False, True])

    result = await refresh_stocks(
        ["7203"],
        market_db,
        store,
        client,
        progress_callback=lambda _current, _total, message: progress_messages.append(
            message
        ),
        cancel_check=lambda: next(checks, True),
    )

    assert result.totalApiCalls == 1
    assert result.totalRecordsStored == 0
    assert result.results == []
    assert result.errors == ["Cancelled"]
    assert store.rows == []
    assert store.index_calls == 0
    assert (
        progress_messages[-1]
        == "Cancelled stock refresh before publishing stock 1/1: 7203"
    )


@pytest.mark.asyncio
async def test_refresh_stocks_handles_empty_api_response_without_callback() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = DummyJQuantsClient(rows=[])

    result = await refresh_stocks(["7203"], market_db, store, client)

    assert result.successCount == 0
    assert result.failedCount == 1
    assert result.results[0].success is False
    assert (
        result.results[0].error
        == "No publishable rows matched the local market snapshot date range"
    )
    assert result.errors == [
        "7203: No publishable rows matched the local market snapshot date range"
    ]
    assert store.index_calls == 0


@pytest.mark.asyncio
async def test_refresh_stocks_continues_after_success_and_failure_across_multiple_codes() -> (
    None
):
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    client = RoutingJQuantsClient(
        responses={
            "72030": [
                {
                    "Code": "72030",
                    "Date": "2026-02-10",
                    "O": 10,
                    "H": 12,
                    "L": 9,
                    "C": 11,
                    "Vo": 1000,
                },
            ],
            "67580": RuntimeError("network error"),
            "99840": [
                {
                    "Code": "99840",
                    "Date": "2026-02-10",
                    "O": 20,
                    "H": 22,
                    "L": 19,
                    "C": 21,
                    "Vo": 2000,
                },
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


@pytest.mark.asyncio
async def test_refresh_stocks_replaces_each_successfully_normalized_code_once() -> None:
    events: list[str] = []

    class EventStore(DummyTimeSeriesStore):
        def replace_stock_provider_window(
            self,
            code: str,
            rows: list[dict[str, Any]],
            coverage: dict[str, str],
            metadata: dict[str, str],
        ) -> SemanticDeltaResult:
            events.append(f"replace:{code}")
            return super().replace_stock_provider_window(code, rows, coverage, metadata)

        def index_stock_data(self) -> None:
            super().index_stock_data()
            events.append("stock_indexed")

    client = RoutingJQuantsClient(
        responses={
            "72030": [
                {
                    "Code": "72030",
                    "Date": "2026-02-10",
                    "O": 10,
                    "H": 12,
                    "L": 9,
                    "C": 11,
                    "Vo": 1000,
                },
            ],
            "67580": RuntimeError("network error"),
        }
    )

    result = await refresh_stocks(
        ["72030", "6758", "7203"],
        DummyMarketDb(),
        EventStore(),
        client,
    )

    assert result.successCount == 1
    assert events == ["replace:7203", "stock_indexed"]
