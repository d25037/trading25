from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from src.application.services.intraday_sync_service import sync_intraday_data
from src.application.services.jquants_bulk_service import (
    BulkFetchPlan,
    BulkFetchResult,
    BulkFileInfo,
)
from src.entrypoints.http.schemas.db import IntradaySyncRequest
from src.infrastructure.db.market.market_db import METADATA_KEYS


class DummyMarketDb:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value


class DummyStore:
    def __init__(self) -> None:
        self.published_batches: list[list[dict[str, object]]] = []
        self.index_calls = 0

    def publish_stock_minute_data(self, rows: list[dict[str, object]]) -> int:
        self.published_batches.append(list(rows))
        return len(rows)

    def index_stock_minute_data(self) -> None:
        self.index_calls += 1


@dataclass
class DummyBulkService:
    plan: BulkFetchPlan
    rows: list[dict[str, object]]
    result: BulkFetchResult

    async def build_plan(  # noqa: D401
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> BulkFetchPlan:
        del endpoint, date_from, date_to, exact_dates
        return self.plan

    async def fetch_with_plan(
        self,
        plan: BulkFetchPlan,
        *,
        on_rows_batch,
        accumulate_rows: bool = True,
    ) -> BulkFetchResult:
        del accumulate_rows
        await on_rows_batch(self.rows, plan.files[0])
        return self.result


@pytest.mark.asyncio
async def test_sync_intraday_data_auto_uses_rest_for_code_requests() -> None:
    market_db = DummyMarketDb()
    store = DummyStore()
    client = AsyncMock()
    client.get_paginated.return_value = [
        {
            "Date": "2026-02-10",
            "Time": "09:00",
            "Code": "72030",
            "O": 100.0,
            "H": 101.0,
            "L": 99.0,
            "C": 100.5,
            "Vo": 1000,
            "Va": 100500.0,
        },
        {
            "Date": "2026-02-10",
            "Time": "09:01",
            "Code": "72030",
            "O": 100.5,
            "H": 102.0,
            "L": 100.0,
            "C": 101.5,
            "Vo": 800,
            "Va": 81200.0,
        },
    ]

    result = await sync_intraday_data(
        IntradaySyncRequest(date="2026-02-10", codes=["7203"]),
        market_db=market_db,
        time_series_store=store,
        jquants_client=client,
    )

    assert result.success is True
    assert result.mode == "rest"
    assert result.requestedCodes == 1
    assert result.storedCodes == 1
    assert result.datesProcessed == 1
    assert result.recordsFetched == 2
    assert result.recordsStored == 2
    assert result.apiCalls == 1
    assert store.index_calls == 1
    assert len(store.published_batches) == 1
    assert store.published_batches[0][0]["code"] == "7203"
    assert market_db.metadata[METADATA_KEYS["LAST_INTRADAY_SYNC"]] == result.lastUpdated
    client.get_paginated.assert_awaited_once_with(
        "/equities/bars/minute",
        params={"code": "72030", "date": "2026-02-10"},
        max_pages=200,
    )


@pytest.mark.asyncio
async def test_sync_intraday_data_bulk_filters_codes_and_counts_cache() -> None:
    market_db = DummyMarketDb()
    store = DummyStore()
    client = AsyncMock()
    plan = BulkFetchPlan(
        endpoint="/equities/bars/minute",
        files=[
            BulkFileInfo(
                key="minute/20260210.csv.gz",
                last_modified="2026-02-10T16:35:00+09:00",
                size=100,
                range_start=None,
                range_end=None,
            )
        ],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = DummyBulkService(
        plan=plan,
        rows=[
            {
                "Date": "2026-02-10",
                "Time": "09:00",
                "Code": "72030",
                "O": 100.0,
                "H": 101.0,
                "L": 99.0,
                "C": 100.5,
                "Vo": 1000,
                "Va": 100500.0,
            },
            {
                "Date": "2026-02-10",
                "Time": "09:00",
                "Code": "67580",
                "O": 200.0,
                "H": 201.0,
                "L": 199.0,
                "C": 200.5,
                "Vo": 900,
                "Va": 180450.0,
            },
            {
                "Date": "2026-02-10",
                "Code": "72030",
                "O": 100.0,
                "H": 101.0,
                "L": 99.0,
                "C": 100.5,
                "Vo": 1000,
            },
        ],
        result=BulkFetchResult(
            rows=[],
            api_calls=2,
            cache_hits=1,
            cache_misses=0,
            selected_files=1,
        ),
    )

    result = await sync_intraday_data(
        IntradaySyncRequest(date="2026-02-10", mode="bulk", codes=["7203"]),
        market_db=market_db,
        time_series_store=store,
        jquants_client=client,
        bulk_service_factory=lambda: bulk_service,
    )

    assert result.success is True
    assert result.mode == "bulk"
    assert result.requestedCodes == 1
    assert result.storedCodes == 1
    assert result.datesProcessed == 1
    assert result.recordsFetched == 1
    assert result.recordsStored == 1
    assert result.apiCalls == 3
    assert result.selectedFiles == 1
    assert result.cacheHits == 1
    assert result.cacheMisses == 0
    assert result.skippedRows == 1
    assert store.index_calls == 1
    assert len(store.published_batches) == 1
    assert store.published_batches[0][0]["code"] == "7203"
    assert market_db.metadata[METADATA_KEYS["LAST_INTRADAY_SYNC"]] == result.lastUpdated
