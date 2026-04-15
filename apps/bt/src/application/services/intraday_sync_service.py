"""
Intraday Sync Service

J-Quants `/equities/bars/minute` を DuckDB `stock_data_minute_raw` へ取り込む。
bulk を既定経路にしつつ、コード指定時は REST を使った targeted ingest を許可する。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any, Literal, Protocol

from src.application.services.jquants_bulk_service import BulkApiClientLike, JQuantsBulkService
from src.application.services.stock_minute_data_row_builder import (
    build_stock_minute_data_row,
)
from src.entrypoints.http.schemas.db import IntradaySyncRequest, IntradaySyncResponse
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code

_MINUTE_BARS_ENDPOINT = "/equities/bars/minute"
_REST_MAX_PAGES = 200


class IntradaySyncMarketDbLike(Protocol):
    def set_sync_metadata(self, key: str, value: str) -> None: ...


class IntradaySyncTimeSeriesStoreLike(Protocol):
    def publish_stock_minute_data(self, rows: list[dict[str, Any]]) -> int: ...
    def index_stock_minute_data(self) -> None: ...


class IntradaySyncClientLike(BulkApiClientLike, Protocol):
    async def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        max_pages: int = 10,
    ) -> list[dict[str, Any]]: ...


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_codes(codes: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in codes:
        resolved = normalize_stock_code(str(code).strip())
        if not resolved or resolved in seen:
            continue
        seen.add(resolved)
        normalized.append(resolved)
    return normalized


def _resolve_mode(
    request: IntradaySyncRequest,
    normalized_codes: list[str],
) -> Literal["bulk", "rest"]:
    if request.mode == "bulk":
        return "bulk"
    if request.mode == "rest":
        return "rest"
    return "rest" if normalized_codes else "bulk"


def _parse_request_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()  # noqa: DTZ007
        except ValueError:
            continue
    return None


def _build_request_date_window(
    request: IntradaySyncRequest,
) -> tuple[date | None, date | None]:
    if request.date:
        target = _parse_request_date(request.date)
        return target, target
    return _parse_request_date(request.dateFrom), _parse_request_date(request.dateTo)


def _row_within_request_window(
    row_date: str | None,
    *,
    min_date: date | None,
    max_date: date | None,
) -> bool:
    parsed = _parse_request_date(row_date)
    if parsed is None:
        return False
    if min_date is not None and parsed < min_date:
        return False
    if max_date is not None and parsed > max_date:
        return False
    return True


async def sync_intraday_data(
    request: IntradaySyncRequest,
    *,
    market_db: IntradaySyncMarketDbLike,
    time_series_store: IntradaySyncTimeSeriesStoreLike,
    jquants_client: IntradaySyncClientLike,
    bulk_service_factory: Callable[[], JQuantsBulkService] | None = None,
) -> IntradaySyncResponse:
    normalized_codes = _normalize_codes(request.codes)
    mode = _resolve_mode(request, normalized_codes)

    if mode == "rest":
        response = await _sync_intraday_via_rest(
            request,
            normalized_codes=normalized_codes,
            time_series_store=time_series_store,
            jquants_client=jquants_client,
        )
    else:
        bulk_service = (
            bulk_service_factory()
            if bulk_service_factory is not None
            else JQuantsBulkService(jquants_client)
        )
        response = await _sync_intraday_via_bulk(
            request,
            normalized_codes=normalized_codes,
            time_series_store=time_series_store,
            bulk_service=bulk_service,
        )

    market_db.set_sync_metadata(METADATA_KEYS["LAST_INTRADAY_SYNC"], response.lastUpdated)
    return response


async def _sync_intraday_via_rest(
    request: IntradaySyncRequest,
    *,
    normalized_codes: list[str],
    time_series_store: IntradaySyncTimeSeriesStoreLike,
    jquants_client: IntradaySyncClientLike,
) -> IntradaySyncResponse:
    if not normalized_codes:
        raise ValueError("REST intraday sync requires at least one code")

    now_iso = _now_iso()
    params_base: dict[str, Any] = {}
    if request.date:
        params_base["date"] = request.date
    if request.dateFrom:
        params_base["from"] = request.dateFrom
    if request.dateTo:
        params_base["to"] = request.dateTo

    fetched_count = 0
    stored_count = 0
    skipped_rows = 0
    dates_seen: set[str] = set()
    stored_codes: set[str] = set()
    api_calls = 0

    for code in normalized_codes:
        params = {"code": expand_stock_code(code), **params_base}
        raw_rows = await jquants_client.get_paginated(
            _MINUTE_BARS_ENDPOINT,
            params=params,
            max_pages=_REST_MAX_PAGES,
        )
        api_calls += 1
        fetched_count += len(raw_rows)

        publish_rows: list[dict[str, Any]] = []
        for raw_row in raw_rows:
            row = build_stock_minute_data_row(
                raw_row,
                normalized_code=code,
                created_at=now_iso,
            )
            if row is None:
                skipped_rows += 1
                continue
            publish_rows.append(row)
            dates_seen.add(str(row["date"]))
            stored_codes.add(str(row["code"]))

        if publish_rows:
            stored_count += await asyncio.to_thread(
                time_series_store.publish_stock_minute_data,
                publish_rows,
            )

    if stored_count > 0:
        await asyncio.to_thread(time_series_store.index_stock_minute_data)

    return IntradaySyncResponse(
        success=True,
        mode="rest",
        requestedCodes=len(normalized_codes),
        storedCodes=len(stored_codes),
        datesProcessed=len(dates_seen),
        recordsFetched=fetched_count,
        recordsStored=stored_count,
        apiCalls=api_calls,
        skippedRows=skipped_rows,
        lastUpdated=now_iso,
    )


async def _sync_intraday_via_bulk(
    request: IntradaySyncRequest,
    *,
    normalized_codes: list[str],
    time_series_store: IntradaySyncTimeSeriesStoreLike,
    bulk_service: JQuantsBulkService,
) -> IntradaySyncResponse:
    now_iso = _now_iso()
    target_codes = set(normalized_codes)
    min_date, max_date = _build_request_date_window(request)
    dates_seen: set[str] = set()
    stored_codes: set[str] = set()
    fetched_count = 0
    stored_count = 0
    skipped_rows = 0

    plan = await bulk_service.build_plan(
        endpoint=_MINUTE_BARS_ENDPOINT,
        date_from=request.dateFrom if request.date is None else None,
        date_to=request.dateTo if request.date is None else None,
        exact_dates=[request.date] if request.date else None,
    )

    async def _on_rows_batch(
        batch_rows: list[dict[str, Any]],
        _file_info: object,
    ) -> None:
        nonlocal fetched_count, stored_count, skipped_rows

        publish_rows: list[dict[str, Any]] = []
        for raw_row in batch_rows:
            row = build_stock_minute_data_row(
                raw_row,
                created_at=now_iso,
            )
            if row is None:
                skipped_rows += 1
                continue
            if target_codes and str(row["code"]) not in target_codes:
                continue
            if not _row_within_request_window(
                str(row["date"]),
                min_date=min_date,
                max_date=max_date,
            ):
                continue
            publish_rows.append(row)
            fetched_count += 1
            dates_seen.add(str(row["date"]))
            stored_codes.add(str(row["code"]))

        if publish_rows:
            stored_count += await asyncio.to_thread(
                time_series_store.publish_stock_minute_data,
                publish_rows,
            )

    result = await bulk_service.fetch_with_plan(
        plan,
        on_rows_batch=_on_rows_batch,
        accumulate_rows=False,
    )
    api_calls = plan.list_api_calls + result.api_calls

    if stored_count > 0:
        await asyncio.to_thread(time_series_store.index_stock_minute_data)

    return IntradaySyncResponse(
        success=True,
        mode="bulk",
        requestedCodes=len(normalized_codes),
        storedCodes=len(stored_codes),
        datesProcessed=len(dates_seen),
        recordsFetched=fetched_count,
        recordsStored=stored_count,
        apiCalls=api_calls,
        selectedFiles=result.selected_files,
        cacheHits=result.cache_hits,
        cacheMisses=result.cache_misses,
        skippedRows=skipped_rows,
        lastUpdated=now_iso,
    )
