"""
Stock Refresh Service

POST /api/db/stocks/refresh のビジネスロジック。
指定銘柄の株価データを JQuants API から再取得して DuckDB time-series を更新する。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar, cast

from loguru import logger

from src.application.contracts.market_data_plane import (
    RefreshResponse,
    RefreshStockResult,
)
from src.shared.provider_stock_window import (
    provider_stock_source_fingerprint,
    validate_provider_plan,
)
from src.application.services.stock_data_row_builder import build_stock_data_row
from src.infrastructure.db.market.market_schema import (
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    METADATA_KEYS,
)
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
)


class StockRefreshMarketDbLike(Protocol):
    def set_sync_metadata(self, key: str, value: str) -> None: ...


class StockRefreshClientLike(Protocol):
    async def get_paginated_with_meta(
        self,
        path: str,
        params: dict[str, str] | None = None,
        max_pages: int = 10,
    ) -> tuple[list[dict[str, Any]], int]: ...


class StockRefreshTimeSeriesStoreLike(Protocol):
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> Any: ...

    def replace_stock_provider_window(
        self,
        code: str,
        rows: list[dict[str, Any]],
        coverage: dict[str, str],
        metadata: dict[str, str],
    ) -> SemanticDeltaResult: ...
    def has_pending_index(self, table_name: str) -> bool: ...
    def index_stock_data(self) -> None: ...


_T = TypeVar("_T")


async def _to_thread_joined_with_cancellation(
    function: Callable[..., _T], /, *args: Any
) -> tuple[_T, asyncio.CancelledError | None]:
    """Join a mutating worker and return any cancellation deferred while joining."""
    worker = asyncio.create_task(asyncio.to_thread(function, *args))
    deferred_cancellation: asyncio.CancelledError | None = None
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as exc:
            deferred_cancellation = deferred_cancellation or exc
    return worker.result(), deferred_cancellation


async def _fetch_complete_provider_window(
    client: StockRefreshClientLike,
    *,
    code: str,
) -> tuple[list[dict[str, Any]], int]:
    params = {"code": code}
    get_with_meta = getattr(client, "get_paginated_with_meta", None)
    if not callable(get_with_meta):
        raise RuntimeError(
            "Stock refresh requires terminal pagination proof before publication"
        )
    get_with_meta_callable = cast(
        Callable[..., Awaitable[tuple[list[dict[str, Any]], int]]],
        get_with_meta,
    )
    rows, calls = await get_with_meta_callable(
        "/equities/bars/daily",
        params=params,
        max_pages=10_000,
    )
    return rows, int(calls)


async def refresh_stocks(
    codes: list[str],
    market_db: StockRefreshMarketDbLike,
    time_series_store: StockRefreshTimeSeriesStoreLike,
    jquants_client: StockRefreshClientLike,
    *,
    progress_callback: Callable[[int, int, str], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> RefreshResponse:
    """銘柄データを再取得"""
    provider_plan = validate_provider_plan(
        getattr(jquants_client, "plan", None)
        or getattr(jquants_client, "provider_plan", None)
    )
    total_calls = 0
    total_stored = 0
    results: list[RefreshStockResult] = []
    errors: list[str] = []
    any_rows_published = False
    cancelled = False
    deferred_cancellation: asyncio.CancelledError | None = None
    unique_codes = list(
        dict.fromkeys(
            normalized for code in codes if (normalized := normalize_stock_code(code))
        )
    )
    total_codes = len(unique_codes)

    # TOPIX 日付範囲を取得（フィルタ用）
    inspection = await asyncio.to_thread(time_series_store.inspect)
    min_date = inspection.topix_min
    max_date = inspection.topix_max

    for index, code in enumerate(unique_codes, start=1):
        if cancel_check is not None and cancel_check():
            cancelled = True
            if progress_callback is not None:
                progress_callback(
                    index - 1,
                    total_codes,
                    f"Cancelled stock refresh before stock {index}/{total_codes}",
                )
            break
        normalized = code
        expanded = expand_stock_code(normalized)
        if progress_callback is not None:
            progress_callback(
                index - 1,
                total_codes,
                f"Refreshing stock {index}/{total_codes}: {normalized}",
            )
        try:
            data, fetch_calls = await _fetch_complete_provider_window(
                jquants_client,
                code=expanded,
            )
            total_calls += fetch_calls

            if cancel_check is not None and cancel_check():
                cancelled = True
                if progress_callback is not None:
                    progress_callback(
                        index - 1,
                        total_codes,
                        f"Cancelled stock refresh after fetching stock {index}/{total_codes}: {normalized}",
                    )
                break

            # TOPIX 日付範囲でフィルタ
            rows: list[dict[str, Any]] = []
            skipped_rows = 0
            created_at = datetime.now(UTC).isoformat()
            for d in data:
                row = build_stock_data_row(
                    d,
                    normalized_code=normalized,
                    created_at=created_at,
                )
                if row is None:
                    skipped_rows += 1
                    continue
                provider_date = str(row["date"])
                if min_date and provider_date < min_date:
                    continue
                if max_date and provider_date > max_date:
                    continue
                rows.append(row)

            if skipped_rows > 0:
                logger.warning(
                    "Skipped {} rows with incomplete OHLCV during stock refresh: {}",
                    skipped_rows,
                    normalized,
                )

                raise ValueError(
                    f"Provider window contains {skipped_rows} incomplete or non-finite rows"
                )

            stored = 0
            if cancel_check is not None and cancel_check():
                cancelled = True
                if progress_callback is not None:
                    progress_callback(
                        index - 1,
                        total_codes,
                        f"Cancelled stock refresh before publishing stock {index}/{total_codes}: {normalized}",
                    )
                break
            elif rows:
                coverage = {
                    "start": min(str(row["date"]) for row in rows),
                    "end": max(str(row["date"]) for row in rows),
                }
                metadata = {
                    METADATA_KEYS["PROVIDER_PLAN"]: provider_plan,
                    METADATA_KEYS["PROVIDER_AS_OF"]: coverage["end"],
                    METADATA_KEYS["PROVIDER_SOURCE_FINGERPRINT"]: (
                        provider_stock_source_fingerprint(rows)
                    ),
                    METADATA_KEYS["LAST_STOCKS_REFRESH"]: created_at,
                    METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]: (
                        PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE
                    ),
                }
                mutation, replacement_cancellation = (
                    await _to_thread_joined_with_cancellation(
                        time_series_store.replace_stock_provider_window,
                        normalized,
                        rows,
                        coverage,
                        metadata,
                    )
                )
                stored = mutation.mutated_rows
                if mutation.mutated_rows or time_series_store.has_pending_index(
                    "stock_data"
                ):
                    any_rows_published = True
            else:
                failure_message = (
                    "No publishable rows matched the local market snapshot date range"
                )
                errors.append(f"{normalized}: {failure_message}")
                results.append(
                    RefreshStockResult(
                        code=normalized,
                        success=False,
                        recordsFetched=len(data),
                        recordsStored=0,
                        error=failure_message,
                    )
                )
                if progress_callback is not None:
                    progress_callback(
                        index,
                        total_codes,
                        f"Refresh failed for stock {index}/{total_codes}: {normalized}",
                    )
                continue
            total_stored += stored
            results.append(
                RefreshStockResult(
                    code=normalized,
                    success=True,
                    recordsFetched=len(data),
                    recordsStored=stored,
                )
            )
            if progress_callback is not None:
                progress_callback(
                    index,
                    total_codes,
                    f"Refreshed stock {index}/{total_codes}: {normalized}",
                )
            if replacement_cancellation is not None:
                cancelled = True
                deferred_cancellation = replacement_cancellation
                break
            if cancel_check is not None and cancel_check():
                cancelled = True
                if progress_callback is not None:
                    progress_callback(
                        index,
                        total_codes,
                        f"Cancelled stock refresh after publishing stock {index}/{total_codes}: {normalized}",
                    )
                break
        except Exception as e:
            logger.warning(f"Refresh failed for {code}: {e}")
            errors.append(f"{code}: {e}")
            results.append(
                RefreshStockResult(
                    code=normalized,
                    success=False,
                    error=str(e),
                )
            )
            if progress_callback is not None:
                progress_callback(
                    index,
                    total_codes,
                    f"Refresh failed for stock {index}/{total_codes}: {normalized}",
                )

    if any_rows_published:
        _, index_cancellation = await _to_thread_joined_with_cancellation(
            time_series_store.index_stock_data
        )
        if index_cancellation is not None:
            cancelled = True
            deferred_cancellation = deferred_cancellation or index_cancellation
    if cancelled:
        errors.append("Cancelled")

    now_iso = datetime.now(UTC).isoformat()

    response = RefreshResponse(
        totalStocks=total_codes,
        successCount=sum(1 for r in results if r.success),
        failedCount=sum(1 for r in results if not r.success),
        totalApiCalls=total_calls,
        totalRecordsStored=total_stored,
        results=results,
        errors=errors,
        lastUpdated=now_iso,
    )
    if deferred_cancellation is not None:
        deferred_cancellation.add_note(
            "Stock refresh cancellation propagated after committed work was finalized"
        )
        setattr(deferred_cancellation, "response", response)
        raise deferred_cancellation
    return response
