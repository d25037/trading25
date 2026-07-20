"""Publish and index helpers for market sync strategies."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.application.services.options_225 import (
    OPTIONS_225_SYNTHETIC_INDEX_CATEGORY,
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
    OPTIONS_225_SYNTHETIC_INDEX_NAME,
    OPTIONS_225_SYNTHETIC_INDEX_NAME_EN,
    build_synthetic_underpx_index_rows,
)
from src.application.services.sync_state_helpers import _require_time_series_store
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult


@dataclass(frozen=True, slots=True)
class StockDataStageResult:
    staged_rows: int
    affected_codes: frozenset[str]


async def _publish_synthetic_nikkei_rows(
    ctx: Any,
    rows: list[dict[str, Any]],
) -> SemanticDeltaResult:
    synthetic_rows = build_synthetic_underpx_index_rows(rows)
    if not synthetic_rows:
        return SemanticDeltaResult.empty()

    await asyncio.to_thread(
        ctx.market_db.upsert_index_master,
        [
            {
                "code": OPTIONS_225_SYNTHETIC_INDEX_CODE,
                "name": OPTIONS_225_SYNTHETIC_INDEX_NAME,
                "name_english": OPTIONS_225_SYNTHETIC_INDEX_NAME_EN,
                "category": OPTIONS_225_SYNTHETIC_INDEX_CATEGORY,
                "data_start_date": min(str(row["date"]) for row in synthetic_rows),
                "created_at": datetime.now(UTC).isoformat(),
            }
        ],
    )
    return await _publish_indices_rows(ctx, synthetic_rows)


async def _publish_topix_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_topix_data, rows)


async def _publish_stock_data_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_stock_data, rows)


async def _stage_stock_data_rows(
    ctx: Any, rows: list[dict[str, Any]]
) -> StockDataStageResult:
    if not rows:
        return StockDataStageResult(staged_rows=0, affected_codes=frozenset())
    store = _require_time_series_store(ctx)
    affected_codes = await asyncio.to_thread(store.detect_stock_provider_drift, rows)
    await asyncio.to_thread(store.stage_stock_data_rows, rows)
    return StockDataStageResult(
        staged_rows=len(rows),
        affected_codes=frozenset(affected_codes),
    )


async def _flush_staged_stock_data_rows(
    ctx: Any, *, exclude_codes: frozenset[str]
) -> SemanticDeltaResult:
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(
        store.flush_staged_stock_data,
        exclude_codes=exclude_codes,
    )


async def _discard_staged_stock_data_rows(ctx: Any) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.discard_staged_stock_data)


async def _publish_indices_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_indices_data, rows)


async def _publish_options_225_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_options_225_data, rows)


async def _publish_margin_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_margin_data, rows)


async def _publish_statement_rows(ctx: Any, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    if not rows:
        return SemanticDeltaResult.empty()
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_statements, rows)


def _emit_index_progress(
    ctx: Any,
    *,
    stage: str,
    current: int | None,
    total: int | None,
    message: str,
) -> None:
    if current is None or total is None:
        return
    ctx.on_progress(stage, current, total, message)


def _index_required(store: Any, table_name: str) -> bool:
    pending = getattr(store, "has_pending_index", None)
    return not callable(pending) or bool(pending(table_name))


async def _index_topix_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "topix_data"):
        return
    _emit_index_progress(
        ctx,
        stage="topix",
        current=progress_current,
        total=progress_total,
        message="Exporting TOPIX Parquet...",
    )
    await asyncio.to_thread(store.index_topix_data)


async def _index_stock_data_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "stock_data"):
        return
    _emit_index_progress(
        ctx,
        stage="stock_data",
        current=progress_current,
        total=progress_total,
        message="Projecting adjusted stock_data and exporting Parquet...",
    )
    await asyncio.to_thread(store.index_stock_data)


async def _index_indices_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "indices_data"):
        return
    _emit_index_progress(
        ctx,
        stage="indices",
        current=progress_current,
        total=progress_total,
        message="Exporting indices Parquet...",
    )
    await asyncio.to_thread(store.index_indices_data)


async def _index_options_225_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "options_225_data"):
        return
    _emit_index_progress(
        ctx,
        stage="options_225",
        current=progress_current,
        total=progress_total,
        message="Exporting N225 options Parquet...",
    )
    await asyncio.to_thread(store.index_options_225_data)


async def _index_margin_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "margin_data"):
        return
    _emit_index_progress(
        ctx,
        stage="margin",
        current=progress_current,
        total=progress_total,
        message="Exporting margin_data Parquet...",
    )
    await asyncio.to_thread(store.index_margin_data)


async def _index_statement_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    store = _require_time_series_store(ctx)
    if not _index_required(store, "statements"):
        return
    _emit_index_progress(
        ctx,
        stage="fundamentals",
        current=progress_current,
        total=progress_total,
        message="Exporting statements Parquet...",
    )
    await asyncio.to_thread(store.index_statements)


__all__ = [
    "_index_indices_rows",
    "_index_margin_rows",
    "_index_options_225_rows",
    "_index_statement_rows",
    "_index_stock_data_rows",
    "_index_topix_rows",
    "_discard_staged_stock_data_rows",
    "_flush_staged_stock_data_rows",
    "_publish_indices_rows",
    "_publish_margin_rows",
    "_publish_options_225_rows",
    "_publish_statement_rows",
    "_publish_stock_data_rows",
    "_publish_synthetic_nikkei_rows",
    "_publish_topix_rows",
    "_stage_stock_data_rows",
]
