"""Publish and index helpers for market sync strategies."""

from __future__ import annotations

import asyncio
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


async def _publish_synthetic_nikkei_rows(
    ctx: Any,
    rows: list[dict[str, Any]],
) -> int:
    synthetic_rows = build_synthetic_underpx_index_rows(rows)
    if not synthetic_rows:
        return 0

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


async def _publish_topix_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_topix_data, rows)


async def _publish_stock_data_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_stock_data, rows)


async def _publish_indices_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_indices_data, rows)


async def _publish_options_225_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_options_225_data, rows)


async def _publish_margin_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_margin_data, rows)


async def _publish_statement_rows(ctx: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
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


async def _index_topix_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="topix",
        current=progress_current,
        total=progress_total,
        message="Exporting TOPIX Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_topix_data)


async def _index_stock_data_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="stock_data",
        current=progress_current,
        total=progress_total,
        message="Projecting adjusted stock_data and exporting Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_stock_data)


async def _index_indices_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="indices",
        current=progress_current,
        total=progress_total,
        message="Exporting indices Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_indices_data)


async def _index_options_225_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="options_225",
        current=progress_current,
        total=progress_total,
        message="Exporting N225 options Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_options_225_data)


async def _index_margin_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="margin",
        current=progress_current,
        total=progress_total,
        message="Exporting margin_data Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_margin_data)


async def _index_statement_rows(
    ctx: Any,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    _emit_index_progress(
        ctx,
        stage="fundamentals",
        current=progress_current,
        total=progress_total,
        message="Exporting statements Parquet...",
    )
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_statements)


__all__ = [
    "_index_indices_rows",
    "_index_margin_rows",
    "_index_options_225_rows",
    "_index_statement_rows",
    "_index_stock_data_rows",
    "_index_topix_rows",
    "_publish_indices_rows",
    "_publish_margin_rows",
    "_publish_options_225_rows",
    "_publish_statement_rows",
    "_publish_stock_data_rows",
    "_publish_synthetic_nikkei_rows",
    "_publish_topix_rows",
]
