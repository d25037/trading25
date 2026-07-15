"""Bulk row ingestion helpers for market sync strategies."""

from __future__ import annotations

import asyncio
from typing import Any

from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.ingestion_pipeline import validate_rows_required_fields
from src.application.services import sync_publish_helpers
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.application.services.sync_row_converters import (
    convert_margin_rows as _convert_margin_rows,
    convert_stock_bulk_rows as _convert_stock_bulk_rows,
    normalize_bulk_fins_rows as _normalize_bulk_fins_rows,
    normalize_bulk_margin_rows as _normalize_bulk_margin_rows,
    normalize_bulk_stock_rows as _normalize_bulk_stock_rows,
    _normalize_iso_date_text,
)


async def _ingest_stock_bulk_batch(
    ctx: Any,
    *,
    batch_rows: list[dict[str, Any]],
    target_dates: set[str] | None,
) -> SemanticDeltaResult:
    normalized_rows = _normalize_bulk_stock_rows(batch_rows)
    rows = _convert_stock_bulk_rows(normalized_rows, target_dates=target_dates)
    if not rows:
        return SemanticDeltaResult.empty()
    return await asyncio.to_thread(ctx.time_series_store.stage_stock_data_rows, rows)


async def _flush_staged_stock_bulk_rows(ctx: Any) -> SemanticDeltaResult:
    return await asyncio.to_thread(ctx.time_series_store.flush_staged_stock_data)


async def _ingest_fins_bulk_batch(
    ctx: Any,
    *,
    batch_rows: list[dict[str, Any]],
    allowed_codes: set[str],
    target_dates: set[str] | None = None,
    published_dates: set[str] | None = None,
) -> SemanticDeltaResult:
    rows = convert_fins_summary_rows(_normalize_bulk_fins_rows(batch_rows))
    if allowed_codes:
        rows = [row for row in rows if row.get("code") in allowed_codes]
    if target_dates is not None:
        rows = [
            row
            for row in rows
            if _normalize_iso_date_text(row.get("disclosed_date")) in target_dates
        ]
    rows = validate_rows_required_fields(
        rows,
        required_fields=("code", "disclosed_date"),
        dedupe_keys=("code", "disclosed_date"),
        stage="fundamentals",
    )
    if not rows:
        return SemanticDeltaResult.empty()
    if published_dates is not None:
        published_dates.update(
            normalized
            for normalized in (_normalize_iso_date_text(row.get("disclosed_date")) for row in rows)
            if normalized is not None
        )
    return await sync_publish_helpers._publish_statement_rows(ctx, rows)


async def _ingest_margin_bulk_batch(
    ctx: Any,
    *,
    batch_rows: list[dict[str, Any]],
    target_codes: set[str] | None,
    min_date_exclusive: str | None,
) -> SemanticDeltaResult:
    normalized_rows = _normalize_bulk_margin_rows(batch_rows)
    rows = validate_rows_required_fields(
        _convert_margin_rows(
            normalized_rows,
            target_codes=target_codes,
            min_date_exclusive=min_date_exclusive,
        ),
        required_fields=("code", "date"),
        dedupe_keys=("code", "date"),
        stage="margin_data",
    )
    if not rows:
        return SemanticDeltaResult.empty()
    return await sync_publish_helpers._publish_margin_rows(ctx, rows)


__all__ = [
    "_flush_staged_stock_bulk_rows",
    "_ingest_fins_bulk_batch",
    "_ingest_margin_bulk_batch",
    "_ingest_stock_bulk_batch",
]
