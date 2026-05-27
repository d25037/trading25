"""Index master placeholder backfill helpers for market sync."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.application.services import sync_publish_helpers
from src.application.services.sync_row_converters import (
    convert_index_master_rows as _convert_index_master_rows,
    _normalize_index_code,
)


async def upsert_indices_rows_with_master_backfill(
    ctx: Any,
    rows: list[dict[str, Any]],
    known_master_codes: set[str],
    *,
    discovery_log: str | None = None,
) -> None:
    missing_master_rows = build_fallback_index_master_rows(rows, known_master_codes)
    if missing_master_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, missing_master_rows)
        known_master_codes.update(
            str(row["code"])
            for row in missing_master_rows
            if row.get("code")
        )
        if discovery_log:
            logger.warning(discovery_log, len(missing_master_rows))

    await sync_publish_helpers._publish_indices_rows(ctx, rows)


def build_fallback_index_master_rows(
    rows: list[dict[str, Any]],
    known_codes: set[str],
) -> list[dict[str, Any]]:
    """index_master 欠損コード向けに最小プレースホルダ行を作る。"""
    missing_master_items_by_code: dict[str, dict[str, Any]] = {}

    for row in rows:
        code = _normalize_index_code(row.get("code"))
        if not code or code in known_codes:
            continue

        row_name = str(row.get("sector_name") or "").strip()
        placeholder_name = row_name or code
        row_date = str(row.get("date") or "").strip() or None

        existing = missing_master_items_by_code.get(code)
        if existing is None:
            missing_master_items_by_code[code] = {
                "code": code,
                "name": placeholder_name,
                "category": "unknown",
                "data_start_date": row_date,
            }
            continue

        if existing["name"] == code and row_name:
            existing["name"] = row_name
        if existing["data_start_date"] is None and row_date:
            existing["data_start_date"] = row_date

    return _convert_index_master_rows(list(missing_master_items_by_code.values()))
