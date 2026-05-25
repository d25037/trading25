"""State and inspection helpers for market sync strategies."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.application.services.sync_row_converters import (
    _date_sort_key,
    _parse_date,
)


_JST = ZoneInfo("Asia/Tokyo")


def _load_metadata_json_list(market_db: Any, key: str) -> list[str]:
    raw = market_db.get_sync_metadata(key)
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(v) for v in loaded if isinstance(v, str | int)]


async def _save_metadata_json_list(
    ctx: Any,
    key: str,
    values: list[str],
) -> None:
    deduped = _dedupe_preserve_order(values)
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        key,
        json.dumps(deduped, ensure_ascii=False),
    )


def _collect_unique_codes(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = normalize_stock_code(str(value).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _normalize_date_list(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        parsed = _parse_date(value)
        if parsed is None:
            continue
        normalized = parsed.isoformat()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return sorted(deduped, key=_date_sort_key)


def _build_incremental_date_targets(anchor: str | None, retry_dates: list[str]) -> list[str]:
    targets: list[str] = list(retry_dates)
    seen = set(targets)

    anchor_date = _parse_date(anchor) if anchor else None
    today_jst = datetime.now(_JST).date()

    if anchor_date is not None:
        current = anchor_date + timedelta(days=1)
        while current <= today_jst:
            value = current.isoformat()
            if value not in seen:
                seen.add(value)
                targets.append(value)
            current += timedelta(days=1)

    return targets


def _require_time_series_store(ctx: Any) -> Any:
    if ctx.time_series_store is None:
        raise RuntimeError("DuckDB time-series store is required for sync strategy execution")
    return ctx.time_series_store


def _inspect_time_series(
    ctx: Any,
    *,
    missing_stock_dates_limit: int = 0,
    missing_options_225_dates_limit: int = 0,
    statement_non_null_columns: list[str] | None = None,
) -> TimeSeriesInspection:
    store = _require_time_series_store(ctx)
    try:
        inspection = store.inspect(
            missing_stock_dates_limit=missing_stock_dates_limit,
            missing_options_225_dates_limit=missing_options_225_dates_limit,
            statement_non_null_columns=statement_non_null_columns,
        )
    except Exception as e:  # noqa: BLE001 - include backend error in sync failure
        raise RuntimeError(f"DuckDB inspection failed during sync: {e}") from e
    if inspection.source != "duckdb-parquet":
        raise RuntimeError(
            f"Unexpected time-series source during sync: {inspection.source}"
        )
    return inspection


__all__ = [
    "_build_incremental_date_targets",
    "_collect_unique_codes",
    "_dedupe_preserve_order",
    "_inspect_time_series",
    "_load_metadata_json_list",
    "_normalize_date_list",
    "_require_time_series_store",
    "_save_metadata_json_list",
]
