"""
Sync Strategies

DuckDB market data 同期のための 3 つの戦略。
Hono sync-strategies.ts からの移植。
"""

from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any, Awaitable, Callable, Literal, NoReturn, Protocol, cast
from zoneinfo import ZoneInfo

from loguru import logger

from src.application.services.jquants_bulk_service import (
    BulkFileInfo,
    BulkFetchPlan,
    BulkFetchResult,
    JQuantsBulkService,
)
from src.application.services.listed_market_targets import (
    build_fundamentals_fetch_codes,
    build_fundamentals_target_map,
    extract_listed_market_codes,
    group_target_codes_by_canonical,
    is_listed_market_code,
    normalize_frontier_date,
    resolve_frontier_cache_codes,
    serialize_frontier_code_cache,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import (
    TimeSeriesInspection,
)
from src.application.services.options_225 import (
    OPTIONS_225_SYNTHETIC_INDEX_CATEGORY,
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
    OPTIONS_225_SYNTHETIC_INDEX_NAME,
    OPTIONS_225_SYNTHETIC_INDEX_NAME_EN,
    build_synthetic_underpx_index_rows,
    normalize_options_225_raw_row,
)
from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
    stock_code_candidates,
)
from src.entrypoints.http.schemas.db import SyncResult
from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.ingestion_pipeline import (
    run_ingestion_batch,
    validate_rows_required_fields,
)
from src.application.services.index_master_catalog import (
    build_index_master_seed_rows,
    get_index_catalog_codes,
)
from src.application.services.stock_data_row_builder import build_stock_data_row


class SyncClientLike(Protocol):  # pragma: no cover
    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...


class SyncMarketDbLike(Protocol):  # pragma: no cover
    def get_sync_metadata(self, key: str) -> str | None: ...
    def set_sync_metadata(self, key: str, value: str) -> None: ...
    def upsert_stocks(self, rows: list[dict[str, Any]]) -> Any: ...
    def upsert_stock_master_daily(self, snapshot_date: str, rows: list[dict[str, Any]]) -> Any: ...
    def rebuild_stock_master_intervals(self) -> int: ...
    def rebuild_stocks_latest(self) -> int: ...
    def get_topix_dates(self, *, start_date: str | None = None, end_date: str | None = None) -> list[str]: ...
    def get_missing_stock_master_dates(self, *, limit: int | None = 20) -> list[str]: ...
    def get_fundamentals_target_codes(self) -> set[str]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_stocks_needing_refresh(self, limit: int | None = None) -> list[str]: ...
    def upsert_index_master(self, rows: list[dict[str, Any]]) -> Any: ...
    def get_index_master_codes(self) -> set[str]: ...


class BulkServiceLike(Protocol):  # pragma: no cover
    async def build_plan(
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> BulkFetchPlan: ...

    async def fetch_with_plan(
        self,
        plan: BulkFetchPlan,
        *,
        on_rows_batch: Callable[[list[dict[str, Any]], BulkFileInfo], Awaitable[None]] | None = None,
        accumulate_rows: bool = True,
    ) -> BulkFetchResult: ...


class SyncTimeSeriesStoreLike(Protocol):  # pragma: no cover
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection: ...

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> int: ...
    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...
    def index_indices_data(self) -> None: ...
    def index_options_225_data(self) -> None: ...
    def index_margin_data(self) -> None: ...
    def index_statements(self) -> None: ...


@dataclass
class SyncContext:
    client: SyncClientLike
    market_db: SyncMarketDbLike
    cancelled: asyncio.Event
    on_progress: Callable[[str, int, int, str], None]
    on_fetch_detail: Callable[[dict[str, Any]], None] | None = None
    time_series_store: SyncTimeSeriesStoreLike | None = None
    bulk_service: BulkServiceLike | None = None
    bulk_probe_disabled: bool = False
    bulk_probe_failure_reason: str | None = None
    enforce_bulk_for_stock_data: bool = False


class SyncStrategy(Protocol):  # pragma: no cover
    async def execute(self, ctx: SyncContext) -> SyncResult: ...
    def estimate_api_calls(self) -> int: ...


_JST = ZoneInfo("Asia/Tokyo")
_MAX_FINS_SUMMARY_PAGES = 2000

_FetchMethod = Literal["rest", "bulk"]


@dataclass(frozen=True)
class _StageFetchDecision:
    method: _FetchMethod
    planner_api_calls: int
    estimated_rest_calls: int
    estimated_bulk_calls: int | None
    plan: BulkFetchPlan | None = None
    reason: str = "unspecified"
    reason_detail: str | None = None


class BulkFetchRequiredError(RuntimeError):
    """Raised when stock_data sync requires bulk but planner/execution cannot use it."""


_BULK_STOCK_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "date": "Date",
    "o": "O",
    "open": "O",
    "h": "H",
    "high": "H",
    "l": "L",
    "low": "L",
    "c": "C",
    "close": "C",
    "vo": "Vo",
    "volume": "Vo",
    "adjo": "AdjO",
    "adjopen": "AdjO",
    "adjh": "AdjH",
    "adjhigh": "AdjH",
    "adjl": "AdjL",
    "adjlow": "AdjL",
    "adjc": "AdjC",
    "adjclose": "AdjC",
    "adjvo": "AdjVo",
    "adjvolume": "AdjVo",
    "adjfactor": "AdjFactor",
}

_BULK_INDEX_KEY_ALIASES: dict[str, str] = {
    **_BULK_STOCK_KEY_ALIASES,
    "sectorname": "SectorName",
    "sector_name": "SectorName",
    "indexcode": "Code",
    "index_code": "Code",
}

_BULK_OPTIONS_225_KEY_ALIASES: dict[str, str] = {
    "date": "Date",
    "code": "Code",
    "o": "O",
    "h": "H",
    "l": "L",
    "c": "C",
    "eo": "EO",
    "eh": "EH",
    "el": "EL",
    "ec": "EC",
    "ao": "AO",
    "ah": "AH",
    "al": "AL",
    "ac": "AC",
    "vo": "Vo",
    "oi": "OI",
    "va": "Va",
    "cm": "CM",
    "strike": "Strike",
    "vooa": "VoOA",
    "emmrgntrgdiv": "EmMrgnTrgDiv",
    "pcdiv": "PCDiv",
    "ltd": "LTD",
    "sqd": "SQD",
    "settle": "Settle",
    "theo": "Theo",
    "basevol": "BaseVol",
    "underpx": "UnderPx",
    "iv": "IV",
    "ir": "IR",
}

_BULK_MARGIN_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "date": "Date",
    "longvol": "LongVol",
    "longvolume": "LongVol",
    "longmarginvolume": "LongVol",
    "longmargintradevolume": "LongVol",
    "longmarginoutstandingbalance": "LongVol",
    "long_margin_volume": "LongVol",
    "shrtvol": "ShrtVol",
    "shortvol": "ShrtVol",
    "shortvolume": "ShrtVol",
    "shortmarginvolume": "ShrtVol",
    "shortmargintradevolume": "ShrtVol",
    "shortmarginoutstandingbalance": "ShrtVol",
    "short_margin_volume": "ShrtVol",
}

_BULK_FINS_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "discdate": "DiscDate",
    "eps": "EPS",
    "np": "NP",
    "eq": "Eq",
    "curpertype": "CurPerType",
    "doctype": "DocType",
    "nxfeps": "NxFEPS",
    "bps": "BPS",
    "sales": "Sales",
    "op": "OP",
    "odp": "OdP",
    "cfo": "CFO",
    "divann": "DivAnn",
    "divfy": "DivFY",
    "fdivann": "FDivAnn",
    "fdivfy": "FDivFY",
    "nxfdivann": "NxFDivAnn",
    "nxfdivfy": "NxFDivFY",
    "payoutratioann": "PayoutRatioAnn",
    "fpayoutratioann": "FPayoutRatioAnn",
    "nxfpayoutratioann": "NxFPayoutRatioAnn",
    "feps": "FEPS",
    "cfi": "CFI",
    "cff": "CFF",
    "casheq": "CashEq",
    "ta": "TA",
    "shoutfy": "ShOutFY",
    "trshfy": "TrShFY",
}


def _get_plan_hint(client: SyncClientLike) -> str:
    return str(getattr(client, "plan", "")).strip().lower()


def _get_bulk_service(ctx: SyncContext) -> BulkServiceLike:
    if ctx.bulk_service is None:
        ctx.bulk_service = JQuantsBulkService(ctx.client)
    return ctx.bulk_service


async def _get_paginated_rows_with_call_count(
    client: SyncClientLike,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    get_with_meta = getattr(client, "get_paginated_with_meta", None)
    if callable(get_with_meta):
        get_with_meta_callable = cast(
            Callable[..., Awaitable[tuple[list[dict[str, Any]], int]]],
            get_with_meta,
        )
        rows, calls = await get_with_meta_callable(path, params=params)
        return rows, int(calls)
    rows = await client.get_paginated(path, params=params)
    return rows, 1


def _to_iso_date_text(value: str | None) -> str | None:
    parsed = _parse_date(value or "")
    if parsed is None:
        return None
    return parsed.isoformat()


def _select_bulk_candidates_from_dates(dates: list[str]) -> tuple[str | None, str | None]:
    parsed = [_parse_date(value) for value in dates]
    normalized = [d for d in parsed if d is not None]
    if not normalized:
        return None, None
    return min(normalized).isoformat(), max(normalized).isoformat()


async def _plan_fetch_method(
    ctx: SyncContext,
    *,
    stage: str,
    endpoint: str,
    estimated_rest_calls: int,
    date_from: str | None = None,
    date_to: str | None = None,
    exact_dates: list[str] | None = None,
    min_rest_calls_to_probe_bulk: int = 3,
    require_bulk: bool = False,
) -> _StageFetchDecision:
    if ctx.bulk_probe_disabled:
        plan_hint = _get_plan_hint(ctx.client)
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="bulk_probe_disabled",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="bulk_probe_disabled",
            reason_detail=ctx.bulk_probe_failure_reason,
        )

    if not require_bulk and estimated_rest_calls < min_rest_calls_to_probe_bulk:
        plan_hint = _get_plan_hint(ctx.client)
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="rest_estimate_too_small",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="rest_estimate_too_small",
        )

    bulk_service = _get_bulk_service(ctx)
    plan_hint = _get_plan_hint(ctx.client)
    try:
        plan = await bulk_service.build_plan(
            endpoint=endpoint,
            date_from=date_from,
            date_to=date_to,
            exact_dates=exact_dates,
        )
    except Exception as e:
        # free/unknown plan や一時障害で /bulk/list が失敗した場合は
        # 同期ジョブ全体を止めず、以降は REST に固定して継続する。
        ctx.bulk_probe_disabled = True
        ctx.bulk_probe_failure_reason = _summarize_exception(e)
        logger.warning(
            "sync bulk plan probe failed, falling back to REST for this job: {}",
            e,
        )
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="bulk_probe_failed",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=1,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=1,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="bulk_probe_failed",
            reason_detail=ctx.bulk_probe_failure_reason,
        )

    if require_bulk:
        selected: _FetchMethod = "bulk"
        reason = "bulk_required"
    else:
        selected = "bulk" if plan.estimated_api_calls < estimated_rest_calls else "rest"
        reason = "bulk_estimate_lower" if selected == "bulk" else "rest_estimate_lower_or_equal"

    logger.info(
        "sync fetch strategy selected",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=selected,
        reason=reason,
        estimatedRestCalls=estimated_rest_calls,
        estimatedBulkCalls=plan.estimated_api_calls,
        plannerApiCalls=plan.list_api_calls,
        estimatedCacheHits=plan.estimated_cache_hits,
        estimatedCacheMisses=plan.estimated_cache_misses,
        selectedFiles=len(plan.files),
        planHint=plan_hint or None,
        requireBulk=require_bulk,
    )
    return _StageFetchDecision(
        method=selected,
        planner_api_calls=plan.list_api_calls,
        estimated_rest_calls=estimated_rest_calls,
        estimated_bulk_calls=plan.estimated_api_calls,
        plan=plan,
        reason=reason,
    )


def _canonicalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _normalize_bulk_row_keys(
    rows: list[dict[str, Any]],
    aliases: dict[str, str],
) -> list[dict[str, Any]]:
    if not rows:
        return rows

    # CSV のヘッダは通常全行で共通なので、キー変換は先頭行だけで解決する。
    first_row = rows[0]
    remap: dict[str, str] = {}
    for raw_key in first_row.keys():
        canonical = _canonicalize_key(str(raw_key))
        target = aliases.get(canonical)
        if target and target != raw_key:
            remap[str(raw_key)] = target

    if not remap:
        return rows

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        for raw_key, target in remap.items():
            if _has_non_empty_value(normalized.get(target)):
                continue
            raw_value = row.get(raw_key)
            if raw_value is None:
                continue
            normalized[target] = raw_value
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_bulk_stock_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_STOCK_KEY_ALIASES)


def _normalize_bulk_indices_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_INDEX_KEY_ALIASES)


def _normalize_bulk_options_225_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_OPTIONS_225_KEY_ALIASES)


def _normalize_bulk_margin_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_MARGIN_KEY_ALIASES)


def _normalize_bulk_fins_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_FINS_KEY_ALIASES)


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _normalize_iso_date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    if (
        len(text) == 10
        and text[4] == "-"
        and text[7] == "-"
        and text[:4].isdigit()
        and text[5:7].isdigit()
        and text[8:10].isdigit()
    ):
        try:
            date(int(text[:4]), int(text[5:7]), int(text[8:10]))
        except ValueError:
            return None
        return text

    if len(text) == 8 and text.isdigit():
        try:
            date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"

    return _to_iso_date_text(text)


def _coerce_float_fast(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _coerce_int_fast(value: Any) -> int | None:
    parsed = _coerce_float_fast(value)
    if parsed is None:
        return None
    return int(parsed)


def _collect_sample_code(sample_codes: list[str], code: str) -> None:
    if code in sample_codes:
        return
    if len(sample_codes) >= 5:
        return
    sample_codes.append(code)


def _convert_stock_bulk_rows(
    data: list[dict[str, Any]],
    *,
    target_dates: set[str] | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skipped = 0
    sample_codes: list[str] = []
    seen: set[tuple[str, str]] = set()
    date_cache: dict[str, str | None] = {}
    created_at = datetime.now(UTC).isoformat()

    for row in data:
        code = normalize_stock_code(row.get("Code", row.get("code", "")))
        if not code:
            continue

        raw_date_value = row.get("Date", row.get("date"))
        if isinstance(raw_date_value, date):
            cache_key = raw_date_value.isoformat()
        else:
            cache_key = str(raw_date_value)
        if cache_key in date_cache:
            date_text = date_cache[cache_key]
        else:
            date_text = _normalize_iso_date_text(raw_date_value)
            date_cache[cache_key] = date_text
        if date_text is None:
            skipped += 1
            _collect_sample_code(sample_codes, code)
            continue

        if target_dates is not None and date_text not in target_dates:
            continue

        open_value = _coerce_float_fast(row.get("O", row.get("open")))
        high_value = _coerce_float_fast(row.get("H", row.get("high")))
        low_value = _coerce_float_fast(row.get("L", row.get("low")))
        close_value = _coerce_float_fast(row.get("C", row.get("close")))
        volume_value = _coerce_int_fast(row.get("Vo", row.get("volume")))
        if any(v is None for v in (open_value, high_value, low_value, close_value, volume_value)):
            skipped += 1
            _collect_sample_code(sample_codes, code)
            continue

        row_key = (code, date_text)
        if row_key in seen:
            continue
        seen.add(row_key)

        rows.append(
            {
                "code": code,
                "date": date_text,
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume": volume_value,
                "adjustment_factor": _coerce_float_fast(row.get("AdjFactor")),
                "created_at": created_at,
            }
        )

    if skipped > 0:
        sample = ", ".join(sample_codes) if sample_codes else "unknown"
        logger.warning(
            "Skipped {} daily quotes with incomplete OHLCV data (sample codes: {})",
            skipped,
            sample,
        )
    return rows


def _build_target_date_set(dates: list[str]) -> set[str] | None:
    normalized = {
        date_text
        for date_text in (_normalize_iso_date_text(value) for value in dates)
        if date_text is not None
    }
    return normalized or None


async def _ingest_stock_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    target_dates: set[str] | None,
) -> int:
    normalized_rows = _normalize_bulk_stock_rows(batch_rows)
    rows = _convert_stock_bulk_rows(normalized_rows, target_dates=target_dates)
    if not rows:
        return 0
    return await _publish_stock_data_rows(ctx, rows)


async def _ingest_fins_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    allowed_codes: set[str],
    target_dates: set[str] | None = None,
) -> int:
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
        return 0
    return await _publish_statement_rows(ctx, rows)


async def _ingest_indices_only_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    target_code_set: set[str],
    known_master_codes: set[str],
) -> None:
    rows = validate_rows_required_fields(
        _convert_indices_data_rows(_normalize_bulk_indices_rows(batch_rows), None),
        required_fields=("code", "date"),
        dedupe_keys=("code", "date"),
        stage="indices_data",
    )
    rows = [
        row
        for row in rows
        if _normalize_index_code(row.get("code")) in target_code_set
    ]
    if rows:
        await _upsert_indices_rows_with_master_backfill(
            ctx,
            rows,
            known_master_codes,
        )


async def _ingest_incremental_indices_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    target_code_set: set[str],
    known_master_codes: set[str],
    latest_index_dates: dict[str, str],
    fallback_date_set: set[str],
) -> None:
    rows = validate_rows_required_fields(
        _convert_indices_data_rows(_normalize_bulk_indices_rows(batch_rows), None),
        required_fields=("code", "date"),
        dedupe_keys=("code", "date"),
        stage="indices_data",
    )
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        code = _normalize_index_code(row.get("code"))
        row_date = _to_iso_date_text(str(row.get("date") or ""))
        if not code or row_date is None:
            continue

        include = False
        code_anchor = latest_index_dates.get(code)
        if code in target_code_set:
            if code_anchor is None:
                include = True
            else:
                include = _is_date_after(row_date, code_anchor)

        if not include and row_date in fallback_date_set:
            include = True

        if include:
            filtered_rows.append(row)

    if filtered_rows:
        await _upsert_indices_rows_with_master_backfill(
            ctx,
            filtered_rows,
            known_master_codes,
            discovery_log="Inserted {} discovered index master rows while syncing by bulk.",
        )


def _is_incremental_cold_start(
    inspection: TimeSeriesInspection,
) -> bool:
    has_anchor_signal = bool(
        inspection.topix_max
        or inspection.stock_max
        or inspection.indices_max
        or inspection.margin_max
        or inspection.latest_indices_dates
    )
    if has_anchor_signal:
        return False
    return (
        inspection.topix_count == 0
        and inspection.stock_count == 0
        and inspection.indices_count == 0
        and inspection.margin_count == 0
    )


def _log_sync_fetch_execution(
    *,
    stage: str,
    endpoint: str,
    decision: _StageFetchDecision,
    executed: _FetchMethod,
    actual_api_calls: int,
    fallback: bool,
    bulk_result: BulkFetchResult | None = None,
) -> None:
    cache_hit_rate: float | None = None
    cache_hits = 0
    cache_misses = 0
    if bulk_result is not None:
        cache_hits = bulk_result.cache_hits
        cache_misses = bulk_result.cache_misses
        total = cache_hits + cache_misses
        cache_hit_rate = (cache_hits / total) if total > 0 else None

    logger.info(
        "sync fetch strategy execution",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=decision.method,
        executed=executed,
        fallbackUsed=fallback,
        estimatedRestCalls=decision.estimated_rest_calls,
        estimatedBulkCalls=decision.estimated_bulk_calls,
        plannerApiCalls=decision.planner_api_calls,
        actualApiCalls=actual_api_calls,
        cacheHits=cache_hits,
        cacheMisses=cache_misses,
        cacheHitRate=cache_hit_rate,
    )


def _format_fetch_estimate(value: int | None) -> str:
    return str(value) if value is not None else "n/a"


def _summarize_exception(exc: Exception, *, limit: int = 200) -> str:
    text = str(exc).replace("\n", " ").strip() or exc.__class__.__name__
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _format_target_label(
    count: int,
    unit: str,
    *,
    skipped_empty: int = 0,
    skipped_market: int = 0,
    issuer_alias: int = 0,
) -> str:
    parts = [f"{count} {unit}"]
    if skipped_market > 0:
        parts.append(f"skipped_market={skipped_market}")
    if skipped_empty > 0:
        parts.append(f"skipped_empty={skipped_empty}")
    if issuer_alias > 0:
        parts.append(f"issuer_alias={issuer_alias}")
    return ", ".join(parts)


def _extract_listed_market_target_rows(
    stock_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in stock_rows:
        if not is_listed_market_code(row.get("market_code")):
            continue
        code = normalize_stock_code(str(row.get("code", "")).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(
            {
                "code": code,
                "company_name": str(row.get("company_name", "") or ""),
                "market_code": str(row.get("market_code", "") or ""),
            }
        )
    return deduped


def _load_frontier_code_cache(
    market_db: SyncMarketDbLike,
    key: str,
    frontier: str | None,
) -> set[str]:
    return resolve_frontier_cache_codes(market_db.get_sync_metadata(key), frontier)


async def _save_frontier_code_cache(
    ctx: SyncContext,
    key: str,
    frontier: str | None,
    codes: set[str] | list[str],
) -> None:
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        key,
        serialize_frontier_code_cache(frontier, codes),
    )


def _describe_bulk_unavailable_reason(
    *,
    reason: str,
    reason_detail: str | None = None,
) -> str:
    reason_map = {
        "bulk_probe_disabled": "bulk probe is disabled after a previous probe failure",
        "bulk_probe_failed": "bulk plan probe failed",
        "rest_estimate_too_small": "rest estimate is below bulk probe threshold",
        "rest_estimate_lower_or_equal": "planner selected REST based on API-call estimate",
        "bulk_plan_missing": "bulk plan is missing",
        "bulk_plan_empty": "bulk/list returned no matching files for requested dates",
        "bulk_fetch_failed": "bulk fetch execution failed",
    }
    base = reason_map.get(reason, f"bulk unavailable ({reason})")
    if not reason_detail:
        return base
    return f"{base}: {reason_detail}"


def _raise_stock_bulk_required_error(
    ctx: SyncContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    reason: str,
    reason_detail: str | None = None,
) -> NoReturn:
    detail = _describe_bulk_unavailable_reason(reason=reason, reason_detail=reason_detail)
    message = (
        f"Bulk fetch required for {endpoint} but unavailable ({detail}). "
        "REST fallback is disabled for stock_data sync."
    )
    ctx.on_progress(progress_stage, current, total, message)
    raise BulkFetchRequiredError(message)


def _enforce_stock_bulk_plan_available(
    ctx: SyncContext,
    *,
    decision: _StageFetchDecision,
    endpoint: str,
    progress_stage: str,
    current: int,
    total: int,
    target_count: int,
) -> None:
    if not ctx.enforce_bulk_for_stock_data or target_count <= 0:
        return

    if decision.method != "bulk":
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason=decision.reason,
            reason_detail=decision.reason_detail,
        )

    if decision.plan is None:
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason="bulk_plan_missing",
        )

    if len(decision.plan.files) == 0:
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason="bulk_plan_empty",
            reason_detail=f"targets={target_count} dates",
        )


def _resolve_bulk_fallback_reason(plan: BulkFetchPlan | None) -> str:
    if plan is None:
        return "bulk_plan_missing"
    if len(plan.files) == 0:
        return "bulk_plan_empty"
    return "bulk_plan_unavailable"


def _emit_fetch_strategy_progress(
    ctx: SyncContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    decision: _StageFetchDecision,
    target_label: str | None = None,
) -> None:
    target_text = f", targets={target_label}" if target_label else ""
    ctx.on_progress(
        progress_stage,
        current,
        total,
        (
            f"Fetch strategy: {endpoint} -> {decision.method.upper()} "
            f"(REST est={decision.estimated_rest_calls}, "
            f"BULK est={_format_fetch_estimate(decision.estimated_bulk_calls)}{target_text})"
        ),
    )
    _emit_fetch_detail(
        ctx,
        {
            "eventType": "strategy",
            "stage": progress_stage,
            "endpoint": endpoint,
            "method": decision.method,
            "targetLabel": target_label,
            "reason": decision.reason,
            "reasonDetail": decision.reason_detail,
            "estimatedRestCalls": decision.estimated_rest_calls,
            "estimatedBulkCalls": decision.estimated_bulk_calls,
            "plannerApiCalls": decision.planner_api_calls,
            "fallback": False,
            "fallbackReason": None,
        },
    )


def _emit_fetch_execution_progress(
    ctx: SyncContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    method: _FetchMethod,
    target_label: str | None = None,
    fallback: bool = False,
    fallback_reason: str | None = None,
) -> None:
    target_text = f", targets={target_label}" if target_label else ""
    fallback_text = ""
    if fallback:
        fallback_text = (
            f" (bulk fallback: {fallback_reason})"
            if fallback_reason
            else " (bulk fallback)"
        )
    ctx.on_progress(
        progress_stage,
        current,
        total,
        f"Fetching {endpoint} via {method.upper()}{fallback_text}{target_text}...",
    )
    _emit_fetch_detail(
        ctx,
        {
            "eventType": "execution",
            "stage": progress_stage,
            "endpoint": endpoint,
            "method": method,
            "targetLabel": target_label,
            "reason": None,
            "reasonDetail": None,
            "estimatedRestCalls": None,
            "estimatedBulkCalls": None,
            "plannerApiCalls": None,
            "fallback": fallback,
            "fallbackReason": fallback_reason,
        },
    )


def _emit_fetch_detail(ctx: SyncContext, detail: dict[str, Any]) -> None:
    if ctx.on_fetch_detail is None:
        return
    try:
        ctx.on_fetch_detail(detail)
    except Exception as e:  # noqa: BLE001 - fetch detail failures should not abort sync
        logger.warning("Failed to emit sync fetch detail: {}", e)


class IndicesOnlySyncStrategy:
    """指数同期: 指数マスタ + 指数データ + optional N225 options sync."""

    def __init__(self, *, include_options: bool = True) -> None:
        self._include_options = include_options

    def estimate_api_calls(self) -> int:
        return 70 if self._include_options else 52

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        errors: list[str] = []
        total_steps = 3 if self._include_options else 2

        try:
            # 1. 指数マスタ（ローカルカタログ）を補完
            ctx.on_progress("indices_master", 0, total_steps, "Syncing index master catalog...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            target_codes = sorted(get_index_catalog_codes() | known_master_codes)
            target_code_set = {_normalize_index_code(code) for code in target_codes}

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, total_steps, f"Fetching data for {len(target_codes)} indices...")
            decision = await _plan_fetch_method(
                ctx,
                stage="indices_data",
                endpoint="/indices/bars/daily",
                estimated_rest_calls=len(target_codes),
            )
            total_calls += decision.planner_api_calls
            _emit_fetch_strategy_progress(
                ctx,
                progress_stage="indices_data",
                current=1,
                total=total_steps,
                endpoint="/indices/bars/daily",
                decision=decision,
                target_label=f"{len(target_codes)} codes",
            )

            used_rest_fallback = False
            stage_api_calls = 0
            bulk_result: BulkFetchResult | None = None
            if decision.method == "bulk" and decision.plan is not None:
                try:
                    _emit_fetch_execution_progress(
                        ctx,
                        progress_stage="indices_data",
                        current=1,
                        total=total_steps,
                        endpoint="/indices/bars/daily",
                        method="bulk",
                        target_label=f"{len(target_codes)} codes",
                    )

                    async def _consume_indices_only_bulk_rows(
                        batch_rows: list[dict[str, Any]],
                        _file_info: BulkFileInfo,
                    ) -> None:
                        await _ingest_indices_only_bulk_batch(
                            ctx,
                            batch_rows=batch_rows,
                            target_code_set=target_code_set,
                            known_master_codes=known_master_codes,
                        )

                    bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                        decision.plan,
                        on_rows_batch=_consume_indices_only_bulk_rows,
                        accumulate_rows=False,
                    )
                    total_calls += bulk_result.api_calls
                    stage_api_calls += bulk_result.api_calls
                    _log_sync_fetch_execution(
                        stage="indices_data",
                        endpoint="/indices/bars/daily",
                        decision=decision,
                        executed="bulk",
                        actual_api_calls=stage_api_calls,
                        fallback=False,
                        bulk_result=bulk_result,
                    )
                except Exception as e:
                    used_rest_fallback = True
                    logger.warning("indices-only bulk fetch failed, falling back to REST: {}", e)

            if decision.method == "rest" or used_rest_fallback:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="indices_data",
                    current=1,
                    total=total_steps,
                    endpoint="/indices/bars/daily",
                    method="rest",
                    target_label=f"{len(target_codes)} codes",
                    fallback=used_rest_fallback,
                )
                for i, code in enumerate(target_codes, start=1):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if i > 1 and i % 50 == 0:
                        ctx.on_progress(
                            "indices_data",
                            1,
                            total_steps,
                            f"Fetching /indices/bars/daily via REST: {i}/{len(target_codes)} codes...",
                        )
                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params={"code": code},
                        )
                        total_calls += page_calls
                        stage_api_calls += page_calls
                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, code),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
                        if rows:
                            await _upsert_indices_rows_with_master_backfill(
                                ctx,
                                rows,
                                known_master_codes,
                            )
                    except Exception as e:
                        errors.append(f"Index {code}: {e}")
                        logger.warning(f"Index {code} sync error: {e}")
                _log_sync_fetch_execution(
                    stage="indices_data",
                    endpoint="/indices/bars/daily",
                    decision=decision,
                    executed="rest",
                    actual_api_calls=stage_api_calls,
                    fallback=used_rest_fallback,
                    bulk_result=bulk_result,
                )

            if not self._include_options:
                await _index_indices_rows(ctx)
                return SyncResult(
                    success=len(errors) == 0,
                    totalApiCalls=total_calls,
                    errors=errors,
                )

            options_dates = await asyncio.to_thread(ctx.market_db.get_topix_dates)
            ctx.on_progress("options_225", 2, total_steps, f"Fetching N225 options for {len(options_dates)} dates...")
            options_result = await _sync_options_225_dates(
                ctx,
                date_targets=options_dates,
                progress_stage="options_225",
                progress_current=2,
                progress_total=total_steps,
                stage_name="options_225_indices_only",
            )
            total_calls += int(options_result["api_calls"])
            errors.extend(cast(list[str], options_result["errors"]))
            if bool(options_result["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class InitialSyncStrategy:
    """初回同期: TOPIX + 全銘柄 + 株価データ + 指数データ"""

    def estimate_api_calls(self) -> int:
        # TOPIX/株価/指数/信用残に加えて Prime 全銘柄の /fins/summary(code=...) を含む。
        return 3200

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        dates_processed = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        failed_dates: list[str] = []
        errors: list[str] = []
        stock_rows: list[dict[str, Any]] = []

        try:
            # Step 1: TOPIX
            ctx.on_progress("topix", 0, 8, "Fetching TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            topix_data_raw, topix_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily/topix",
            )

            async def _prefetched_topix_rows() -> list[dict[str, Any]]:
                return topix_data_raw

            topix_batch = await run_ingestion_batch(
                stage="topix",
                fetch=_prefetched_topix_rows,
                normalize=_convert_topix_rows,
                validate=lambda rows: validate_rows_required_fields(
                    rows,
                    required_fields=("date", "open", "high", "low", "close"),
                    dedupe_keys=("date",),
                    stage="topix",
                ),
                publish=lambda rows: _publish_topix_rows(ctx, rows),
                index=lambda _rows: _index_topix_rows(ctx),
            )
            total_calls += topix_calls
            topix_rows = topix_batch.rows
            dates_processed = len(topix_rows)

            # Step 2: 日次銘柄マスタ（TOPIX 取引日ベース）
            ctx.on_progress("stocks", 1, 8, "Fetching daily stock master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            trading_dates = sorted({r["date"] for r in topix_rows})
            master_sync = await _sync_daily_stock_master(
                ctx,
                target_dates=trading_dates,
                progress_current=1,
                progress_total=8,
            )
            total_calls += master_sync["api_calls"]
            stock_rows = master_sync["latest_rows"]
            errors.extend(master_sync["errors"])
            if master_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 3: listed markets fundamentals（初回: code指定フル取得）
            ctx.on_progress("fundamentals", 2, 8, "Fetching listed-market fundamentals (full)...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            fundamentals_target_rows = _extract_listed_market_target_rows(stock_rows)
            if not fundamentals_target_rows:
                fundamentals_target_rows = await asyncio.to_thread(
                    ctx.market_db.get_fundamentals_target_stock_rows
                )

            fundamentals_sync = await _sync_fundamentals_initial(
                ctx,
                fundamentals_target_rows,
                progress_current=2,
                progress_total=8,
            )
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 4: 株価データ（日付ベース、TOPIX 日付を使用）
            ctx.on_progress("stock_data", 3, 8, "Fetching daily stock prices...")
            from_date, to_date = _select_bulk_candidates_from_dates(trading_dates)
            decision = await _plan_fetch_method(
                ctx,
                stage="stock_data_initial",
                endpoint="/equities/bars/daily",
                estimated_rest_calls=max(len(trading_dates), 1),
                date_from=from_date,
                date_to=to_date,
                exact_dates=trading_dates,
                require_bulk=ctx.enforce_bulk_for_stock_data,
            )
            total_calls += decision.planner_api_calls
            _emit_fetch_strategy_progress(
                ctx,
                progress_stage="stock_data",
                current=3,
                total=8,
                endpoint="/equities/bars/daily",
                decision=decision,
                target_label=f"{len(trading_dates)} dates",
            )

            _enforce_stock_bulk_plan_available(
                ctx,
                decision=decision,
                endpoint="/equities/bars/daily",
                progress_stage="stock_data",
                current=3,
                total=8,
                target_count=len(trading_dates),
            )

            used_rest_fallback = False
            stock_bulk_fallback_reason: str | None = None
            stage_api_calls = 0
            bulk_result: BulkFetchResult | None = None
            if decision.method == "bulk" and decision.plan is not None:
                try:
                    _emit_fetch_execution_progress(
                        ctx,
                        progress_stage="stock_data",
                        current=3,
                        total=8,
                        endpoint="/equities/bars/daily",
                        method="bulk",
                        target_label=f"{len(trading_dates)} dates",
                    )
                    trading_date_set = _build_target_date_set(trading_dates)

                    async def _consume_stock_bulk_rows(
                        batch_rows: list[dict[str, Any]],
                        _file_info: BulkFileInfo,
                    ) -> None:
                        nonlocal stocks_updated
                        stocks_updated += await _ingest_stock_bulk_batch(
                            ctx,
                            batch_rows=batch_rows,
                            target_dates=trading_date_set,
                        )

                    bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                        decision.plan,
                        on_rows_batch=_consume_stock_bulk_rows,
                        accumulate_rows=False,
                    )
                    total_calls += bulk_result.api_calls
                    stage_api_calls += bulk_result.api_calls
                    _log_sync_fetch_execution(
                        stage="stock_data_initial",
                        endpoint="/equities/bars/daily",
                        decision=decision,
                        executed="bulk",
                        actual_api_calls=stage_api_calls,
                        fallback=False,
                        bulk_result=bulk_result,
                    )
                except Exception as e:
                    if ctx.enforce_bulk_for_stock_data and len(trading_dates) > 0:
                        _raise_stock_bulk_required_error(
                            ctx,
                            progress_stage="stock_data",
                            current=3,
                            total=8,
                            endpoint="/equities/bars/daily",
                            reason="bulk_fetch_failed",
                            reason_detail=_summarize_exception(e),
                        )
                    used_rest_fallback = True
                    stock_bulk_fallback_reason = _summarize_exception(e)
                    logger.exception(
                        "Initial stock_data bulk fetch failed, falling back to REST: {}",
                        stock_bulk_fallback_reason,
                    )

            if decision.method == "rest" or used_rest_fallback:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="stock_data",
                    current=3,
                    total=8,
                    endpoint="/equities/bars/daily",
                    method="rest",
                    target_label=f"{len(trading_dates)} dates",
                    fallback=used_rest_fallback,
                    fallback_reason=stock_bulk_fallback_reason,
                )
                consecutive_failures = 0
                for i, date in enumerate(trading_dates):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if i % 50 == 0:
                        ctx.on_progress(
                            "stock_data",
                            3,
                            8,
                            f"Fetching /equities/bars/daily via REST: {i}/{len(trading_dates)} dates...",
                        )
                    try:
                        payload, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/equities/bars/daily",
                            params={"date": date},
                        )
                        total_calls += page_calls
                        stage_api_calls += page_calls

                        async def _prefetched_stock_rows() -> list[dict[str, Any]]:
                            return payload

                        batch = await run_ingestion_batch(
                            stage="stock_data",
                            fetch=_prefetched_stock_rows,
                            normalize=_convert_stock_data_rows,
                            validate=lambda rows: validate_rows_required_fields(
                                rows,
                                required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                                dedupe_keys=("code", "date"),
                                stage="stock_data",
                            ),
                            publish=lambda rows: _publish_stock_data_rows(ctx, rows),
                        )
                        stocks_updated += batch.published_count
                        consecutive_failures = 0
                    except Exception:
                        failed_dates.append(date)
                        consecutive_failures += 1
                        if consecutive_failures >= 5:
                            errors.append(f"Too many consecutive failures at {date}")
                            break
                _log_sync_fetch_execution(
                    stage="stock_data_initial",
                    endpoint="/equities/bars/daily",
                    decision=decision,
                    executed="rest",
                    actual_api_calls=stage_api_calls,
                    fallback=used_rest_fallback,
                    bulk_result=bulk_result,
                )

            await _index_stock_data_rows(ctx)

            # Step 5: 指数データ
            ctx.on_progress("indices", 4, 8, "Fetching index data...")
            indices_strategy = IndicesOnlySyncStrategy(include_options=False)
            indices_result = await indices_strategy.execute(ctx)
            total_calls += indices_result.totalApiCalls
            errors.extend(indices_result.errors)

            # Step 6: N225 options data
            ctx.on_progress("options_225", 5, 8, f"Fetching N225 options for {len(trading_dates)} dates...")
            options_sync = await _sync_options_225_dates(
                ctx,
                date_targets=trading_dates,
                progress_stage="options_225",
                progress_current=5,
                progress_total=8,
                stage_name="options_225_initial",
            )
            total_calls += int(options_sync["api_calls"])
            errors.extend(cast(list[str], options_sync["errors"]))
            if bool(options_sync["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 7: 信用残データ
            ctx.on_progress("margin", 6, 8, "Fetching margin data...")
            all_stock_codes = _collect_unique_codes(
                [str(row.get("code", "")) for row in stock_rows if row.get("code")]
            )
            margin_target_codes = extract_listed_market_codes(stock_rows)
            margin_frontier = (
                _latest_date([str(row.get("date")) for row in topix_rows if row.get("date")])
                or ctx.market_db.get_latest_trading_date()
                or ctx.market_db.get_latest_stock_data_date()
                or ctx.market_db.get_latest_margin_date()
            )
            margin_sync = await _sync_margin_data(
                ctx,
                margin_target_codes,
                progress_current=6,
                progress_total=8,
                stage_name="margin_initial",
                trading_frontier=margin_frontier,
                skipped_market_count=max(len(all_stock_codes) - len(margin_target_codes), 0),
            )
            total_calls += margin_sync["api_calls"]
            errors.extend(margin_sync["errors"])
            if margin_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 8: メタデータ更新
            ctx.on_progress("finalize", 7, 8, "Finalizing sync...")
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["INIT_COMPLETED"], "true")
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)
            if failed_dates:
                await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FAILED_DATES"], json.dumps(failed_dates))
            else:
                await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FAILED_DATES"], "[]")

            ctx.on_progress("complete", 8, 8, "Sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=dates_processed,
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                failedDates=failed_dates,
                errors=errors,
            )
        except BulkFetchRequiredError:
            raise
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class IncrementalSyncStrategy:
    """増分同期: 最終同期日以降のデータのみ取得"""

    def estimate_api_calls(self) -> int:
        return 180

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        errors: list[str] = []
        stock_rows: list[dict[str, Any]] = []

        try:
            if not ctx.market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"]):
                logger.warning(
                    "LAST_SYNC_DATE metadata is missing; proceeding incremental sync with DuckDB inspection anchors",
                    event="sync_fetch_strategy",
                    stage="incremental_bootstrap",
                    selected="incremental",
                    reason="missing_last_sync_metadata",
                )

            # Step 1: TOPIX（増分）
            ctx.on_progress("topix", 0, 7, "Fetching incremental TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # NOTE:
            # stock_data が一部日付で取りこぼされると topix_data より遅れる場合があるため、
            # 増分同期の基準日は stock_data の最新日を優先する。
            inspection = _inspect_time_series(ctx)
            cold_start_bootstrap = _is_incremental_cold_start(inspection)
            last_topix_date = inspection.topix_max
            last_stock_date = inspection.stock_max
            last_date = last_stock_date or last_topix_date
            if cold_start_bootstrap:
                logger.info(
                    "Incremental sync detected empty time-series SoT; switching to bootstrap path",
                    event="sync_fetch_strategy",
                    stage="incremental_bootstrap",
                    selected="bootstrap",
                    reason="empty_timeseries",
                )
                last_date = None
            params: dict[str, Any] = {}
            if last_date:
                # J-Quants は YYYYMMDD 形式が安定しているため、既存データ形式（YYYY-MM-DD / YYYYMMDD）を吸収する
                params["from"] = _to_jquants_date_param(last_date)

            topix_payload, topix_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily/topix",
                params=params,
            )

            async def _prefetched_incremental_topix_rows() -> list[dict[str, Any]]:
                return topix_payload

            topix_batch = await run_ingestion_batch(
                stage="topix",
                fetch=_prefetched_incremental_topix_rows,
                normalize=_convert_topix_rows,
                validate=lambda rows: validate_rows_required_fields(
                    rows,
                    required_fields=("date", "open", "high", "low", "close"),
                    dedupe_keys=("date",),
                    stage="topix",
                ),
                publish=lambda rows: _publish_topix_rows(ctx, rows),
                index=lambda _rows: _index_topix_rows(ctx),
            )
            total_calls += topix_calls
            topix_rows = topix_batch.rows

            # Step 2: 日次銘柄マスタ更新
            ctx.on_progress("stocks", 1, 7, "Updating daily stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            missing_master_dates = await asyncio.to_thread(
                ctx.market_db.get_missing_stock_master_dates,
                limit=None,
            )
            master_sync = await _sync_daily_stock_master(
                ctx,
                target_dates=missing_master_dates,
                progress_current=1,
                progress_total=7,
            )
            total_calls += master_sync["api_calls"]
            stock_rows = master_sync["latest_rows"]
            errors.extend(master_sync["errors"])
            if master_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 3: 新しい日付の株価データ
            ctx.on_progress("stock_data", 2, 7, "Fetching new stock data...")
            stock_target_dates = await _resolve_incremental_stock_date_targets(
                ctx,
                topix_rows=topix_rows,
                anchor=last_date,
            )
            from_date_new, to_date_new = _select_bulk_candidates_from_dates(stock_target_dates)
            decision_stock_data = await _plan_fetch_method(
                ctx,
                stage="stock_data_incremental",
                endpoint="/equities/bars/daily",
                estimated_rest_calls=max(len(stock_target_dates), 1),
                date_from=from_date_new,
                date_to=to_date_new,
                exact_dates=stock_target_dates,
                require_bulk=ctx.enforce_bulk_for_stock_data,
            )
            total_calls += decision_stock_data.planner_api_calls
            _emit_fetch_strategy_progress(
                ctx,
                progress_stage="stock_data",
                current=2,
                total=7,
                endpoint="/equities/bars/daily",
                decision=decision_stock_data,
                target_label=f"{len(stock_target_dates)} dates",
            )

            _enforce_stock_bulk_plan_available(
                ctx,
                decision=decision_stock_data,
                endpoint="/equities/bars/daily",
                progress_stage="stock_data",
                current=2,
                total=7,
                target_count=len(stock_target_dates),
            )

            used_stock_rest_fallback = False
            stock_bulk_fallback_reason: str | None = None
            stock_stage_api_calls = 0
            stock_bulk_result: BulkFetchResult | None = None
            if decision_stock_data.method == "bulk" and decision_stock_data.plan is not None:
                try:
                    _emit_fetch_execution_progress(
                        ctx,
                        progress_stage="stock_data",
                        current=2,
                        total=7,
                        endpoint="/equities/bars/daily",
                        method="bulk",
                        target_label=f"{len(stock_target_dates)} dates",
                    )
                    new_date_set = _build_target_date_set(stock_target_dates)

                    async def _consume_incremental_stock_bulk_rows(
                        batch_rows: list[dict[str, Any]],
                        _file_info: BulkFileInfo,
                    ) -> None:
                        nonlocal stocks_updated
                        stocks_updated += await _ingest_stock_bulk_batch(
                            ctx,
                            batch_rows=batch_rows,
                            target_dates=new_date_set,
                        )

                    stock_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                        decision_stock_data.plan,
                        on_rows_batch=_consume_incremental_stock_bulk_rows,
                        accumulate_rows=False,
                    )
                    total_calls += stock_bulk_result.api_calls
                    stock_stage_api_calls += stock_bulk_result.api_calls
                    _log_sync_fetch_execution(
                        stage="stock_data_incremental",
                        endpoint="/equities/bars/daily",
                        decision=decision_stock_data,
                        executed="bulk",
                        actual_api_calls=stock_stage_api_calls,
                        fallback=False,
                        bulk_result=stock_bulk_result,
                    )
                except Exception as e:
                    if ctx.enforce_bulk_for_stock_data and len(stock_target_dates) > 0:
                        _raise_stock_bulk_required_error(
                            ctx,
                            progress_stage="stock_data",
                            current=2,
                            total=7,
                            endpoint="/equities/bars/daily",
                            reason="bulk_fetch_failed",
                            reason_detail=_summarize_exception(e),
                        )
                    used_stock_rest_fallback = True
                    stock_bulk_fallback_reason = _summarize_exception(e)
                    logger.exception(
                        "Incremental stock_data bulk fetch failed, falling back to REST: {}",
                        stock_bulk_fallback_reason,
                    )

            if decision_stock_data.method == "rest" or used_stock_rest_fallback:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="stock_data",
                    current=2,
                    total=7,
                    endpoint="/equities/bars/daily",
                    method="rest",
                    target_label=f"{len(stock_target_dates)} dates",
                    fallback=used_stock_rest_fallback,
                    fallback_reason=stock_bulk_fallback_reason,
                )
                for i, date in enumerate(stock_target_dates, start=1):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if i > 1 and i % 50 == 0:
                        ctx.on_progress(
                            "stock_data",
                            2,
                            7,
                            f"Fetching /equities/bars/daily via REST: {i}/{len(stock_target_dates)} dates...",
                        )
                    try:
                        payload, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/equities/bars/daily",
                            params={"date": date},
                        )
                        total_calls += page_calls
                        stock_stage_api_calls += page_calls

                        async def _prefetched_new_date_rows() -> list[dict[str, Any]]:
                            return payload

                        batch = await run_ingestion_batch(
                            stage="stock_data",
                            fetch=_prefetched_new_date_rows,
                            normalize=_convert_stock_data_rows,
                            validate=lambda rows: validate_rows_required_fields(
                                rows,
                                required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                                dedupe_keys=("code", "date"),
                                stage="stock_data",
                            ),
                            publish=lambda rows: _publish_stock_data_rows(ctx, rows),
                        )
                        stocks_updated += batch.published_count
                    except Exception as e:
                        errors.append(f"Date {date}: {e}")
                _log_sync_fetch_execution(
                    stage="stock_data_incremental",
                    endpoint="/equities/bars/daily",
                    decision=decision_stock_data,
                    executed="rest",
                    actual_api_calls=stock_stage_api_calls,
                    fallback=used_stock_rest_fallback,
                    bulk_result=stock_bulk_result,
                )

            await _index_stock_data_rows(ctx)

            # Step 4: 指数データ（増分）
            ctx.on_progress("indices", 3, 7, "Fetching incremental index data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            raw_latest_index_dates = dict(inspection.latest_indices_dates)
            latest_index_dates = {
                _normalize_index_code(code): value
                for code, value in raw_latest_index_dates.items()
                if _normalize_index_code(code)
            }
            target_codes = sorted(
                get_index_catalog_codes()
                | set(latest_index_dates.keys())
                | known_master_codes
            )
            target_code_set = {_normalize_index_code(code) for code in target_codes}

            # code 指定同期の補完として、日付指定で新規コードを探索する。
            latest_index_date = _latest_date(list(latest_index_dates.values()))
            fallback_dates = _extract_dates_after(
                topix_rows,
                latest_index_date,
                include_anchor=True,
            )

            # indices_data が遅れている場合、topix を indices 側アンカーで再取得して候補日を補完する。
            if (
                latest_index_date
                and last_date
                and _is_date_after(last_date, latest_index_date)
            ):
                topix_for_indices, topix_for_indices_calls = await _get_paginated_rows_with_call_count(
                    ctx.client,
                    "/indices/bars/daily/topix",
                    params={"from": _to_jquants_date_param(latest_index_date)},
                )
                total_calls += topix_for_indices_calls
                topix_dates = [
                    {"date": d.get("Date", "")}
                    for d in topix_for_indices
                    if d.get("Date")
                ]
                fallback_dates = sorted(
                    set(fallback_dates) | set(
                        _extract_dates_after(topix_dates, latest_index_date, include_anchor=True)
                    ),
                    key=_date_sort_key,
                )

            all_code_has_anchor = all(latest_index_dates.get(_normalize_index_code(code)) for code in target_codes)
            decision_indices = await _plan_fetch_method(
                ctx,
                stage="indices_incremental",
                endpoint="/indices/bars/daily",
                estimated_rest_calls=max(len(target_codes) + len(fallback_dates), 1),
                date_from=latest_index_date if all_code_has_anchor else None,
            )
            total_calls += decision_indices.planner_api_calls
            _emit_fetch_strategy_progress(
                ctx,
                progress_stage="indices",
                current=3,
                total=7,
                endpoint="/indices/bars/daily",
                decision=decision_indices,
                target_label=f"{len(target_codes)} codes + {len(fallback_dates)} dates",
            )

            used_indices_rest_fallback = False
            indices_stage_api_calls = 0
            indices_bulk_result: BulkFetchResult | None = None
            if decision_indices.method == "bulk" and decision_indices.plan is not None:
                try:
                    fallback_date_set = {
                        normalized
                        for normalized in (_to_iso_date_text(value) for value in fallback_dates)
                        if normalized is not None
                    }
                    _emit_fetch_execution_progress(
                        ctx,
                        progress_stage="indices",
                        current=3,
                        total=7,
                        endpoint="/indices/bars/daily",
                        method="bulk",
                        target_label=f"{len(target_codes)} codes + {len(fallback_dates)} dates",
                    )

                    async def _consume_incremental_indices_bulk_rows(
                        batch_rows: list[dict[str, Any]],
                        _file_info: BulkFileInfo,
                    ) -> None:
                        await _ingest_incremental_indices_bulk_batch(
                            ctx,
                            batch_rows=batch_rows,
                            target_code_set=target_code_set,
                            known_master_codes=known_master_codes,
                            latest_index_dates=latest_index_dates,
                            fallback_date_set=fallback_date_set,
                        )

                    indices_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                        decision_indices.plan,
                        on_rows_batch=_consume_incremental_indices_bulk_rows,
                        accumulate_rows=False,
                    )
                    total_calls += indices_bulk_result.api_calls
                    indices_stage_api_calls += indices_bulk_result.api_calls
                    _log_sync_fetch_execution(
                        stage="indices_incremental",
                        endpoint="/indices/bars/daily",
                        decision=decision_indices,
                        executed="bulk",
                        actual_api_calls=indices_stage_api_calls,
                        fallback=False,
                        bulk_result=indices_bulk_result,
                    )
                except Exception as e:
                    used_indices_rest_fallback = True
                    logger.warning("Incremental indices bulk fetch failed, falling back to REST: {}", e)

            if decision_indices.method == "rest" or used_indices_rest_fallback:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="indices",
                    current=3,
                    total=7,
                    endpoint="/indices/bars/daily",
                    method="rest",
                    target_label=f"{len(target_codes)} codes + {len(fallback_dates)} dates",
                    fallback=used_indices_rest_fallback,
                )
                for code_idx, code in enumerate(target_codes, start=1):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if code_idx > 1 and code_idx % 50 == 0:
                        ctx.on_progress(
                            "indices",
                            3,
                            7,
                            f"Fetching /indices/bars/daily via REST: {code_idx}/{len(target_codes)} codes...",
                        )

                    params: dict[str, Any] = {"code": code}
                    normalized_code = _normalize_index_code(code)
                    last_index_date = latest_index_dates.get(normalized_code)
                    if last_index_date:
                        params["from"] = _to_jquants_date_param(last_index_date)

                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params=params,
                        )
                        total_calls += page_calls
                        indices_stage_api_calls += page_calls

                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, code),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
                        if last_index_date:
                            rows = [r for r in rows if _is_date_after(r["date"], last_index_date)]

                        if rows:
                            await _upsert_indices_rows_with_master_backfill(
                                ctx,
                                rows,
                                known_master_codes,
                                discovery_log="Inserted {} discovered index master rows while syncing by code.",
                            )
                    except Exception as e:
                        errors.append(f"Index {code}: {e}")
                        logger.warning(f"Index {code} incremental sync error: {e}")

                for date_idx, index_date in enumerate(fallback_dates, start=1):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if date_idx > 1 and date_idx % 50 == 0:
                        ctx.on_progress(
                            "indices",
                            3,
                            7,
                            f"Fetching /indices/bars/daily via REST: {date_idx}/{len(fallback_dates)} dates...",
                        )

                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params={"date": _to_jquants_date_param(index_date)},
                        )
                        total_calls += page_calls
                        indices_stage_api_calls += page_calls
                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, None),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
                        if rows:
                            await _upsert_indices_rows_with_master_backfill(
                                ctx,
                                rows,
                                known_master_codes,
                                discovery_log="Inserted {} discovered index master rows while syncing by date.",
                            )
                    except Exception as e:
                        errors.append(f"Index date {index_date}: {e}")
                        logger.warning("Index date {} incremental sync error: {}", index_date, e)
                _log_sync_fetch_execution(
                    stage="indices_incremental",
                    endpoint="/indices/bars/daily",
                    decision=decision_indices,
                    executed="rest",
                    actual_api_calls=indices_stage_api_calls,
                    fallback=used_indices_rest_fallback,
                    bulk_result=indices_bulk_result,
                )

            await _index_indices_rows(ctx)

            # Step 5: N225 options（増分 + 欠損履歴補完）
            options_new_dates = await _resolve_incremental_options_date_targets(
                ctx,
                inspection=inspection,
                topix_rows=topix_rows,
            )
            ctx.on_progress("options_225", 4, 7, f"Fetching N225 options for {len(options_new_dates)} dates...")
            options_sync = await _sync_options_225_dates(
                ctx,
                date_targets=options_new_dates,
                progress_stage="options_225",
                progress_current=4,
                progress_total=7,
                stage_name="options_225_incremental",
            )
            total_calls += int(options_sync["api_calls"])
            errors.extend(cast(list[str], options_sync["errors"]))
            if bool(options_sync["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 6: listed markets fundamentals（増分: date 指定 + 欠損補完）
            ctx.on_progress("fundamentals", 5, 7, "Fetching incremental listed-market fundamentals...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            fundamentals_target_rows = _extract_listed_market_target_rows(stock_rows)
            if not fundamentals_target_rows:
                fundamentals_target_rows = await asyncio.to_thread(
                    ctx.market_db.get_fundamentals_target_stock_rows
                )

            fundamentals_sync = await _sync_fundamentals_incremental(
                ctx,
                fundamentals_target_rows,
                progress_current=5,
                progress_total=7,
            )
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 7: 信用残データ（増分 + 欠損コードバックフィル）
            ctx.on_progress("margin", 6, 7, "Fetching incremental margin data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            all_stock_codes = _collect_unique_codes(
                [str(row.get("code", "")) for row in stock_rows if row.get("code")]
            )
            margin_target_codes = extract_listed_market_codes(stock_rows)
            margin_frontier = (
                _latest_date([str(row.get("date")) for row in topix_rows if row.get("date")])
                or inspection.topix_max
                or inspection.stock_max
                or inspection.margin_max
            )
            margin_sync = await _sync_margin_data(
                ctx,
                margin_target_codes,
                progress_current=6,
                progress_total=7,
                stage_name="margin_incremental",
                anchor=inspection.margin_max,
                existing_margin_codes=set(inspection.margin_codes),
                trading_frontier=margin_frontier,
                skipped_market_count=max(len(all_stock_codes) - len(margin_target_codes), 0),
            )
            total_calls += margin_sync["api_calls"]
            errors.extend(margin_sync["errors"])
            if margin_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # メタデータ更新
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)

            ctx.on_progress("complete", 7, 7, "Incremental sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=len(stock_target_dates),
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                errors=errors,
            )
        except BulkFetchRequiredError:
            raise
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class RepairSyncStrategy:
    """warning 解消向け: fundamentals / metadata warnings の backfill"""

    def estimate_api_calls(self) -> int:
        return 200

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        errors: list[str] = []

        try:
            ctx.on_progress("repair", 0, 200, "Inspecting DuckDB warning repair targets...")
            if ctx.cancelled.is_set():
                return _cancelled_sync_result(total_calls)

            fundamentals_target_rows = await asyncio.to_thread(
                ctx.market_db.get_fundamentals_target_stock_rows
            )

            if fundamentals_target_rows:
                def _on_fundamentals_progress(stage: str, current: int, total: int, message: str) -> None:
                    ctx.on_progress(
                        stage,
                        _scale_repair_progress(current, total, base=0),
                        200,
                        message,
                    )

                fundamentals_ctx = replace(ctx, on_progress=_on_fundamentals_progress)
                fundamentals_sync = await _sync_fundamentals_incremental(
                    fundamentals_ctx,
                    fundamentals_target_rows,
                )
                total_calls += fundamentals_sync["api_calls"]
                fundamentals_updated += fundamentals_sync["updated"]
                fundamentals_dates_processed += fundamentals_sync["dates_processed"]
                errors.extend(fundamentals_sync["errors"])
                if fundamentals_sync["cancelled"]:
                    return _cancelled_sync_result(total_calls)
            else:
                ctx.on_progress("fundamentals", 200, 200, "No listed-market fundamentals repair needed.")

            ctx.on_progress("complete", 200, 200, "Repair sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=0,
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


def _scale_repair_progress(current: int, total: int, *, base: int) -> int:
    ratio = current / total if total > 0 else 1.0
    return base + round(min(max(ratio, 0.0), 1.0) * 100)


def _cancelled_sync_result(total_api_calls: int) -> SyncResult:
    return SyncResult(success=False, totalApiCalls=total_api_calls, errors=["Cancelled"])


async def _sync_options_225_dates(
    ctx: SyncContext,
    *,
    date_targets: list[str],
    progress_stage: str,
    progress_current: int,
    progress_total: int,
    stage_name: str,
) -> dict[str, Any]:
    target_dates = sorted(
        {
            normalized
            for normalized in (_normalize_iso_date_text(value) for value in date_targets)
            if normalized is not None
        },
        key=_date_sort_key,
    )
    if not target_dates:
        ctx.on_progress(progress_stage, progress_current, progress_total, "No N225 options dates to sync.")
        return {"api_calls": 0, "errors": [], "cancelled": False}

    from_date, to_date = _select_bulk_candidates_from_dates(target_dates)
    decision = await _plan_fetch_method(
        ctx,
        stage=stage_name,
        endpoint="/derivatives/bars/daily/options/225",
        estimated_rest_calls=max(len(target_dates), 1),
        date_from=from_date,
        date_to=to_date,
        exact_dates=target_dates,
    )
    api_calls = decision.planner_api_calls
    errors: list[str] = []
    _emit_fetch_strategy_progress(
        ctx,
        progress_stage=progress_stage,
        current=progress_current,
        total=progress_total,
        endpoint="/derivatives/bars/daily/options/225",
        decision=decision,
        target_label=f"{len(target_dates)} dates",
    )

    target_date_set = _build_target_date_set(target_dates)
    used_rest_fallback = False
    bulk_fallback_reason: str | None = None
    stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    if decision.method == "bulk":
        if decision.plan is None or len(decision.plan.files) == 0:
            used_rest_fallback = True
            bulk_fallback_reason = _resolve_bulk_fallback_reason(decision.plan)
            logger.warning(
                "{} bulk fetch selected but no bulk files were available, falling back to REST: {}",
                stage_name,
                bulk_fallback_reason,
            )
        else:
            try:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage=progress_stage,
                    current=progress_current,
                    total=progress_total,
                    endpoint="/derivatives/bars/daily/options/225",
                    method="bulk",
                    target_label=f"{len(target_dates)} dates",
                )

                async def _consume_options_bulk_rows(
                    batch_rows: list[dict[str, Any]],
                    _file_info: BulkFileInfo,
                ) -> None:
                    normalized_rows = _convert_options_225_rows(_normalize_bulk_options_225_rows(batch_rows))
                    if target_date_set is not None:
                        normalized_rows[:] = [row for row in normalized_rows if row.get("date") in target_date_set]
                    rows = validate_rows_required_fields(
                        normalized_rows,
                        required_fields=("code", "date"),
                        dedupe_keys=("code", "date"),
                        stage="options_225",
                    )
                    if not rows:
                        return
                    await _publish_options_225_rows(ctx, rows)
                    await _publish_synthetic_nikkei_rows(ctx, rows)

                bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                    decision.plan,
                    on_rows_batch=_consume_options_bulk_rows,
                    accumulate_rows=False,
                )
                api_calls += bulk_result.api_calls
                stage_api_calls += bulk_result.api_calls
                _log_sync_fetch_execution(
                    stage=stage_name,
                    endpoint="/derivatives/bars/daily/options/225",
                    decision=decision,
                    executed="bulk",
                    actual_api_calls=stage_api_calls,
                    fallback=False,
                    bulk_result=bulk_result,
                )
            except Exception as e:
                used_rest_fallback = True
                bulk_fallback_reason = _summarize_exception(e)
                logger.warning("options_225 bulk fetch failed, falling back to REST: {}", e)

    if decision.method == "rest" or used_rest_fallback:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage=progress_stage,
            current=progress_current,
            total=progress_total,
            endpoint="/derivatives/bars/daily/options/225",
            method="rest",
            target_label=f"{len(target_dates)} dates",
            fallback=used_rest_fallback,
            fallback_reason=bulk_fallback_reason,
        )
        for index, target_date in enumerate(target_dates, start=1):
            if ctx.cancelled.is_set():
                return {"api_calls": api_calls, "errors": errors, "cancelled": True}
            if index > 1 and index % 50 == 0:
                ctx.on_progress(
                    progress_stage,
                    progress_current,
                    progress_total,
                    f"Fetching /derivatives/bars/daily/options/225 via REST: {index}/{len(target_dates)} dates...",
                )
            try:
                payload, page_calls = await _get_paginated_rows_with_call_count(
                    ctx.client,
                    "/derivatives/bars/daily/options/225",
                    params={"date": _to_jquants_date_param(target_date)},
                )
                api_calls += page_calls
                stage_api_calls += page_calls
                rows = validate_rows_required_fields(
                    _convert_options_225_rows(payload),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="options_225",
                )
                if rows:
                    await _publish_options_225_rows(ctx, rows)
                    await _publish_synthetic_nikkei_rows(ctx, rows)
            except Exception as e:
                errors.append(f"Options {target_date}: {e}")
                logger.warning("Options date {} sync error: {}", target_date, e)
        _log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/derivatives/bars/daily/options/225",
            decision=decision,
            executed="rest",
            actual_api_calls=stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=bulk_result,
        )

    await _index_options_225_rows(ctx)
    await _index_indices_rows(ctx)
    return {"api_calls": api_calls, "errors": errors, "cancelled": False}


async def _sync_fundamentals_initial(
    ctx: SyncContext,
    target_rows: list[dict[str, Any]],
    *,
    progress_current: int = 2,
    progress_total: int = 6,
) -> dict[str, Any]:
    """fundamentals 対象市場を code 指定でフル同期"""
    api_calls = 0
    updated = 0
    failed_codes: list[str] = []
    errors: list[str] = []
    target_map = build_fundamentals_target_map(target_rows)
    if not target_map:
        return {
            "api_calls": api_calls,
            "updated": updated,
            "dates_processed": 0,
            "errors": errors,
            "cancelled": False,
        }
    allowed_statement_codes = set(target_map) | set(target_map.values())
    target_groups = group_target_codes_by_canonical(target_map)
    statement_codes = _get_statement_codes(ctx)
    current_frontier = normalize_frontier_date(
        ctx.market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or _get_latest_statement_disclosed_date(ctx)
    )
    current_empty_codes = _load_frontier_code_cache(
        ctx.market_db,
        METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"],
        current_frontier,
    )
    target_codes = build_fundamentals_fetch_codes(
        target_map,
        statement_codes,
        empty_skipped_codes=current_empty_codes,
    )
    skipped_empty_exact_codes = {
        code
        for code, canonical in target_map.items()
        if canonical not in statement_codes and code in current_empty_codes
    }
    issuer_alias_count = sum(1 for code, canonical in target_map.items() if canonical != code)
    bulk_succeeded = False
    stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    empty_fetch_codes: set[str] = set()

    decision = await _plan_fetch_method(
        ctx,
        stage="fundamentals_initial",
        endpoint="/fins/summary",
        estimated_rest_calls=max(len(target_codes), 1),
    )
    api_calls += decision.planner_api_calls
    _emit_fetch_strategy_progress(
        ctx,
        progress_stage="fundamentals",
        current=progress_current,
        total=progress_total,
        endpoint="/fins/summary",
        decision=decision,
        target_label=_format_target_label(
            len(target_codes),
            "listed-market fetch codes",
            skipped_empty=len(skipped_empty_exact_codes),
            issuer_alias=issuer_alias_count,
        ),
    )

    if decision.method == "bulk" and decision.plan is not None:
        try:
            _emit_fetch_execution_progress(
                ctx,
                progress_stage="fundamentals",
                current=progress_current,
                total=progress_total,
                endpoint="/fins/summary",
                method="bulk",
                target_label=_format_target_label(
                    len(target_codes),
                    "listed-market fetch codes",
                    skipped_empty=len(skipped_empty_exact_codes),
                    issuer_alias=issuer_alias_count,
                ),
            )

            async def _consume_initial_fundamentals_bulk_rows(
                batch_rows: list[dict[str, Any]],
                _file_info: BulkFileInfo,
            ) -> None:
                nonlocal updated
                updated += await _ingest_fins_bulk_batch(
                    ctx,
                    batch_rows=batch_rows,
                    allowed_codes=allowed_statement_codes,
                )

            bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                decision.plan,
                on_rows_batch=_consume_initial_fundamentals_bulk_rows,
                accumulate_rows=False,
            )
            api_calls += bulk_result.api_calls
            stage_api_calls += bulk_result.api_calls
            bulk_succeeded = True
            _log_sync_fetch_execution(
                stage="fundamentals_initial",
                endpoint="/fins/summary",
                decision=decision,
                executed="bulk",
                actual_api_calls=stage_api_calls,
                fallback=False,
                bulk_result=bulk_result,
            )
        except Exception as e:
            logger.warning("Initial fundamentals bulk fetch failed, falling back to REST: {}", e)

    if not bulk_succeeded:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage="fundamentals",
            current=progress_current,
            total=progress_total,
            endpoint="/fins/summary",
            method="rest",
            target_label=_format_target_label(
                len(target_codes),
                "listed-market fetch codes",
                skipped_empty=len(skipped_empty_exact_codes),
                issuer_alias=issuer_alias_count,
            ),
            fallback=decision.method == "bulk",
        )
        for idx, code in enumerate(target_codes):
            if ctx.cancelled.is_set():
                return {
                    "api_calls": api_calls,
                    "updated": updated,
                    "dates_processed": 0,
                    "errors": errors,
                    "cancelled": True,
                }

            if idx > 0 and idx % 100 == 0:
                ctx.on_progress(
                    "fundamentals",
                    progress_current,
                    progress_total,
                    f"Fetching /fins/summary via REST: {idx}/{len(target_codes)} codes...",
                )

            try:
                data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
                api_calls += page_calls
                stage_api_calls += page_calls
                if not data:
                    empty_fetch_codes.add(code)
                    continue
                rows = validate_rows_required_fields(
                    convert_fins_summary_rows(data, default_code=code),
                    required_fields=("code", "disclosed_date"),
                    dedupe_keys=("code", "disclosed_date"),
                    stage="fundamentals",
                )
                rows = [row for row in rows if row.get("code") in allowed_statement_codes]
                if rows:
                    updated += await _publish_statement_rows(ctx, rows)
                else:
                    empty_fetch_codes.add(code)
            except Exception as e:
                failed_codes.append(code)
                errors.append(f"Fundamentals code {code}: {e}")
        _log_sync_fetch_execution(
            stage="fundamentals_initial",
            endpoint="/fins/summary",
            decision=decision,
            executed="rest",
            actual_api_calls=stage_api_calls,
            fallback=decision.method == "bulk",
            bulk_result=bulk_result,
        )

    await _index_statement_rows(ctx)

    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
    normalized_latest_disclosed = normalize_frontier_date(latest_disclosed)
    now_iso = datetime.now(UTC).isoformat()

    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"],
        now_iso,
    )
    if latest_disclosed:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            latest_disclosed,
        )
    empty_cache_frontier = normalized_latest_disclosed or current_frontier
    next_empty_codes = set(current_empty_codes) if empty_cache_frontier == current_frontier else set()
    for fetch_code in empty_fetch_codes:
        next_empty_codes.update(target_groups.get(fetch_code, ()))
    latest_statement_codes = _get_statement_codes(ctx)
    next_empty_codes = {
        code
        for code in next_empty_codes
        if code in target_map and target_map[code] not in latest_statement_codes
    }
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
        "[]",
    )
    await _save_frontier_code_cache(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"],
        empty_cache_frontier,
        next_empty_codes,
    )
    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
        failed_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": 0,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_fundamentals_incremental(
    ctx: SyncContext,
    target_rows: list[dict[str, Any]],
    *,
    progress_current: int = 4,
    progress_total: int = 5,
) -> dict[str, Any]:
    """date 指定増分 + 欠損 listed-market 補完"""
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_dates: list[str] = []
    failed_codes: list[str] = []
    target_map = build_fundamentals_target_map(target_rows)
    if not target_map:
        return {
            "api_calls": api_calls,
            "updated": updated,
            "dates_processed": 0,
            "errors": errors,
            "cancelled": False,
        }
    allowed_statement_codes = set(target_map) | set(target_map.values())
    target_groups = group_target_codes_by_canonical(target_map)
    issuer_alias_count = sum(1 for code, canonical in target_map.items() if canonical != code)

    previous_failed_dates = _normalize_date_list(
        _load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"])
    )
    previous_failed_codes = _collect_unique_codes(
        _load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"])
    )

    anchor = (
        ctx.market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or _get_latest_statement_disclosed_date(ctx)
    )
    date_targets = _build_incremental_date_targets(anchor, previous_failed_dates)
    dates_phase_completed = 0
    bulk_dates_succeeded = False
    date_phase_api_calls = 0
    date_phase_bulk_result: BulkFetchResult | None = None
    if date_targets:
        normalized_target_dates = _build_target_date_set(date_targets)
        decision = await _plan_fetch_method(
            ctx,
            stage="fundamentals_incremental_dates",
            endpoint="/fins/summary",
            estimated_rest_calls=max(len(date_targets), 1),
            exact_dates=date_targets,
        )
        api_calls += decision.planner_api_calls
        _emit_fetch_strategy_progress(
            ctx,
            progress_stage="fundamentals",
            current=progress_current,
            total=progress_total,
            endpoint="/fins/summary",
            decision=decision,
            target_label=f"{len(date_targets)} dates",
        )

        if decision.method == "bulk" and decision.plan is not None:
            try:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="fundamentals",
                    current=progress_current,
                    total=progress_total,
                    endpoint="/fins/summary",
                    method="bulk",
                    target_label=f"{len(date_targets)} dates",
                )

                async def _consume_incremental_fundamentals_bulk_rows(
                    batch_rows: list[dict[str, Any]],
                    _file_info: BulkFileInfo,
                ) -> None:
                    nonlocal updated
                    updated += await _ingest_fins_bulk_batch(
                        ctx,
                        batch_rows=batch_rows,
                        allowed_codes=allowed_statement_codes,
                        target_dates=normalized_target_dates,
                    )

                date_phase_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                    decision.plan,
                    on_rows_batch=_consume_incremental_fundamentals_bulk_rows,
                    accumulate_rows=False,
                )
                api_calls += date_phase_bulk_result.api_calls
                date_phase_api_calls += date_phase_bulk_result.api_calls
                bulk_dates_succeeded = True
                dates_phase_completed = len(date_targets)
                _log_sync_fetch_execution(
                    stage="fundamentals_incremental_dates",
                    endpoint="/fins/summary",
                    decision=decision,
                    executed="bulk",
                    actual_api_calls=date_phase_api_calls,
                    fallback=False,
                    bulk_result=date_phase_bulk_result,
                )
            except Exception as e:
                logger.warning("Incremental fundamentals bulk date fetch failed, falling back to REST: {}", e)

        if not bulk_dates_succeeded:
            _emit_fetch_execution_progress(
                ctx,
                progress_stage="fundamentals",
                current=progress_current,
                total=progress_total,
                endpoint="/fins/summary",
                method="rest",
                target_label=f"{len(date_targets)} dates",
                fallback=decision.method == "bulk",
            )
            for idx, disclosed_date in enumerate(date_targets):
                if ctx.cancelled.is_set():
                    return {
                        "api_calls": api_calls,
                        "updated": updated,
                        "dates_processed": idx,
                        "errors": errors,
                        "cancelled": True,
                    }

                if idx > 0 and idx % 30 == 0:
                    ctx.on_progress(
                        "fundamentals",
                        progress_current,
                        progress_total,
                        f"Fetching /fins/summary via REST: {idx}/{len(date_targets)} dates...",
                    )

                try:
                    data, page_calls = await _fetch_fins_summary_paginated(
                        ctx.client,
                        {"date": _to_jquants_date_param(disclosed_date)},
                    )
                    api_calls += page_calls
                    date_phase_api_calls += page_calls
                    rows = convert_fins_summary_rows(data)
                    rows = [row for row in rows if row.get("code") in allowed_statement_codes]
                    rows = validate_rows_required_fields(
                        rows,
                        required_fields=("code", "disclosed_date"),
                        dedupe_keys=("code", "disclosed_date"),
                        stage="fundamentals",
                    )
                    if rows:
                        updated += await _publish_statement_rows(ctx, rows)
                except Exception as e:
                    failed_dates.append(disclosed_date)
                    errors.append(f"Fundamentals date {disclosed_date}: {e}")
            dates_phase_completed = len(date_targets)
            _log_sync_fetch_execution(
                stage="fundamentals_incremental_dates",
                endpoint="/fins/summary",
                decision=decision,
                executed="rest",
                actual_api_calls=date_phase_api_calls,
                fallback=decision.method == "bulk",
                bulk_result=date_phase_bulk_result,
            )

    current_frontier = normalize_frontier_date(
        _get_latest_statement_disclosed_date(ctx) or anchor
    )
    current_empty_codes = _load_frontier_code_cache(
        ctx.market_db,
        METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"],
        current_frontier,
    )
    statement_codes = _get_statement_codes(ctx)
    skipped_empty_exact_codes = {
        code
        for code, canonical in target_map.items()
        if canonical not in statement_codes and code in current_empty_codes
    }
    code_targets = build_fundamentals_fetch_codes(
        target_map,
        statement_codes,
        previous_failed_codes=previous_failed_codes,
        empty_skipped_codes=current_empty_codes,
    )
    empty_fetch_codes: set[str] = set()
    code_phase_api_calls = 0

    if code_targets:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage="fundamentals",
            current=progress_current,
            total=progress_total,
            endpoint="/fins/summary",
            method="rest",
            target_label=_format_target_label(
                len(code_targets),
                "backfill codes",
                skipped_empty=len(skipped_empty_exact_codes),
                issuer_alias=issuer_alias_count,
            ),
        )

    for idx, code in enumerate(code_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": dates_phase_completed,
                "errors": errors,
                "cancelled": True,
            }

        if idx > 0 and idx % 100 == 0:
            ctx.on_progress(
                "fundamentals",
                progress_current,
                progress_total,
                f"Fetching /fins/summary via REST: {idx}/{len(code_targets)} backfill codes...",
            )

        try:
            data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
            api_calls += page_calls
            code_phase_api_calls += page_calls
            if not data:
                empty_fetch_codes.add(code)
                continue
            rows = convert_fins_summary_rows(data, default_code=code)
            rows = [row for row in rows if row.get("code") in allowed_statement_codes]
            rows = validate_rows_required_fields(
                rows,
                required_fields=("code", "disclosed_date"),
                dedupe_keys=("code", "disclosed_date"),
                stage="fundamentals",
            )
            if rows:
                updated += await _publish_statement_rows(ctx, rows)
            else:
                empty_fetch_codes.add(code)
        except Exception as e:
            failed_codes.append(code)
            errors.append(f"Fundamentals code {code}: {e}")

    if code_targets:
        _log_sync_fetch_execution(
            stage="fundamentals_incremental_backfill",
            endpoint="/fins/summary",
            decision=_StageFetchDecision(
                method="rest",
                planner_api_calls=0,
                estimated_rest_calls=len(code_targets),
                estimated_bulk_calls=None,
                reason="code_backfill",
            ),
            executed="rest",
            actual_api_calls=code_phase_api_calls,
            fallback=False,
            bulk_result=None,
        )

    await _index_statement_rows(ctx)

    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
    normalized_latest_disclosed = normalize_frontier_date(latest_disclosed)
    now_iso = datetime.now(UTC).isoformat()
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"],
        now_iso,
    )
    if latest_disclosed:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            latest_disclosed,
        )
    empty_cache_frontier = normalized_latest_disclosed or current_frontier
    next_empty_codes = set(current_empty_codes) if empty_cache_frontier == current_frontier else set()
    for fetch_code in empty_fetch_codes:
        next_empty_codes.update(target_groups.get(fetch_code, ()))
    latest_statement_codes = _get_statement_codes(ctx)
    next_empty_codes = {
        code
        for code in next_empty_codes
        if code in target_map and target_map[code] not in latest_statement_codes
    }

    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
        failed_dates,
    )
    await _save_frontier_code_cache(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"],
        empty_cache_frontier,
        next_empty_codes,
    )
    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
        failed_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": dates_phase_completed,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_margin_data(
    ctx: SyncContext,
    target_codes: list[str],
    *,
    progress_current: int,
    progress_total: int,
    stage_name: str,
    anchor: str | None = None,
    existing_margin_codes: set[str] | None = None,
    trading_frontier: str | None = None,
    skipped_market_count: int = 0,
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []

    normalized_codes = _collect_unique_codes(target_codes)
    if not normalized_codes:
        return {
            "api_calls": api_calls,
            "updated": updated,
            "errors": errors,
            "cancelled": False,
        }

    has_existing_margin_snapshot = anchor is not None and existing_margin_codes is not None
    existing_margin_code_set = set(existing_margin_codes or set())
    current_frontier = normalize_frontier_date(trading_frontier)
    current_empty_codes = _load_frontier_code_cache(
        ctx.market_db,
        METADATA_KEYS["MARGIN_EMPTY_CODES"],
        current_frontier,
    )
    all_backfill_codes = (
        sorted(set(normalized_codes) - existing_margin_code_set)
        if has_existing_margin_snapshot
        else []
    )
    skipped_empty_codes = sorted(code for code in all_backfill_codes if code in current_empty_codes)
    backfill_codes = [code for code in all_backfill_codes if code not in current_empty_codes]
    rest_codes = normalized_codes
    if has_existing_margin_snapshot:
        rest_codes = [code for code in normalized_codes if code in existing_margin_code_set]
    target_code_set = set(rest_codes) | set(backfill_codes)
    target_label = _format_target_label(
        len(normalized_codes),
        "codes",
        skipped_empty=len(skipped_empty_codes),
        skipped_market=skipped_market_count,
    )

    decision = await _plan_fetch_method(
        ctx,
        stage=stage_name,
        endpoint="/markets/margin-interest",
        estimated_rest_calls=max(len(rest_codes) + len(backfill_codes), 1),
        date_from=anchor,
    )
    api_calls += decision.planner_api_calls
    _emit_fetch_strategy_progress(
        ctx,
        progress_stage="margin",
        current=progress_current,
        total=progress_total,
        endpoint="/markets/margin-interest",
        decision=decision,
        target_label=target_label,
    )

    used_rest_fallback = False
    bulk_fallback_reason: str | None = None
    bulk_stage_api_calls = 0
    rest_stage_api_calls = 0
    backfill_stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    empty_fetch_codes: set[str] = set()

    if decision.method == "bulk":
        if decision.plan is None or len(decision.plan.files) == 0:
            used_rest_fallback = True
            bulk_fallback_reason = _resolve_bulk_fallback_reason(decision.plan)
            logger.warning(
                "{} bulk fetch selected but no bulk files were available, falling back to REST: {}",
                stage_name,
                bulk_fallback_reason,
            )
        else:
            try:
                _emit_fetch_execution_progress(
                    ctx,
                    progress_stage="margin",
                    current=progress_current,
                    total=progress_total,
                    endpoint="/markets/margin-interest",
                    method="bulk",
                    target_label=target_label,
                )

                async def _consume_margin_bulk_rows(
                    batch_rows: list[dict[str, Any]],
                    _file_info: BulkFileInfo,
                ) -> None:
                    nonlocal updated
                    updated += await _ingest_margin_bulk_batch(
                        ctx,
                        batch_rows=batch_rows,
                        target_codes=target_code_set,
                        min_date_exclusive=anchor,
                    )

                bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
                    decision.plan,
                    on_rows_batch=_consume_margin_bulk_rows,
                    accumulate_rows=False,
                )
                api_calls += bulk_result.api_calls
                bulk_stage_api_calls += bulk_result.api_calls
                _log_sync_fetch_execution(
                    stage=stage_name,
                    endpoint="/markets/margin-interest",
                    decision=decision,
                    executed="bulk",
                    actual_api_calls=bulk_stage_api_calls,
                    fallback=False,
                    bulk_result=bulk_result,
                )
            except Exception as e:
                used_rest_fallback = True
                bulk_fallback_reason = _summarize_exception(e)
                logger.warning(
                    "{} bulk fetch failed, falling back to REST: {}",
                    stage_name,
                    bulk_fallback_reason,
                )

    if (decision.method == "rest" or used_rest_fallback) and rest_codes:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage="margin",
            current=progress_current,
            total=progress_total,
            endpoint="/markets/margin-interest",
            method="rest",
            target_label=target_label,
            fallback=used_rest_fallback,
            fallback_reason=bulk_fallback_reason,
        )
        for idx, code in enumerate(rest_codes, start=1):
            if ctx.cancelled.is_set():
                return {
                    "api_calls": api_calls,
                    "updated": updated,
                    "errors": errors,
                    "cancelled": True,
                }

            if idx > 1 and idx % 100 == 0:
                ctx.on_progress(
                    "margin",
                    progress_current,
                    progress_total,
                    f"Fetching /markets/margin-interest via REST: {idx}/{len(rest_codes)} codes...",
                )

            try:
                data, page_calls = await _fetch_margin_by_code(
                    ctx.client,
                    code,
                    date_from=anchor,
                )
                api_calls += page_calls
                rest_stage_api_calls += page_calls
                rows = validate_rows_required_fields(
                    _convert_margin_rows(
                        data,
                        default_code=code,
                        min_date_exclusive=anchor,
                    ),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="margin_data",
                )
                if rows:
                    updated += await _publish_margin_rows(ctx, rows)
            except Exception as e:
                errors.append(f"Margin code {code}: {e}")

        _log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/markets/margin-interest",
            decision=decision,
            executed="rest",
            actual_api_calls=rest_stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=bulk_result,
        )

    if backfill_codes:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage="margin",
            current=progress_current,
            total=progress_total,
            endpoint="/markets/margin-interest",
            method="rest",
            target_label=_format_target_label(
                len(backfill_codes),
                "backfill codes",
                skipped_empty=len(skipped_empty_codes),
            ),
        )
        for idx, code in enumerate(backfill_codes, start=1):
            if ctx.cancelled.is_set():
                return {
                    "api_calls": api_calls,
                    "updated": updated,
                    "errors": errors,
                    "cancelled": True,
                }

            if idx > 1 and idx % 100 == 0:
                ctx.on_progress(
                    "margin",
                    progress_current,
                    progress_total,
                    (
                        "Fetching /markets/margin-interest via REST: "
                        f"{idx}/{len(backfill_codes)} backfill codes..."
                    ),
                )

            try:
                data, page_calls = await _fetch_margin_by_code(ctx.client, code)
                api_calls += page_calls
                backfill_stage_api_calls += page_calls
                if not data:
                    empty_fetch_codes.add(code)
                    continue
                rows = validate_rows_required_fields(
                    _convert_margin_rows(data, default_code=code),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="margin_data",
                )
                if rows:
                    updated += await _publish_margin_rows(ctx, rows)
                else:
                    empty_fetch_codes.add(code)
            except Exception as e:
                errors.append(f"Margin backfill code {code}: {e}")

        _log_sync_fetch_execution(
            stage=f"{stage_name}_backfill",
            endpoint="/markets/margin-interest",
            decision=decision,
            executed="rest",
            actual_api_calls=backfill_stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=None,
        )

    await _index_margin_rows(ctx)
    next_empty_codes = set(current_empty_codes)
    next_empty_codes.update(empty_fetch_codes)
    latest_margin_codes = set(_inspect_time_series(ctx).margin_codes)
    next_empty_codes = {
        code for code in next_empty_codes
        if code not in latest_margin_codes
    }
    await _save_frontier_code_cache(
        ctx,
        METADATA_KEYS["MARGIN_EMPTY_CODES"],
        current_frontier,
        next_empty_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "cancelled": False,
    }


async def _fetch_fins_summary_paginated(
    client: SyncClientLike,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    """`/fins/summary` を pagination_key が尽きるまで取得する。"""
    current_params = dict(params)
    all_rows: list[dict[str, Any]] = []
    api_calls = 0

    while True:
        body = await client.get("/fins/summary", params=current_params)
        api_calls += 1

        page_rows = _extract_list_items(body, preferred_keys=("data",))
        all_rows.extend(page_rows)

        pagination_key = body.get("pagination_key")
        if not pagination_key:
            break

        if api_calls >= _MAX_FINS_SUMMARY_PAGES:
            raise RuntimeError("fins/summary pagination exceeded safety limit")

        current_params = {**current_params, "pagination_key": pagination_key}

    return all_rows, api_calls


async def _fetch_fins_summary_by_code(
    client: SyncClientLike,
    code: str,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch /fins/summary by trying both 5-digit and 4-digit code formats.

    dataset builder は 5桁コードで fetch しているため、
    market sync も 5桁優先で試行し、空結果やエラー時のみ 4桁へフォールバックする。
    """
    normalized_code = normalize_stock_code(code)
    candidates = list(
        dict.fromkeys(
            (
                expand_stock_code(normalized_code),
                *stock_code_candidates(normalized_code),
            )
        )
    )

    total_calls = 0
    last_error: Exception | None = None
    saw_empty_payload = False

    for candidate in candidates:
        try:
            data, page_calls = await _fetch_fins_summary_paginated(
                client,
                {"code": candidate},
            )
            total_calls += page_calls
            if data:
                return data, total_calls
            saw_empty_payload = True
            continue
        except Exception as exc:
            last_error = exc
            continue

    if saw_empty_payload:
        return [], total_calls

    if last_error is None:
        raise RuntimeError(f"fins/summary code fetch failed for {code}")
    raise last_error


async def _fetch_margin_by_code(
    client: SyncClientLike,
    code: str,
    *,
    date_from: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch /markets/margin-interest by trying 5-digit then 4-digit code formats."""
    normalized_code = normalize_stock_code(code)
    candidates = list(
        dict.fromkeys(
            (
                expand_stock_code(normalized_code),
                *stock_code_candidates(normalized_code),
            )
        )
    )

    total_calls = 0
    last_error: Exception | None = None
    saw_empty_payload = False

    for candidate in candidates:
        params: dict[str, Any] = {"code": candidate}
        if date_from:
            params["from"] = _to_jquants_date_param(date_from)
        try:
            data, page_calls = await _get_paginated_rows_with_call_count(
                client,
                "/markets/margin-interest",
                params=params,
            )
            total_calls += page_calls
            if data:
                return data, total_calls
            saw_empty_payload = True
            continue
        except Exception as exc:
            last_error = exc
            continue

    if saw_empty_payload:
        return [], total_calls

    if last_error is None:
        raise RuntimeError(f"margin-interest code fetch failed for {code}")
    raise last_error

def _load_metadata_json_list(market_db: SyncMarketDbLike, key: str) -> list[str]:
    raw = market_db.get_sync_metadata(key)
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(v) for v in loaded if isinstance(v, str) or isinstance(v, int)]


async def _save_metadata_json_list(
    ctx: SyncContext,
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


def _require_time_series_store(ctx: SyncContext) -> SyncTimeSeriesStoreLike:
    if ctx.time_series_store is None:
        raise RuntimeError("DuckDB time-series store is required for sync strategy execution")
    return ctx.time_series_store


def _inspect_time_series(
    ctx: SyncContext,
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


async def _resolve_incremental_options_date_targets(
    ctx: SyncContext,
    *,
    inspection: TimeSeriesInspection,
    topix_rows: list[dict[str, Any]],
) -> list[str]:
    last_options_225_date = inspection.latest_options_225_date
    if not last_options_225_date:
        options_dates = await asyncio.to_thread(ctx.market_db.get_topix_dates)
        if options_dates:
            return options_dates
        return sorted(
            {str(r["date"]) for r in topix_rows if r.get("date")},
            key=_date_sort_key,
        )

    options_dates = _normalize_date_list(
        [
            str(r["date"])
            for r in topix_rows
            if r.get("date") and _is_date_after(str(r["date"]), last_options_225_date)
        ]
    )
    if inspection.missing_options_225_dates_count <= 0:
        return options_dates

    missing_coverage = _inspect_time_series(
        ctx,
        missing_options_225_dates_limit=inspection.missing_options_225_dates_count,
    )
    return _normalize_date_list(options_dates + list(missing_coverage.missing_options_225_dates))


async def _resolve_incremental_stock_date_targets(
    ctx: SyncContext,
    *,
    topix_rows: list[dict[str, Any]],
    anchor: str | None,
) -> list[str]:
    inspection = _inspect_time_series(ctx)
    topix_dates = _normalize_date_list(
        [
            str(r["date"])
            for r in topix_rows
            if r.get("date") and (anchor is None or _is_date_after(str(r["date"]), anchor))
        ]
    )
    if inspection.missing_stock_dates_count <= 0:
        return topix_dates

    missing_coverage = _inspect_time_series(
        ctx,
        missing_stock_dates_limit=inspection.missing_stock_dates_count,
    )
    return _normalize_date_list(topix_dates + list(missing_coverage.missing_stock_dates))


def _get_latest_statement_disclosed_date(ctx: SyncContext) -> str | None:
    inspection = _inspect_time_series(ctx)
    return inspection.latest_statement_disclosed_date


def _get_statement_codes(ctx: SyncContext) -> set[str]:
    inspection = _inspect_time_series(ctx)
    return set(inspection.statement_codes)


def get_strategy(resolved_mode: str) -> SyncStrategy:
    """モード名から戦略インスタンスを返す"""
    if resolved_mode == "initial":
        return InitialSyncStrategy()
    elif resolved_mode == "incremental":
        return IncrementalSyncStrategy()
    elif resolved_mode == "repair":
        return RepairSyncStrategy()
    return InitialSyncStrategy()


async def _seed_index_master_from_catalog(ctx: SyncContext) -> set[str]:
    seed_rows = build_index_master_seed_rows()
    if seed_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, seed_rows)
    return ctx.market_db.get_index_master_codes()


async def _upsert_indices_rows_with_master_backfill(
    ctx: SyncContext,
    rows: list[dict[str, Any]],
    known_master_codes: set[str],
    *,
    discovery_log: str | None = None,
) -> None:
    missing_master_rows = _build_fallback_index_master_rows(rows, known_master_codes)
    if missing_master_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, missing_master_rows)
        known_master_codes.update(
            str(row["code"])
            for row in missing_master_rows
            if row.get("code")
        )
        if discovery_log:
            logger.warning(discovery_log, len(missing_master_rows))

    await _publish_indices_rows(ctx, rows)


async def _publish_synthetic_nikkei_rows(
    ctx: SyncContext,
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


async def _publish_topix_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_topix_data, rows)


async def _publish_stock_data_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_stock_data, rows)


async def _publish_indices_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_indices_data, rows)


async def _publish_options_225_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_options_225_data, rows)


async def _publish_margin_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_margin_data, rows)


async def _publish_statement_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    store = _require_time_series_store(ctx)
    return await asyncio.to_thread(store.publish_statements, rows)


async def _index_topix_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_topix_data)


async def _index_stock_data_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_stock_data)


async def _index_indices_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_indices_data)


async def _index_options_225_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_options_225_data)


async def _index_margin_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_margin_data)


async def _index_statement_rows(ctx: SyncContext) -> None:
    store = _require_time_series_store(ctx)
    await asyncio.to_thread(store.index_statements)


def _convert_topix_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for d in data:
        rows.append(
            {
                "date": d.get("Date", d.get("date", "")),
                "open": d.get("O", d.get("open")),
                "high": d.get("H", d.get("high")),
                "low": d.get("L", d.get("low")),
                "close": d.get("C", d.get("close")),
                "created_at": created_at,
            }
        )
    return rows


def _convert_stock_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 銘柄マスタ → DB 行"""
    rows = []
    for d in data:
        code = normalize_stock_code(d.get("Code", ""))
        if not code:
            continue
        rows.append({
            "code": code,
            "company_name": d.get("CoName", ""),
            "company_name_english": d.get("CoNameEn"),
            "market_code": d.get("Mkt", ""),
            "market_name": d.get("MktNm", ""),
            "sector_17_code": d.get("S17", ""),
            "sector_17_name": d.get("S17Nm", ""),
            "sector_33_code": d.get("S33", ""),
            "sector_33_name": d.get("S33Nm", ""),
            "scale_category": d.get("ScaleCat"),
            "listed_date": d.get("Date", ""),
            "created_at": datetime.now(UTC).isoformat(),
        })
    return rows


async def _sync_daily_stock_master(
    ctx: SyncContext,
    *,
    target_dates: list[str],
    progress_current: int,
    progress_total: int,
) -> dict[str, Any]:
    """Fetch `/equities/master?date=...` for TOPIX dates and rebuild derived master tables."""
    api_calls = 0
    rows_updated = 0
    latest_rows: list[dict[str, Any]] = []
    failed_dates: list[str] = []
    normalized_dates = sorted({date for date in target_dates if date})
    if not normalized_dates:
        return {
            "api_calls": 0,
            "updated": 0,
            "latest_rows": [],
            "errors": [],
            "cancelled": False,
        }

    total_dates = len(normalized_dates)
    for index, snapshot_date in enumerate(normalized_dates, start=1):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": rows_updated,
                "latest_rows": latest_rows,
                "errors": [],
                "cancelled": True,
            }
        ctx.on_progress(
            "stocks",
            progress_current,
            progress_total,
            f"Fetching daily stock master {index}/{total_dates}: {snapshot_date}",
        )
        payload, calls = await _get_paginated_rows_with_call_count(
            ctx.client,
            "/equities/master",
            params={"date": _to_jquants_date_param(snapshot_date)},
        )
        api_calls += calls
        rows = _convert_stock_rows(payload)
        if not rows:
            failed_dates.append(snapshot_date)
            continue
        await asyncio.to_thread(ctx.market_db.upsert_stock_master_daily, snapshot_date, rows)
        rows_updated += len(rows)
        latest_rows = rows

    if rows_updated > 0:
        await asyncio.to_thread(ctx.market_db.rebuild_stock_master_intervals)
        await asyncio.to_thread(ctx.market_db.rebuild_stocks_latest)
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["LAST_STOCKS_REFRESH"],
            datetime.now(UTC).isoformat(),
        )

    errors = [
        f"No stock master rows returned for {len(failed_dates)} TOPIX dates: {', '.join(failed_dates[:10])}"
    ] if failed_dates else []
    return {
        "api_calls": api_calls,
        "updated": rows_updated,
        "latest_rows": latest_rows,
        "errors": errors,
        "cancelled": False,
    }


def _convert_options_225_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = datetime.now(UTC).isoformat()
    return [
        normalize_options_225_raw_row(item, created_at=created_at)
        for item in data
    ]


def _convert_stock_data_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 株価データ → DB 行"""
    rows: list[dict[str, Any]] = []
    skipped = 0
    sample_codes: list[str] = []
    created_at = datetime.now(UTC).isoformat()

    for d in data:
        row = build_stock_data_row(d, created_at=created_at)
        if row is None:
            skipped += 1
            code = normalize_stock_code(d.get("Code", ""))
            if code and code not in sample_codes and len(sample_codes) < 5:
                sample_codes.append(code)
            continue
        rows.append(row)

    if skipped > 0:
        sample = ", ".join(sample_codes) if sample_codes else "unknown"
        logger.warning(
            "Skipped {} daily quotes with incomplete OHLCV data (sample codes: {})",
            skipped,
            sample,
        )
    return rows


def _extract_list_items(
    body: dict[str, Any],
    *,
    preferred_keys: tuple[str, ...] = ("data",),
) -> list[dict[str, Any]]:
    """レスポンスの配列ペイロードをキー揺れ込みで取り出す。"""
    def _coerce_dict_items(value: Any) -> list[dict[str, Any]] | None:
        if not isinstance(value, list):
            return None
        return [item for item in value if isinstance(item, dict)]

    for key in preferred_keys:
        dict_items = _coerce_dict_items(body.get(key))
        if dict_items is not None:
            return dict_items

    for value in body.values():
        dict_items = _coerce_dict_items(value)
        if dict_items is not None:
            return dict_items

    return []


def _extract_index_code(index_info: dict[str, Any]) -> str:
    """指数コードをキー揺れを吸収して取得。"""
    code = (
        index_info.get("code")
        or index_info.get("Code")
        or index_info.get("index_code")
        or index_info.get("indexCode")
    )
    return _normalize_index_code(code)


def _convert_index_master_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 指数マスタ → DB 行。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    for idx in data:
        code = _extract_index_code(idx)
        if not code:
            continue
        rows.append({
            "code": code,
            "name": idx.get("name") or idx.get("Name") or "",
            "name_english": idx.get("name_english") or idx.get("nameEnglish"),
            "category": idx.get("category") or idx.get("Category") or "",
            "data_start_date": idx.get("data_start_date") or idx.get("dataStartDate"),
            "created_at": created_at,
        })
    return rows


def _convert_indices_data_rows(data: list[dict[str, Any]], code: str | None) -> list[dict[str, Any]]:
    """JQuants 指数データ → DB 行。日付欠損行はスキップ。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    skipped_missing_date = 0
    skipped_missing_code = 0

    for d in data:
        row_code = _extract_index_code(d) or _normalize_index_code(code)
        if not row_code:
            skipped_missing_code += 1
            continue

        row_date = d.get("Date") or d.get("date") or ""
        if not row_date:
            skipped_missing_date += 1
            continue

        rows.append({
            "code": row_code,
            "date": row_date,
            "open": d.get("O", d.get("open")),
            "high": d.get("H", d.get("high")),
            "low": d.get("L", d.get("low")),
            "close": d.get("C", d.get("close")),
            "sector_name": d.get("SectorName", d.get("sector_name")),
            "created_at": created_at,
        })

    if skipped_missing_date > 0:
        logger.warning("Skipped {} index rows with missing date (code={})", skipped_missing_date, code)
    if skipped_missing_code > 0:
        logger.warning("Skipped {} index rows with missing code", skipped_missing_code)
    return rows


def _resolve_margin_volume(
    row: dict[str, Any],
    *keys: str,
) -> float | None:
    for key in keys:
        if key in row:
            return _coerce_float_fast(row.get(key))
    return None


def _convert_margin_rows(
    data: list[dict[str, Any]],
    *,
    default_code: str | None = None,
    target_codes: set[str] | None = None,
    min_date_exclusive: str | None = None,
) -> list[dict[str, Any]]:
    """JQuants 信用残データ → DB 行。"""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    skipped_missing_date = 0
    skipped_missing_code = 0

    for item in data:
        code = normalize_stock_code(item.get("Code") or item.get("code") or default_code or "")
        if not code:
            skipped_missing_code += 1
            continue
        if target_codes is not None and code not in target_codes:
            continue

        row_date = _normalize_iso_date_text(item.get("Date", item.get("date")))
        if row_date is None:
            skipped_missing_date += 1
            continue
        if min_date_exclusive and not _is_date_after(row_date, min_date_exclusive):
            continue

        row_key = (code, row_date)
        if row_key in seen:
            continue
        seen.add(row_key)

        rows.append(
            {
                "code": code,
                "date": row_date,
                "long_margin_volume": _resolve_margin_volume(
                    item,
                    "LongVol",
                    "longVol",
                    "long_volume",
                    "longMarginVolume",
                    "longMarginTradeVolume",
                    "longMarginOutstandingBalance",
                    "long_margin_volume",
                ),
                "short_margin_volume": _resolve_margin_volume(
                    item,
                    "ShrtVol",
                    "shortVol",
                    "short_volume",
                    "shortMarginVolume",
                    "shortMarginTradeVolume",
                    "shortMarginOutstandingBalance",
                    "short_margin_volume",
                ),
            }
        )

    if skipped_missing_date > 0:
        logger.warning("Skipped {} margin rows with missing date", skipped_missing_date)
    if skipped_missing_code > 0:
        logger.warning("Skipped {} margin rows with missing code", skipped_missing_code)
    return rows


async def _ingest_margin_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    target_codes: set[str] | None,
    min_date_exclusive: str | None,
) -> int:
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
        return 0
    return await _publish_margin_rows(ctx, rows)


def _build_fallback_index_master_rows(
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


def _normalize_index_code(value: Any) -> str:
    """指数コードを文字列化し、数字コードは 4 桁に正規化する。"""
    text = str(value).strip() if value is not None else ""
    if not text:
        return ""
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text.upper()


def _latest_date(values: list[str]) -> str | None:
    """日付文字列配列から最新日を返す。"""
    latest: str | None = None
    for value in values:
        if not value:
            continue
        if latest is None or _is_date_after(value, latest):
            latest = value
    return latest


def _extract_dates_after(
    rows: list[dict[str, Any]],
    anchor_date: str | None,
    *,
    include_anchor: bool = False,
) -> list[str]:
    """行配列から anchor_date 以降（またはより後）の日付を抽出して昇順化する。"""
    dates: set[str] = set()
    for row in rows:
        row_date = row.get("date")
        if not row_date:
            continue
        if anchor_date:
            if include_anchor:
                if _is_date_after(anchor_date, row_date):
                    continue
            elif not _is_date_after(row_date, anchor_date):
                continue
        dates.add(row_date)
    return sorted(dates, key=_date_sort_key)


def _parse_date(value: str) -> date | None:
    """YYYY-MM-DD / YYYYMMDD を date に正規化して返す。"""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()  # noqa: DTZ007
        return datetime.strptime(text, "%Y-%m-%d").date()  # noqa: DTZ007
    except ValueError:
        return None


def _to_jquants_date_param(value: str) -> str:
    """J-Quants 向けの日付パラメータ（YYYYMMDD）に変換。"""
    parsed = _parse_date(value)
    if parsed is None:
        return value
    return parsed.strftime("%Y%m%d")


def _is_date_after(lhs: str, rhs: str) -> bool:
    """日付文字列（YYYY-MM-DD / YYYYMMDD）の大小比較。"""
    left = _parse_date(lhs)
    right = _parse_date(rhs)
    if left is None or right is None:
        return lhs > rhs
    return left > right


def _date_sort_key(value: str) -> tuple[int, str]:
    """日付ソート用キー（parse 失敗時は末尾に回す）。"""
    parsed = _parse_date(value)
    if parsed is None:
        return (1, value)
    return (0, parsed.isoformat())
