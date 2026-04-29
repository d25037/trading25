from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import src.application.services.sync_strategies as sync_strategies_module
from src.application.services.jquants_bulk_service import BulkFetchPlan, BulkFetchResult, BulkFileInfo
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.application.services.sync_strategies import (
    BulkFetchRequiredError,
    IncrementalSyncStrategy,
    IndicesOnlySyncStrategy,
    InitialSyncStrategy,
    RepairSyncStrategy,
    SyncContext,
    _StageFetchDecision,
    _build_fallback_index_master_rows,
    _build_incremental_date_targets,
    _collect_unique_codes,
    _convert_margin_rows,
    _convert_index_master_rows,
    _convert_indices_data_rows,
    _convert_stock_bulk_rows,
    _convert_stock_rows,
    _date_sort_key,
    _fetch_margin_by_code,
    _dedupe_preserve_order,
    _extract_dates_after,
    _extract_list_items,
    _fetch_fins_summary_by_code,
    _get_bulk_service,
    _get_paginated_rows_with_call_count,
    _inspect_time_series,
    _is_date_after,
    _latest_date,
    _load_metadata_json_list,
    _normalize_date_list,
    _normalize_iso_date_text,
    _plan_fetch_method,
    _parse_date,
    _publish_indices_rows,
    _publish_statement_rows,
    _publish_stock_data_rows,
    _publish_topix_rows,
    _resolve_bulk_fallback_reason,
    _enforce_stock_bulk_plan_available,
    _sync_daily_stock_master,
    _sync_margin_data,
    _to_iso_date_text,
    _to_jquants_date_param,
    get_strategy,
)


class DummyMarketDb:
    def __init__(
        self,
        latest_trading_date: str | None = "20260206",
        latest_stock_data_date: str | None = None,
        latest_indices_data_dates: dict[str, str] | None = None,
        stocks_needing_refresh: list[str] | None = None,
    ) -> None:
        self._default_last_sync = "2026-02-06T00:00:00+00:00"
        self.latest_trading_date = latest_trading_date
        self.latest_stock_data_date = latest_stock_data_date or latest_trading_date
        if latest_indices_data_dates is not None:
            self.latest_indices_data_dates = latest_indices_data_dates
        elif latest_trading_date:
            self.latest_indices_data_dates = {"0000": latest_trading_date}
        else:
            self.latest_indices_data_dates = {}
        self.stocks_rows: list[dict[str, Any]] = []
        self.stock_master_daily_rows: list[dict[str, Any]] = []
        self.stock_master_interval_rebuilds = 0
        self.stocks_latest_rebuilds = 0
        self.stock_rows: list[dict[str, Any]] = []
        self.topix_rows: list[dict[str, Any]] = []
        self.index_master_rows: list[dict[str, Any]] = []
        self.indices_rows: list[dict[str, Any]] = []
        self.options_225_rows: list[dict[str, Any]] = []
        self.margin_rows: list[dict[str, Any]] = []
        self.statements_rows: list[dict[str, Any]] = []
        self.metadata: dict[str, str] = {}
        self._prime_codes: set[str] = set()
        self._fundamentals_target_codes: set[str] = set()
        self._stocks_needing_refresh = list(stocks_needing_refresh or [])
        self.resolved_adjustment_calls: list[list[str] | None] = []

    def get_sync_metadata(self, key: str) -> str | None:
        if key in self.metadata:
            return self.metadata[key]
        if key == METADATA_KEYS["LAST_SYNC_DATE"]:
            return self._default_last_sync
        return None

    def get_latest_trading_date(self) -> str | None:
        return self.latest_trading_date

    def get_latest_stock_data_date(self) -> str | None:
        return self.latest_stock_data_date

    def get_latest_indices_data_dates(self) -> dict[str, str]:
        return dict(self.latest_indices_data_dates)

    def get_topix_dates(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        dates = sorted(
            {
                normalized
                for normalized in (
                    _normalize_iso_date_text(row.get("date"))
                    for row in self.topix_rows
                    if row.get("date")
                )
                if normalized is not None
            },
            key=_date_sort_key,
        )
        if start_date is not None:
            normalized_start = _normalize_iso_date_text(start_date)
            if normalized_start is not None:
                dates = [value for value in dates if not _is_date_after(normalized_start, value)]
        if end_date is not None:
            normalized_end = _normalize_iso_date_text(end_date)
            if normalized_end is not None:
                dates = [value for value in dates if not _is_date_after(value, normalized_end)]
        return dates

    def get_latest_margin_date(self) -> str | None:
        margin_dates = [
            str(row["date"])
            for row in self.margin_rows
            if row.get("date")
        ]
        if not margin_dates:
            return None
        return max(margin_dates, key=_date_sort_key)

    def get_margin_codes(self) -> set[str]:
        return {
            str(row["code"])
            for row in self.margin_rows
            if row.get("code")
        }

    def get_index_master_codes(self) -> set[str]:
        codes: set[str] = set()
        for row in self.index_master_rows:
            code = row.get("code")
            if code is not None:
                codes.add(str(code))
        return codes

    def get_latest_statement_disclosed_date(self) -> str | None:
        if not self.statements_rows:
            return None
        return max(str(row["disclosed_date"]) for row in self.statements_rows if row.get("disclosed_date"))

    def get_statement_codes(self) -> set[str]:
        return {
            str(row["code"])
            for row in self.statements_rows
            if row.get("code")
        }

    def get_prime_codes(self) -> set[str]:
        if self._prime_codes:
            return set(self._prime_codes)
        return {
            str(row["code"])
            for row in self.stocks_rows
            if str(row.get("market_code", "")).lower() in {"0111", "prime"}
        }

    def get_fundamentals_target_codes(self) -> set[str]:
        configured = set(self._fundamentals_target_codes) | set(self._prime_codes)
        if configured:
            return configured
        return {
            str(row["code"])
            for row in self.stocks_rows
            if str(row.get("market_code", "")).lower() in {"0111", "0112", "0113", "prime", "standard", "growth"}
        }

    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]:
        configured = self.get_fundamentals_target_codes()
        if configured:
            rows_by_code = {
                str(row["code"]): {
                    "code": str(row["code"]),
                    "company_name": str(row.get("company_name", "") or ""),
                    "market_code": str(row.get("market_code", "") or "0111"),
                }
                for row in self.stocks_rows
                if row.get("code")
            }
            return [
                rows_by_code.get(
                    code,
                    {"code": code, "company_name": "", "market_code": "0111"},
                )
                for code in sorted(configured)
            ]
        return [
            {
                "code": str(row["code"]),
                "company_name": str(row.get("company_name", "") or ""),
                "market_code": str(row.get("market_code", "") or ""),
            }
            for row in self.stocks_rows
            if str(row.get("market_code", "")).lower() in {"0111", "0112", "0113", "prime", "standard", "growth"}
            and row.get("code")
        ]

    def get_stocks_needing_refresh(self, limit: int | None = None) -> list[str]:
        codes = list(dict.fromkeys(self._stocks_needing_refresh))
        if limit is None:
            return codes
        return codes[:limit]

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            str(row["date"]): dict(row)
            for row in self.topix_rows
            if row.get("date")
        }
        for row in rows:
            row_date = str(row.get("date", "")).strip()
            if not row_date:
                continue
            upserted[row_date] = dict(row)
        self.topix_rows = list(upserted.values())
        return len(rows)

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        self.stocks_rows.extend(rows)
        for row in rows:
            if str(row.get("market_code", "")).lower() in {"0111", "prime"} and row.get("code"):
                self._prime_codes.add(str(row["code"]))
            if str(row.get("market_code", "")).lower() in {"0111", "0112", "0113", "prime", "standard", "growth"} and row.get("code"):
                self._fundamentals_target_codes.add(str(row["code"]))
        return len(rows)

    def upsert_stock_master_daily(self, snapshot_date: str, rows: list[dict[str, Any]]) -> int:
        self.stock_master_daily_rows = [
            row
            for row in self.stock_master_daily_rows
            if str(row.get("date")) != snapshot_date
        ]
        for row in rows:
            enriched = dict(row)
            enriched["date"] = snapshot_date
            self.stock_master_daily_rows.append(enriched)
        return len(rows)

    def rebuild_stock_master_intervals(self) -> int:
        self.stock_master_interval_rebuilds += 1
        return len(self.stock_master_daily_rows)

    def rebuild_stocks_latest(self) -> int:
        self.stocks_latest_rebuilds += 1
        latest_date = max(
            (str(row["date"]) for row in self.stock_master_daily_rows if row.get("date")),
            default=None,
        )
        if latest_date is None:
            return 0
        latest_rows = [
            {key: value for key, value in row.items() if key != "date"}
            for row in self.stock_master_daily_rows
            if row.get("date") == latest_date
        ]
        self.upsert_stocks(latest_rows)
        return len(latest_rows)

    def get_missing_stock_master_dates(self, *, limit: int | None = 20) -> list[str]:
        master_dates = {str(row["date"]) for row in self.stock_master_daily_rows if row.get("date")}
        missing = [date for date in self.get_topix_dates() if date not in master_dates]
        if limit is None:
            return missing
        return missing[:limit]

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            (str(row["code"]), str(row["date"])): dict(row)
            for row in self.stock_rows
            if row.get("code") and row.get("date")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            row_date = str(row.get("date", "")).strip()
            if not code or not row_date:
                continue
            upserted[(code, row_date)] = dict(row)
        self.stock_rows = list(upserted.values())
        return len(rows)

    def upsert_index_master(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            str(row["code"]): dict(row)
            for row in self.index_master_rows
            if row.get("code")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            upserted[code] = dict(row)
        self.index_master_rows = list(upserted.values())
        return len(rows)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            (str(row["code"]), str(row["date"])): dict(row)
            for row in self.indices_rows
            if row.get("code") and row.get("date")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            row_date = str(row.get("date", "")).strip()
            if not code or not row_date:
                continue
            upserted[(code, row_date)] = dict(row)
        self.indices_rows = list(upserted.values())
        return len(rows)

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            (str(row["code"]), str(row["date"])): dict(row)
            for row in self.margin_rows
            if row.get("code") and row.get("date")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            row_date = str(row.get("date", "")).strip()
            if not code or not row_date:
                continue
            upserted[(code, row_date)] = dict(row)
        self.margin_rows = list(upserted.values())
        return len(rows)

    def upsert_options_225_data(self, rows: list[dict[str, Any]]) -> int:
        upserted = {
            (str(row["code"]), str(row["date"])): dict(row)
            for row in self.options_225_rows
            if row.get("code") and row.get("date")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            row_date = str(row.get("date", "")).strip()
            if not code or not row_date:
                continue
            upserted[(code, row_date)] = dict(row)
        self.options_225_rows = list(upserted.values())
        return len(rows)

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        upserted: dict[tuple[str, str], dict[str, Any]] = {
            (str(row["code"]), str(row["disclosed_date"])): dict(row)
            for row in self.statements_rows
            if row.get("code") and row.get("disclosed_date")
        }
        for row in rows:
            code = str(row.get("code", "")).strip()
            disclosed_date = str(row.get("disclosed_date", "")).strip()
            if not code or not disclosed_date:
                continue
            key = (code, disclosed_date)
            existing = upserted.get(key, {})
            merged = dict(existing)
            for column, value in row.items():
                if value is not None:
                    merged[column] = value
                elif column not in merged:
                    merged[column] = value
            upserted[key] = merged
        self.statements_rows = list(upserted.values())
        return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def mark_stock_adjustments_resolved(self, codes: list[str] | None = None) -> int:
        normalized = None if codes is None else list(dict.fromkeys(str(code) for code in codes))
        self.resolved_adjustment_calls.append(normalized)
        if normalized is None:
            self._stocks_needing_refresh = []
            return 0
        resolved = set(normalized)
        self._stocks_needing_refresh = [
            code for code in self._stocks_needing_refresh
            if code not in resolved
        ]
        return len(normalized)

    def ensure_schema(self) -> None:
        return None


def _normalize_index_code(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        return ""
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text.upper()


def _inspection_from_market_db(
    market_db: DummyMarketDb,
    *,
    missing_stock_dates_limit: int = 0,
    missing_options_225_dates_limit: int = 0,
) -> TimeSeriesInspection:
    topix_dates = sorted(
        {
            str(row.get("date"))
            for row in market_db.topix_rows
            if row.get("date")
        },
        key=_date_sort_key,
    )
    stock_dates = sorted(
        {
            str(row.get("date"))
            for row in market_db.stock_rows
            if row.get("date")
        },
        key=_date_sort_key,
    )
    indices_dates = sorted(
        {
            str(row.get("date"))
            for row in market_db.indices_rows
            if row.get("date")
        },
        key=_date_sort_key,
    )

    latest_indices_dates: dict[str, str] = {}
    for row in market_db.indices_rows:
        code = _normalize_index_code(row.get("code"))
        row_date = str(row.get("date", "")).strip()
        if not code or not row_date:
            continue
        current = latest_indices_dates.get(code)
        if current is None or _is_date_after(row_date, current):
            latest_indices_dates[code] = row_date
    if not latest_indices_dates:
        latest_indices_dates = {
            _normalize_index_code(code): value
            for code, value in market_db.get_latest_indices_data_dates().items()
            if _normalize_index_code(code) and value
        }

    statement_dates = [
        str(row.get("disclosed_date"))
        for row in market_db.statements_rows
        if row.get("disclosed_date")
    ]
    latest_statement_disclosed_date = (
        max(statement_dates, key=_date_sort_key) if statement_dates else None
    )
    statement_codes = {
        str(row.get("code"))
        for row in market_db.statements_rows
        if row.get("code")
    }
    margin_dates = sorted(
        {
            str(row.get("date"))
            for row in market_db.margin_rows
            if row.get("date")
        },
        key=_date_sort_key,
    )
    options_225_dates = sorted(
        {
            str(row.get("date"))
            for row in market_db.options_225_rows
            if row.get("date")
        },
        key=_date_sort_key,
    )
    stock_date_set = set(stock_dates)
    options_225_date_set = set(options_225_dates)
    missing_stock_dates_all = [date for date in reversed(topix_dates) if date not in stock_date_set]
    missing_options_225_dates_all = [date for date in reversed(topix_dates) if date not in options_225_date_set]
    margin_codes = {
        str(row.get("code"))
        for row in market_db.margin_rows
        if row.get("code")
    }

    return TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=len(market_db.topix_rows),
        topix_min=topix_dates[0] if topix_dates else market_db.latest_trading_date,
        topix_max=topix_dates[-1] if topix_dates else market_db.latest_trading_date,
        stock_count=len(market_db.stock_rows),
        stock_min=stock_dates[0] if stock_dates else market_db.latest_stock_data_date,
        stock_max=stock_dates[-1] if stock_dates else market_db.latest_stock_data_date,
        stock_date_count=len(stock_dates),
        missing_stock_dates=missing_stock_dates_all[:missing_stock_dates_limit],
        missing_stock_dates_count=len(missing_stock_dates_all),
        indices_count=len(market_db.indices_rows),
        indices_min=indices_dates[0] if indices_dates else _latest_date(list(latest_indices_dates.values())),
        indices_max=indices_dates[-1] if indices_dates else _latest_date(list(latest_indices_dates.values())),
        indices_date_count=len(indices_dates),
        latest_indices_dates=latest_indices_dates,
        options_225_count=len(market_db.options_225_rows),
        options_225_min=options_225_dates[0] if options_225_dates else None,
        options_225_max=options_225_dates[-1] if options_225_dates else None,
        options_225_date_count=len(options_225_dates),
        latest_options_225_date=options_225_dates[-1] if options_225_dates else None,
        missing_options_225_dates=missing_options_225_dates_all[:missing_options_225_dates_limit],
        missing_options_225_dates_count=len(missing_options_225_dates_all),
        margin_count=len(market_db.margin_rows),
        margin_min=margin_dates[0] if margin_dates else market_db.get_latest_margin_date(),
        margin_max=margin_dates[-1] if margin_dates else market_db.get_latest_margin_date(),
        margin_date_count=len(margin_dates),
        margin_codes=margin_codes,
        statements_count=len(market_db.statements_rows),
        latest_statement_disclosed_date=latest_statement_disclosed_date,
        statement_codes=statement_codes,
    )


class DummyTimeSeriesStore:
    def __init__(
        self,
        market_db: DummyMarketDb,
        inspection: TimeSeriesInspection | None = None,
    ) -> None:
        self._market_db = market_db
        self._inspection = inspection

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_topix_data(rows)

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_stock_data(rows)

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_indices_data(rows)

    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_options_225_data(rows)

    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_margin_data(rows)

    def publish_statements(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_statements(rows)

    def index_topix_data(self) -> None:
        return None

    def index_stock_data(self) -> None:
        return None

    def index_indices_data(self) -> None:
        return None

    def index_options_225_data(self) -> None:
        return None

    def index_margin_data(self) -> None:
        return None

    def index_statements(self) -> None:
        return None

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        del statement_non_null_columns
        if self._inspection is not None:
            return self._inspection
        return _inspection_from_market_db(
            self._market_db,
            missing_stock_dates_limit=missing_stock_dates_limit,
            missing_options_225_dates_limit=missing_options_225_dates_limit,
        )

    def close(self) -> None:
        return None


class FailingInspectionStore(DummyTimeSeriesStore):
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        del missing_stock_dates_limit, missing_options_225_dates_limit, statement_non_null_columns
        raise RuntimeError("inspect failed")


def _build_ctx(
    *,
    client: Any,
    market_db: DummyMarketDb,
    cancelled: asyncio.Event | None = None,
    on_progress: Any = None,
    time_series_store: DummyTimeSeriesStore | None = None,
    bulk_service: Any = None,
    bulk_probe_disabled: bool = True,
    enforce_bulk_for_stock_data: bool = False,
) -> SyncContext:
    resolved_cancelled = cancelled or asyncio.Event()
    resolved_on_progress = on_progress or (lambda *_: None)
    resolved_store = time_series_store or DummyTimeSeriesStore(market_db)
    return SyncContext(
        client=client,
        market_db=market_db,
        cancelled=resolved_cancelled,
        on_progress=resolved_on_progress,
        time_series_store=resolved_store,
        bulk_service=bulk_service,
        bulk_probe_disabled=bulk_probe_disabled,
        enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
    )


class DummyClient:
    def __init__(
        self,
        daily_quotes: list[dict[str, Any]] | None = None,
        indices_quotes: list[dict[str, Any]] | None = None,
        master_quotes: list[dict[str, Any]] | None = None,
        options_quotes: list[dict[str, Any]] | None = None,
        margin_by_code: dict[str, list[dict[str, Any]]] | None = None,
        daily_error_dates: set[str] | None = None,
        fins_by_code: dict[str, list[dict[str, Any]]] | None = None,
        fins_by_date: dict[str, list[dict[str, Any]]] | None = None,
        fins_paginated_codes: dict[str, list[list[dict[str, Any]]]] | None = None,
        fins_error_codes: set[str] | None = None,
        fins_error_dates: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.daily_quotes = daily_quotes
        self.indices_quotes = indices_quotes
        self.master_quotes = master_quotes
        self.options_quotes = options_quotes
        self.margin_by_code = margin_by_code or {}
        self.daily_error_dates = daily_error_dates or set()
        self.fins_by_code = fins_by_code or {}
        self.fins_by_date = fins_by_date or {}
        self.fins_paginated_codes = fins_paginated_codes or {}
        self.fins_error_codes = fins_error_codes or set()
        self.fins_error_dates = fins_error_dates or set()

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/indices":
            return {
                "data": [
                    {
                        "code": "0000",
                        "name": "TOPIX",
                        "name_english": "TOPIX",
                        "category": "topix",
                        "data_start_date": "2008-05-07",
                    }
                ]
            }
        if path == "/fins/summary":
            params = params or {}
            code = str(params.get("code", ""))
            date = str(params.get("date", ""))
            pagination = str(params.get("pagination_key", ""))

            if code in self.fins_error_codes:
                raise RuntimeError("fins code failed")
            if date in self.fins_error_dates:
                raise RuntimeError("fins date failed")

            if code in self.fins_paginated_codes:
                pages = self.fins_paginated_codes[code]
                index = int(pagination) if pagination else 0
                body: dict[str, Any] = {"data": pages[index] if index < len(pages) else []}
                if index + 1 < len(pages):
                    body["pagination_key"] = str(index + 1)
                return body

            if code and code in self.fins_by_code:
                return {"data": self.fins_by_code[code]}
            if date and date in self.fins_by_date:
                return {"data": self.fins_by_date[date]}
            return {"data": []}
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/indices/bars/daily/topix":
            if params and params.get("from") == "20260210":
                return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102}]
            return [
                {"Date": "2026-02-06", "O": 100, "H": 101, "L": 99, "C": 100},
                {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102},
            ]
        if path == "/indices/bars/daily":
            if self.indices_quotes is not None:
                return self._filter_indices_quotes(params)

            requested_code = self._normalize_index_code((params or {}).get("code"))
            if requested_code:
                if (params or {}).get("from") == "20260210":
                    return [{
                        "Date": "2026-02-10",
                        "Code": requested_code,
                        "O": 102,
                        "H": 103,
                        "L": 101,
                        "C": 102,
                        "SectorName": "TOPIX",
                    }]
                return [
                    {
                        "Date": "2026-02-06",
                        "Code": requested_code,
                        "O": 100,
                        "H": 101,
                        "L": 99,
                        "C": 100,
                        "SectorName": "TOPIX",
                    },
                    {
                        "Date": "2026-02-10",
                        "Code": requested_code,
                        "O": 102,
                        "H": 103,
                        "L": 101,
                        "C": 102,
                        "SectorName": "TOPIX",
                    },
                ]

            requested_date = self._normalize_date((params or {}).get("date"))
            if requested_date:
                return [{
                    "Date": requested_date,
                    "Code": "0000",
                    "O": 102,
                    "H": 103,
                    "L": 101,
                    "C": 102,
                    "SectorName": "TOPIX",
                }]

            return []
        if path == "/equities/master":
            if self.master_quotes is not None:
                return self.master_quotes
            return [
                {
                    "Code": "72030",
                    "CoName": "トヨタ自動車",
                    "Mkt": "0111",
                    "MktNm": "プライム",
                    "S17": "6",
                    "S17Nm": "輸送用機器",
                    "S33": "3700",
                    "S33Nm": "輸送用機器",
                    "Date": "1949-05-16",
                }
            ]
        if path == "/markets/margin-interest":
            code = str((params or {}).get("code", ""))
            if code and code in self.margin_by_code:
                return self.margin_by_code[code]
            if params and params.get("from") == "20260210":
                return [{"Code": code or "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200}]
            return [
                {"Code": code or "72030", "Date": "2026-02-06", "LongVol": 900, "ShrtVol": 250},
                {"Code": code or "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        if path == "/derivatives/bars/daily/options/225":
            requested_date = self._normalize_date((params or {}).get("date"))
            if self.options_quotes is not None:
                rows: list[dict[str, Any]] = []
                for source in self.options_quotes:
                    row = dict(source)
                    row_date = self._normalize_date(row.get("Date") or row.get("date"))
                    if requested_date and row_date and row_date != requested_date:
                        continue
                    rows.append(row)
                return rows
            option_date = requested_date or "2026-02-06"
            return [
                {
                    "Date": option_date,
                    "Code": "131040018",
                    "CM": "2026-04",
                    "Strike": 20000,
                    "PCDiv": "1",
                    "UnderPx": 39000.0,
                }
            ]
        if path == "/equities/bars/daily":
            if self.daily_quotes is not None:
                rows: list[dict[str, Any]] = []
                for quote in self.daily_quotes:
                    row = dict(quote)
                    if "Date" not in row:
                        row["Date"] = params["date"] if params else ""
                    rows.append(row)
                return rows
            date_value = params["date"] if params else ""
            if date_value in self.daily_error_dates:
                raise RuntimeError("daily fetch failed")
            return [{"Code": "72030", "Date": date_value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}]
        return []

    @staticmethod
    def _normalize_index_code(value: Any) -> str:
        code = str(value).strip() if value is not None else ""
        if not code:
            return ""
        if code.isdigit() and len(code) < 4:
            return code.zfill(4)
        return code.upper()

    @staticmethod
    def _normalize_date(value: Any) -> str:
        text = str(value).strip() if value is not None else ""
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        return text

    def _filter_indices_quotes(self, params: dict[str, Any] | None) -> list[dict[str, Any]]:
        requested_code = self._normalize_index_code((params or {}).get("code"))
        requested_date = self._normalize_date((params or {}).get("date"))
        rows: list[dict[str, Any]] = []

        for source in self.indices_quotes or []:
            row = dict(source)
            row_code = self._normalize_index_code(row.get("Code") or row.get("code"))
            row_date = self._normalize_date(row.get("Date") or row.get("date"))

            if requested_code:
                if row_code:
                    if row_code != requested_code:
                        continue
                elif requested_code != "0000":
                    continue

            if requested_date and row_date and row_date != requested_date:
                continue

            rows.append(row)

        return rows


def test_resolve_bulk_fallback_reason_handles_missing_empty_and_unavailable_plan() -> None:
    assert _resolve_bulk_fallback_reason(None) == "bulk_plan_missing"
    assert (
        _resolve_bulk_fallback_reason(
            BulkFetchPlan(
                endpoint="/equities/bars/daily",
                files=[],
                list_api_calls=1,
                estimated_api_calls=1,
                estimated_cache_hits=0,
                estimated_cache_misses=0,
            )
        )
        == "bulk_plan_empty"
    )
    assert (
        _resolve_bulk_fallback_reason(
            BulkFetchPlan(
                endpoint="/equities/bars/daily",
                files=[
                    BulkFileInfo(
                        key="test.csv.gz",
                        last_modified="2026-03-19T00:00:00Z",
                        size=123,
                        range_start=None,
                        range_end=None,
                    )
                ],
                list_api_calls=1,
                estimated_api_calls=2,
                estimated_cache_hits=0,
                estimated_cache_misses=1,
            )
        )
        == "bulk_plan_unavailable"
    )


def test_enforce_stock_bulk_plan_available_raises_for_empty_bulk_plan_files() -> None:
    progress_messages: list[str] = []
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=DummyMarketDb(),
        on_progress=lambda _stage, _current, _total, message: progress_messages.append(message),
        enforce_bulk_for_stock_data=True,
    )

    decision = _StageFetchDecision(
        method="bulk",
        planner_api_calls=1,
        estimated_rest_calls=10,
        estimated_bulk_calls=1,
        plan=BulkFetchPlan(
            endpoint="/equities/bars/daily",
            files=[],
            list_api_calls=1,
            estimated_api_calls=1,
            estimated_cache_hits=0,
            estimated_cache_misses=0,
        ),
        reason="unspecified",
    )

    with pytest.raises(BulkFetchRequiredError, match="bulk/list returned no matching files"):
        _enforce_stock_bulk_plan_available(
            ctx,
            decision=decision,
            endpoint="/equities/bars/daily",
            progress_stage="stock_data",
            current=2,
            total=5,
            target_count=3,
        )

    assert any("REST fallback is disabled for stock_data sync." in message for message in progress_messages)


def test_enforce_stock_bulk_plan_available_accepts_non_empty_bulk_plan_files() -> None:
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=DummyMarketDb(),
        enforce_bulk_for_stock_data=True,
    )
    decision = _StageFetchDecision(
        method="bulk",
        planner_api_calls=1,
        estimated_rest_calls=10,
        estimated_bulk_calls=1,
        plan=BulkFetchPlan(
            endpoint="/equities/bars/daily",
            files=[
                BulkFileInfo(
                    key="test.csv.gz",
                    last_modified="2026-03-19T00:00:00Z",
                    size=123,
                    range_start=None,
                    range_end=None,
                )
            ],
            list_api_calls=1,
            estimated_api_calls=1,
            estimated_cache_hits=0,
            estimated_cache_misses=0,
        ),
        reason="unspecified",
    )

    _enforce_stock_bulk_plan_available(
        ctx,
        decision=decision,
        endpoint="/equities/bars/daily",
        progress_stage="stock_data",
        current=2,
        total=5,
        target_count=3,
    )


@pytest.mark.asyncio
async def test_sync_daily_stock_master_fetches_each_topix_date_and_rebuilds_latest() -> None:
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=["2026-02-06", "2026-02-10"])
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        on_progress=lambda *_: None,
    )

    result = await _sync_daily_stock_master(
        ctx,
        target_dates=["2026-02-06", "2026-02-10"],
        progress_current=1,
        progress_total=8,
    )

    assert result["cancelled"] is False
    assert result["updated"] == 2
    assert result["api_calls"] == 2
    assert market_db.stock_master_interval_rebuilds == 1
    assert market_db.stocks_latest_rebuilds == 1
    assert {row["date"] for row in market_db.stock_master_daily_rows} == {
        "2026-02-06",
        "2026-02-10",
    }
    assert [
        params for path, params in client.calls if path == "/equities/master"
    ] == [{"date": "20260206"}, {"date": "20260210"}]
    assert METADATA_KEYS["LAST_STOCKS_REFRESH"] in market_db.metadata

    _enforce_stock_bulk_plan_available(
        ctx,
        decision=_StageFetchDecision(
            method="bulk",
            planner_api_calls=1,
            estimated_rest_calls=10,
            estimated_bulk_calls=1,
            plan=BulkFetchPlan(
                endpoint="/equities/bars/daily",
                files=[
                    BulkFileInfo(
                        key="test.csv.gz",
                        last_modified="2026-03-19T00:00:00Z",
                        size=123,
                        range_start=None,
                        range_end=None,
                    )
                ],
                list_api_calls=1,
                estimated_api_calls=2,
                estimated_cache_hits=0,
                estimated_cache_misses=1,
            ),
            reason="unspecified",
        ),
        endpoint="/equities/bars/daily",
        progress_stage="stock_data",
        current=2,
        total=5,
        target_count=3,
    )


class InitialSyncClient:
    def __init__(
        self,
        *,
        topix_dates: list[str],
        fail_stock_dates: set[str] | None = None,
        master_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self._topix_dates = topix_dates
        self._fail_stock_dates = fail_stock_dates or set()
        self._master_rows = master_rows

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/indices":
            return {
                "data": [
                    {
                        "code": "0000",
                        "name": "TOPIX",
                        "name_english": "TOPIX",
                        "category": "topix",
                        "data_start_date": "2008-05-07",
                    }
                ]
            }
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/indices/bars/daily/topix":
            return [{"Date": d, "O": 100, "H": 101, "L": 99, "C": 100} for d in self._topix_dates]
        if path == "/equities/master":
            if self._master_rows is not None:
                return self._master_rows
            return [
                {
                    "Code": "72030",
                    "CoName": "トヨタ自動車",
                    "Mkt": "0111",
                    "MktNm": "プライム",
                    "S17": "6",
                    "S17Nm": "輸送用機器",
                    "S33": "3700",
                    "S33Nm": "輸送用機器",
                    "Date": "1949-05-16",
                },
                {"Code": "", "CoName": "invalid"},
            ]
        if path == "/equities/bars/daily":
            date_value = (params or {}).get("date", "")
            if date_value in self._fail_stock_dates:
                raise RuntimeError("daily quotes unavailable")
            return [{"Code": "72030", "Date": date_value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}]
        if path == "/markets/margin-interest":
            code = str((params or {}).get("code", "")) or "72030"
            return [
                {"Code": code, "Date": "2026-02-06", "LongVol": 900, "ShrtVol": 250},
                {"Code": code, "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        if path == "/derivatives/bars/daily/options/225":
            option_date = DummyClient._normalize_date((params or {}).get("date")) or "2026-02-06"
            return [
                {
                    "Date": option_date,
                    "Code": "131040018",
                    "CM": "2026-04",
                    "Strike": 20000,
                    "PCDiv": "1",
                    "UnderPx": 39000.0,
                }
            ]
        if path == "/indices/bars/daily":
            return [{"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"}]
        return []


class IndicesOnlyClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((path, params))
        if path == "/derivatives/bars/daily/options/225":
            return []
        if path != "/indices/bars/daily":
            raise RuntimeError(f"unexpected path: {path}")
        if (params or {}).get("code") == "9999":
            raise RuntimeError("boom")
        return [
            {"Date": "", "O": 1, "H": 2, "L": 1, "C": 2, "SectorName": "TOPIX"},
            {"Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11, "SectorName": "TOPIX"},
        ]


class _FakeBulkService:
    def __init__(
        self,
        *,
        results_by_endpoint: dict[str, BulkFetchResult] | None = None,
        fail_endpoints: set[str] | None = None,
    ) -> None:
        self.results_by_endpoint = results_by_endpoint or {}
        self.fail_endpoints = fail_endpoints or set()
        self.fetch_calls: list[str] = []

    async def fetch_with_plan(
        self,
        plan: BulkFetchPlan,
        *,
        on_rows_batch: Any | None = None,
        accumulate_rows: bool = True,
    ) -> BulkFetchResult:
        self.fetch_calls.append(plan.endpoint)
        if plan.endpoint in self.fail_endpoints:
            raise RuntimeError("bulk failed")
        result = self.results_by_endpoint.get(plan.endpoint)
        if result is None:
            return BulkFetchResult(rows=[], api_calls=0, cache_hits=0, cache_misses=0, selected_files=0)
        if on_rows_batch is not None:
            await on_rows_batch(result.rows, None)
        if not accumulate_rows:
            return BulkFetchResult(
                rows=[],
                api_calls=result.api_calls,
                cache_hits=result.cache_hits,
                cache_misses=result.cache_misses,
                selected_files=result.selected_files,
            )
        return result


class _PlanOnlyClient:
    def __init__(self, plan: str) -> None:
        self.plan = plan

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        del path, params
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        del path, params
        return []


class _PlanOnlyBulkService:
    def __init__(self, plan: BulkFetchPlan | Exception) -> None:
        self._plan = plan
        self.build_calls = 0

    async def build_plan(
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> BulkFetchPlan:
        del endpoint, date_from, date_to, exact_dates
        self.build_calls += 1
        if isinstance(self._plan, Exception):
            raise self._plan
        return self._plan


def _rest_decision(estimated_rest_calls: int) -> _StageFetchDecision:
    return _StageFetchDecision(
        method="rest",
        planner_api_calls=0,
        estimated_rest_calls=estimated_rest_calls,
        estimated_bulk_calls=None,
        plan=None,
    )


@pytest.fixture(autouse=True)
def patch_small_index_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    def _seed_rows(*, existing_codes: set[str] | None = None) -> list[dict[str, str | None]]:
        existing = existing_codes or set()
        rows: list[dict[str, str | None]] = []
        if "0000" not in existing:
            rows.append({
                "code": "0000",
                "name": "TOPIX",
                "name_english": None,
                "category": "topix",
                "data_start_date": "2008-05-07",
                "created_at": "2026-02-10T00:00:00+00:00",
            })
        if "0040" not in existing:
            rows.append({
                "code": "0040",
                "name": "東証業種別 水産・農林業",
                "name_english": None,
                "category": "sector33",
                "data_start_date": "2010-01-04",
                "created_at": "2026-02-10T00:00:00+00:00",
            })
        return rows

    monkeypatch.setattr(
        "src.application.services.sync_strategies.get_index_catalog_codes",
        lambda: {"0000", "0040"},
    )
    monkeypatch.setattr(
        "src.application.services.sync_strategies.build_index_master_seed_rows",
        _seed_rows,
    )


@pytest.mark.asyncio
async def test_plan_fetch_method_probes_bulk_even_if_plan_hint_is_free() -> None:
    bulk_plan = BulkFetchPlan(
        endpoint="/equities/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=1,
        estimated_cache_hits=0,
        estimated_cache_misses=0,
    )
    bulk_service = _PlanOnlyBulkService(bulk_plan)
    ctx = _build_ctx(
        client=_PlanOnlyClient("free"),
        market_db=DummyMarketDb(),
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    decision = await _plan_fetch_method(
        ctx,
        stage="stock_data_incremental",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=1200,
    )

    assert decision.method == "bulk"
    assert decision.planner_api_calls == 1
    assert bulk_service.build_calls == 1
    assert ctx.bulk_probe_disabled is False


@pytest.mark.asyncio
async def test_plan_fetch_method_disables_future_probe_after_bulk_probe_failure() -> None:
    bulk_service = _PlanOnlyBulkService(RuntimeError("bulk list forbidden"))
    ctx = _build_ctx(
        client=_PlanOnlyClient("free"),
        market_db=DummyMarketDb(),
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    first = await _plan_fetch_method(
        ctx,
        stage="stock_data_incremental",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=1200,
    )
    second = await _plan_fetch_method(
        ctx,
        stage="indices_incremental",
        endpoint="/indices/bars/daily",
        estimated_rest_calls=1200,
    )

    assert first.method == "rest"
    assert first.planner_api_calls == 1
    assert second.method == "rest"
    assert second.planner_api_calls == 0
    assert bulk_service.build_calls == 1
    assert ctx.bulk_probe_disabled is True


@pytest.mark.asyncio
async def test_plan_fetch_method_requires_bulk_even_when_rest_estimate_is_small() -> None:
    bulk_plan = BulkFetchPlan(
        endpoint="/equities/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=5,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _PlanOnlyBulkService(bulk_plan)
    ctx = _build_ctx(
        client=_PlanOnlyClient("premium"),
        market_db=DummyMarketDb(),
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    decision = await _plan_fetch_method(
        ctx,
        stage="stock_data_incremental",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=1,
        require_bulk=True,
    )

    assert decision.method == "bulk"
    assert decision.reason == "bulk_required"
    assert decision.planner_api_calls == 1
    assert bulk_service.build_calls == 1


@pytest.mark.asyncio
async def test_incremental_sync_handles_mixed_date_formats() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient()

    progresses: list[tuple[str, int, int, str]] = []

    def on_progress(stage: str, current: int, total: int, message: str) -> None:
        progresses.append((stage, current, total, message))

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=on_progress,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 2
    assert result.stocksUpdated == 2
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-06"} for path, params in client.calls)
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-10"} for path, params in client.calls)

    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {"from": "20260206"}

    assert market_db.metadata.get(METADATA_KEYS["LAST_SYNC_DATE"])
    assert any(
        "/equities/bars/daily" in message and ("REST" in message or "BULK" in message)
        for _, _, _, message in progresses
    )
    assert progresses[-1][0] == "complete"


@pytest.mark.asyncio
async def test_incremental_sync_backfills_margin_when_market_snapshot_lacks_margin() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        topix_min="2026-02-06",
        topix_max="2026-02-06",
        stock_count=1,
        stock_min="2026-02-06",
        stock_max="2026-02-06",
        stock_date_count=1,
        indices_count=1,
        indices_min="2026-02-06",
        indices_max="2026-02-06",
        indices_date_count=1,
        latest_indices_dates={"0000": "2026-02-06"},
        margin_count=0,
        margin_codes=set(),
    )
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0112",
                "MktNm": "スタンダード",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ],
        margin_by_code={
            "72030": [
                {"Code": "72030", "Date": "2026-02-06", "LongVol": 900, "ShrtVol": 250},
                {"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        },
    )
    store = DummyTimeSeriesStore(market_db, inspection)
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        time_series_store=store,
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert any(path == "/markets/margin-interest" for path, _ in client.calls)
    assert market_db.get_margin_codes() == {"7203"}
    assert market_db.get_latest_margin_date() == "2026-02-10"


@pytest.mark.asyncio
async def test_incremental_sync_logs_margin_backfill_execution_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        topix_min="2026-02-06",
        topix_max="2026-02-06",
        stock_count=1,
        stock_min="2026-02-06",
        stock_max="2026-02-06",
        stock_date_count=1,
        indices_count=1,
        indices_min="2026-02-06",
        indices_max="2026-02-06",
        indices_date_count=1,
        latest_indices_dates={"0000": "2026-02-06"},
        margin_count=1,
        margin_min="2026-02-06",
        margin_max="2026-02-06",
        margin_date_count=1,
        margin_codes={"7203"},
    )
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0112",
                "MktNm": "スタンダード",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {
                "Code": "67580",
                "CoName": "ソニーグループ",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "10",
                "S17Nm": "電気機器",
                "S33": "3650",
                "S33Nm": "電気機器",
                "Date": "1958-12-01",
            },
        ],
        margin_by_code={
            "72030": [
                {"Code": "72030", "Date": "2026-02-10", "LongVol": 1100, "ShrtVol": 210},
            ],
            "67580": [
                {"Code": "67580", "Date": "2026-02-06", "LongVol": 700, "ShrtVol": 180},
                {"Code": "67580", "Date": "2026-02-10", "LongVol": 720, "ShrtVol": 185},
            ],
        },
    )
    store = DummyTimeSeriesStore(market_db, inspection)
    logged_stages: list[tuple[str, str, int]] = []

    def fake_log_sync_fetch_execution(
        *,
        stage: str,
        endpoint: str,
        decision: _StageFetchDecision,
        executed: str,
        actual_api_calls: int,
        fallback: bool,
        bulk_result: BulkFetchResult | None = None,
    ) -> None:
        del decision, fallback, bulk_result
        logged_stages.append((stage, endpoint, actual_api_calls))

    monkeypatch.setattr(
        sync_strategies_module,
        "_log_sync_fetch_execution",
        fake_log_sync_fetch_execution,
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        time_series_store=store,
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert ("margin_incremental", "/markets/margin-interest", 1) in logged_stages
    assert ("margin_incremental_backfill", "/markets/margin-interest", 1) in logged_stages


@pytest.mark.asyncio
async def test_initial_sync_populates_margin_data() -> None:
    market_db = DummyMarketDb()
    client = InitialSyncClient(
        topix_dates=["2026-02-06", "2026-02-10"],
        master_rows=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0112",
                "MktNm": "スタンダード",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ],
    )
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert any(path == "/markets/margin-interest" for path, _ in client.calls)
    assert market_db.get_margin_codes() == {"7203"}


@pytest.mark.asyncio
async def test_sync_margin_data_bulk_success_backfills_missing_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    bulk_plan = BulkFetchPlan(
        endpoint="/markets/margin-interest",
        files=[],
        list_api_calls=1,
        estimated_api_calls=2,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _FakeBulkService(
        results_by_endpoint={
            "/markets/margin-interest": BulkFetchResult(
                rows=[
                    {"Code": "72030", "Date": "2026-02-06", "LongVol": 900, "ShrtVol": 250},
                    {"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
                ],
                api_calls=2,
                cache_hits=0,
                cache_misses=1,
                selected_files=1,
            )
        }
    )
    client = DummyClient(
        margin_by_code={
            "6758": [
                {"Code": "6758", "Date": "2026-02-04", "LongVol": 400, "ShrtVol": 100},
                {"Code": "6758", "Date": "2026-02-10", "LongVol": 450, "ShrtVol": 120},
            ]
        }
    )
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="bulk",
            planner_api_calls=1,
            estimated_rest_calls=2,
            estimated_bulk_calls=2,
            plan=bulk_plan,
            reason="planner_selected_bulk",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203", "6758"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
        anchor="2026-02-06",
        existing_margin_codes={"7203"},
    )

    assert result["cancelled"] is False
    assert result["errors"] == []
    assert result["updated"] == 3
    assert market_db.get_margin_codes() == {"7203", "6758"}
    assert any(path == "/markets/margin-interest" and params == {"code": "67580"} for path, params in client.calls)


@pytest.mark.asyncio
async def test_sync_margin_data_bulk_fallback_to_rest_collects_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    bulk_plan = BulkFetchPlan(
        endpoint="/markets/margin-interest",
        files=[],
        list_api_calls=1,
        estimated_api_calls=2,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _FakeBulkService(fail_endpoints={"/markets/margin-interest"})

    class ErrorMarginClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest" and str((params or {}).get("code", "")) in {"99990", "9999"}:
                raise RuntimeError("margin failed")
            return await super().get_paginated(path, params)

    client = ErrorMarginClient()
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="bulk",
            planner_api_calls=1,
            estimated_rest_calls=2,
            estimated_bulk_calls=2,
            plan=bulk_plan,
            reason="planner_selected_bulk",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203", "9999"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
    )

    assert result["cancelled"] is False
    assert len(result["errors"]) == 1
    assert "Margin code 9999" in result["errors"][0]
    assert market_db.get_margin_codes() == {"7203"}


@pytest.mark.asyncio
async def test_sync_margin_data_bulk_empty_plan_falls_back_to_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    bulk_plan = BulkFetchPlan(
        endpoint="/markets/margin-interest",
        files=[],
        list_api_calls=1,
        estimated_api_calls=1,
        estimated_cache_hits=0,
        estimated_cache_misses=0,
    )
    client = DummyClient(
        margin_by_code={
            "72030": [
                {"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        }
    )
    ctx = _build_ctx(client=client, market_db=market_db)

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="bulk",
            planner_api_calls=0,
            estimated_rest_calls=1,
            estimated_bulk_calls=1,
            plan=bulk_plan,
            reason="planner_selected_bulk",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
    )

    assert result["cancelled"] is False
    assert result["errors"] == []
    assert result["updated"] == 1
    assert market_db.get_margin_codes() == {"7203"}


@pytest.mark.asyncio
async def test_sync_margin_data_bulk_missing_plan_falls_back_to_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    client = DummyClient(
        margin_by_code={
            "72030": [
                {"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        }
    )
    ctx = _build_ctx(client=client, market_db=market_db)

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="bulk",
            planner_api_calls=0,
            estimated_rest_calls=1,
            estimated_bulk_calls=1,
            plan=None,
            reason="planner_selected_bulk",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
    )

    assert result["cancelled"] is False
    assert result["errors"] == []
    assert result["updated"] == 1
    assert market_db.get_margin_codes() == {"7203"}


@pytest.mark.asyncio
async def test_sync_margin_data_bulk_fallback_avoids_duplicate_rest_for_backfill_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    bulk_plan = BulkFetchPlan(
        endpoint="/markets/margin-interest",
        files=[],
        list_api_calls=1,
        estimated_api_calls=2,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _FakeBulkService(fail_endpoints={"/markets/margin-interest"})

    class StrictMarginClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest":
                self.calls.append((path, params))
                code = str((params or {}).get("code", ""))
                return list(self.margin_by_code.get(code, []))
            return await super().get_paginated(path, params)

    client = StrictMarginClient(
        margin_by_code={
            "7203": [
                {"Code": "7203", "Date": "2026-02-04", "LongVol": 400, "ShrtVol": 100},
                {"Code": "7203", "Date": "2026-02-10", "LongVol": 450, "ShrtVol": 120},
            ]
        }
    )
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        bulk_service=bulk_service,
        bulk_probe_disabled=False,
    )

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="bulk",
            planner_api_calls=1,
            estimated_rest_calls=1,
            estimated_bulk_calls=1,
            plan=bulk_plan,
            reason="planner_selected_bulk",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
        anchor="2026-02-06",
        existing_margin_codes=set(),
    )

    margin_calls = [params for path, params in client.calls if path == "/markets/margin-interest"]

    assert result["cancelled"] is False
    assert result["errors"] == []
    assert len(margin_calls) == 2
    assert all("from" not in (params or {}) for params in margin_calls)


@pytest.mark.asyncio
async def test_sync_margin_data_returns_cancelled_when_rest_loop_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    cancelled = asyncio.Event()
    cancelled.set()
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=market_db,
        cancelled=cancelled,
    )

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=1,
            estimated_bulk_calls=None,
            reason="rest_estimate_too_small",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result = await _sync_margin_data(
        ctx,
        ["7203"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
    )

    assert result == {
        "api_calls": 0,
        "updated": 0,
        "errors": [],
        "cancelled": True,
    }


@pytest.mark.asyncio
async def test_fetch_margin_by_code_falls_back_to_4digit_after_empty_5digit() -> None:
    class StrictMarginClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest":
                self.calls.append((path, params))
                code = str((params or {}).get("code", ""))
                return list(self.margin_by_code.get(code, []))
            return await super().get_paginated(path, params)

    client = StrictMarginClient(
        margin_by_code={
            "7203": [
                {"Code": "7203", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            ]
        }
    )

    rows, api_calls = await _fetch_margin_by_code(client, "7203")

    assert api_calls == 2
    assert rows == [{"Code": "7203", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200}]


@pytest.mark.asyncio
async def test_fetch_margin_by_code_returns_empty_when_all_candidates_are_empty() -> None:
    class EmptyMarginClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest":
                return []
            return await super().get_paginated(path, params)

    client = EmptyMarginClient(margin_by_code={})

    rows, api_calls = await _fetch_margin_by_code(client, "7203")

    assert rows == []
    assert api_calls == 2


def test_convert_margin_rows_filters_and_deduplicates() -> None:
    rows = _convert_margin_rows(
        [
            {"Code": "", "Date": "2026-02-10", "LongVol": 1, "ShrtVol": 2},
            {"Code": "72030", "Date": None, "LongVol": 1, "ShrtVol": 2},
            {"Code": "72030", "Date": "2026-02-05", "LongVol": 1, "ShrtVol": 2},
            {"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200},
            {"Code": "72030", "Date": "2026-02-10", "longMarginTradeVolume": 1200, "shortMarginTradeVolume": 300},
            {"Code": "67580", "Date": "2026-02-10", "long_margin_volume": 400, "short_margin_volume": 100},
        ],
        target_codes={"7203"},
        min_date_exclusive="2026-02-06",
    )

    assert rows == [
        {
            "code": "7203",
            "date": "2026-02-10",
            "long_margin_volume": 1000.0,
            "short_margin_volume": 200.0,
        }
    ]


@pytest.mark.asyncio
async def test_sync_margin_data_logs_backfill_execution_api_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()

    class StrictMarginClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest":
                self.calls.append((path, params))
                code = str((params or {}).get("code", ""))
                return list(self.margin_by_code.get(code, []))
            return await super().get_paginated(path, params)

    client = StrictMarginClient(
        margin_by_code={
            "6758": [
                {"Code": "6758", "Date": "2026-02-04", "LongVol": 400, "ShrtVol": 100},
                {"Code": "6758", "Date": "2026-02-10", "LongVol": 450, "ShrtVol": 120},
            ]
        }
    )
    ctx = _build_ctx(client=client, market_db=market_db)
    logged_calls: list[dict[str, Any]] = []

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=1,
            estimated_bulk_calls=None,
            reason="rest_estimate_too_small",
        )

    def _capture_log(**kwargs: Any) -> None:
        logged_calls.append(kwargs)

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )
    monkeypatch.setattr(
        "src.application.services.sync_strategies._log_sync_fetch_execution",
        _capture_log,
    )

    result = await _sync_margin_data(
        ctx,
        ["6758"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
        anchor="2026-02-06",
        existing_margin_codes=set(),
    )

    assert result["cancelled"] is False
    assert result["errors"] == []
    assert logged_calls == [
        {
            "stage": "margin_incremental_backfill",
            "endpoint": "/markets/margin-interest",
            "decision": _StageFetchDecision(
                method="rest",
                planner_api_calls=0,
                estimated_rest_calls=1,
                estimated_bulk_calls=None,
                reason="rest_estimate_too_small",
            ),
            "executed": "rest",
            "actual_api_calls": 2,
            "fallback": False,
            "bulk_result": None,
        }
    ]


@pytest.mark.asyncio
async def test_sync_margin_data_persists_empty_cache_and_skips_same_frontier_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()

    class EmptyMarginClient(DummyClient):
        async def get_paginated(
            self,
            path: str,
            params: dict[str, Any] | None = None,
        ) -> list[dict[str, Any]]:
            if path == "/markets/margin-interest":
                self.calls.append((path, params))
                return []
            return await super().get_paginated(path, params)

    client = EmptyMarginClient()
    ctx = _build_ctx(client=client, market_db=market_db)

    async def _fake_plan_fetch_method(*_args: Any, **_kwargs: Any) -> _StageFetchDecision:
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=1,
            estimated_bulk_calls=None,
            reason="rest_estimate_too_small",
        )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._plan_fetch_method",
        _fake_plan_fetch_method,
    )

    result_first = await _sync_margin_data(
        ctx,
        ["4957"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
        anchor="2026-02-27",
        existing_margin_codes=set(),
        trading_frontier="2026-03-06",
    )
    margin_calls_after_first = len([call for call in client.calls if call[0] == "/markets/margin-interest"])

    assert result_first["cancelled"] is False
    assert result_first["updated"] == 0
    assert margin_calls_after_first == 2
    assert json.loads(market_db.metadata[METADATA_KEYS["MARGIN_EMPTY_CODES"]]) == {
        "frontier": "2026-03-06",
        "codes": ["4957"],
    }

    result_second = await _sync_margin_data(
        ctx,
        ["4957"],
        progress_current=1,
        progress_total=2,
        stage_name="margin_incremental",
        anchor="2026-02-27",
        existing_margin_codes=set(),
        trading_frontier="2026-03-06",
    )
    margin_calls_after_second = len([call for call in client.calls if call[0] == "/markets/margin-interest"])

    assert result_second["cancelled"] is False
    assert result_second["updated"] == 0
    assert margin_calls_after_second == margin_calls_after_first


@pytest.mark.asyncio
async def test_incremental_sync_does_not_fallback_to_sqlite_anchor_when_duckdb_has_no_anchor() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206", latest_stock_data_date="20260206")
    client = DummyClient()
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        topix_min="2026-02-06",
        topix_max=None,
        stock_count=1,
        stock_min="2026-02-06",
        stock_max=None,
        indices_count=1,
        indices_min="2026-02-06",
        indices_max=None,
        latest_indices_dates={},
        statements_count=0,
    )
    store = DummyTimeSeriesStore(market_db, inspection)

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=store,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {}


@pytest.mark.asyncio
async def test_incremental_sync_fails_when_duckdb_inspection_raises() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient()
    store = FailingInspectionStore(
        market_db,
        TimeSeriesInspection(source="duckdb-parquet"),
    )
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=store,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert any("DuckDB inspection failed during sync" in err for err in result.errors)


@pytest.mark.asyncio
async def test_incremental_sync_fails_when_time_series_store_is_missing() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient()
    ctx = SyncContext(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert any("DuckDB time-series store is required for sync strategy execution" in err for err in result.errors)


@pytest.mark.asyncio
async def test_incremental_sync_uses_stock_data_anchor_when_topix_is_ahead() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260210", latest_stock_data_date="20260206")
    client = DummyClient()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 2
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-06"} for path, params in client.calls)
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-10"} for path, params in client.calls)

    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    # topix_data ではなく stock_data の最新日（2026-02-06）を基準に差分取得する
    assert topix_calls[0][1] == {"from": "20260206"}


@pytest.mark.asyncio
async def test_incremental_sync_backfills_missing_stock_dates_without_new_topix_dates() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260210", latest_stock_data_date="20260210")
    market_db.topix_rows = [
        {"date": "2026-02-06", "open": 100, "high": 101, "low": 99, "close": 100},
        {"date": "2026-02-10", "open": 102, "high": 103, "low": 101, "close": 102},
    ]
    market_db.stock_rows = [
        {"code": "7203", "date": "2026-02-10", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 1000}
    ]
    client = DummyClient()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 1
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-06"} for path, params in client.calls)
    assert any(row.get("date") == "2026-02-06" for row in market_db.stock_rows)


@pytest.mark.asyncio
async def test_incremental_sync_uses_timeseries_inspection_anchor_when_sqlite_is_stale() -> None:
    market_db = DummyMarketDb(
        latest_trading_date=None,
        latest_stock_data_date=None,
        latest_indices_data_dates={},
    )
    client = DummyClient()
    store = DummyTimeSeriesStore(
        market_db=market_db,
        inspection=TimeSeriesInspection(
            source="duckdb-parquet",
            topix_max="20260206",
            stock_max="20260206",
            latest_indices_dates={"0000": "20260206"},
            latest_statement_disclosed_date="2026-02-06",
            statement_codes={"7203"},
        ),
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=store,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {"from": "20260206"}
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-10"} for path, params in client.calls)


@pytest.mark.asyncio
async def test_incremental_sync_bootstraps_when_timeseries_store_is_empty() -> None:
    market_db = DummyMarketDb(
        latest_trading_date="20260206",
        latest_stock_data_date="20260206",
        latest_indices_data_dates={"0000": "20260206"},
    )
    client = DummyClient()
    store = DummyTimeSeriesStore(
        market_db=market_db,
        inspection=TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=0,
            stock_count=0,
            indices_count=0,
            latest_indices_dates={},
        ),
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=store,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    topix_calls = [c for c in client.calls if c[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {}
    assert any(path == "/equities/bars/daily" and params == {"date": "2026-02-06"} for path, params in client.calls)


@pytest.mark.asyncio
async def test_incremental_sync_skips_rows_with_missing_ohlcv() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        daily_quotes=[
            {"Code": "131A0", "O": None, "H": None, "L": None, "C": None, "Vo": None, "AdjFactor": 1.0},
            {"Code": "72030", "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000, "AdjFactor": 1.0},
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert result.stocksUpdated == 2
    assert len(market_db.stock_rows) == 2
    assert all(row["code"] == "7203" for row in market_db.stock_rows)
    assert sorted(str(row["date"]) for row in market_db.stock_rows) == ["2026-02-06", "2026-02-10"]


@pytest.mark.asyncio
async def test_incremental_sync_skips_index_rows_with_missing_date() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "", "O": 100, "H": 101, "L": 99, "C": 100, "SectorName": "TOPIX"},
            {"Date": "2026-02-10", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    non_synthetic_rows = [row for row in market_db.indices_rows if row.get("code") != "N225_UNDERPX"]
    assert len(non_synthetic_rows) == 1
    assert non_synthetic_rows[0]["date"] == "2026-02-10"


@pytest.mark.asyncio
async def test_incremental_sync_publishes_options_225_and_synthetic_nikkei() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        options_quotes=[
            {
                "Date": "2026-02-10",
                "Code": "131040018",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 39000.0,
            },
            {
                "Date": "2026-02-10",
                "Code": "141040018",
                "CM": "2026-04",
                "Strike": 36000,
                "PCDiv": "2",
                "UnderPx": 39000.0,
            },
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert len(market_db.options_225_rows) == 2
    assert any(row.get("code") == "N225_UNDERPX" for row in market_db.index_master_rows)
    synthetic_rows = [row for row in market_db.indices_rows if row.get("code") == "N225_UNDERPX"]
    assert len(synthetic_rows) == 1
    assert synthetic_rows[0]["close"] == 39000.0


@pytest.mark.asyncio
async def test_incremental_sync_backfills_options_225_from_full_topix_dates_when_anchor_missing() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.topix_rows = [
        {
            "date": "2026-02-05",
            "open": 99.0,
            "high": 100.0,
            "low": 98.0,
            "close": 99.5,
            "created_at": "2026-03-19T00:00:00+00:00",
        }
    ]
    client = DummyClient(
        options_quotes=[
            {
                "Date": "2026-02-05",
                "Code": "131040015",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 38800.0,
            },
            {
                "Date": "2026-02-06",
                "Code": "131040016",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 38900.0,
            },
            {
                "Date": "2026-02-10",
                "Code": "131040020",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 39000.0,
            },
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert sorted({str(row["date"]) for row in market_db.options_225_rows}, key=_date_sort_key) == [
        "2026-02-05",
        "2026-02-06",
        "2026-02-10",
    ]
    assert [
        params.get("date")
        for path, params in client.calls
        if path == "/derivatives/bars/daily/options/225" and params is not None
    ] == ["20260205", "20260206", "20260210"]


@pytest.mark.asyncio
async def test_incremental_sync_backfills_partial_options_225_history_when_anchor_exists() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260210")
    market_db.topix_rows = [
        {
            "date": "2026-02-05",
            "open": 99.0,
            "high": 100.0,
            "low": 98.0,
            "close": 99.5,
            "created_at": "2026-03-19T00:00:00+00:00",
        },
        {
            "date": "2026-02-06",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "created_at": "2026-03-19T00:00:00+00:00",
        },
        {
            "date": "2026-02-10",
            "open": 102.0,
            "high": 103.0,
            "low": 101.0,
            "close": 102.0,
            "created_at": "2026-03-19T00:00:00+00:00",
        },
    ]
    market_db.options_225_rows = [
        {
            "code": "131040020",
            "date": "2026-02-10",
            "contract_month": "2026-04",
            "strike_price": 32000.0,
            "put_call_division": "1",
            "underlying_price": 39000.0,
            "created_at": "2026-03-19T00:00:00+00:00",
        }
    ]
    client = DummyClient(
        options_quotes=[
            {
                "Date": "2026-02-05",
                "Code": "131040015",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 38800.0,
            },
            {
                "Date": "2026-02-06",
                "Code": "131040016",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 38900.0,
            },
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert sorted({str(row["date"]) for row in market_db.options_225_rows}, key=_date_sort_key) == [
        "2026-02-05",
        "2026-02-06",
        "2026-02-10",
    ]
    assert [
        params.get("date")
        for path, params in client.calls
        if path == "/derivatives/bars/daily/options/225" and params is not None
    ] == ["20260205", "20260206"]


@pytest.mark.asyncio
async def test_indices_only_sync_options_225_falls_back_to_rest_when_bulk_plan_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.topix_rows = [
        {
            "date": "2026-02-05",
            "open": 99.0,
            "high": 100.0,
            "low": 98.0,
            "close": 99.5,
            "created_at": "2026-03-19T00:00:00+00:00",
        }
    ]
    client = DummyClient(
        options_quotes=[
            {
                "Date": "2026-02-05",
                "Code": "131040015",
                "CM": "2026-04",
                "Strike": 32000,
                "PCDiv": "1",
                "UnderPx": 38800.0,
            }
        ]
    )
    bulk_plan = BulkFetchPlan(
        endpoint="/derivatives/bars/daily/options/225",
        files=[],
        list_api_calls=1,
        estimated_api_calls=1,
        estimated_cache_hits=0,
        estimated_cache_misses=0,
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "options_225_indices_only":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=1,
                plan=bulk_plan,
                reason="bulk_estimate_lower",
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)

    bulk_service = _FakeBulkService()
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert sorted({str(row["date"]) for row in market_db.options_225_rows}, key=_date_sort_key) == [
        "2026-02-05"
    ]
    assert [
        params.get("date")
        for path, params in client.calls
        if path == "/derivatives/bars/daily/options/225" and params is not None
    ] == ["20260205"]
    assert bulk_service.fetch_calls == []


@pytest.mark.asyncio
async def test_incremental_sync_supplements_indices_with_date_based_discovery() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(path == "/indices/bars/daily" and params == {"date": "20260210"} for path, params in client.calls)
    assert any(row["code"] == "0040" for row in market_db.indices_rows)
    assert any(row["date"] == "2026-02-10" for row in market_db.indices_rows)


@pytest.mark.asyncio
async def test_incremental_sync_indices_bulk_result_matches_rest_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indices_quotes = [
        {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
    ]

    rest_market_db = DummyMarketDb(latest_trading_date="20260206")
    rest_client = DummyClient(indices_quotes=indices_quotes)
    rest_ctx = _build_ctx(
        client=rest_client,
        market_db=rest_market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )
    rest_result = await IncrementalSyncStrategy().execute(rest_ctx)
    assert rest_result.success
    rest_keys = {(str(row["code"]), str(row["date"])) for row in rest_market_db.indices_rows}

    bulk_market_db = DummyMarketDb(latest_trading_date="20260206")
    bulk_client = DummyClient(indices_quotes=indices_quotes)
    bulk_plan = BulkFetchPlan(
        endpoint="/indices/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _FakeBulkService(
        results_by_endpoint={
            "/indices/bars/daily": BulkFetchResult(
                rows=indices_quotes,
                api_calls=2,
                cache_hits=0,
                cache_misses=1,
                selected_files=1,
            )
        }
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "indices_incremental":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)

    bulk_ctx = _build_ctx(
        client=bulk_client,
        market_db=bulk_market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
    )
    bulk_result = await IncrementalSyncStrategy().execute(bulk_ctx)
    assert bulk_result.success
    bulk_keys = {(str(row["code"]), str(row["date"])) for row in bulk_market_db.indices_rows}

    assert bulk_keys == rest_keys


@pytest.mark.asyncio
async def test_incremental_sync_fallback_inserts_missing_master_for_fk_compatibility() -> None:
    class FkAwareMarketDb(DummyMarketDb):
        def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
            known_codes = self.get_index_master_codes()
            missing_codes = sorted(
                {
                    str(row.get("code"))
                    for row in rows
                    if row.get("code") and row.get("code") not in known_codes
                }
            )
            if missing_codes:
                raise RuntimeError(f"FOREIGN KEY constraint failed for codes: {','.join(missing_codes)}")
            return super().upsert_indices_data(rows)

    market_db = FkAwareMarketDb(latest_trading_date="20260206")
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "0999", "O": 102, "H": 103, "L": 101, "C": 102},
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(row["code"] == "0999" for row in market_db.index_master_rows)
    fallback_master = next(row for row in market_db.index_master_rows if row["code"] == "0999")
    assert fallback_master["category"] == "unknown"
    assert fallback_master["name"] == "0999"
    assert any(row["code"] == "0999" for row in market_db.indices_rows)


@pytest.mark.asyncio
async def test_incremental_sync_rechecks_anchor_date_for_index_discovery() -> None:
    market_db = DummyMarketDb(
        latest_trading_date="20260210",
        latest_stock_data_date="20260210",
        latest_indices_data_dates={"0000": "20260210"},
    )
    client = DummyClient(
        indices_quotes=[
            {"Date": "2026-02-10", "Code": "40", "O": 102, "H": 103, "L": 101, "C": 102, "SectorName": "TOPIX"},
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.errors == []
    assert any(path == "/indices/bars/daily" and params == {"date": "20260210"} for path, params in client.calls)
    non_synthetic_rows = [row for row in market_db.indices_rows if row.get("code") != "N225_UNDERPX"]
    assert len(non_synthetic_rows) == 1
    assert non_synthetic_rows[0]["code"] == "0040"


@pytest.mark.asyncio
async def test_incremental_sync_works_without_last_sync_metadata() -> None:
    class _NoLastSyncMarketDb(DummyMarketDb):
        def get_sync_metadata(self, key: str) -> str | None:
            if key == METADATA_KEYS["LAST_SYNC_DATE"]:
                return None
            return self.metadata.get(key)

    market_db = _NoLastSyncMarketDb()
    market_db.metadata = {}
    client = DummyClient()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    topix_calls = [call for call in client.calls if call[0] == "/indices/bars/daily/topix"]
    assert topix_calls
    assert topix_calls[0][1] == {"from": "20260206"}


@pytest.mark.asyncio
async def test_incremental_sync_cancelled_before_start() -> None:
    market_db = DummyMarketDb()
    client = DummyClient()
    cancelled = asyncio.Event()
    cancelled.set()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]


@pytest.mark.asyncio
async def test_incremental_sync_handles_unexpected_topix_exception() -> None:
    market_db = DummyMarketDb()

    class _TopixFailingClient(DummyClient):
        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            del path, params
            raise RuntimeError("topix fail")

    client = _TopixFailingClient()
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["topix fail"]


@pytest.mark.asyncio
async def test_indices_only_sync_collects_errors_and_continues_other_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _seed_rows(*, existing_codes: set[str] | None = None) -> list[dict[str, str | None]]:
        existing = existing_codes or set()
        rows: list[dict[str, str | None]] = []
        for code, category in (("0000", "topix"), ("9999", "unknown")):
            if code in existing:
                continue
            rows.append({
                "code": code,
                "name": code,
                "name_english": None,
                "category": category,
                "data_start_date": None,
                "created_at": "2026-02-10T00:00:00+00:00",
            })
        return rows

    monkeypatch.setattr(
        "src.application.services.sync_strategies.get_index_catalog_codes",
        lambda: {"0000", "9999"},
    )
    monkeypatch.setattr(
        "src.application.services.sync_strategies.build_index_master_seed_rows",
        _seed_rows,
    )

    market_db = DummyMarketDb()
    client = IndicesOnlyClient()
    progresses: list[tuple[str, int, int, str]] = []

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda stage, current, total, msg: progresses.append((stage, current, total, msg)),
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert len(result.errors) == 1
    assert "Index 9999" in result.errors[0]
    assert len(market_db.index_master_rows) == 2
    assert len(market_db.indices_rows) == 1
    assert any(stage == "indices_master" for stage, *_ in progresses)
    assert any(stage == "indices_data" for stage, *_ in progresses)


@pytest.mark.asyncio
async def test_indices_only_sync_uses_bulk_when_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    client = IndicesOnlyClient()
    progresses: list[tuple[str, int, int, str]] = []

    bulk_plan = BulkFetchPlan(
        endpoint="/indices/bars/daily",
        files=[
            BulkFileInfo(
                key="indices-2026-02-10.csv.gz",
                last_modified="2026-02-10T00:00:00Z",
                size=1024,
                range_start=None,
                range_end=None,
            )
        ],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )
    bulk_service = _FakeBulkService(
        results_by_endpoint={
            "/indices/bars/daily": BulkFetchResult(
                rows=[
                    {"IndexCode": "0000", "Date": "2026-02-10", "O": 100, "H": 101, "L": 99, "C": 100},
                    {"IndexCode": "0040", "Date": "2026-02-10", "O": 200, "H": 202, "L": 198, "C": 201},
                ],
                api_calls=3,
                cache_hits=0,
                cache_misses=1,
                selected_files=1,
            )
        },
    )

    async def _plan_stub(
        _ctx: Any,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
        min_rest_calls_to_probe_bulk: int = 3,
    ) -> _StageFetchDecision:
        del date_from, date_to, exact_dates, min_rest_calls_to_probe_bulk
        if stage == "indices_data" and endpoint == "/indices/bars/daily":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=1,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)
    monkeypatch.setattr(
        "src.application.services.sync_strategies._get_bulk_service",
        lambda _ctx: bulk_service,
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda stage, current, total, message: progresses.append((stage, current, total, message)),
        bulk_service=bulk_service,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert result.success
    assert result.totalApiCalls == 4
    assert "/indices/bars/daily" in bulk_service.fetch_calls
    assert len(market_db.indices_rows) >= 2
    assert any("-> BULK" in message for _, _, _, message in progresses)
    assert any("via BULK" in message for _, _, _, message in progresses)


@pytest.mark.asyncio
async def test_indices_only_sync_cancelled_immediately() -> None:
    market_db = DummyMarketDb()
    client = IndicesOnlyClient()
    cancelled = asyncio.Event()
    cancelled.set()
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert result.totalApiCalls == 0


@pytest.mark.asyncio
async def test_indices_only_sync_cancelled_during_loop() -> None:
    market_db = DummyMarketDb()
    cancelled = asyncio.Event()

    class _Client:
        async def get(self, _path: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {"data": [{"code": "0000"}, {"code": "0001"}]}

        async def get_paginated(self, _path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if (params or {}).get("code") == "0000":
                cancelled.set()
            return [{"Date": "2026-02-10", "O": 10, "H": 12, "L": 9, "C": 11}]

    ctx = _build_ctx(
        client=_Client(),
        market_db=market_db,
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert result.totalApiCalls == 1


@pytest.mark.asyncio
async def test_indices_only_sync_handles_seed_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    monkeypatch.setattr(
        "src.application.services.sync_strategies._seed_index_master_from_catalog",
        lambda _ctx: (_ for _ in ()).throw(RuntimeError("seed failed")),
    )

    class _Client:
        async def get_paginated(self, _path: str, _params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            return []

    ctx = _build_ctx(
        client=_Client(),
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IndicesOnlySyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["seed failed"]


@pytest.mark.asyncio
async def test_initial_sync_breaks_after_consecutive_failures_and_sets_metadata() -> None:
    topix_dates = [f"2026-02-{day:02d}" for day in range(6, 11)]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates, fail_stock_dates=set(topix_dates))

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.datesProcessed == len(topix_dates)
    assert len(result.failedDates) == len(topix_dates)
    assert any("Too many consecutive failures" in err for err in result.errors)
    assert market_db.metadata[METADATA_KEYS["INIT_COMPLETED"]] == "true"
    assert METADATA_KEYS["LAST_SYNC_DATE"] in market_db.metadata
    failed_dates = json.loads(market_db.metadata[METADATA_KEYS["FAILED_DATES"]])
    assert failed_dates == topix_dates


@pytest.mark.asyncio
async def test_initial_sync_success_path_without_failed_dates() -> None:
    topix_dates = ["2026-02-10", "2026-02-12"]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.failedDates == []
    assert result.stocksUpdated == len(topix_dates)
    assert market_db.metadata[METADATA_KEYS["FAILED_DATES"]] == "[]"


@pytest.mark.asyncio
async def test_initial_sync_stock_data_uses_bulk_when_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topix_dates = [f"2026-02-{day:02d}" for day in range(10, 14)]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)
    bulk_service = _FakeBulkService(
        results_by_endpoint={
            "/equities/bars/daily": BulkFetchResult(
                rows=[
                    {"Code": "72030", "Date": value, "O": 1, "H": 2, "L": 1, "C": 2, "Vo": 1000}
                    for value in topix_dates
                ],
                api_calls=2,
                cache_hits=0,
                cache_misses=1,
                selected_files=1,
            )
        }
    )
    bulk_plan = BulkFetchPlan(
        endpoint="/equities/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "stock_data_initial":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)
    monkeypatch.setattr(
        "src.application.services.sync_strategies._get_bulk_service",
        lambda _ctx: bulk_service,
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert "/equities/bars/daily" in bulk_service.fetch_calls
    daily_calls = [call for call in client.calls if call[0] == "/equities/bars/daily"]
    assert daily_calls == []
    assert result.stocksUpdated == len(topix_dates)


@pytest.mark.asyncio
async def test_initial_sync_stock_data_bulk_failure_falls_back_to_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topix_dates = [f"2026-02-{day:02d}" for day in range(10, 13)]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)
    bulk_service = _FakeBulkService(fail_endpoints={"/equities/bars/daily"})
    bulk_plan = BulkFetchPlan(
        endpoint="/equities/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "stock_data_initial":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)
    monkeypatch.setattr(
        "src.application.services.sync_strategies._get_bulk_service",
        lambda _ctx: bulk_service,
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    daily_calls = [call for call in client.calls if call[0] == "/equities/bars/daily"]
    assert len(daily_calls) == len(topix_dates)
    assert "/equities/bars/daily" in bulk_service.fetch_calls


@pytest.mark.asyncio
async def test_initial_sync_stock_data_bulk_failure_raises_when_bulk_enforced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topix_dates = [f"2026-02-{day:02d}" for day in range(10, 13)]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)
    bulk_service = _FakeBulkService(fail_endpoints={"/equities/bars/daily"})
    bulk_plan = BulkFetchPlan(
        endpoint="/equities/bars/daily",
        files=[],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "stock_data_initial":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)
    monkeypatch.setattr(
        "src.application.services.sync_strategies._get_bulk_service",
        lambda _ctx: bulk_service,
    )

    progress_messages: list[str] = []
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda _stage, _current, _total, message: progress_messages.append(message),
        enforce_bulk_for_stock_data=True,
    )

    with pytest.raises(BulkFetchRequiredError, match="REST fallback is disabled"):
        await InitialSyncStrategy().execute(ctx)

    daily_calls = [call for call in client.calls if call[0] == "/equities/bars/daily"]
    assert daily_calls == []
    assert any("Bulk fetch required for /equities/bars/daily" in message for message in progress_messages)


@pytest.mark.asyncio
async def test_initial_sync_with_empty_topix_and_empty_master() -> None:
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=[], master_rows=[])

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.datesProcessed == 0
    assert result.stocksUpdated == 0


@pytest.mark.asyncio
async def test_initial_sync_returns_cancelled_when_flag_set_during_stock_loop() -> None:
    topix_dates = ["2026-02-10", "2026-02-12"]
    market_db = DummyMarketDb()
    client = InitialSyncClient(topix_dates=topix_dates)
    cancelled = asyncio.Event()

    def on_progress(stage: str, *_args: Any) -> None:
        if stage == "stock_data":
            cancelled.set()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=cancelled,
        on_progress=on_progress,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]
    assert METADATA_KEYS["LAST_SYNC_DATE"] not in market_db.metadata


@pytest.mark.asyncio
async def test_initial_sync_cancelled_before_start() -> None:
    market_db = DummyMarketDb()
    cancelled = asyncio.Event()
    cancelled.set()

    ctx = _build_ctx(
        client=InitialSyncClient(topix_dates=["2026-02-10"]),
        market_db=market_db,
        cancelled=cancelled,
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert not result.success
    assert result.errors == ["Cancelled"]


@pytest.mark.asyncio
async def test_incremental_sync_without_anchor_date_and_with_stock_master_update() -> None:
    market_db = DummyMarketDb(latest_trading_date=None, latest_stock_data_date=None, latest_indices_data_dates={})
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ]
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert len(market_db.stocks_rows) == 1
    assert any(path == "/indices/bars/daily/topix" and params == {} for path, params in client.calls)


@pytest.mark.asyncio
async def test_incremental_sync_collects_stock_daily_fetch_errors() -> None:
    market_db = DummyMarketDb(latest_trading_date=None, latest_stock_data_date=None, latest_indices_data_dates={})
    client = DummyClient(daily_error_dates={"2026-02-06", "2026-02-10"})

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert not result.success
    assert len(result.errors) >= 1
    assert any(err.startswith("Date 2026-02-") for err in result.errors)


@pytest.mark.asyncio
async def test_initial_sync_fundamentals_fetches_listed_markets_and_handles_pagination() -> None:
    market_db = DummyMarketDb()
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {
                "Code": "99990",
                "CoName": "NonPrime",
                "Mkt": "0112",
                "MktNm": "スタンダード",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
        ],
        fins_paginated_codes={
            "72030": [
                [{"Code": "72030", "DiscDate": "2026-02-10", "EPS": 100.0}],
                [{"Code": "72030", "DiscDate": "2026-02-11", "EPS": 120.0}],
            ]
        },
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsUpdated == 2
    assert {row["code"] for row in market_db.statements_rows} == {"7203"}
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert any((params or {}).get("code") == "72030" for _, params in fins_calls)
    assert any((params or {}).get("code") == "99990" for _, params in fins_calls)


@pytest.mark.asyncio
async def test_initial_sync_fundamentals_retries_with_4digit_code_when_5digit_fails() -> None:
    market_db = DummyMarketDb()
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ],
        fins_by_code={
            "7203": [{"Code": "72030", "DiscDate": "2026-02-10", "EPS": 100.0}],
        },
        fins_error_codes={"72030"},
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await InitialSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsUpdated == 1
    assert {row["code"] for row in market_db.statements_rows} == {"7203"}
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert any((params or {}).get("code") == "72030" for _, params in fins_calls)
    assert any((params or {}).get("code") == "7203" for _, params in fins_calls)


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_date_and_missing_listed_market_backfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-02-09"
    market_db.statements_rows = [{"code": "7203", "disclosed_date": "2026-02-09"}]

    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {
                "Code": "67580",
                "CoName": "ソニーグループ",
                "Mkt": "0112",
                "MktNm": "スタンダード",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1958-12-01",
            },
        ],
        fins_by_date={
            "20260210": [
                {"Code": "72030", "DiscDate": "2026-02-10", "EPS": 110.0},
                {"Code": "99990", "DiscDate": "2026-02-10", "EPS": 90.0},
            ]
        },
        fins_by_code={
            "67580": [{"Code": "67580", "DiscDate": "2026-02-10", "EPS": 80.0}],
        },
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: ["2026-02-10"],
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsDatesProcessed == 1
    assert result.fundamentalsUpdated >= 2
    assert "6758" in {row["code"] for row in market_db.statements_rows}
    assert "9999" not in {row["code"] for row in market_db.statements_rows}
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert any((params or {}).get("date") == "20260210" for _, params in fins_calls)
    assert any((params or {}).get("code") == "67580" for _, params in fins_calls)


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_bulk_date_phase_keeps_listed_market_code_backfill_equivalent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-02-09"
    market_db.statements_rows = [{"code": "7203", "disclosed_date": "2026-02-09"}]

    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {
                "Code": "67580",
                "CoName": "ソニーグループ",
                "Mkt": "0113",
                "MktNm": "グロース",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1958-12-01",
            },
        ],
        fins_by_code={
            "67580": [{"Code": "67580", "DiscDate": "2026-02-10", "EPS": 80.0}],
        },
    )
    bulk_service = _FakeBulkService(
        results_by_endpoint={
            "/fins/summary": BulkFetchResult(
                rows=[
                    {"Code": "72030", "DiscDate": "2026-02-10", "EPS": 110.0},
                    {"Code": "99990", "DiscDate": "2026-02-10", "EPS": 90.0},
                ],
                api_calls=2,
                cache_hits=0,
                cache_misses=1,
                selected_files=1,
            )
        }
    )
    bulk_plan = BulkFetchPlan(
        endpoint="/fins/summary",
        files=[],
        list_api_calls=1,
        estimated_api_calls=3,
        estimated_cache_hits=0,
        estimated_cache_misses=1,
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: ["2026-02-10"],
    )

    async def _plan_stub(
        _ctx: SyncContext,
        *,
        stage: str,
        endpoint: str,
        estimated_rest_calls: int,
        **_kwargs: Any,
    ) -> _StageFetchDecision:
        if stage == "fundamentals_incremental_dates":
            return _StageFetchDecision(
                method="bulk",
                planner_api_calls=0,
                estimated_rest_calls=estimated_rest_calls,
                estimated_bulk_calls=3,
                plan=bulk_plan,
            )
        return _rest_decision(estimated_rest_calls)

    monkeypatch.setattr("src.application.services.sync_strategies._plan_fetch_method", _plan_stub)

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_service=bulk_service,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsDatesProcessed == 1
    assert {"7203", "6758"} <= {row["code"] for row in market_db.statements_rows}
    assert "9999" not in {row["code"] for row in market_db.statements_rows}
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert not any((params or {}).get("date") == "20260210" for _, params in fins_calls)
    assert any((params or {}).get("code") == "67580" for _, params in fins_calls)


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_backfill_uses_5digit_code_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"]] = json.dumps(["7203"])
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"]] = "[]"

    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ],
        fins_by_code={
            "72030": [{"Code": "72030", "DiscDate": "2026-02-10", "EPS": 100.0}],
        },
        fins_error_codes={"7203"},
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsUpdated == 1
    assert {row["code"] for row in market_db.statements_rows} == {"7203"}
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert any((params or {}).get("code") == "72030" for _, params in fins_calls)
    assert not any((params or {}).get("code") == "7203" for _, params in fins_calls)


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_alias_coverage_skips_preferred_share_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.statements_rows = [
        {"code": "2593", "disclosed_date": "2026-03-02", "earnings_per_share": 100.0},
    ]

    client = DummyClient(
        master_quotes=[
            {
                "Code": "259350",
                "CoName": "伊藤園（優先株式）",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "1",
                "S17Nm": "食品",
                "S33": "1050",
                "S33Nm": "食料品",
                "Date": "2026-03-06",
            }
        ],
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert result.fundamentalsUpdated == 0
    assert [call for call in client.calls if call[0] == "/fins/summary"] == []


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_persists_empty_cache_and_skips_same_frontier_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-03-06"

    client = DummyClient(
        master_quotes=[
            {
                "Code": "464A0",
                "CoName": "ＱＰＳホールディングス",
                "Mkt": "0113",
                "MktNm": "グロース",
                "S17": "10",
                "S17Nm": "情報・通信",
                "S33": "5250",
                "S33Nm": "情報・通信",
                "Date": "2026-03-06",
            }
        ],
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result_first = await IncrementalSyncStrategy().execute(ctx)
    fins_calls_after_first = len([call for call in client.calls if call[0] == "/fins/summary"])

    assert result_first.success
    assert result_first.fundamentalsUpdated == 0
    assert json.loads(market_db.metadata[METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"]]) == {
        "frontier": "2026-03-06",
        "codes": ["464A"],
    }
    assert fins_calls_after_first == 2

    result_second = await IncrementalSyncStrategy().execute(ctx)
    fins_calls_after_second = len([call for call in client.calls if call[0] == "/fins/summary"])

    assert result_second.success
    assert result_second.fundamentalsUpdated == 0
    assert fins_calls_after_second == fins_calls_after_first


@pytest.mark.asyncio
async def test_incremental_sync_margin_filters_non_listed_market_backfill_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-03-06"

    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {
                "Code": "511A0",
                "CoName": "ヴァンガードスミス",
                "Mkt": "0105",
                "MktNm": "TOKYO PRO MARKET",
                "Date": "2026-03-06",
            },
            {
                "Code": "516A0",
                "CoName": "大和アセットマネジメント株式会社　ｉＦｒｅｅＥＴＦ　米ドル・ブル（１倍）",
                "Mkt": "0109",
                "MktNm": "その他",
                "Date": "2026-03-06",
            },
        ],
        margin_by_code={
            "72030": [{"Code": "72030", "Date": "2026-02-10", "LongVol": 1000, "ShrtVol": 200}],
        },
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    progress_messages: list[str] = []
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda _stage, _current, _total, message: progress_messages.append(message),
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    margin_calls = [
        str((params or {}).get("code"))
        for path, params in client.calls
        if path == "/markets/margin-interest"
    ]
    assert margin_calls == ["72030"]
    assert any("skipped_market=2" in message for message in progress_messages)


@pytest.mark.asyncio
async def test_repair_sync_backfills_fundamentals_without_stock_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(
        latest_trading_date="20260206",
        stocks_needing_refresh=["7203"],
    )
    market_db._fundamentals_target_codes = {"7203"}
    market_db.topix_rows = [
        {"date": "2026-02-05", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        {"date": "2026-02-06", "open": 101.0, "high": 102.0, "low": 100.0, "close": 101.0},
    ]

    client = DummyClient(
        daily_quotes=[
            {"Code": "72030", "Date": "2026-02-05", "O": 10.0, "H": 11.0, "L": 9.0, "C": 10.0, "Vo": 1000, "AdjFactor": 1.0},
            {"Code": "72030", "Date": "2026-02-06", "O": 5.0, "H": 6.0, "L": 4.0, "C": 5.0, "Vo": 1000, "AdjFactor": 0.5},
        ],
        fins_by_code={
            "72030": [{"Code": "72030", "DiscDate": "2026-02-10", "EPS": 100.0}],
        },
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    progress_events: list[tuple[str, int, int, str]] = []
    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda stage, current, total, message: progress_events.append((stage, current, total, message)),
    )

    result = await RepairSyncStrategy().execute(ctx)

    assert result.success
    assert result.stocksUpdated == 0
    assert result.fundamentalsUpdated == 1
    assert market_db.stock_rows == []
    assert {row["code"] for row in market_db.statements_rows} == {"7203"}
    assert any(stage == "fundamentals" for stage, _, _, _ in progress_events)


@pytest.mark.asyncio
async def test_repair_sync_stops_after_cancel_during_fundamentals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb(
        latest_trading_date="20260206",
        stocks_needing_refresh=["7203", "6758"],
    )
    market_db._fundamentals_target_codes = {"7203", "6758"}
    market_db.topix_rows = [
        {"date": "2026-02-05", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        {"date": "2026-02-06", "open": 101.0, "high": 102.0, "low": 100.0, "close": 101.0},
    ]
    client = DummyClient(
        daily_quotes=[
            {"Code": "72030", "Date": "2026-02-05", "O": 10.0, "H": 11.0, "L": 9.0, "C": 10.0, "Vo": 1000, "AdjFactor": 1.0},
            {"Code": "72030", "Date": "2026-02-06", "O": 5.0, "H": 6.0, "L": 4.0, "C": 5.0, "Vo": 1000, "AdjFactor": 0.5},
        ],
    )

    monkeypatch.setattr(
        "src.application.services.sync_strategies._build_incremental_date_targets",
        lambda _anchor, _retry: [],
    )

    cancelled = asyncio.Event()

    def _on_progress(stage: str, current: int, total: int, message: str) -> None:
        del current, total
        if stage == "fundamentals" and message:
            cancelled.set()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=cancelled,
        on_progress=_on_progress,
    )

    result = await RepairSyncStrategy().execute(ctx)

    assert result.success is False
    assert result.errors == ["Cancelled"]
    assert client.calls == []
    assert market_db.stock_rows == []
    assert market_db.statements_rows == []


@pytest.mark.asyncio
async def test_incremental_sync_fundamentals_uses_latest_disclosed_when_metadata_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db = DummyMarketDb()
    market_db.statements_rows = [{"code": "7203", "disclosed_date": "2026-02-08"}]
    client = DummyClient(
        master_quotes=[
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            }
        ]
    )

    captured: dict[str, Any] = {}

    def _capture(anchor: str | None, retry_dates: list[str]) -> list[str]:
        captured["anchor"] = anchor
        captured["retry"] = retry_dates
        return []

    monkeypatch.setattr("src.application.services.sync_strategies._build_incremental_date_targets", _capture)

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result = await IncrementalSyncStrategy().execute(ctx)

    assert result.success
    assert captured["anchor"] == "2026-02-08"
    assert captured["retry"] == []


def test_strategy_selection_and_api_call_estimates() -> None:
    assert isinstance(get_strategy("initial"), InitialSyncStrategy)
    assert isinstance(get_strategy("incremental"), IncrementalSyncStrategy)
    assert isinstance(get_strategy("repair"), RepairSyncStrategy)
    assert isinstance(get_strategy("unknown"), InitialSyncStrategy)

    assert IndicesOnlySyncStrategy().estimate_api_calls() == 70
    assert InitialSyncStrategy().estimate_api_calls() == 3200
    assert IncrementalSyncStrategy().estimate_api_calls() == 180
    assert RepairSyncStrategy().estimate_api_calls() == 200


def test_data_conversion_helpers_handle_aliases_and_invalid_rows() -> None:
    stock_rows = _convert_stock_rows(
        [
            {
                "Code": "72030",
                "CoName": "トヨタ自動車",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "Date": "1949-05-16",
            },
            {"Code": "", "CoName": "skip"},
        ]
    )
    assert len(stock_rows) == 1
    assert stock_rows[0]["code"] == "7203"

    master_rows = _convert_index_master_rows(
        [
            {"Code": "0001", "Name": "Electric", "nameEnglish": "Electric", "Category": "sector33", "dataStartDate": "2010-01-04"},
            {"code": "", "name": "skip"},
        ]
    )
    assert len(master_rows) == 1
    assert master_rows[0]["code"] == "0001"
    assert master_rows[0]["name"] == "Electric"

    index_rows = _convert_indices_data_rows(
        [
            {"date": "2026-02-10", "open": 1, "high": 2, "low": 1, "close": 2, "sector_name": "TOPIX"},
            {"Date": "", "O": 10, "H": 12, "L": 9, "C": 11},
        ],
        code="0001",
    )
    assert len(index_rows) == 1
    assert index_rows[0]["date"] == "2026-02-10"
    assert index_rows[0]["open"] == 1

    index_rows_from_payload_code = _convert_indices_data_rows(
        [
            {"Date": "2026-02-10", "Code": "40", "O": 1, "H": 2, "L": 1, "C": 2, "SectorName": "TOPIX"},
        ],
        code=None,
    )
    assert len(index_rows_from_payload_code) == 1
    assert index_rows_from_payload_code[0]["code"] == "0040"

    index_rows_from_lower_hex_code = _convert_indices_data_rows(
        [
            {"Date": "2026-02-10", "Code": "004a", "O": 1, "H": 2, "L": 1, "C": 2, "SectorName": "Glass"},
        ],
        code=None,
    )
    assert len(index_rows_from_lower_hex_code) == 1
    assert index_rows_from_lower_hex_code[0]["code"] == "004A"


def test_convert_stock_bulk_rows_skips_invalid_dates_and_dedupes() -> None:
    rows = _convert_stock_bulk_rows(
        [
            {"Code": "72030", "Date": "20260210", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "100"},
            {"Code": "72030", "Date": "20260210", "O": "9", "H": "9", "L": "9", "C": "9", "Vo": "999"},
            {"Code": "72030", "Date": "20260230", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "100"},
            {"Code": "72030", "Date": "2026-13-01", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "100"},
        ],
        target_dates={"2026-02-10"},
    )

    assert len(rows) == 1
    assert rows[0]["code"] == "7203"
    assert rows[0]["date"] == "2026-02-10"
    assert rows[0]["open"] == 1.0
    assert rows[0]["volume"] == 100


def test_normalize_iso_date_text_validates_yyyymmdd_and_hyphen_formats() -> None:
    assert _normalize_iso_date_text("20260210") == "2026-02-10"
    assert _normalize_iso_date_text("2026-02-10") == "2026-02-10"
    assert _normalize_iso_date_text("20260230") is None
    assert _normalize_iso_date_text("2026-13-01") is None


def test_build_fallback_index_master_rows_deduplicates_and_keeps_inputs_immutable() -> None:
    known_codes = {"0000"}
    rows = [
        {"code": "40", "date": "2026-02-10", "sector_name": None},
        {"code": "0040", "date": "2026-02-11", "sector_name": "Electric"},
        {"code": "0000", "date": "2026-02-11", "sector_name": "TOPIX"},
    ]

    fallback_rows = _build_fallback_index_master_rows(rows, known_codes)

    assert known_codes == {"0000"}
    assert len(fallback_rows) == 1
    assert fallback_rows[0]["code"] == "0040"
    assert fallback_rows[0]["name"] == "Electric"
    assert fallback_rows[0]["category"] == "unknown"
    assert fallback_rows[0]["data_start_date"] == "2026-02-10"
    assert fallback_rows[0]["created_at"]


def test_extract_list_items_handles_key_aliases_and_fallback() -> None:
    assert _extract_list_items({"data": [{"code": "0000"}]}) == [{"code": "0000"}]
    assert _extract_list_items({"indices": [{"code": "0000"}]}, preferred_keys=("data", "indices")) == [{"code": "0000"}]
    assert _extract_list_items({"other": [{"code": "0000"}]}) == [{"code": "0000"}]
    assert _extract_list_items({"data": ["not-dict", {"code": "0001"}]}) == [{"code": "0001"}]
    assert _extract_list_items({"count": 0}) == []


def test_date_helpers_cover_parse_and_fallback_paths() -> None:
    assert _parse_date("20260210")
    assert _parse_date("2026-02-10")
    assert _parse_date("") is None
    assert _parse_date("   ") is None
    assert _parse_date("2026-02-30") is None

    assert _to_jquants_date_param("2026-02-10") == "20260210"
    assert _to_jquants_date_param("not-a-date") == "not-a-date"

    assert _is_date_after("2026-02-11", "2026-02-10")
    assert _is_date_after("zz", "aa")

    assert _date_sort_key("invalid") == (1, "invalid")
    assert _date_sort_key("2026-02-10") == (0, "2026-02-10")


def test_metadata_and_dedupe_helpers_cover_invalid_and_duplicate_paths() -> None:
    market_db = DummyMarketDb()
    market_db.metadata["missing"] = ""
    market_db.metadata["invalid"] = "{not-json"
    market_db.metadata["not-list"] = json.dumps({"a": 1})
    market_db.metadata["mixed-list"] = json.dumps(["7203", 6758, None, {"x": 1}])

    assert _load_metadata_json_list(market_db, "missing") == []
    assert _load_metadata_json_list(market_db, "invalid") == []
    assert _load_metadata_json_list(market_db, "not-list") == []
    assert _load_metadata_json_list(market_db, "mixed-list") == ["7203", "6758"]

    assert _collect_unique_codes(["72030", "7203", " 7203 ", "", "bad"]) == ["7203", "bad"]
    assert _dedupe_preserve_order(["a", " a ", "b", "", "b"]) == ["a", "b"]
    assert _normalize_date_list(["2026-02-10", "20260210", "bad", "", "2026-02-09"]) == [
        "2026-02-09",
        "2026-02-10",
    ]


def test_incremental_date_and_extract_date_helpers_cover_anchor_paths() -> None:
    today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    yesterday = (today_jst - timedelta(days=1)).isoformat()
    retry_date = (today_jst - timedelta(days=2)).isoformat()

    targets = _build_incremental_date_targets(yesterday, [retry_date, retry_date])
    assert retry_date in targets
    assert today_jst.isoformat() in targets
    assert _build_incremental_date_targets(None, ["2026-02-10"]) == ["2026-02-10"]

    rows = [
        {"date": retry_date},
        {"date": today_jst.isoformat()},
        {"date": ""},
    ]
    assert _extract_dates_after(rows, retry_date, include_anchor=False) == [today_jst.isoformat()]
    assert _extract_dates_after(rows, retry_date, include_anchor=True) == [retry_date, today_jst.isoformat()]

    assert _latest_date(["", retry_date, today_jst.isoformat()]) == today_jst.isoformat()
    assert _latest_date(["", ""]) is None


def test_convert_indices_data_rows_skips_missing_code_when_no_fallback() -> None:
    rows = _convert_indices_data_rows(
        [
            {"Date": "2026-02-10", "O": 1, "H": 2, "L": 1, "C": 2},
        ],
        code=None,
    )
    assert rows == []


@pytest.mark.asyncio
async def test_incremental_sync_repeated_run_keeps_idempotent_rows() -> None:
    market_db = DummyMarketDb(latest_trading_date="20260206")
    market_db.metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = datetime.now(
        ZoneInfo("Asia/Tokyo")
    ).date().isoformat()
    client = DummyClient()
    strategy = IncrementalSyncStrategy()

    ctx = _build_ctx(
        client=client,
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    result_first = await strategy.execute(ctx)
    assert result_first.success is True

    stock_keys_first = sorted(
        (str(row["code"]), str(row["date"]))
        for row in market_db.stock_rows
    )
    topix_dates_first = sorted(str(row["date"]) for row in market_db.topix_rows)
    indices_keys_first = sorted(
        (str(row["code"]), str(row["date"]))
        for row in market_db.indices_rows
    )

    result_second = await strategy.execute(ctx)
    assert result_second.success is True

    assert sorted((str(row["code"]), str(row["date"])) for row in market_db.stock_rows) == stock_keys_first
    assert sorted(str(row["date"]) for row in market_db.topix_rows) == topix_dates_first
    assert sorted((str(row["code"]), str(row["date"])) for row in market_db.indices_rows) == indices_keys_first


@pytest.mark.asyncio
async def test_fetch_fins_summary_by_code_returns_empty_when_all_candidates_empty() -> None:
    client = DummyClient()
    rows, calls = await _fetch_fins_summary_by_code(client, "7203")

    assert rows == []
    assert calls == 2
    fins_calls = [call for call in client.calls if call[0] == "/fins/summary"]
    assert [str((params or {}).get("code")) for _, params in fins_calls] == ["72030", "7203"]


@pytest.mark.asyncio
async def test_get_paginated_rows_with_call_count_uses_meta_when_available() -> None:
    class _ClientWithMeta:
        async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            del path, params
            return {"data": []}

        async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            del path, params
            return []

        async def get_paginated_with_meta(
            self,
            path: str,
            *,
            params: dict[str, Any] | None = None,
        ) -> tuple[list[dict[str, Any]], int]:
            assert path == "/fins/summary"
            assert params == {"code": "72030"}
            return ([{"code": "72030"}], 3)

    rows, calls = await _get_paginated_rows_with_call_count(
        _ClientWithMeta(),
        "/fins/summary",
        params={"code": "72030"},
    )

    assert rows == [{"code": "72030"}]
    assert calls == 3


@pytest.mark.asyncio
async def test_get_paginated_rows_with_call_count_falls_back_without_meta() -> None:
    class _ClientWithoutMeta:
        async def get_paginated(
            self,
            path: str,
            params: dict[str, Any] | None = None,
        ) -> list[dict[str, Any]]:
            assert path == "/prices/daily_quotes"
            assert params == {"date": "20260210"}
            return [{"Date": "2026-02-10"}]

        async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            del path, params
            return {"data": []}

    rows, calls = await _get_paginated_rows_with_call_count(
        _ClientWithoutMeta(),
        "/prices/daily_quotes",
        params={"date": "20260210"},
    )

    assert rows == [{"Date": "2026-02-10"}]
    assert calls == 1


@pytest.mark.asyncio
async def test_plan_fetch_method_uses_rest_when_rest_estimate_is_too_small() -> None:
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=DummyMarketDb(),
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        bulk_probe_disabled=False,
    )

    decision = await _plan_fetch_method(
        ctx,
        stage="stock_data",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=1,
    )

    assert decision.method == "rest"
    assert decision.reason == "rest_estimate_too_small"
    assert decision.planner_api_calls == 0


def test_to_iso_date_text_handles_fast_paths_and_invalid_input() -> None:
    assert _to_iso_date_text(None) is None
    assert _to_iso_date_text("   ") is None
    assert _to_iso_date_text("2026-02-10") == "2026-02-10"
    assert _to_iso_date_text("20260210") == "2026-02-10"
    assert _to_iso_date_text("invalid-date") is None


def test_get_bulk_service_initializes_once_and_reuses_instance() -> None:
    market_db = DummyMarketDb()
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )
    ctx.bulk_service = None

    first = _get_bulk_service(ctx)
    second = _get_bulk_service(ctx)

    assert first is second
    assert ctx.bulk_service is first


def test_inspect_time_series_rejects_non_duckdb_source() -> None:
    market_db = DummyMarketDb()
    bad_store = DummyTimeSeriesStore(
        market_db,
        inspection=TimeSeriesInspection(source="sqlite"),
    )
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
        time_series_store=bad_store,
    )

    with pytest.raises(RuntimeError, match="Unexpected time-series source"):
        _inspect_time_series(ctx)


@pytest.mark.asyncio
async def test_publish_helpers_return_zero_when_rows_empty() -> None:
    market_db = DummyMarketDb()
    ctx = _build_ctx(
        client=DummyClient(),
        market_db=market_db,
        cancelled=asyncio.Event(),
        on_progress=lambda *_: None,
    )

    assert await _publish_topix_rows(ctx, []) == 0
    assert await _publish_stock_data_rows(ctx, []) == 0
    assert await _publish_indices_rows(ctx, []) == 0
    assert await _publish_statement_rows(ctx, []) == 0
