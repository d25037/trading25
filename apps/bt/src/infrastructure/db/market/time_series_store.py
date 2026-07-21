"""Market time-series storage (DuckDB + Parquet SoT)."""

from __future__ import annotations

import hashlib
import json
import shutil
from time import perf_counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, cast

import pandas as pd
from loguru import logger

from src.shared.provider_stock_window import (
    PROVIDER_DRIFT_COLUMNS,
    ProviderStockCoverage,
    ProviderStockMetadata,
    ProviderStockStage,
    combine_provider_stock_source_fingerprints,
    provider_stock_source_fingerprint,
    validate_provider_stock_window,
)
from src.infrastructure.db.market.duckdb_connection import (
    MarketWriterToken,
    connect_market_duckdb,
    parse_duckdb_size_bytes,
    resolve_directory_size,
)
from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
    deterministic_last_wins,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.db.market.market_schema import (
    IncompatibleMarketSchemaError,
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
)


class MarketTimeSeriesStore(Protocol):  # pragma: no cover
    """時系列 publish/index インターフェース。"""

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_stock_data(
        self, rows: list[dict[str, Any]], *, stage: ProviderStockStage
    ) -> SemanticDeltaResult: ...
    def detect_stock_provider_drift(
        self, rows: list[dict[str, Any]]
    ) -> frozenset[str]: ...
    def replace_stock_provider_window(
        self,
        code: str,
        rows: list[dict[str, Any]],
        coverage: ProviderStockCoverage | dict[str, Any],
        metadata: ProviderStockMetadata | dict[str, Any],
    ) -> SemanticDeltaResult: ...
    def publish_stock_minute_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult: ...
    def publish_indices_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult: ...
    def publish_options_225_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult: ...
    def publish_margin_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def stage_stock_data_rows(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult: ...
    def flush_staged_stock_data(
        self,
        *,
        stage: ProviderStockStage,
        exclude_codes: frozenset[str] = frozenset(),
    ) -> SemanticDeltaResult: ...
    def discard_staged_stock_data(self) -> None: ...

    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...

    def has_pending_index(self, table_name: str) -> bool: ...
    def index_stock_minute_data(self) -> None: ...
    def index_indices_data(self) -> None: ...
    def index_options_225_data(self) -> None: ...
    def index_margin_data(self) -> None: ...
    def index_statements(self) -> None: ...

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> "TimeSeriesInspection": ...

    def get_storage_stats(self) -> "TimeSeriesStorageStats": ...
    def close(self) -> None: ...


@dataclass
class _TableSpec:
    table_name: str
    parquet_name: str
    order_by: str | None = None


@dataclass(frozen=True)
class _RelationUpsertSpec:
    table_name: str
    relation_name: str
    columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]
    update_columns: tuple[str, ...] | None = None
    update_assignments: tuple[str, ...] | None = None


@dataclass
class TimeSeriesInspection:
    """同期/検証のための時系列データ面スナップショット。"""

    source: str
    topix_count: int = 0
    topix_min: str | None = None
    topix_max: str | None = None
    stock_count: int = 0
    stock_min: str | None = None
    stock_max: str | None = None
    stock_date_count: int = 0
    stock_minute_count: int = 0
    stock_minute_min: str | None = None
    stock_minute_max: str | None = None
    stock_minute_date_count: int = 0
    stock_minute_code_count: int = 0
    latest_stock_minute_time: str | None = None
    missing_stock_dates: list[str] = field(default_factory=list)
    missing_stock_dates_count: int = 0
    indices_count: int = 0
    indices_min: str | None = None
    indices_max: str | None = None
    indices_date_count: int = 0
    latest_indices_dates: dict[str, str] = field(default_factory=dict)
    options_225_count: int = 0
    options_225_min: str | None = None
    options_225_max: str | None = None
    options_225_date_count: int = 0
    latest_options_225_date: str | None = None
    missing_options_225_dates: list[str] = field(default_factory=list)
    missing_options_225_dates_count: int = 0
    margin_count: int = 0
    margin_min: str | None = None
    margin_max: str | None = None
    margin_date_count: int = 0
    margin_codes: set[str] = field(default_factory=set)
    margin_orphan_count: int = 0
    statements_count: int = 0
    latest_statement_disclosed_date: str | None = None
    statement_codes: set[str] = field(default_factory=set)
    statement_non_null_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class TimeSeriesStorageStats:
    duckdb_bytes: int = 0
    parquet_bytes: int = 0
    duckdb_blocks_total: int = 0
    duckdb_blocks_used: int = 0
    duckdb_blocks_free: int = 0
    duckdb_bytes_free: int = 0
    duckdb_wal_bytes: int = 0
    temp_directory: str | None = None
    temp_bytes: int = 0
    stale_artifact_count: int = 0
    stale_artifacts: list[str] = field(default_factory=list)

    @property
    def total_bytes(self) -> int:
        return self.duckdb_bytes + self.parquet_bytes


def _build_coalesce_update_assignments(
    columns: tuple[str, ...],
    *,
    target_table: str,
) -> tuple[str, ...]:
    return tuple(
        f"{column} = COALESCE(excluded.{column}, {target_table}.{column})"
        for column in columns
    )


class DuckDbParquetTimeSeriesStore:
    """DuckDB へ upsert し、Parquet を再生成する Data Plane store。"""

    _TOPIX_DATA_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="topix_data",
        relation_name="__tmp_topix_publish",
        columns=("date", "open", "high", "low", "close", "created_at"),
        conflict_columns=("date",),
    )

    _STOCK_DATA_RAW_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="stock_data_raw",
        relation_name="__tmp_stock_data_raw_publish",
        columns=(
            "code",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_value",
            "adjustment_factor",
            "adjusted_open",
            "adjusted_high",
            "adjusted_low",
            "adjusted_close",
            "adjusted_volume",
            "created_at",
        ),
        conflict_columns=("code", "date"),
    )
    _STOCK_DATA_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="stock_data",
        relation_name="__tmp_stock_data_publish",
        columns=(
            "code",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjustment_factor",
            "created_at",
        ),
        conflict_columns=("code", "date"),
    )
    _STOCK_ADJUSTMENT_EVENTS_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="stock_adjustment_events",
        relation_name="__tmp_stock_adjustment_events_publish",
        columns=(
            "code",
            "date",
            "adjustment_factor",
            "source_fingerprint",
            "created_at",
        ),
        conflict_columns=("code", "date"),
    )
    _STOCK_MINUTE_DATA_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="stock_data_minute_raw",
        relation_name="__tmp_stock_data_minute_raw_publish",
        columns=(
            "code",
            "date",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_value",
            "created_at",
        ),
        conflict_columns=("code", "date", "time"),
    )
    _INDICES_DATA_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="indices_data",
        relation_name="__tmp_indices_data_publish",
        columns=(
            "code",
            "date",
            "open",
            "high",
            "low",
            "close",
            "sector_name",
            "created_at",
        ),
        conflict_columns=("code", "date"),
    )
    _OPTIONS_225_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="options_225_data",
        relation_name="__tmp_options_225_publish",
        columns=(
            "code",
            "date",
            "whole_day_open",
            "whole_day_high",
            "whole_day_low",
            "whole_day_close",
            "night_session_open",
            "night_session_high",
            "night_session_low",
            "night_session_close",
            "day_session_open",
            "day_session_high",
            "day_session_low",
            "day_session_close",
            "volume",
            "open_interest",
            "turnover_value",
            "contract_month",
            "strike_price",
            "only_auction_volume",
            "emergency_margin_trigger_division",
            "put_call_division",
            "last_trading_day",
            "special_quotation_day",
            "settlement_price",
            "theoretical_price",
            "base_volatility",
            "underlying_price",
            "implied_volatility",
            "interest_rate",
            "created_at",
        ),
        conflict_columns=("code", "date"),
    )
    _MARGIN_DATA_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="margin_data",
        relation_name="__tmp_margin_data_publish",
        columns=(
            "code",
            "date",
            "long_margin_volume",
            "short_margin_volume",
        ),
        conflict_columns=("code", "date"),
    )
    _TABLE_SPECS = {
        "topix_data": _TableSpec("topix_data", "topix_data.parquet", "date"),
        "stock_data_raw": _TableSpec("stock_data_raw", "stock_data_raw.parquet"),
        # 高カーディナリティ表は export 時の全件 sort が支配的になりやすいため非ソートで出力する。
        "stock_data": _TableSpec("stock_data", "stock_data.parquet"),
        "stock_adjustment_events": _TableSpec(
            "stock_adjustment_events", "stock_adjustment_events.parquet", "code, date"
        ),
        "indices_data": _TableSpec("indices_data", "indices_data.parquet"),
        "options_225_data": _TableSpec("options_225_data", "options_225_data.parquet"),
        "margin_data": _TableSpec("margin_data", "margin_data.parquet"),
        "statements": _TableSpec(
            "statements", "statements.parquet", "disclosed_date, code"
        ),
    }
    _DATE_PARTITIONED_TABLES = frozenset(
        {
            "stock_data_raw",
            "stock_data",
            "options_225_data",
        }
    )

    _STATEMENT_UPDATABLE_COLUMNS = (
        "disclosure_number",
        "disclosed_date",
        "disclosed_at",
        "period_start",
        "period_end",
        "earnings_per_share",
        "diluted_earnings_per_share",
        "profit",
        "equity",
        "type_of_current_period",
        "type_of_document",
        "next_year_forecast_earnings_per_share",
        "bps",
        "sales",
        "forecast_sales",
        "next_year_forecast_sales",
        "operating_profit",
        "forecast_operating_profit",
        "next_year_forecast_operating_profit",
        "ordinary_profit",
        "operating_cash_flow",
        "dividend_fy",
        "forecast_dividend_fy",
        "next_year_forecast_dividend_fy",
        "payout_ratio",
        "forecast_payout_ratio",
        "next_year_forecast_payout_ratio",
        "forecast_eps",
        "investing_cash_flow",
        "financing_cash_flow",
        "cash_and_equivalents",
        "total_assets",
        "shares_outstanding",
        "treasury_shares",
    )
    _STATEMENTS_UPSERT_SPEC = _RelationUpsertSpec(
        table_name="statements",
        relation_name="__tmp_statements_publish",
        columns=(
            "code",
            "statement_id",
            *_STATEMENT_UPDATABLE_COLUMNS,
        ),
        conflict_columns=("code", "statement_id"),
        update_assignments=_build_coalesce_update_assignments(
            _STATEMENT_UPDATABLE_COLUMNS,
            target_table="statements",
        ),
    )

    _INVALID_TOPIX_DATE_SUBQUERY = """
        SELECT date
        FROM (
            SELECT
                date,
                open,
                high,
                low,
                close,
                LAG(close) OVER (ORDER BY date) AS prev_close
            FROM topix_data
        ) ordered_rows
        WHERE open IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
          AND prev_close IS NOT NULL
          AND open = high
          AND high = low
          AND low = close
          AND open = prev_close
    """
    _STOCK_ADJUSTMENT_PROBE_RELATION = "__tmp_stock_adjustment_probe"
    _STOCK_DATA_STAGE_TABLE = "__tmp_stock_data_stage"

    def __init__(
        self,
        *,
        duckdb_path: str,
        parquet_dir: str,
        read_only: bool = True,
        writer_token: MarketWriterToken | None = None,
    ) -> None:
        self._duckdb_path = Path(duckdb_path)
        self._parquet_dir = Path(parquet_dir)
        self._read_only = read_only
        if not read_only:
            self._duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            self._parquet_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._conn = cast(
                Any,
                connect_market_duckdb(
                    self._duckdb_path,
                    read_only=read_only,
                    writer_token=writer_token,
                ),
            )
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDB backend requested but `duckdb` package is not installed. "
                "Install duckdb and retry."
            ) from exc

        # app state で共有されるため、sync 書き込みと stats/validate 読み取りを直列化する。
        self._lock = RLock()
        self._dirty_tables: set[str] = set()
        self._dirty_stock_minute_dates: set[str] = set()
        self._dirty_partition_dates: dict[str, set[str]] = {}
        if not read_only:
            self._ensure_schema()
            self._cleanup_invalid_topix_rows_on_startup()

    def _assert_writable(self) -> None:
        if getattr(self, "_read_only", False):
            raise PermissionError("market time-series store is read-only")

    def _ensure_schema(self) -> None:
        with self._lock:
            existing_version_table = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'market_schema_version'
                """
            ).fetchone()
            if existing_version_table and int(existing_version_table[0] or 0) > 0:
                version_row = self._conn.execute(
                    "SELECT MAX(version) FROM market_schema_version"
                ).fetchone()
                existing_version = (
                    int(version_row[0])
                    if version_row and version_row[0] is not None
                    else None
                )
                if existing_version != MARKET_SCHEMA_VERSION:
                    raise IncompatibleMarketSchemaError(
                        "Incompatible market schema version "
                        f"{existing_version}; required version {MARKET_SCHEMA_VERSION}"
                    )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS topix_data (
                    date TEXT PRIMARY KEY,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    created_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_data_raw (
                    code TEXT,
                    date TEXT,
                    open DOUBLE NOT NULL,
                    high DOUBLE NOT NULL,
                    low DOUBLE NOT NULL,
                    close DOUBLE NOT NULL,
                    volume BIGINT NOT NULL,
                    turnover_value DOUBLE,
                    adjustment_factor DOUBLE,
                    adjusted_open DOUBLE,
                    adjusted_high DOUBLE,
                    adjusted_low DOUBLE,
                    adjusted_close DOUBLE,
                    adjusted_volume BIGINT,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_adjustment_events (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    adjustment_factor DOUBLE NOT NULL CHECK (
                        adjustment_factor > 0 AND adjustment_factor <> 1
                    ),
                    source_fingerprint TEXT NOT NULL,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_provider_windows (
                    code TEXT PRIMARY KEY,
                    coverage_start TEXT NOT NULL,
                    coverage_end TEXT NOT NULL,
                    provider_plan TEXT NOT NULL,
                    provider_as_of TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS current_basis_fundamentals_state (
                    code TEXT PRIMARY KEY,
                    fundamentals_adjustment_basis_date TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    statement_count BIGINT NOT NULL CHECK (statement_count >= 0),
                    materialized_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS current_basis_recompute_pending (
                    code TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_data (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open DOUBLE NOT NULL,
                    high DOUBLE NOT NULL,
                    low DOUBLE NOT NULL,
                    close DOUBLE NOT NULL,
                    volume BIGINT NOT NULL,
                    adjustment_factor DOUBLE,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_data_minute_raw (
                    code TEXT,
                    date TEXT,
                    time TEXT,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    turnover_value DOUBLE,
                    created_at TEXT,
                    PRIMARY KEY (code, date, time)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS indices_data (
                    code TEXT,
                    date TEXT,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    sector_name TEXT,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS options_225_data (
                    code TEXT,
                    date TEXT,
                    whole_day_open DOUBLE,
                    whole_day_high DOUBLE,
                    whole_day_low DOUBLE,
                    whole_day_close DOUBLE,
                    night_session_open DOUBLE,
                    night_session_high DOUBLE,
                    night_session_low DOUBLE,
                    night_session_close DOUBLE,
                    day_session_open DOUBLE,
                    day_session_high DOUBLE,
                    day_session_low DOUBLE,
                    day_session_close DOUBLE,
                    volume DOUBLE,
                    open_interest DOUBLE,
                    turnover_value DOUBLE,
                    contract_month TEXT,
                    strike_price DOUBLE,
                    only_auction_volume DOUBLE,
                    emergency_margin_trigger_division TEXT,
                    put_call_division TEXT,
                    last_trading_day TEXT,
                    special_quotation_day TEXT,
                    settlement_price DOUBLE,
                    theoretical_price DOUBLE,
                    base_volatility DOUBLE,
                    underlying_price DOUBLE,
                    implied_volatility DOUBLE,
                    interest_rate DOUBLE,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS margin_data (
                    code TEXT,
                    date TEXT,
                    long_margin_volume DOUBLE,
                    short_margin_volume DOUBLE,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS statements (
                    code TEXT NOT NULL,
                    statement_id TEXT NOT NULL,
                    disclosure_number TEXT,
                    disclosed_date TEXT NOT NULL,
                    disclosed_at TEXT NOT NULL,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    earnings_per_share DOUBLE,
                    diluted_earnings_per_share DOUBLE,
                    profit DOUBLE,
                    equity DOUBLE,
                    type_of_current_period TEXT,
                    type_of_document TEXT,
                    next_year_forecast_earnings_per_share DOUBLE,
                    bps DOUBLE,
                    sales DOUBLE,
                    forecast_sales DOUBLE,
                    next_year_forecast_sales DOUBLE,
                    operating_profit DOUBLE,
                    forecast_operating_profit DOUBLE,
                    next_year_forecast_operating_profit DOUBLE,
                    ordinary_profit DOUBLE,
                    operating_cash_flow DOUBLE,
                    dividend_fy DOUBLE,
                    forecast_dividend_fy DOUBLE,
                    next_year_forecast_dividend_fy DOUBLE,
                    payout_ratio DOUBLE,
                    forecast_payout_ratio DOUBLE,
                    next_year_forecast_payout_ratio DOUBLE,
                    forecast_eps DOUBLE,
                    investing_cash_flow DOUBLE,
                    financing_cash_flow DOUBLE,
                    cash_and_equivalents DOUBLE,
                    total_assets DOUBLE,
                    shares_outstanding DOUBLE,
                    treasury_shares DOUBLE,
                    PRIMARY KEY (code, statement_id)
                )
                """
            )
            self._ensure_statements_columns()

    def _ensure_statements_columns(self) -> None:
        existing_columns = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info('statements')").fetchall()
            if row and len(row) > 1
        }
        for column in (
            "forecast_sales",
            "next_year_forecast_sales",
            "forecast_operating_profit",
            "next_year_forecast_operating_profit",
        ):
            if column in existing_columns:
                continue
            self._conn.execute(
                f"ALTER TABLE statements ADD COLUMN {self._quote_identifier(column)} DOUBLE"
            )

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            return self._publish_topix_data_locked(rows)

    def _publish_topix_data_locked(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult:
        if not rows:
            return SemanticDeltaResult.empty()
        valid_rows, invalid_dates = self._filter_invalid_topix_input(rows)
        result = self._apply_semantic_delta(
            valid_rows, spec=self._TOPIX_DATA_UPSERT_SPEC
        )
        deleted_keys: tuple[tuple[Any, ...], ...] = ()
        if invalid_dates:
            existing_invalid = tuple(
                str(row[0])
                for row in self._conn.execute(
                    "SELECT date FROM topix_data WHERE date IN (SELECT unnest(?))",
                    [sorted(invalid_dates)],
                ).fetchall()
            )
            if existing_invalid:
                self._conn.execute(
                    "DELETE FROM topix_data WHERE date IN (SELECT unnest(?))",
                    [list(existing_invalid)],
                )
                deleted_keys = tuple((date_value,) for date_value in existing_invalid)
        result = SemanticDeltaResult(
            stats=MarketMutationStats(
                input=len(rows),
                inserted=result.stats.inserted,
                updated=result.stats.updated,
                unchanged=len(rows) - result.stats.inserted - result.stats.updated,
                deleted=len(deleted_keys),
            ),
            inserted_keys=result.inserted_keys,
            updated_keys=result.updated_keys,
            deleted_keys=deleted_keys,
            affected_dates=result.affected_dates
            | frozenset(key[0] for key in deleted_keys),
        )
        if result.mutated_rows:
            self._dirty_tables.add("topix_data")
        return result

    def _filter_invalid_topix_input(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], set[str]]:
        deduplicated = deterministic_last_wins(rows, key_columns=("date",))
        desired_by_date = {
            str(row[0]): {
                "date": str(row[0]),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
            }
            for row in self._conn.execute(
                "SELECT date, open, high, low, close FROM topix_data"
            ).fetchall()
        }
        desired_by_date.update(
            (str(row.get("date")), row)
            for row in deduplicated
            if row.get("date") is not None
        )
        invalid_dates: set[str] = set()
        previous_close: Any = None
        for date_value in sorted(desired_by_date):
            row = desired_by_date[date_value]
            values = (
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
            )
            invalid = (
                previous_close is not None
                and all(value is not None for value in values)
                and values[0] == values[1] == values[2] == values[3] == previous_close
            )
            if invalid:
                invalid_dates.add(date_value)
            previous_close = row.get("close")
        return [
            row for row in deduplicated if str(row.get("date")) not in invalid_dates
        ], invalid_dates

    def _cleanup_invalid_topix_rows_on_startup(self) -> None:
        with self._lock:
            removed_count = self._remove_invalid_topix_rows()
            if removed_count <= 0:
                return
            logger.warning(
                "Removed {} invalid TOPIX rows from existing snapshot (flat OHLC equal to previous close)",
                removed_count,
            )
            self._dirty_tables.add("topix_data")
            self._export_if_dirty("topix_data")

    def _remove_invalid_topix_rows(self) -> int:
        count_row = self._conn.execute(
            f"""
            SELECT COUNT(*)
            FROM ({self._INVALID_TOPIX_DATE_SUBQUERY}) invalid_dates
            """
        ).fetchone()
        invalid_count = int(count_row[0] or 0) if count_row else 0
        if invalid_count <= 0:
            return 0

        self._conn.execute(
            f"""
            DELETE FROM topix_data
            WHERE date IN ({self._INVALID_TOPIX_DATE_SUBQUERY})
            """
        )
        return invalid_count

    def publish_stock_data(
        self, rows: list[dict[str, Any]], *, stage: ProviderStockStage
    ) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            return self._publish_stock_data_locked(rows, stage=stage)

    def detect_stock_provider_drift(self, rows: list[dict[str, Any]]) -> frozenset[str]:
        """Return codes requiring a full provider-window refresh before append."""
        if not rows:
            return frozenset()
        dataframe = pd.DataFrame.from_records(
            (
                {
                    column: row.get(column)
                    for column in self._STOCK_DATA_RAW_UPSERT_SPEC.columns
                }
                for row in rows
            ),
            columns=self._STOCK_DATA_RAW_UPSERT_SPEC.columns,
        )
        drift_predicate = " OR ".join(
            f"incoming.{column} IS DISTINCT FROM existing.{column}"
            for column in PROVIDER_DRIFT_COLUMNS
        )
        relation_name = self._STOCK_ADJUSTMENT_PROBE_RELATION
        existing_keys: set[tuple[str, str]] = set()
        with self._lock:
            self._conn.register(relation_name, dataframe)
            try:
                drift_rows = self._conn.execute(
                    f"""
                    SELECT DISTINCT incoming.code
                    FROM {relation_name} incoming
                    LEFT JOIN stock_data_raw existing
                      ON existing.code = incoming.code
                     AND existing.date = incoming.date
                    WHERE incoming.adjustment_factor IS NOT NULL
                      AND incoming.adjustment_factor != 1.0
                       OR (
                            existing.code IS NOT NULL
                            AND ({drift_predicate})
                       )
                    """
                ).fetchall()
                existing_keys = {
                    (str(row[0]), str(row[1]))
                    for row in self._conn.execute(
                        f"""
                        SELECT incoming.code, incoming.date
                        FROM {relation_name} incoming
                        JOIN stock_data_raw existing
                          ON existing.code = incoming.code
                         AND existing.date = incoming.date
                        """
                    ).fetchall()
                }
            finally:
                self._conn.unregister(relation_name)
        drift_codes = {str(row[0]) for row in drift_rows if row and row[0]}
        for row in rows:
            key = (str(row.get("code") or ""), str(row.get("date") or ""))
            if key in existing_keys:
                continue
            try:
                factor = float(row["adjustment_factor"])
                price_consistent = all(
                    abs(float(row[adjusted]) - float(row[raw])) <= 0.0500001
                    for raw, adjusted in (
                        ("open", "adjusted_open"),
                        ("high", "adjusted_high"),
                        ("low", "adjusted_low"),
                        ("close", "adjusted_close"),
                    )
                )
                volume_consistent = (
                    abs(int(row["adjusted_volume"]) - int(row["volume"])) <= 1
                )
            except (KeyError, TypeError, ValueError):
                drift_codes.add(str(row.get("code") or ""))
                continue
            if factor == 1.0 and not (price_consistent and volume_consistent):
                drift_codes.add(str(row.get("code") or ""))
        return frozenset(code for code in drift_codes if code)

    def _mark_current_basis_recompute_pending_unlocked(
        self,
        code: str,
        *,
        reason: str,
        source_fingerprint: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO current_basis_recompute_pending (
                code, reason, source_fingerprint, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT (code) DO UPDATE SET
                reason = excluded.reason,
                source_fingerprint = excluded.source_fingerprint,
                updated_at = excluded.updated_at
            """,
            [code, reason, source_fingerprint, datetime.now(UTC).isoformat()],
        )

    def _current_statement_source_fingerprint_unlocked(self, code: str) -> str:
        columns = self._STATEMENTS_UPSERT_SPEC.columns
        rows = self._conn.execute(
            f"SELECT {', '.join(columns)} FROM statements "
            "WHERE code = ? ORDER BY statement_id",
            [code],
        ).fetchall()
        payload = json.dumps(
            [dict(zip(columns, row, strict=True)) for row in rows],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def replace_stock_provider_window(
        self,
        code: str,
        rows: list[dict[str, Any]],
        coverage: ProviderStockCoverage | dict[str, Any],
        metadata: ProviderStockMetadata | dict[str, Any],
    ) -> SemanticDeltaResult:
        """Atomically replace one code's complete validated provider window."""
        self._assert_writable()
        normalized_code, normalized_rows, normalized_coverage, normalized_metadata = (
            validate_provider_stock_window(code, rows, coverage, metadata)
        )
        metadata_values = {
            METADATA_KEYS["PROVIDER_PLAN"]: normalized_metadata.provider_plan
        }
        if isinstance(metadata, dict):
            last_refresh = metadata.get(METADATA_KEYS["LAST_STOCKS_REFRESH"])
            if last_refresh is not None and str(last_refresh).strip():
                metadata_values[METADATA_KEYS["LAST_STOCKS_REFRESH"]] = str(
                    last_refresh
                )
            adjustment_mode = metadata.get(METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"])
            if adjustment_mode is not None:
                if adjustment_mode != PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE:
                    raise ValueError("Provider stock window adjustment mode is invalid")
                metadata_values[METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]] = str(
                    adjustment_mode
                )
        raw_columns = self._STOCK_DATA_RAW_UPSERT_SPEC.columns
        projection_columns = self._STOCK_DATA_UPSERT_SPEC.columns
        event_columns = self._STOCK_ADJUSTMENT_EVENTS_UPSERT_SPEC.columns
        projected_rows = [
            {
                "code": row["code"],
                "date": row["date"],
                "open": row["adjusted_open"],
                "high": row["adjusted_high"],
                "low": row["adjusted_low"],
                "close": row["adjusted_close"],
                "volume": row["adjusted_volume"],
                "adjustment_factor": row["adjustment_factor"],
                "created_at": row.get("created_at"),
            }
            for row in normalized_rows
        ]
        event_rows = [
            {
                "code": row["code"],
                "date": row["date"],
                "adjustment_factor": row["adjustment_factor"],
                "source_fingerprint": normalized_metadata.provider_source_fingerprint,
                "created_at": row.get("created_at"),
            }
            for row in normalized_rows
            if float(row["adjustment_factor"]) != 1.0
        ]

        with self._lock:
            existing_rows = [
                dict(zip(raw_columns, row, strict=True))
                for row in self._conn.execute(
                    f"SELECT {', '.join(raw_columns)} FROM stock_data_raw WHERE code = ?",
                    [normalized_code],
                ).fetchall()
            ]
            result = self._classify_provider_window_delta(
                existing_rows=existing_rows,
                desired_rows=normalized_rows,
                columns=raw_columns,
            )
            existing_projection_rows = [
                dict(zip(projection_columns, row, strict=True))
                for row in self._conn.execute(
                    f"SELECT {', '.join(projection_columns)} "
                    "FROM stock_data WHERE code = ?",
                    [normalized_code],
                ).fetchall()
            ]
            projection_result = self._classify_provider_window_delta(
                existing_rows=existing_projection_rows,
                desired_rows=projected_rows,
                columns=projection_columns,
            )
            existing_events = self._conn.execute(
                """
                SELECT code, date, adjustment_factor, source_fingerprint
                FROM stock_adjustment_events
                WHERE code = ?
                ORDER BY date
                """,
                [normalized_code],
            ).fetchall()
            desired_events = [
                (
                    row["code"],
                    row["date"],
                    row["adjustment_factor"],
                    row["source_fingerprint"],
                )
                for row in event_rows
            ]
            events_changed = existing_events != desired_events
            desired_ledger = (
                normalized_coverage.start,
                normalized_coverage.end,
                normalized_metadata.provider_plan,
                normalized_metadata.provider_as_of,
                normalized_metadata.provider_source_fingerprint,
            )
            existing_ledger = self._conn.execute(
                """
                SELECT coverage_start, coverage_end, provider_plan, provider_as_of,
                       source_fingerprint
                FROM stock_provider_windows
                WHERE code = ?
                """,
                [normalized_code],
            ).fetchone()
            ledger_changed = existing_ledger != desired_ledger
            existing_metadata = dict(
                self._conn.execute(
                    "SELECT key, value FROM sync_metadata WHERE key IN ("
                    + ", ".join("?" for _key in metadata_values)
                    + ")",
                    list(metadata_values),
                ).fetchall()
            )
            metadata_changed = existing_metadata != metadata_values
            if (
                not result.mutated_rows
                and not projection_result.mutated_rows
                and not events_changed
                and not ledger_changed
                and not metadata_changed
            ):
                return result

            relations: list[tuple[str, pd.DataFrame]] = []
            if result.mutated_rows:
                relations.append(
                    (
                        self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name,
                        pd.DataFrame.from_records(
                            (
                                {column: row.get(column) for column in raw_columns}
                                for row in normalized_rows
                            ),
                            columns=raw_columns,
                        ),
                    )
                )
            if projection_result.mutated_rows:
                relations.append(
                    (
                        self._STOCK_DATA_UPSERT_SPEC.relation_name,
                        pd.DataFrame.from_records(
                            projected_rows, columns=projection_columns
                        ),
                    )
                )
            if events_changed and event_rows:
                relations.append(
                    (
                        self._STOCK_ADJUSTMENT_EVENTS_UPSERT_SPEC.relation_name,
                        pd.DataFrame.from_records(event_rows, columns=event_columns),
                    )
                )
            for relation_name, dataframe in relations:
                self._conn.register(relation_name, dataframe)
            transaction_started = False
            try:
                self._conn.execute("BEGIN TRANSACTION")
                transaction_started = True
                if result.mutated_rows:
                    self._conn.execute(
                        "DELETE FROM stock_data_raw WHERE code = ?", [normalized_code]
                    )
                    self._conn.execute(
                        f"""
                        INSERT INTO stock_data_raw ({", ".join(raw_columns)})
                        SELECT {", ".join(raw_columns)}
                        FROM {self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name}
                        """
                    )
                if projection_result.mutated_rows:
                    self._conn.execute(
                        "DELETE FROM stock_data WHERE code = ?", [normalized_code]
                    )
                    self._conn.execute(
                        f"""
                        INSERT INTO stock_data ({", ".join(projection_columns)})
                        SELECT {", ".join(projection_columns)}
                        FROM {self._STOCK_DATA_UPSERT_SPEC.relation_name}
                        """
                    )
                if events_changed:
                    self._conn.execute(
                        "DELETE FROM stock_adjustment_events WHERE code = ?",
                        [normalized_code],
                    )
                    if event_rows:
                        self._conn.execute(
                            f"""
                            INSERT INTO stock_adjustment_events ({", ".join(event_columns)})
                            SELECT {", ".join(event_columns)}
                            FROM {self._STOCK_ADJUSTMENT_EVENTS_UPSERT_SPEC.relation_name}
                            """
                        )
                updated_at = datetime.now(UTC).isoformat()
                if ledger_changed:
                    self._conn.execute(
                        """
                        INSERT INTO stock_provider_windows (
                            code, coverage_start, coverage_end, provider_plan, provider_as_of,
                            source_fingerprint, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (code) DO UPDATE SET
                            coverage_start = excluded.coverage_start,
                            coverage_end = excluded.coverage_end,
                            provider_plan = excluded.provider_plan,
                            provider_as_of = excluded.provider_as_of,
                            source_fingerprint = excluded.source_fingerprint,
                            updated_at = excluded.updated_at
                        """,
                        [normalized_code, *desired_ledger, updated_at],
                    )
                if metadata_changed:
                    for key, value in metadata_values.items():
                        self._conn.execute(
                            """
                            INSERT INTO sync_metadata (key, value, updated_at)
                            VALUES (?, ?, ?)
                            ON CONFLICT (key) DO UPDATE SET
                                value = excluded.value,
                                updated_at = excluded.updated_at
                            WHERE sync_metadata.value IS DISTINCT FROM excluded.value
                            """,
                            [key, value, updated_at],
                        )
                if events_changed:
                    self._mark_current_basis_recompute_pending_unlocked(
                        normalized_code,
                        reason="provider_basis_change",
                        source_fingerprint=(
                            normalized_metadata.provider_source_fingerprint
                        ),
                    )
                self._conn.execute("COMMIT")
                transaction_started = False
            except BaseException:
                if transaction_started:
                    self._conn.execute("ROLLBACK")
                raise
            finally:
                for relation_name, _dataframe in relations:
                    self._conn.unregister(relation_name)

            if result.mutated_rows:
                affected_dates = {
                    str(row["date"]) for row in (*existing_rows, *normalized_rows)
                }
                self._dirty_tables.add("stock_data_raw")
                self._dirty_partition_dates.setdefault("stock_data_raw", set()).update(
                    affected_dates
                )
            if projection_result.mutated_rows:
                affected_projection_dates = {
                    str(row["date"])
                    for row in (*existing_projection_rows, *projected_rows)
                }
                self._dirty_tables.add("stock_data")
                self._dirty_partition_dates.setdefault("stock_data", set()).update(
                    affected_projection_dates
                )
            if events_changed:
                self._dirty_tables.add("stock_adjustment_events")
            return result

    @classmethod
    def _classify_provider_window_delta(
        cls,
        *,
        existing_rows: list[dict[str, Any]],
        desired_rows: list[dict[str, Any]],
        columns: tuple[str, ...],
    ) -> SemanticDeltaResult:
        semantic_columns = tuple(
            column
            for column in columns
            if column not in {"code", "date", "created_at"}
        )
        existing_by_date = {str(row["date"]): row for row in existing_rows}
        desired_by_date = {str(row["date"]): row for row in desired_rows}
        inserted_keys: list[tuple[str, str]] = []
        updated_keys: list[tuple[str, str]] = []
        unchanged = 0
        for date_value, desired in desired_by_date.items():
            existing = existing_by_date.get(date_value)
            key = (str(desired["code"]), date_value)
            if existing is None:
                inserted_keys.append(key)
            elif any(
                existing.get(column) != desired.get(column)
                for column in semantic_columns
            ):
                updated_keys.append(key)
            else:
                unchanged += 1
        deleted_keys = [
            (str(existing["code"]), date_value)
            for date_value, existing in existing_by_date.items()
            if date_value not in desired_by_date
        ]
        mutated_keys = (*inserted_keys, *updated_keys, *deleted_keys)
        return SemanticDeltaResult(
            stats=MarketMutationStats(
                input=len(desired_rows),
                inserted=len(inserted_keys),
                updated=len(updated_keys),
                unchanged=unchanged,
                deleted=len(deleted_keys),
            ),
            inserted_keys=tuple(inserted_keys),
            updated_keys=tuple(updated_keys),
            deleted_keys=tuple(deleted_keys),
            affected_dates=frozenset(key[1] for key in mutated_keys),
            affected_codes=frozenset(key[0] for key in mutated_keys),
        )

    def _publish_stock_data_locked(
        self, rows: list[dict[str, Any]], *, stage: ProviderStockStage
    ) -> SemanticDeltaResult:
        normalized_rows = [
            {
                **row,
                "code": normalize_stock_code(str(row.get("code") or "")),
            }
            for row in rows
        ]
        staged_rows = deterministic_last_wins(
            normalized_rows,
            key_columns=self._STOCK_DATA_RAW_UPSERT_SPEC.conflict_columns,
        )
        staged_codes = {
            normalize_stock_code(str(row.get("code") or "")) for row in staged_rows
        }
        if not staged_codes <= stage.provider_codes:
            raise ValueError("Provider stock rows must be contained in stage code scope")
        if any(str(row.get("date") or "") > stage.provider_as_of for row in staged_rows):
            raise ValueError("Provider stock row date must not exceed stage provider as-of")
        projected_rows = self._provider_adjusted_stock_rows(staged_rows)
        staged_by_code: dict[str, list[dict[str, Any]]] = {}
        for row in staged_rows:
            staged_by_code.setdefault(str(row["code"]), []).append(row)

        probe_relation = "__tmp_stock_window_append_probe"
        self._conn.register(
            probe_relation,
            pd.DataFrame.from_records(
                (
                    {"code": str(row["code"]), "date": str(row["date"])}
                    for row in staged_rows
                ),
                columns=("code", "date"),
            ),
        )
        raw_columns = self._STOCK_DATA_RAW_UPSERT_SPEC.columns
        try:
            existing_incoming_rows = [
                dict(zip(raw_columns, row, strict=True))
                for row in self._conn.execute(
                    f"""
                    SELECT {", ".join(f"existing.{column}" for column in raw_columns)}
                    FROM stock_data_raw AS existing
                    JOIN {probe_relation} AS incoming
                      ON incoming.code = existing.code
                     AND incoming.date = existing.date
                    """
                ).fetchall()
            ]
        finally:
            self._conn.unregister(probe_relation)
        existing_incoming_by_code: dict[str, list[dict[str, Any]]] = {}
        for row in existing_incoming_rows:
            existing_incoming_by_code.setdefault(str(row["code"]), []).append(row)

        existing_ledgers: dict[str, tuple[str, str, str, str, str] | None] = {}
        desired_ledgers: dict[str, tuple[str, str, str, str, str]] = {}
        expired_rows_by_code: dict[str, list[dict[str, Any]]] = {}
        for code, code_rows in staged_by_code.items():
            ledger_row = self._conn.execute(
                """
                SELECT coverage_start, coverage_end, provider_plan, provider_as_of,
                       source_fingerprint
                FROM stock_provider_windows WHERE code = ?
                """,
                [code],
            ).fetchone()
            existing_ledger = (
                None
                if ledger_row is None
                else (
                    str(ledger_row[0]),
                    str(ledger_row[1]),
                    str(ledger_row[2]),
                    str(ledger_row[3]),
                    str(ledger_row[4]),
                )
            )
            existing_ledgers[code] = existing_ledger
            dates = [str(row["date"]) for row in code_rows]
            desired_end = (
                max(dates)
                if existing_ledger is None
                else max(existing_ledger[1], *dates)
            )
            desired_start = min(dates) if existing_ledger is None else existing_ledger[0]
            listed_row = (
                self._conn.execute(
                    """
                    SELECT MIN(NULLIF(listed_date, ''))
                    FROM stock_master_daily
                    WHERE code = ? OR code = ?
                    """,
                    [code, f"{code}0"],
                ).fetchone()
                if self._table_exists("stock_master_daily")
                else None
            )
            listed_date = (
                str(listed_row[0])
                if listed_row is not None and listed_row[0] is not None
                else None
            )
            provider_limited_frontier = (
                listed_date is None
                or (existing_ledger is not None and existing_ledger[0] > listed_date)
            )
            if (
                existing_ledger is not None
                and existing_ledger[0] < existing_ledger[1]
                and provider_limited_frontier
                and desired_end > existing_ledger[1]
            ):
                elapsed = date.fromisoformat(desired_end) - date.fromisoformat(
                    existing_ledger[1]
                )
                desired_start = (
                    date.fromisoformat(existing_ledger[0]) + timedelta(days=elapsed.days)
                ).isoformat()
            expired_rows = (
                []
                if existing_ledger is None or desired_start == existing_ledger[0]
                else [
                    dict(zip(raw_columns, row, strict=True))
                    for row in self._conn.execute(
                        f"SELECT {', '.join(raw_columns)} FROM stock_data_raw "
                        "WHERE code = ? AND date < ?",
                        [code, desired_start],
                    ).fetchall()
                ]
            )
            expired_rows_by_code[code] = expired_rows
            old_fingerprint = (
                provider_stock_source_fingerprint(())
                if existing_ledger is None
                else existing_ledger[4]
            )
            desired_fingerprint = combine_provider_stock_source_fingerprints(
                old_fingerprint,
                provider_stock_source_fingerprint(
                    existing_incoming_by_code.get(code, ())
                ),
                provider_stock_source_fingerprint(expired_rows),
                provider_stock_source_fingerprint(code_rows),
            )
            desired_provider_as_of = (
                stage.provider_as_of
                if existing_ledger is None
                or existing_ledger[2] != stage.provider_plan
                else max(existing_ledger[3], stage.provider_as_of)
            )
            if desired_provider_as_of < desired_end:
                raise ValueError(
                    "Provider stock stage provider as-of precedes resulting coverage"
                )
            desired_ledgers[code] = (
                desired_start,
                desired_end,
                stage.provider_plan,
                desired_provider_as_of,
                desired_fingerprint,
            )
        for code in stage.provider_codes - staged_codes:
            ledger_row = self._conn.execute(
                """
                SELECT coverage_start, coverage_end, provider_plan, provider_as_of,
                       source_fingerprint
                FROM stock_provider_windows WHERE code = ?
                """,
                [code],
            ).fetchone()
            if ledger_row is None:
                continue
            existing = tuple(str(value) for value in ledger_row)
            existing_ledgers[code] = cast(tuple[str, str, str, str, str], existing)
            if existing[2] != stage.provider_plan:
                continue
            desired_provider_as_of = max(existing[3], stage.provider_as_of)
            if desired_provider_as_of < existing[1]:
                raise ValueError(
                    "Provider stock stage provider as-of precedes existing coverage"
                )
            desired_ledgers[code] = (
                existing[0],
                existing[1],
                existing[2],
                desired_provider_as_of,
                existing[4],
            )
        event_rows = [
            {
                "code": row.get("code"),
                "date": row.get("date"),
                "adjustment_factor": row.get("adjustment_factor"),
                "source_fingerprint": desired_ledgers[str(row["code"])][4],
                "created_at": row.get("created_at"),
            }
            for row in staged_rows
            if row.get("adjustment_factor") is not None
            and float(row["adjustment_factor"]) != 1.0
        ]
        rebound_event_count = 0
        expired_event_codes: set[str] = set()
        transaction_started = False
        try:
            self._conn.execute("BEGIN TRANSACTION")
            transaction_started = True
            for code, expired_rows in expired_rows_by_code.items():
                if not expired_rows:
                    continue
                desired_start = desired_ledgers[code][0]
                self._conn.execute(
                    "DELETE FROM stock_data_raw WHERE code = ? AND date < ?",
                    [code, desired_start],
                )
                self._conn.execute(
                    "DELETE FROM stock_data WHERE code = ? AND date < ?",
                    [code, desired_start],
                )
                deleted_events = self._conn.execute(
                    "DELETE FROM stock_adjustment_events "
                    "WHERE code = ? AND date < ? RETURNING code",
                    [code, desired_start],
                ).fetchall()
                if deleted_events:
                    expired_event_codes.add(code)
            raw_result = self._apply_semantic_delta(
                normalized_rows, spec=self._STOCK_DATA_RAW_UPSERT_SPEC
            )
            consumer_result = self._apply_semantic_delta(
                projected_rows, spec=self._STOCK_DATA_UPSERT_SPEC
            )
            event_result = self._apply_semantic_delta(
                event_rows, spec=self._STOCK_ADJUSTMENT_EVENTS_UPSERT_SPEC
            )
            pending_codes = event_result.affected_codes | frozenset(expired_event_codes)
            updated_at = datetime.now(UTC).isoformat()
            self._conn.execute(
                """
                INSERT INTO sync_metadata (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                WHERE sync_metadata.value IS DISTINCT FROM excluded.value
                """,
                [METADATA_KEYS["PROVIDER_PLAN"], stage.provider_plan, updated_at],
            )
            for code, desired in desired_ledgers.items():
                rebound_event_count += len(
                    self._conn.execute(
                        """
                        UPDATE stock_adjustment_events
                        SET source_fingerprint = ?
                        WHERE code = ?
                          AND source_fingerprint IS DISTINCT FROM ?
                        RETURNING code
                        """,
                        [desired[4], code, desired[4]],
                    ).fetchall()
                )
            for code, desired in desired_ledgers.items():
                if existing_ledgers[code] == desired:
                    continue
                self._conn.execute(
                    """
                    INSERT INTO stock_provider_windows (
                        code, coverage_start, coverage_end, provider_plan, provider_as_of,
                        source_fingerprint, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (code) DO UPDATE SET
                        coverage_start = excluded.coverage_start,
                        coverage_end = excluded.coverage_end,
                        provider_plan = excluded.provider_plan,
                        provider_as_of = excluded.provider_as_of,
                        source_fingerprint = excluded.source_fingerprint,
                        updated_at = excluded.updated_at
                    """,
                    [code, *desired, updated_at],
                )
            for code in pending_codes:
                self._mark_current_basis_recompute_pending_unlocked(
                    code,
                    reason="provider_basis_change",
                    source_fingerprint=desired_ledgers[code][4],
                )
            self._conn.execute("COMMIT")
            transaction_started = False
        except BaseException:
            if transaction_started:
                self._conn.execute("ROLLBACK")
            raise

        affected_dates = (
            raw_result.affected_dates
            | consumer_result.affected_dates
            | event_result.affected_dates
        )
        expired_dates = {
            str(row["date"])
            for expired_rows in expired_rows_by_code.values()
            for row in expired_rows
        }
        affected_dates |= frozenset(expired_dates)
        if raw_result.mutated_rows or expired_dates:
            self._dirty_tables.add("stock_data_raw")
            self._dirty_partition_dates.setdefault("stock_data_raw", set()).update(
                affected_dates
            )
        if consumer_result.mutated_rows or expired_dates:
            self._dirty_tables.add("stock_data")
            self._dirty_partition_dates.setdefault("stock_data", set()).update(
                affected_dates
            )
        if event_result.mutated_rows or rebound_event_count or expired_event_codes:
            self._dirty_tables.add("stock_adjustment_events")
        if raw_result.mutated_rows:
            return raw_result
        if consumer_result.mutated_rows:
            return consumer_result
        return event_result if event_result.mutated_rows else raw_result

    def stage_stock_data_rows(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        if not rows:
            return SemanticDeltaResult.empty()
        dataframe = pd.DataFrame.from_records(
            [
                {
                    column: row.get(column)
                    for column in self._STOCK_DATA_RAW_UPSERT_SPEC.columns
                }
                for row in rows
            ],
            columns=self._STOCK_DATA_RAW_UPSERT_SPEC.columns,
        )
        with self._lock:
            self._ensure_stock_data_stage_table()
            self._conn.register(
                self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name, dataframe
            )
            try:
                columns_sql = ", ".join(self._STOCK_DATA_RAW_UPSERT_SPEC.columns)
                self._conn.execute(
                    f"""
                    INSERT INTO {self._STOCK_DATA_STAGE_TABLE} ({columns_sql})
                    SELECT {columns_sql}
                    FROM {self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name}
                    """
                )
            finally:
                self._conn.unregister(self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name)
        return SemanticDeltaResult.empty(input_count=len(rows))

    def flush_staged_stock_data(
        self,
        *,
        stage: ProviderStockStage,
        exclude_codes: frozenset[str] = frozenset(),
    ) -> SemanticDeltaResult:
        """Append staged rows whose keys are new, excluding full-refresh codes."""
        self._assert_writable()
        with self._lock:
            self._ensure_stock_data_stage_table()
            excluded = sorted(exclude_codes)
            exclusion_sql = ""
            if excluded:
                exclusion_sql = " AND staged.code NOT IN (" + ", ".join(
                    "?" for _code in excluded
                ) + ")"
            selected_columns = ", ".join(
                f"staged.{column}"
                for column in self._STOCK_DATA_RAW_UPSERT_SPEC.columns
            )
            staged_rows = [
                dict(zip(self._STOCK_DATA_RAW_UPSERT_SPEC.columns, row, strict=True))
                for row in self._conn.execute(
                    f"""
                    SELECT {selected_columns}
                    FROM {self._STOCK_DATA_STAGE_TABLE} staged
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM stock_data_raw existing
                        WHERE existing.code = staged.code
                          AND existing.date = staged.date
                    )
                    {exclusion_sql}
                    ORDER BY staged.rowid
                    """,
                    excluded,
                ).fetchall()
            ]
            self._conn.execute(f"DELETE FROM {self._STOCK_DATA_STAGE_TABLE}")
            return self._publish_stock_data_locked(
                staged_rows,
                stage=stage,
            )

    def discard_staged_stock_data(self) -> None:
        self._assert_writable()
        with self._lock:
            self._ensure_stock_data_stage_table()
            self._conn.execute(f"DELETE FROM {self._STOCK_DATA_STAGE_TABLE}")

    def _ensure_stock_data_stage_table(self) -> None:
        columns_sql = ", ".join(self._STOCK_DATA_RAW_UPSERT_SPEC.columns)
        self._conn.execute(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS {self._STOCK_DATA_STAGE_TABLE} AS
            SELECT {columns_sql}
            FROM stock_data_raw
            WHERE 1 = 0
            """
        )

    def publish_stock_minute_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            result = self._publish_and_mark_delta(
                rows, spec=self._STOCK_MINUTE_DATA_UPSERT_SPEC
            )
            if result.mutated_rows:
                self._dirty_stock_minute_dates.update(result.affected_dates)
            return result

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._INDICES_DATA_UPSERT_SPEC)

    def publish_options_225_data(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._OPTIONS_225_UPSERT_SPEC)

    def publish_margin_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._MARGIN_DATA_UPSERT_SPEC)

    def publish_statements(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            self._validate_fallback_statement_identities_unlocked(rows)
            transaction_started = False
            try:
                self._conn.execute("BEGIN TRANSACTION")
                transaction_started = True
                result = self._apply_semantic_delta(
                    rows, spec=self._STATEMENTS_UPSERT_SPEC
                )
                for code in result.affected_codes:
                    self._mark_current_basis_recompute_pending_unlocked(
                        code,
                        reason="statement_change",
                        source_fingerprint=(
                            self._current_statement_source_fingerprint_unlocked(code)
                        ),
                    )
                self._conn.execute("COMMIT")
                transaction_started = False
            except BaseException:
                if transaction_started:
                    self._conn.execute("ROLLBACK")
                raise
            if result.mutated_rows:
                self._dirty_tables.add(self._STATEMENTS_UPSERT_SPEC.table_name)
                self._dirty_partition_dates.setdefault(
                    self._STATEMENTS_UPSERT_SPEC.table_name, set()
                ).update(result.affected_dates)
            return result

    def _validate_fallback_statement_identities_unlocked(
        self, rows: list[dict[str, Any]]
    ) -> None:
        columns = self._STATEMENTS_UPSERT_SPEC.columns
        fallback_rows: dict[tuple[str, str], tuple[Any, ...]] = {}
        for row in rows:
            code = str(row.get("code") or "")
            statement_id = str(row.get("statement_id") or "")
            if not statement_id.startswith("fallback:"):
                continue
            key = (code, statement_id)
            payload = tuple(row.get(column) for column in columns)
            previous = fallback_rows.get(key)
            if previous is not None and previous != payload:
                raise ValueError(
                    "fallback statement identity collision within publish batch: "
                    f"code={code} statement_id={statement_id}"
                )
            fallback_rows[key] = payload

        for (code, statement_id), incoming in fallback_rows.items():
            existing = self._conn.execute(
                f"SELECT {', '.join(columns)} FROM statements "
                "WHERE code = ? AND statement_id = ?",
                [code, statement_id],
            ).fetchone()
            if existing is not None and tuple(existing) != incoming:
                raise ValueError(
                    "fallback statement identity collision with existing row: "
                    f"code={code} statement_id={statement_id}"
                )

    def _publish_and_mark_delta(
        self,
        rows: list[dict[str, Any]],
        *,
        spec: _RelationUpsertSpec,
    ) -> SemanticDeltaResult:
        with self._lock:
            result = self._apply_semantic_delta(rows, spec=spec)
            if result.mutated_rows:
                self._dirty_tables.add(spec.table_name)
                self._dirty_partition_dates.setdefault(spec.table_name, set()).update(
                    result.affected_dates
                )
            return result

    def index_topix_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("topix_data")

    def has_pending_index(self, table_name: str) -> bool:
        """Return whether semantic mutations require an index/export pass."""
        with self._lock:
            if table_name == "stock_data":
                return bool(
                    "stock_data_raw" in self._dirty_tables
                    or "stock_data" in self._dirty_tables
                    or "stock_adjustment_events" in self._dirty_tables
                )
            return table_name in self._dirty_tables

    def index_stock_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("stock_data_raw")
        self._export_if_dirty("stock_data")
        self._export_if_dirty("stock_adjustment_events")

    def index_stock_minute_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("stock_data_minute_raw")

    def index_indices_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("indices_data")

    def index_options_225_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("options_225_data")

    def index_margin_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("margin_data")

    def index_statements(self) -> None:
        self._assert_writable()
        self._export_if_dirty("statements")

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        with self._lock:
            topix_row_raw = self._conn.execute(
                "SELECT COUNT(*) AS count, MIN(date) AS min_date, MAX(date) AS max_date FROM topix_data"
            ).fetchone()
            stock_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT date) AS date_count
                FROM stock_data
                """
            ).fetchone()
            stock_minute_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT date) AS date_count,
                    COUNT(DISTINCT code) AS code_count
                FROM stock_data_minute_raw
                """
            ).fetchone()
            latest_stock_minute_row = self._conn.execute(
                """
                SELECT date, time
                FROM stock_data_minute_raw
                ORDER BY date DESC, time DESC
                LIMIT 1
                """
            ).fetchone()
            indices_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT date) AS date_count
                FROM indices_data
                """
            ).fetchone()
            indices_rows = self._conn.execute(
                """
                SELECT code, MAX(date) AS max_date
                FROM indices_data
                GROUP BY code
                """
            ).fetchall()
            options_225_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT date) AS date_count
                FROM options_225_data
                """
            ).fetchone()
            margin_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT date) AS date_count
                FROM margin_data
                """
            ).fetchone()
            margin_codes_rows = self._conn.execute(
                "SELECT DISTINCT code FROM margin_data WHERE code IS NOT NULL"
            ).fetchall()
            statements_row_raw = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS count,
                    MAX(disclosed_date) AS max_disclosed
                FROM statements
                """
            ).fetchone()
            missing_count_row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM topix_data t
                LEFT JOIN (SELECT DISTINCT date FROM stock_data) s ON t.date = s.date
                WHERE s.date IS NULL
                """
            ).fetchone()
            missing_options_225_count_row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM topix_data t
                LEFT JOIN (SELECT DISTINCT date FROM options_225_data) o ON t.date = o.date
                WHERE o.date IS NULL
                """
            ).fetchone()
            statement_codes_rows = self._conn.execute(
                "SELECT DISTINCT code FROM statements WHERE code IS NOT NULL"
            ).fetchall()
            topix_row = topix_row_raw if topix_row_raw is not None else (0, None, None)
            stock_row = (
                stock_row_raw if stock_row_raw is not None else (0, None, None, 0)
            )
            stock_minute_row = (
                stock_minute_row_raw
                if stock_minute_row_raw is not None
                else (0, None, None, 0, 0)
            )
            indices_row = (
                indices_row_raw if indices_row_raw is not None else (0, None, None, 0)
            )
            options_225_row = (
                options_225_row_raw
                if options_225_row_raw is not None
                else (0, None, None, 0)
            )
            margin_row = (
                margin_row_raw if margin_row_raw is not None else (0, None, None, 0)
            )
            statements_row = (
                statements_row_raw if statements_row_raw is not None else (0, None)
            )
            missing_stock_dates_count = (
                int(missing_count_row[0] or 0) if missing_count_row else 0
            )
            missing_options_225_dates_count = (
                int(missing_options_225_count_row[0] or 0)
                if missing_options_225_count_row
                else 0
            )
            margin_codes = {str(row[0]) for row in margin_codes_rows if row and row[0]}
            margin_orphan_count = 0
            if self._table_exists("stocks"):
                margin_orphan_row = self._conn.execute(
                    """
                    SELECT COUNT(DISTINCT m.code)
                    FROM margin_data m
                    LEFT JOIN stocks s ON m.code = s.code
                    WHERE m.code IS NOT NULL
                      AND s.code IS NULL
                    """
                ).fetchone()
                margin_orphan_count = (
                    int(margin_orphan_row[0] or 0) if margin_orphan_row else 0
                )

            missing_stock_dates: list[str] = []
            if missing_stock_dates_limit > 0:
                missing_rows = self._conn.execute(
                    """
                    SELECT t.date
                    FROM topix_data t
                    LEFT JOIN (SELECT DISTINCT date FROM stock_data) s ON t.date = s.date
                    WHERE s.date IS NULL
                    ORDER BY t.date DESC
                    LIMIT ?
                    """,
                    [missing_stock_dates_limit],
                ).fetchall()
                missing_stock_dates = [
                    str(row[0]) for row in missing_rows if row and row[0]
                ]

            missing_options_225_dates: list[str] = []
            if missing_options_225_dates_limit > 0:
                missing_options_rows = self._conn.execute(
                    """
                    SELECT t.date
                    FROM topix_data t
                    LEFT JOIN (SELECT DISTINCT date FROM options_225_data) o ON t.date = o.date
                    WHERE o.date IS NULL
                    ORDER BY t.date DESC
                    LIMIT ?
                    """,
                    [missing_options_225_dates_limit],
                ).fetchall()
                missing_options_225_dates = [
                    str(row[0]) for row in missing_options_rows if row and row[0]
                ]

            statement_non_null_counts = self._duckdb_statement_non_null_counts(
                statement_non_null_columns or []
            )

            latest_indices_dates = {
                str(row[0]): str(row[1])
                for row in indices_rows
                if row and row[0] and row[1]
            }
            statement_codes = {
                str(row[0]) for row in statement_codes_rows if row and row[0]
            }

            return TimeSeriesInspection(
                source="duckdb-parquet",
                topix_count=int(topix_row[0] or 0),
                topix_min=cast(str | None, topix_row[1]),
                topix_max=cast(str | None, topix_row[2]),
                stock_count=int(stock_row[0] or 0),
                stock_min=cast(str | None, stock_row[1]),
                stock_max=cast(str | None, stock_row[2]),
                stock_date_count=int(stock_row[3] or 0),
                stock_minute_count=int(stock_minute_row[0] or 0),
                stock_minute_min=cast(str | None, stock_minute_row[1]),
                stock_minute_max=cast(str | None, stock_minute_row[2]),
                stock_minute_date_count=int(stock_minute_row[3] or 0),
                stock_minute_code_count=int(stock_minute_row[4] or 0),
                latest_stock_minute_time=cast(
                    str | None,
                    latest_stock_minute_row[1] if latest_stock_minute_row else None,
                ),
                missing_stock_dates=missing_stock_dates,
                missing_stock_dates_count=missing_stock_dates_count,
                indices_count=int(indices_row[0] or 0),
                indices_min=cast(str | None, indices_row[1]),
                indices_max=cast(str | None, indices_row[2]),
                indices_date_count=int(indices_row[3] or 0),
                latest_indices_dates=latest_indices_dates,
                options_225_count=int(options_225_row[0] or 0),
                options_225_min=cast(str | None, options_225_row[1]),
                options_225_max=cast(str | None, options_225_row[2]),
                options_225_date_count=int(options_225_row[3] or 0),
                latest_options_225_date=cast(str | None, options_225_row[2]),
                missing_options_225_dates=missing_options_225_dates,
                missing_options_225_dates_count=missing_options_225_dates_count,
                margin_count=int(margin_row[0] or 0),
                margin_min=cast(str | None, margin_row[1]),
                margin_max=cast(str | None, margin_row[2]),
                margin_date_count=int(margin_row[3] or 0),
                margin_codes=margin_codes,
                margin_orphan_count=margin_orphan_count,
                statements_count=int(statements_row[0] or 0),
                latest_statement_disclosed_date=cast(str | None, statements_row[1]),
                statement_codes=statement_codes,
                statement_non_null_counts=statement_non_null_counts,
            )

    def _duckdb_statement_non_null_counts(self, columns: list[str]) -> dict[str, int]:
        if not columns:
            return {}

        existing = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info('statements')").fetchall()
            if row and len(row) > 1
        }
        counts: dict[str, int] = {}

        for column in columns:
            if column not in existing:
                counts[column] = 0
                continue

            escaped = self._quote_identifier(column)
            count_row = self._conn.execute(
                f"SELECT COUNT(*) FROM statements WHERE {escaped} IS NOT NULL"
            ).fetchone()
            counts[column] = int(count_row[0] or 0) if count_row else 0

        return counts

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _table_exists(self, table_name: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        return row is not None

    def _export_if_dirty(self, table_name: str) -> None:
        with self._lock:
            if table_name not in self._dirty_tables:
                return
            if table_name == "stock_data_minute_raw":
                self._export_stock_minute_partitions()
                return
            if table_name in self._DATE_PARTITIONED_TABLES:
                self._export_date_partitions(table_name)
                return
            spec = self._TABLE_SPECS[table_name]
            output_path = self._parquet_dir / spec.parquet_name
            started_at = perf_counter()
            row_count = self._count_table_rows(spec.table_name)
            tmp_output_path = output_path.with_name(f"{output_path.name}.tmp")
            if tmp_output_path.exists():
                tmp_output_path.unlink()

            escaped = str(tmp_output_path).replace("'", "''")
            if spec.order_by:
                source_sql = (
                    f"(SELECT * FROM {spec.table_name} ORDER BY {spec.order_by})"
                )
            else:
                source_sql = spec.table_name
            try:
                self._conn.execute(f"COPY {source_sql} TO '{escaped}' (FORMAT PARQUET)")
            except Exception:
                if tmp_output_path.exists():
                    tmp_output_path.unlink()
                raise
            if tmp_output_path.exists():
                tmp_output_path.replace(output_path)
            elif output_path.exists():
                output_path.unlink()
            self._dirty_tables.discard(table_name)
            elapsed_ms = (perf_counter() - started_at) * 1000
            logger.info(
                "market store phase timing",
                event="market_store_phase_timing",
                operation="parquet_export",
                table=table_name,
                rows=row_count,
                elapsedMs=elapsed_ms,
                outputBytes=self._resolve_path_size(output_path),
            )

    def _export_date_partitions(self, table_name: str) -> None:
        started_at = perf_counter()
        output_root = self._parquet_dir / table_name
        output_root.mkdir(parents=True, exist_ok=True)
        flat_output = self._parquet_dir / self._TABLE_SPECS[table_name].parquet_name
        if flat_output.exists():
            flat_output.unlink()
        tmp_flat_output = flat_output.with_name(f"{flat_output.name}.tmp")
        if tmp_flat_output.exists():
            tmp_flat_output.unlink()

        dirty_dates = getattr(self, "_dirty_partition_dates", {}).get(table_name, set())
        target_dates = sorted(dirty_dates)
        exported_rows = 0
        for date_value in target_dates:
            partition_dir = output_root / f"date={date_value}"
            count_row = self._conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE date = ?",
                [date_value],
            ).fetchone()
            row_count = int(count_row[0] or 0) if count_row else 0
            if row_count <= 0:
                shutil.rmtree(partition_dir, ignore_errors=True)
                continue
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = partition_dir / "data.parquet"
            self._copy_partition_atomically(
                output_path,
                f"""
                COPY (
                    SELECT *
                    FROM {table_name}
                    WHERE date = ?
                ) TO '{{output_path}}' (FORMAT PARQUET)
                """,
                [date_value],
            )
            exported_rows += row_count

        self._dirty_partition_dates.get(table_name, set()).clear()
        self._dirty_tables.discard(table_name)
        elapsed_ms = (perf_counter() - started_at) * 1000
        logger.info(
            "market store phase timing",
            event="market_store_phase_timing",
            operation="parquet_partition_export",
            table=table_name,
            rows=exported_rows,
            partitions=len(target_dates),
            elapsedMs=elapsed_ms,
            outputBytes=resolve_directory_size(output_root),
        )

    def _count_table_rows(self, table_name: str) -> int:
        try:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return int(row[0] or 0) if row else 0
        except Exception:
            return 0

    def _export_stock_minute_partitions(self) -> None:
        output_root = self._parquet_dir / "stock_data_minute_raw"
        output_root.mkdir(parents=True, exist_ok=True)

        target_dates = sorted(self._dirty_stock_minute_dates)

        for date_value in target_dates:
            partition_dir = output_root / f"date={date_value}"

            count_row = self._conn.execute(
                "SELECT COUNT(*) FROM stock_data_minute_raw WHERE date = ?",
                [date_value],
            ).fetchone()
            if not count_row or int(count_row[0] or 0) <= 0:
                shutil.rmtree(partition_dir, ignore_errors=True)
                continue

            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = partition_dir / "data.parquet"
            escaped_date = date_value.replace("'", "''")
            self._copy_partition_atomically(
                output_path,
                f"""
                COPY (
                    SELECT *
                    FROM stock_data_minute_raw
                    WHERE date = '{escaped_date}'
                    ORDER BY code, time
                ) TO '{{output_path}}' (FORMAT PARQUET)
                """,
            )

        self._dirty_stock_minute_dates.clear()
        self._dirty_tables.discard("stock_data_minute_raw")

    def _copy_partition_atomically(
        self,
        output_path: Path,
        copy_sql: str,
        parameters: list[str] | None = None,
    ) -> None:
        staging_path = output_path.with_name(f"{output_path.name}.tmp")
        if staging_path.exists():
            staging_path.unlink()
        escaped_staging_path = str(staging_path).replace("'", "''")
        try:
            statement = copy_sql.format(output_path=escaped_staging_path)
            if parameters is None:
                self._conn.execute(statement)
            else:
                self._conn.execute(statement, parameters)
            staging_path.replace(output_path)
        except BaseException:
            if staging_path.exists():
                staging_path.unlink()
            raise

    @staticmethod
    def _provider_adjusted_stock_rows(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return [
            {
                "code": row.get("code"),
                "date": row.get("date"),
                "open": row.get("adjusted_open"),
                "high": row.get("adjusted_high"),
                "low": row.get("adjusted_low"),
                "close": row.get("adjusted_close"),
                "volume": row.get("adjusted_volume"),
                "adjustment_factor": row.get("adjustment_factor"),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]

    @staticmethod
    def _resolve_upsert_update_columns(spec: _RelationUpsertSpec) -> tuple[str, ...]:
        if spec.update_columns is not None:
            return spec.update_columns
        return tuple(
            column for column in spec.columns if column not in spec.conflict_columns
        )

    @classmethod
    def _build_upsert_update_clause(cls, spec: _RelationUpsertSpec) -> str:
        if spec.update_assignments is not None:
            return ", ".join(spec.update_assignments)
        return ", ".join(
            f"{column} = excluded.{column}"
            for column in cls._resolve_upsert_update_columns(spec)
        )

    @classmethod
    def _semantic_candidate_expression(
        cls,
        spec: _RelationUpsertSpec,
        column: str,
        *,
        source_alias: str,
        target_alias: str,
    ) -> str:
        if spec.update_assignments is not None:
            return f"COALESCE({source_alias}.{column}, {target_alias}.{column})"
        return f"{source_alias}.{column}"

    @classmethod
    def _semantic_distinct_predicate(
        cls,
        spec: _RelationUpsertSpec,
        *,
        source_alias: str,
        target_alias: str,
    ) -> str:
        semantic_columns = tuple(
            column
            for column in cls._resolve_upsert_update_columns(spec)
            if column != "created_at"
        )
        if not semantic_columns:
            return "FALSE"
        return " OR ".join(
            f"{cls._semantic_candidate_expression(spec, column, source_alias=source_alias, target_alias=target_alias)} "
            f"IS DISTINCT FROM {target_alias}.{column}"
            for column in semantic_columns
        )

    def _apply_semantic_delta(
        self,
        rows: list[dict[str, Any]],
        *,
        spec: _RelationUpsertSpec,
    ) -> SemanticDeltaResult:
        """Classify a deduplicated staged relation and persist only its delta."""
        if not rows:
            return SemanticDeltaResult.empty()
        staged_rows = deterministic_last_wins(rows, key_columns=spec.conflict_columns)
        dataframe = pd.DataFrame.from_records(
            [
                {column: row.get(column) for column in spec.columns}
                for row in staged_rows
            ],
            columns=spec.columns,
        )
        join_sql = " AND ".join(
            f"target.{column} IS NOT DISTINCT FROM staged.{column}"
            for column in spec.conflict_columns
        )
        target_missing = f"target.{spec.conflict_columns[0]} IS NULL"
        distinct_sql = self._semantic_distinct_predicate(
            spec,
            source_alias="staged",
            target_alias="target",
        )
        with self._lock:
            self._conn.register(spec.relation_name, dataframe)
            try:
                classified = self._conn.execute(
                    f"""
                    SELECT
                        {", ".join(f"staged.{column}" for column in spec.conflict_columns)},
                        CASE
                            WHEN {target_missing} THEN 'inserted'
                            WHEN {distinct_sql} THEN 'updated'
                            ELSE 'unchanged'
                        END AS delta_kind
                    FROM {spec.relation_name} staged
                    LEFT JOIN {spec.table_name} target ON {join_sql}
                    """
                ).fetchall()
                key_width = len(spec.conflict_columns)
                inserted_keys = tuple(
                    tuple(row[:key_width])
                    for row in classified
                    if row[-1] == "inserted"
                )
                updated_keys = tuple(
                    tuple(row[:key_width]) for row in classified if row[-1] == "updated"
                )
                unchanged = (
                    sum(1 for row in classified if row[-1] == "unchanged")
                    + len(rows)
                    - len(staged_rows)
                )
                if inserted_keys or updated_keys:
                    columns_sql = ", ".join(spec.columns)
                    conflict_sql = ", ".join(spec.conflict_columns)
                    update_clause = self._build_upsert_update_clause(spec)
                    conflict_distinct = self._semantic_distinct_predicate(
                        spec,
                        source_alias="excluded",
                        target_alias=spec.table_name,
                    )
                    self._conn.execute(
                        f"""
                        INSERT INTO {spec.table_name} ({columns_sql})
                        SELECT {columns_sql}
                        FROM {spec.relation_name} staged
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {spec.table_name} target
                            WHERE {join_sql}
                              AND NOT ({distinct_sql})
                        )
                        ON CONFLICT ({conflict_sql}) DO UPDATE
                        SET {update_clause}
                        WHERE {conflict_distinct}
                        """
                    )
            finally:
                self._conn.unregister(spec.relation_name)

        mutated_keys = inserted_keys + updated_keys
        date_index = next(
            (
                index
                for index, column in enumerate(spec.conflict_columns)
                if column in {"date", "disclosed_date"}
            ),
            None,
        )
        code_index = next(
            (
                index
                for index, column in enumerate(spec.conflict_columns)
                if column == "code"
            ),
            None,
        )
        return SemanticDeltaResult(
            stats=MarketMutationStats(
                input=len(rows),
                inserted=len(inserted_keys),
                updated=len(updated_keys),
                unchanged=unchanged,
                deleted=0,
            ),
            inserted_keys=inserted_keys,
            updated_keys=updated_keys,
            affected_dates=frozenset(str(key[date_index]) for key in mutated_keys)
            if date_index is not None
            else frozenset(),
            affected_codes=frozenset(str(key[code_index]) for key in mutated_keys)
            if code_index is not None
            else frozenset(),
        )

    def get_storage_stats(self) -> TimeSeriesStorageStats:
        with self._lock:
            stale_artifact_count, stale_artifacts = (
                self._resolve_stale_storage_artifacts()
            )
            database_size = self._resolve_duckdb_database_size()
            return TimeSeriesStorageStats(
                duckdb_bytes=self._resolve_path_size(self._duckdb_path),
                parquet_bytes=self._resolve_parquet_dir_size(),
                duckdb_blocks_total=database_size["total_blocks"],
                duckdb_blocks_used=database_size["used_blocks"],
                duckdb_blocks_free=database_size["free_blocks"],
                duckdb_bytes_free=database_size["free_bytes"],
                duckdb_wal_bytes=database_size["wal_bytes"],
                temp_directory=database_size["temp_directory"],
                temp_bytes=database_size["temp_bytes"],
                stale_artifact_count=stale_artifact_count,
                stale_artifacts=stale_artifacts,
            )

    @staticmethod
    def _resolve_path_size(path: Path) -> int:
        try:
            return int(path.stat().st_size) if path.exists() else 0
        except OSError:
            return 0

    def _resolve_parquet_dir_size(self) -> int:
        try:
            if not self._parquet_dir.exists():
                return 0
            total = 0
            for file_path in self._parquet_dir.rglob("*.parquet"):
                if not file_path.is_file():
                    continue
                total += int(file_path.stat().st_size)
            return total
        except OSError:
            return 0

    def _resolve_duckdb_database_size(self) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "total_blocks": 0,
            "used_blocks": 0,
            "free_blocks": 0,
            "free_bytes": 0,
            "wal_bytes": 0,
            "temp_directory": None,
            "temp_bytes": 0,
        }
        try:
            row = self._conn.execute("PRAGMA database_size").fetchone()
            if row is None:
                return defaults
            columns = [str(item[0]) for item in self._conn.description or []]
            values = dict(zip(columns, row, strict=False))
            block_size = int(values.get("block_size") or 0)
            free_blocks = int(values.get("free_blocks") or 0)
            temp_directory_row = self._conn.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()
            temp_directory = (
                str(temp_directory_row[0])
                if temp_directory_row and temp_directory_row[0] is not None
                else None
            )
            return {
                "total_blocks": int(values.get("total_blocks") or 0),
                "used_blocks": int(values.get("used_blocks") or 0),
                "free_blocks": free_blocks,
                "free_bytes": free_blocks * block_size,
                "wal_bytes": parse_duckdb_size_bytes(values.get("wal_size")),
                "temp_directory": temp_directory,
                "temp_bytes": resolve_directory_size(Path(temp_directory))
                if temp_directory
                else 0,
            }
        except Exception:
            return defaults

    def _resolve_stale_storage_artifacts(
        self, limit: int = 20
    ) -> tuple[int, list[str]]:
        artifacts: list[str] = []
        try:
            if self._duckdb_path.parent.exists():
                for path in self._duckdb_path.parent.iterdir():
                    if not path.is_file() or not self._is_stale_storage_artifact(path):
                        continue
                    artifacts.append(path.name)
            if self._parquet_dir.exists():
                for path in self._parquet_dir.rglob("*"):
                    if not path.is_file() or not self._is_stale_storage_artifact(path):
                        continue
                    rel_path = path.relative_to(self._parquet_dir)
                    artifacts.append(f"{self._parquet_dir.name}/{rel_path.as_posix()}")
        except OSError:
            return 0, []
        unique_artifacts = sorted(set(artifacts))
        return len(unique_artifacts), unique_artifacts[:limit]

    @staticmethod
    def _is_stale_storage_artifact(path: Path) -> bool:
        name = path.name.lower()
        return name.endswith((".tmp", ".bak", ".backup", ".old"))

    def close(self) -> None:
        with self._lock:
            if not getattr(self, "_read_only", False):
                for table_name in list(self._dirty_tables):
                    self._export_if_dirty(table_name)
            self._conn.close()


def create_time_series_store(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = True,
    writer_token: MarketWriterToken | None = None,
) -> MarketTimeSeriesStore | None:
    """設定に応じて DuckDB 時系列ストアを組み立てる。"""
    normalized_backend = backend.strip().lower()
    if normalized_backend not in {"duckdb", "duckdb-parquet"}:
        logger.warning("Unsupported market time-series backend: {}", backend)
        return None
    try:
        store = DuckDbParquetTimeSeriesStore(
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir,
            read_only=read_only,
            writer_token=writer_token,
        )
    except Exception as exc:  # noqa: BLE001 - backend初期化失敗を呼び出し側で扱う
        logger.warning("DuckDB backend is unavailable: {}", exc)
        return None
    mode = "read-only" if read_only else "read-write"
    logger.info("Market time-series backend enabled: duckdb-parquet ({})", mode)
    return store
