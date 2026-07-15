"""Market time-series storage (DuckDB + Parquet SoT)."""

from __future__ import annotations

import shutil
from time import perf_counter
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, cast

import pandas as pd
from loguru import logger

from src.infrastructure.db.market.duckdb_connection import (
    connect_market_duckdb,
    parse_duckdb_size_bytes,
    resolve_directory_size,
)
from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
    deterministic_last_wins,
)


class MarketTimeSeriesStore(Protocol):  # pragma: no cover
    """時系列 publish/index インターフェース。"""

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_stock_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_stock_minute_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_indices_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_margin_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult: ...

    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...
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
            "adjustment_factor",
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
        "earnings_per_share",
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
            "disclosed_date",
            *_STATEMENT_UPDATABLE_COLUMNS,
        ),
        conflict_columns=("code", "disclosed_date"),
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
    _STOCK_PROJECTION_TARGET_KEYS_RELATION = "__tmp_stock_projection_target_keys"
    _STOCK_PROJECTION_TARGET_CODES_RELATION = "__tmp_stock_projection_target_codes"
    _STOCK_DIRECT_PROJECTION_RELATION = "__tmp_stock_direct_projection_rows"
    _STOCK_ADJUSTMENT_PROBE_RELATION = "__tmp_stock_adjustment_probe"
    _STOCK_PROJECTION_DESIRED_KEYS_RELATION = "__tmp_stock_projection_desired_keys"
    _STOCK_PROJECTION_STALE_KEYS_RELATION = "__tmp_stock_projection_stale_keys"
    _STOCK_DATA_STAGE_TABLE = "__tmp_stock_data_stage"

    def __init__(
        self,
        *,
        duckdb_path: str,
        parquet_dir: str,
        read_only: bool = False,
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
                connect_market_duckdb(self._duckdb_path, read_only=read_only),
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
        self._stock_projection_full_rebuild_codes: set[str] = set()
        if not read_only:
            self._ensure_schema()
            self._cleanup_invalid_topix_rows_on_startup()

    def _assert_writable(self) -> None:
        if getattr(self, "_read_only", False):
            raise PermissionError("market time-series store is read-only")

    def _ensure_schema(self) -> None:
        with self._lock:
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
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    adjustment_factor DOUBLE,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_data (
                    code TEXT,
                    date TEXT,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
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
                    code TEXT,
                    disclosed_date TEXT,
                    earnings_per_share DOUBLE,
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
                    PRIMARY KEY (code, disclosed_date)
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
        result = self._apply_semantic_delta(valid_rows, spec=self._TOPIX_DATA_UPSERT_SPEC)
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
            affected_dates=result.affected_dates | frozenset(key[0] for key in deleted_keys),
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
            values = (row.get("open"), row.get("high"), row.get("low"), row.get("close"))
            invalid = (
                previous_close is not None
                and all(value is not None for value in values)
                and values[0] == values[1] == values[2] == values[3] == previous_close
            )
            if invalid:
                invalid_dates.add(date_value)
            previous_close = row.get("close")
        return [
            row
            for row in deduplicated
            if str(row.get("date")) not in invalid_dates
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

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            return self._publish_stock_data_locked(rows)

    def _publish_stock_data_locked(
        self, rows: list[dict[str, Any]]
    ) -> SemanticDeltaResult:
        if not rows:
            return SemanticDeltaResult.empty()

        staged_rows = deterministic_last_wins(
            rows, key_columns=self._STOCK_DATA_RAW_UPSERT_SPEC.conflict_columns
        )
        old_adjustment_factors = self._load_existing_adjustment_factors(staged_rows)

        result = self._publish_and_mark_delta(rows, spec=self._STOCK_DATA_RAW_UPSERT_SPEC)
        if not result.mutated_rows:
            return result

        mutated_keys = set(result.mutated_keys)
        mutated_rows = [
            row for row in staged_rows
            if (row.get("code"), row.get("date")) in mutated_keys
        ]

        rebuild_codes = {
            str(row.get("code"))
            for row in mutated_rows
            if row.get("code") and (
                self._requires_full_stock_reprojection(row.get("adjustment_factor"))
                or (
                    (row.get("code"), row.get("date")) in old_adjustment_factors
                    and self._normalized_adjustment_factor(
                        old_adjustment_factors[(row.get("code"), row.get("date"))]
                    )
                    != self._normalized_adjustment_factor(row.get("adjustment_factor"))
                )
            )
        }
        self._stock_projection_full_rebuild_codes.update(rebuild_codes)

        point_projection_rows = [
            row
            for row in mutated_rows
            if row.get("code")
            and str(row.get("code")) not in self._stock_projection_full_rebuild_codes
        ]
        direct_projection_rows, window_projection_rows = (
            self._partition_direct_stock_projection_rows(point_projection_rows)
        )
        if direct_projection_rows:
            self._direct_project_stock_rows(direct_projection_rows)
        if window_projection_rows:
            self._project_stock_rows(window_projection_rows)

        return result

    def _load_existing_adjustment_factors(
        self, rows: list[dict[str, Any]]
    ) -> dict[tuple[Any, Any], Any]:
        keys = pd.DataFrame.from_records(
            ({"code": row.get("code"), "date": row.get("date")} for row in rows),
            columns=("code", "date"),
        )
        self._conn.register(self._STOCK_ADJUSTMENT_PROBE_RELATION, keys)
        try:
            return {
                (row[0], row[1]): row[2]
                for row in self._conn.execute(
                    f"""
                    SELECT raw.code, raw.date, raw.adjustment_factor
                    FROM stock_data_raw raw
                    INNER JOIN {self._STOCK_ADJUSTMENT_PROBE_RELATION} staged
                      ON staged.code = raw.code AND staged.date = raw.date
                    """
                ).fetchall()
            }
        finally:
            self._conn.unregister(self._STOCK_ADJUSTMENT_PROBE_RELATION)

    def stage_stock_data_rows(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        if not rows:
            return SemanticDeltaResult.empty()
        dataframe = pd.DataFrame.from_records(
            [
                {column: row.get(column) for column in self._STOCK_DATA_RAW_UPSERT_SPEC.columns}
                for row in rows
            ],
            columns=self._STOCK_DATA_RAW_UPSERT_SPEC.columns,
        )
        with self._lock:
            self._ensure_stock_data_stage_table()
            self._conn.register(self._STOCK_DATA_RAW_UPSERT_SPEC.relation_name, dataframe)
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

    def flush_staged_stock_data(self) -> SemanticDeltaResult:
        self._assert_writable()
        with self._lock:
            self._ensure_stock_data_stage_table()
            count_row = self._conn.execute(
                f"SELECT COUNT(*) FROM {self._STOCK_DATA_STAGE_TABLE}"
            ).fetchone()
            staged_count = int(count_row[0] or 0) if count_row else 0
            if staged_count <= 0:
                return SemanticDeltaResult.empty()
            staged_rows = [
                dict(zip(self._STOCK_DATA_RAW_UPSERT_SPEC.columns, row, strict=True))
                for row in self._conn.execute(
                    f"SELECT {', '.join(self._STOCK_DATA_RAW_UPSERT_SPEC.columns)} "
                    f"FROM {self._STOCK_DATA_STAGE_TABLE} ORDER BY rowid"
                ).fetchall()
            ]
            self._conn.execute(f"DELETE FROM {self._STOCK_DATA_STAGE_TABLE}")
            return self._publish_stock_data_locked(staged_rows)

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

    def publish_stock_minute_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
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

    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._OPTIONS_225_UPSERT_SPEC)

    def publish_margin_data(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._MARGIN_DATA_UPSERT_SPEC)

    def publish_statements(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        self._assert_writable()
        return self._publish_and_mark_delta(rows, spec=self._STATEMENTS_UPSERT_SPEC)

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
                    self._stock_projection_full_rebuild_codes
                    or "stock_data_raw" in self._dirty_tables
                    or "stock_data" in self._dirty_tables
                )
            return table_name in self._dirty_tables

    def index_stock_data(self) -> None:
        self._assert_writable()
        self._reproject_pending_stock_codes()
        self._export_if_dirty("stock_data_raw")
        self._export_if_dirty("stock_data")

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
            shutil.rmtree(partition_dir, ignore_errors=True)
            count_row = self._conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE date = ?",
                [date_value],
            ).fetchone()
            row_count = int(count_row[0] or 0) if count_row else 0
            if row_count <= 0:
                continue
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = partition_dir / "data.parquet"
            escaped_path = str(output_path).replace("'", "''")
            self._conn.execute(
                f"""
                COPY (
                    SELECT *
                    FROM {table_name}
                    WHERE date = ?
                ) TO '{escaped_path}' (FORMAT PARQUET)
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

        target_dates = sorted(
            self._dirty_stock_minute_dates
        )

        for date_value in target_dates:
            partition_dir = output_root / f"date={date_value}"
            shutil.rmtree(partition_dir, ignore_errors=True)

            count_row = self._conn.execute(
                "SELECT COUNT(*) FROM stock_data_minute_raw WHERE date = ?",
                [date_value],
            ).fetchone()
            if not count_row or int(count_row[0] or 0) <= 0:
                continue

            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = partition_dir / "data.parquet"
            escaped_path = str(output_path).replace("'", "''")
            escaped_date = date_value.replace("'", "''")
            self._conn.execute(
                f"""
                COPY (
                    SELECT *
                    FROM stock_data_minute_raw
                    WHERE date = '{escaped_date}'
                    ORDER BY code, time
                ) TO '{escaped_path}' (FORMAT PARQUET)
                """
            )

        self._dirty_stock_minute_dates.clear()
        self._dirty_tables.discard("stock_data_minute_raw")

    @staticmethod
    def _requires_full_stock_reprojection(adjustment_factor: Any) -> bool:
        if adjustment_factor is None:
            return False
        try:
            return float(adjustment_factor) != 1.0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _normalized_adjustment_factor(adjustment_factor: Any) -> float:
        try:
            value = float(adjustment_factor)
        except (TypeError, ValueError):
            return 1.0
        return value if value > 0 else 1.0

    @staticmethod
    def _can_project_stock_row_directly(row: dict[str, Any]) -> bool:
        adjustment_factor = row.get("adjustment_factor")
        if adjustment_factor is None:
            return True
        try:
            return float(adjustment_factor) <= 0 or float(adjustment_factor) == 1.0
        except (TypeError, ValueError):
            return True

    def _partition_direct_stock_projection_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidate_rows = [
            row for row in rows
            if row.get("code")
            and row.get("date")
            and self._can_project_stock_row_directly(row)
        ]
        if not candidate_rows:
            return [], rows

        dataframe = pd.DataFrame.from_records(
            [
                {
                    "code": str(row["code"]),
                    "date": str(row["date"]),
                }
                for row in candidate_rows
            ],
            columns=("code", "date"),
        )
        with self._lock:
            self._conn.register(self._STOCK_DIRECT_PROJECTION_RELATION, dataframe)
            try:
                direct_keys = {
                    (str(row[0]), str(row[1]))
                    for row in self._conn.execute(
                        f"""
                        SELECT candidate.code, candidate.date
                        FROM {self._STOCK_DIRECT_PROJECTION_RELATION} candidate
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM stock_data_raw future
                            WHERE future.code = candidate.code
                              AND future.date > candidate.date
                              AND future.adjustment_factor IS NOT NULL
                              AND future.adjustment_factor > 0
                              AND future.adjustment_factor != 1.0
                        )
                        """
                    ).fetchall()
                }
            finally:
                self._conn.unregister(self._STOCK_DIRECT_PROJECTION_RELATION)

        direct_rows: list[dict[str, Any]] = []
        window_rows: list[dict[str, Any]] = []
        for row in rows:
            key = (str(row.get("code")), str(row.get("date")))
            if key in direct_keys:
                direct_rows.append(row)
            else:
                window_rows.append(row)
        return direct_rows, window_rows

    def _direct_project_stock_rows(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        if not rows:
            return SemanticDeltaResult.empty()
        return self._publish_and_mark_delta(rows, spec=self._STOCK_DATA_UPSERT_SPEC)

    def _stock_projection_sql(
        self,
        *,
        target_codes_relation: str | None = None,
        target_keys_relation: str | None = None,
    ) -> str:
        raw_filters: list[str] = []
        if target_codes_relation is not None:
            raw_filters.append(
                f"code IN (SELECT code FROM {target_codes_relation})"
            )

        target_join = ""
        if target_keys_relation is not None:
            target_join = (
                f"INNER JOIN {target_keys_relation} target_keys "
                "ON target_keys.code = projected.code "
                "AND target_keys.date = projected.date"
            )

        raw_where = ""
        if raw_filters:
            raw_where = "WHERE " + " AND ".join(raw_filters)

        return f"""
            WITH normalized_raw AS (
                SELECT
                    code,
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    CASE
                        WHEN adjustment_factor IS NULL OR adjustment_factor <= 0 THEN 1.0
                        ELSE adjustment_factor
                    END AS normalized_adjustment_factor,
                    adjustment_factor,
                    created_at
                FROM stock_data_raw
                {raw_where}
            ),
            projected AS (
                SELECT
                    code,
                    date,
                    open * future_factor AS open,
                    high * future_factor AS high,
                    low * future_factor AS low,
                    close * future_factor AS close,
                    CAST(ROUND(volume / future_factor) AS BIGINT) AS volume,
                    adjustment_factor,
                    created_at
                FROM (
                    SELECT
                        code,
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        adjustment_factor,
                        created_at,
                        COALESCE(
                            EXP(
                                SUM(LN(normalized_adjustment_factor)) OVER (
                                    PARTITION BY code
                                    ORDER BY date DESC
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                                )
                            ),
                            1.0
                        ) AS future_factor
                    FROM normalized_raw
                ) projected_source
            )
            SELECT
                projected.code,
                projected.date,
                projected.open,
                projected.high,
                projected.low,
                projected.close,
                projected.volume,
                projected.adjustment_factor,
                projected.created_at
            FROM projected
            {target_join}
        """

    def _project_stock_rows(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        key_rows = [
            {
                "code": str(row["code"]),
                "date": str(row["date"]),
            }
            for row in rows
            if row.get("code") and row.get("date")
        ]
        if not key_rows:
            return SemanticDeltaResult.empty()

        codes = sorted({row["code"] for row in key_rows})
        keys_df = pd.DataFrame.from_records(key_rows, columns=("code", "date"))
        codes_df = pd.DataFrame.from_records(
            [{"code": code} for code in codes],
            columns=("code",),
        )
        with self._lock:
            self._conn.register(self._STOCK_PROJECTION_TARGET_KEYS_RELATION, keys_df)
            self._conn.register(self._STOCK_PROJECTION_TARGET_CODES_RELATION, codes_df)
            try:
                projected_rows = [
                    dict(zip(self._STOCK_DATA_UPSERT_SPEC.columns, row, strict=True))
                    for row in self._conn.execute(
                        self._stock_projection_sql(
                        target_codes_relation=self._STOCK_PROJECTION_TARGET_CODES_RELATION,
                        target_keys_relation=self._STOCK_PROJECTION_TARGET_KEYS_RELATION,
                        )
                    ).fetchall()
                ]
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_KEYS_RELATION)
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_CODES_RELATION)
        return self._publish_and_mark_delta(projected_rows, spec=self._STOCK_DATA_UPSERT_SPEC)

    def _reproject_pending_stock_codes(self) -> SemanticDeltaResult:
        codes = sorted(self._stock_projection_full_rebuild_codes)
        if not codes:
            return SemanticDeltaResult.empty()

        code_df = pd.DataFrame.from_records(
            [{"code": code} for code in codes],
            columns=("code",),
        )
        with self._lock:
            self._conn.register(self._STOCK_PROJECTION_TARGET_CODES_RELATION, code_df)
            try:
                projected_rows = [
                    dict(zip(self._STOCK_DATA_UPSERT_SPEC.columns, row, strict=True))
                    for row in self._conn.execute(
                        self._stock_projection_sql(
                            target_codes_relation=self._STOCK_PROJECTION_TARGET_CODES_RELATION
                        )
                    ).fetchall()
                ]
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_CODES_RELATION)
        upsert_result = self._publish_and_mark_delta(
            projected_rows, spec=self._STOCK_DATA_UPSERT_SPEC
        )
        delete_result = self._delete_stale_stock_projection_rows(
            desired_rows=projected_rows,
            codes=codes,
        )
        self._stock_projection_full_rebuild_codes.clear()
        return SemanticDeltaResult(
            stats=MarketMutationStats(
                input=upsert_result.stats.input,
                inserted=upsert_result.stats.inserted,
                updated=upsert_result.stats.updated,
                unchanged=upsert_result.stats.unchanged,
                deleted=delete_result.stats.deleted,
            ),
            inserted_keys=upsert_result.inserted_keys,
            updated_keys=upsert_result.updated_keys,
            deleted_keys=delete_result.deleted_keys,
            affected_dates=upsert_result.affected_dates | delete_result.affected_dates,
            affected_codes=upsert_result.affected_codes | delete_result.affected_codes,
        )

    def _delete_stale_stock_projection_rows(
        self,
        *,
        desired_rows: list[dict[str, Any]],
        codes: list[str],
    ) -> SemanticDeltaResult:
        desired_keys = pd.DataFrame.from_records(
            (
                {"code": row.get("code"), "date": row.get("date")}
                for row in desired_rows
            ),
            columns=("code", "date"),
        )
        code_scope = pd.DataFrame.from_records(
            ({"code": code} for code in codes), columns=("code",)
        )
        with self._lock:
            self._conn.register(self._STOCK_PROJECTION_DESIRED_KEYS_RELATION, desired_keys)
            self._conn.register(self._STOCK_PROJECTION_TARGET_CODES_RELATION, code_scope)
            try:
                stale_keys = tuple(
                    (str(row[0]), str(row[1]))
                    for row in self._conn.execute(
                        f"""
                        SELECT target.code, target.date
                        FROM stock_data target
                        INNER JOIN {self._STOCK_PROJECTION_TARGET_CODES_RELATION} scope
                          ON scope.code = target.code
                        LEFT JOIN {self._STOCK_PROJECTION_DESIRED_KEYS_RELATION} desired
                          ON desired.code = target.code AND desired.date = target.date
                        WHERE desired.code IS NULL
                        ORDER BY target.code, target.date
                        """
                    ).fetchall()
                )
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_DESIRED_KEYS_RELATION)
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_CODES_RELATION)
            if not stale_keys:
                return SemanticDeltaResult.empty()
            stale_frame = pd.DataFrame.from_records(
                ({"code": key[0], "date": key[1]} for key in stale_keys),
                columns=("code", "date"),
            )
            self._conn.register(self._STOCK_PROJECTION_STALE_KEYS_RELATION, stale_frame)
            try:
                self._conn.execute(
                    f"""
                    DELETE FROM stock_data target
                    WHERE EXISTS (
                        SELECT 1
                        FROM {self._STOCK_PROJECTION_STALE_KEYS_RELATION} stale
                        WHERE stale.code = target.code AND stale.date = target.date
                    )
                    """
                )
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_STALE_KEYS_RELATION)
            affected_dates = frozenset(key[1] for key in stale_keys)
            self._dirty_tables.add("stock_data")
            self._dirty_partition_dates.setdefault("stock_data", set()).update(
                affected_dates
            )
            return SemanticDeltaResult(
                stats=MarketMutationStats(
                    input=0,
                    inserted=0,
                    updated=0,
                    unchanged=0,
                    deleted=len(stale_keys),
                ),
                deleted_keys=stale_keys,
                affected_dates=affected_dates,
                affected_codes=frozenset(key[0] for key in stale_keys),
            )

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
            [{column: row.get(column) for column in spec.columns} for row in staged_rows],
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
                        {', '.join(f'staged.{column}' for column in spec.conflict_columns)},
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
                    tuple(row[:key_width]) for row in classified if row[-1] == "inserted"
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
            (index for index, column in enumerate(spec.conflict_columns) if column in {"date", "disclosed_date"}),
            None,
        )
        code_index = next(
            (index for index, column in enumerate(spec.conflict_columns) if column == "code"),
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
            affected_dates=frozenset(
                str(key[date_index]) for key in mutated_keys
            ) if date_index is not None else frozenset(),
            affected_codes=frozenset(
                str(key[code_index]) for key in mutated_keys
            ) if code_index is not None else frozenset(),
        )

    def get_storage_stats(self) -> TimeSeriesStorageStats:
        with self._lock:
            stale_artifact_count, stale_artifacts = self._resolve_stale_storage_artifacts()
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

    def _resolve_stale_storage_artifacts(self, limit: int = 20) -> tuple[int, list[str]]:
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
                self._reproject_pending_stock_codes()
                for table_name in list(self._dirty_tables):
                    self._export_if_dirty(table_name)
            self._conn.close()


def create_time_series_store(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = False,
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
        )
    except Exception as exc:  # noqa: BLE001 - backend初期化失敗を呼び出し側で扱う
        logger.warning("DuckDB backend is unavailable: {}", exc)
        return None
    mode = "read-only" if read_only else "read-write"
    logger.info("Market time-series backend enabled: duckdb-parquet ({})", mode)
    return store
