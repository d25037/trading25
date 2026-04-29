"""Market time-series storage (DuckDB + Parquet SoT)."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Protocol, cast

import pandas as pd
from loguru import logger


class MarketTimeSeriesStore(Protocol):  # pragma: no cover
    """時系列 publish/index インターフェース。"""

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_stock_minute_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> int: ...

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

    _STOCK_DATA_RELATION_INSERT_THRESHOLD = 1000
    _STOCK_MINUTE_DATA_RELATION_INSERT_THRESHOLD = 1000
    _INDICES_DATA_RELATION_INSERT_THRESHOLD = 1000
    _OPTIONS_225_RELATION_INSERT_THRESHOLD = 1000
    _MARGIN_DATA_RELATION_INSERT_THRESHOLD = 1000
    _STATEMENTS_RELATION_INSERT_THRESHOLD = 1000

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

    _STATEMENT_UPDATABLE_COLUMNS = (
        "earnings_per_share",
        "profit",
        "equity",
        "type_of_current_period",
        "type_of_document",
        "next_year_forecast_earnings_per_share",
        "bps",
        "sales",
        "operating_profit",
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
            duckdb = __import__("duckdb")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDB backend requested but `duckdb` package is not installed. "
                "Install duckdb and retry."
            ) from exc

        self._conn = cast(Any, duckdb).connect(str(self._duckdb_path), read_only=read_only)
        # app state で共有されるため、sync 書き込みと stats/validate 読み取りを直列化する。
        self._lock = RLock()
        self._dirty_tables: set[str] = set()
        self._dirty_stock_minute_dates: set[str] = set()
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
                    operating_profit DOUBLE,
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

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        if not rows:
            return 0
        values = [
            (
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("created_at"),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO topix_data (date, open, high, low, close, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    created_at = excluded.created_at
                """,
                values,
            )
            removed_count = self._remove_invalid_topix_rows()
            if removed_count > 0:
                logger.warning(
                    "Removed {} invalid TOPIX rows (flat OHLC equal to previous close)",
                    removed_count,
                )
            self._dirty_tables.add("topix_data")
        return len(rows)

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

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        if not rows:
            return 0

        if len(rows) >= self._STOCK_DATA_RELATION_INSERT_THRESHOLD:
            published = self._publish_stock_data_via_relation(rows)
        else:
            published = self._publish_stock_data_via_executemany(rows)

        rebuild_codes = {
            str(row.get("code"))
            for row in rows
            if row.get("code")
            and self._requires_full_stock_reprojection(row.get("adjustment_factor"))
        }
        self._stock_projection_full_rebuild_codes.update(rebuild_codes)

        point_projection_rows = [
            row
            for row in rows
            if row.get("code")
            and str(row.get("code")) not in self._stock_projection_full_rebuild_codes
        ]
        if point_projection_rows:
            self._project_stock_rows(point_projection_rows)

        return published

    def publish_stock_minute_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        published = self._publish_rows_with_upsert_spec(
            rows,
            spec=self._STOCK_MINUTE_DATA_UPSERT_SPEC,
            relation_insert_threshold=self._STOCK_MINUTE_DATA_RELATION_INSERT_THRESHOLD,
            relation_publisher=self._publish_stock_minute_data_via_relation,
        )
        if published <= 0:
            return 0

        with self._lock:
            self._dirty_stock_minute_dates.update(
                str(row.get("date"))
                for row in rows
                if row.get("date")
            )
        return published

    def _publish_stock_minute_data_via_relation(
        self,
        rows: list[dict[str, Any]],
    ) -> int:
        return self._publish_rows_via_relation(
            rows,
            spec=self._STOCK_MINUTE_DATA_UPSERT_SPEC,
        )

    def _publish_stock_data_via_relation(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_rows_via_relation(
            rows,
            spec=self._STOCK_DATA_RAW_UPSERT_SPEC,
        )

    def _publish_stock_data_via_executemany(self, rows: list[dict[str, Any]]) -> int:
        values = self._build_upsert_values(
            rows,
            columns=self._STOCK_DATA_RAW_UPSERT_SPEC.columns,
        )
        with self._lock:
            self._conn.executemany(
                self._build_executemany_upsert_sql(self._STOCK_DATA_RAW_UPSERT_SPEC),
                values,
            )
            self._dirty_tables.add("stock_data_raw")
        return len(rows)

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        return self._publish_rows_with_upsert_spec(
            rows,
            spec=self._INDICES_DATA_UPSERT_SPEC,
            relation_insert_threshold=self._INDICES_DATA_RELATION_INSERT_THRESHOLD,
            relation_publisher=self._publish_indices_data_via_relation,
        )

    def _publish_indices_data_via_relation(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_rows_via_relation(
            rows, spec=self._INDICES_DATA_UPSERT_SPEC
        )

    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        return self._publish_rows_with_upsert_spec(
            rows,
            spec=self._OPTIONS_225_UPSERT_SPEC,
            relation_insert_threshold=self._OPTIONS_225_RELATION_INSERT_THRESHOLD,
            relation_publisher=self._publish_options_225_data_via_relation,
        )

    def _publish_options_225_data_via_relation(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_rows_via_relation(rows, spec=self._OPTIONS_225_UPSERT_SPEC)

    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        return self._publish_rows_with_upsert_spec(
            rows,
            spec=self._MARGIN_DATA_UPSERT_SPEC,
            relation_insert_threshold=self._MARGIN_DATA_RELATION_INSERT_THRESHOLD,
            relation_publisher=self._publish_margin_data_via_relation,
        )

    def _publish_margin_data_via_relation(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_rows_via_relation(rows, spec=self._MARGIN_DATA_UPSERT_SPEC)

    def publish_statements(self, rows: list[dict[str, Any]]) -> int:
        self._assert_writable()
        return self._publish_rows_with_upsert_spec(
            rows,
            spec=self._STATEMENTS_UPSERT_SPEC,
            relation_insert_threshold=self._STATEMENTS_RELATION_INSERT_THRESHOLD,
            relation_publisher=self._publish_statements_via_relation,
        )

    def _publish_statements_via_relation(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_rows_via_relation(rows, spec=self._STATEMENTS_UPSERT_SPEC)

    def index_topix_data(self) -> None:
        self._assert_writable()
        self._export_if_dirty("topix_data")

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
            spec = self._TABLE_SPECS[table_name]
            output_path = self._parquet_dir / spec.parquet_name
            if output_path.exists():
                output_path.unlink()

            escaped = str(output_path).replace("'", "''")
            if spec.order_by:
                source_sql = (
                    f"(SELECT * FROM {spec.table_name} ORDER BY {spec.order_by})"
                )
            else:
                source_sql = spec.table_name
            self._conn.execute(f"COPY {source_sql} TO '{escaped}' (FORMAT PARQUET)")
            self._dirty_tables.discard(table_name)

    def _export_stock_minute_partitions(self) -> None:
        output_root = self._parquet_dir / "stock_data_minute_raw"
        output_root.mkdir(parents=True, exist_ok=True)

        target_dates = sorted(
            self._dirty_stock_minute_dates or self._load_existing_stock_minute_dates()
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

    def _load_existing_stock_minute_dates(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT DISTINCT date FROM stock_data_minute_raw ORDER BY date"
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0]]

    @staticmethod
    def _requires_full_stock_reprojection(adjustment_factor: Any) -> bool:
        if adjustment_factor is None:
            return False
        try:
            return float(adjustment_factor) != 1.0
        except (TypeError, ValueError):
            return False

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

    def _project_stock_rows(self, rows: list[dict[str, Any]]) -> None:
        key_rows = [
            {
                "code": str(row["code"]),
                "date": str(row["date"]),
            }
            for row in rows
            if row.get("code") and row.get("date")
        ]
        if not key_rows:
            return

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
                columns_sql = ", ".join(self._STOCK_DATA_UPSERT_SPEC.columns)
                conflict_sql = ", ".join(self._STOCK_DATA_UPSERT_SPEC.conflict_columns)
                update_clause = self._build_upsert_update_clause(
                    self._STOCK_DATA_UPSERT_SPEC
                )
                self._conn.execute(
                    f"""
                    INSERT INTO stock_data ({columns_sql})
                    {self._stock_projection_sql(
                        target_codes_relation=self._STOCK_PROJECTION_TARGET_CODES_RELATION,
                        target_keys_relation=self._STOCK_PROJECTION_TARGET_KEYS_RELATION,
                    )}
                    ON CONFLICT ({conflict_sql}) DO UPDATE
                    SET {update_clause}
                    """
                )
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_KEYS_RELATION)
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_CODES_RELATION)
            self._dirty_tables.add("stock_data")

    def _reproject_pending_stock_codes(self) -> None:
        codes = sorted(self._stock_projection_full_rebuild_codes)
        if not codes:
            return

        code_df = pd.DataFrame.from_records(
            [{"code": code} for code in codes],
            columns=("code",),
        )
        with self._lock:
            self._conn.register(self._STOCK_PROJECTION_TARGET_CODES_RELATION, code_df)
            try:
                self._conn.execute(
                    f"""
                    DELETE FROM stock_data
                    WHERE code IN (
                        SELECT code
                        FROM {self._STOCK_PROJECTION_TARGET_CODES_RELATION}
                    )
                    """
                )
                self._conn.execute(
                    f"""
                    INSERT INTO stock_data (
                        code,
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        adjustment_factor,
                        created_at
                    )
                    {self._stock_projection_sql(
                        target_codes_relation=self._STOCK_PROJECTION_TARGET_CODES_RELATION
                    )}
                    """
                )
            finally:
                self._conn.unregister(self._STOCK_PROJECTION_TARGET_CODES_RELATION)
            self._dirty_tables.add("stock_data")
            self._stock_projection_full_rebuild_codes.clear()

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
    def _build_executemany_upsert_sql(cls, spec: _RelationUpsertSpec) -> str:
        columns_sql = ", ".join(spec.columns)
        placeholders = ", ".join("?" for _ in spec.columns)
        conflict_sql = ", ".join(spec.conflict_columns)
        update_clause = cls._build_upsert_update_clause(spec)
        return (
            f"INSERT INTO {spec.table_name} ({columns_sql}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_clause}"
        )

    @classmethod
    def _build_relation_upsert_sql(cls, spec: _RelationUpsertSpec) -> str:
        columns_sql = ", ".join(spec.columns)
        conflict_sql = ", ".join(spec.conflict_columns)
        update_clause = cls._build_upsert_update_clause(spec)
        return (
            f"INSERT INTO {spec.table_name} ({columns_sql}) "
            f"SELECT {columns_sql} FROM {spec.relation_name} "
            f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_clause}"
        )

    @staticmethod
    def _build_upsert_values(
        rows: list[dict[str, Any]],
        *,
        columns: tuple[str, ...],
    ) -> list[tuple[Any, ...]]:
        return [tuple(row.get(column) for column in columns) for row in rows]

    def _publish_rows_with_upsert_spec(
        self,
        rows: list[dict[str, Any]],
        *,
        spec: _RelationUpsertSpec,
        relation_insert_threshold: int,
        relation_publisher: Callable[[list[dict[str, Any]]], int],
    ) -> int:
        if not rows:
            return 0
        if len(rows) >= relation_insert_threshold:
            return relation_publisher(rows)
        values = self._build_upsert_values(rows, columns=spec.columns)
        with self._lock:
            self._conn.executemany(self._build_executemany_upsert_sql(spec), values)
            self._dirty_tables.add(spec.table_name)
        return len(rows)

    def _publish_rows_via_relation(
        self,
        rows: list[dict[str, Any]],
        *,
        spec: _RelationUpsertSpec,
    ) -> int:
        dataframe = pd.DataFrame.from_records(
            [{column: row.get(column) for column in spec.columns} for row in rows],
            columns=spec.columns,
        )
        with self._lock:
            self._conn.register(spec.relation_name, dataframe)
            try:
                self._conn.execute(self._build_relation_upsert_sql(spec))
            finally:
                self._conn.unregister(spec.relation_name)
            self._dirty_tables.add(spec.table_name)
        return len(rows)

    def get_storage_stats(self) -> TimeSeriesStorageStats:
        with self._lock:
            return TimeSeriesStorageStats(
                duckdb_bytes=self._resolve_path_size(self._duckdb_path),
                parquet_bytes=self._resolve_parquet_dir_size(),
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
