"""DuckDB-only dataset snapshot writer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import importlib
from pathlib import Path
import shutil
import tempfile
from threading import RLock
from typing import Any, cast

from src.infrastructure.db.dataset_io.snapshot_contract import (
    DATASET_V3_PARQUET_EXPORTS,
    EVENT_TIME_PIT_DATE_TO_INFO_KEY,
    MARKET_V4_EVENT_TIME_REQUIRED_TABLES,
)
from src.infrastructure.db.dataset_io.pit_validation import (
    find_dataset_pit_audit_error,
)

_SOURCE_ALIAS = "market_source"
_TEMP_STOCK_CODE_TABLE = "_dataset_copy_target_stock_codes"
_TEMP_INDEX_CODE_TABLE = "_dataset_copy_target_index_codes"
_TEMP_STOCK_DATA_TABLE = "_dataset_copy_stock_data"
_TEMP_STATEMENTS_TABLE = "_dataset_copy_statements"
_TEMP_MARGIN_TABLE = "_dataset_copy_margin"
_PIT_STAGE_TABLES: tuple[tuple[str, str], ...] = (
    ("_dataset_pit_stock_data_raw", "stock_data_raw"),
    ("_dataset_pit_stock_master_daily", "stock_master_daily"),
    ("_dataset_pit_bases", "stock_adjustment_bases"),
    ("_dataset_pit_segments", "stock_adjustment_basis_segments"),
    ("_dataset_pit_statement_metrics", "statement_metrics_adjusted"),
    ("_dataset_pit_daily_valuation", "daily_valuation"),
)
_PIT_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "stock_data_raw": ("code", "date"),
    "stock_master_daily": ("date", "code"),
    "stock_adjustment_bases": ("code", "basis_id"),
    "stock_adjustment_basis_segments": ("code", "basis_id", "source_date_from"),
    "statement_metrics_adjusted": (
        "code",
        "disclosed_date",
        "period_end",
        "period_type",
        "basis_version",
    ),
    "daily_valuation": ("code", "date", "basis_version"),
}


@dataclass(frozen=True)
class StockDataCopyCodeStats:
    total_rows: int = 0
    valid_rows: int = 0
    skipped_rows: int = 0


@dataclass(frozen=True)
class StockDataCopyResult:
    inserted_rows: int
    code_stats: dict[str, StockDataCopyCodeStats]


class DatasetSnapshotError(RuntimeError):
    """The source cannot produce a complete dataset v3 PIT snapshot."""


@dataclass(frozen=True)
class EventTimePitCopyResult:
    raw_price_rows: int
    stock_master_rows: int
    basis_rows: int
    segment_rows: int
    statement_metric_rows: int
    daily_valuation_rows: int


def snapshot_dir_for_path(path: str) -> Path:
    """Resolve the dataset snapshot directory."""
    source = Path(path)
    if source.name == "dataset.duckdb":
        return source.parent
    if source.suffix:
        raise ValueError(
            "DatasetWriter expects a snapshot directory or a dataset.duckdb path"
        )
    return source


def duckdb_path_for_path(path: str) -> Path:
    return snapshot_dir_for_path(path) / "dataset.duckdb"


def parquet_dir_for_path(path: str) -> Path:
    return snapshot_dir_for_path(path) / "parquet"


class _DatasetDuckDbStore:
    _STATEMENT_TEXT_COLUMNS: frozenset[str] = frozenset(
        {"type_of_current_period", "type_of_document"}
    )
    _STATEMENT_COLUMNS: tuple[str, ...] = (
        "code",
        "disclosed_date",
        "earnings_per_share",
        "profit",
        "equity",
        "type_of_current_period",
        "type_of_document",
        "next_year_forecast_earnings_per_share",
        "bps",
        "sales",
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
    _ADJUSTED_STATEMENT_COLUMNS: tuple[str, ...] = (
        "code",
        "disclosed_date",
        "period_end",
        "period_type",
        "price_basis_date",
        "raw_eps",
        "adjusted_eps",
        "raw_bps",
        "adjusted_bps",
        "raw_forecast_eps",
        "adjusted_forecast_eps",
        "raw_dividend_fy",
        "adjusted_dividend_fy",
        "raw_shares_outstanding",
        "adjusted_shares_outstanding",
        "raw_treasury_shares",
        "adjusted_treasury_shares",
        "adjustment_factor_cumulative",
        "basis_version",
        "created_at",
    )
    _DAILY_VALUATION_COLUMNS: tuple[str, ...] = (
        "code",
        "date",
        "price_basis_date",
        "close",
        "eps",
        "bps",
        "forward_eps",
        "per",
        "forward_per",
        "sales",
        "forward_sales",
        "psr",
        "forward_psr",
        "p_op",
        "forward_p_op",
        "pbr",
        "market_cap",
        "free_float_market_cap",
        "statement_disclosed_date",
        "forward_eps_disclosed_date",
        "forward_eps_source",
        "forward_sales_disclosed_date",
        "forward_sales_source",
        "basis_version",
        "created_at",
    )

    def __init__(self, *, duckdb_path: str, parquet_dir: str) -> None:
        self._duckdb_path = Path(duckdb_path)
        self._parquet_dir = Path(parquet_dir)
        self._duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self._parquet_dir.mkdir(parents=True, exist_ok=True)

        duckdb = importlib.import_module("duckdb")
        self._conn = cast(Any, duckdb).connect(str(self._duckdb_path))
        self._lock = RLock()
        self._dirty_tables: set[str] = set()
        self._closed = False
        self._attached_source_path: str | None = None
        self._source_copy_dir: tempfile.TemporaryDirectory[str] | None = None
        self._ensure_schema()

    @staticmethod
    def _escape_sql_string(value: str) -> str:
        return value.replace("'", "''")

    @staticmethod
    def _normalize_stock_code_expr(column: str) -> str:
        return (
            f"CASE WHEN {column} IS NULL THEN '' "
            f"WHEN length({column}) IN (5, 6) AND right({column}, 1) = '0' "
            f"THEN left({column}, length({column}) - 1) "
            f"ELSE {column} END"
        )

    @staticmethod
    def _stock_code_priority_expr(column: str) -> str:
        return (
            f"CASE WHEN {column} IS NOT NULL "
            f"AND length({column}) IN (5, 6) "
            f"AND right({column}, 1) = '0' THEN 1 ELSE 0 END"
        )

    @staticmethod
    def _normalize_index_code_expr(column: str) -> str:
        return (
            f"CASE WHEN {column} IS NULL THEN '' "
            f"WHEN try_cast({column} AS BIGINT) IS NOT NULL AND length({column}) < 4 "
            f"THEN lpad({column}, 4, '0') "
            f"ELSE upper({column}) END"
        )

    def _attach_source_database(self, source_duckdb_path: str) -> str:
        source_path = str(Path(source_duckdb_path).resolve())
        if self._attached_source_path == source_path:
            return _SOURCE_ALIAS
        if self._attached_source_path is not None:
            raise RuntimeError(
                "DatasetWriter already attached to a different source database: "
                f"{self._attached_source_path}"
            )
        escaped_path = self._escape_sql_string(source_path)
        try:
            self._conn.execute(f"ATTACH '{escaped_path}' AS {_SOURCE_ALIAS}")
        except Exception as exc:
            if "Unique file handle conflict" not in str(exc):
                raise
            fallback_path = self._create_temp_source_copy(source_path)
            escaped_fallback_path = self._escape_sql_string(fallback_path)
            self._conn.execute(f"ATTACH '{escaped_fallback_path}' AS {_SOURCE_ALIAS}")
        self._attached_source_path = source_path
        return _SOURCE_ALIAS

    def _get_source_table_columns(self, source_alias: str, table: str) -> set[str]:
        rows = self._conn.execute(
            f"SELECT name FROM pragma_table_info('{source_alias}.{table}')"
        ).fetchall()
        return {str(row[0]) for row in rows}

    def _statement_source_select_expr(
        self,
        column: str,
        source_columns: set[str],
    ) -> str:
        if column in source_columns:
            return column
        sql_type = "VARCHAR" if column in self._STATEMENT_TEXT_COLUMNS else "DOUBLE"
        return f"CAST(NULL AS {sql_type}) AS {column}"

    def _create_temp_source_copy(self, source_path: str) -> str:
        if self._source_copy_dir is None:
            self._source_copy_dir = tempfile.TemporaryDirectory(prefix="dataset-source-copy-")
        target_path = Path(self._source_copy_dir.name) / Path(source_path).name
        shutil.copy2(source_path, target_path)
        return str(target_path)

    def _reset_temp_table(self, table_name: str, ddl: str) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._conn.execute(f"CREATE TEMP TABLE {table_name} ({ddl})")

    def _load_temp_codes(self, table_name: str, codes: list[str]) -> None:
        self._conn.executemany(
            f"INSERT INTO {table_name} (code) VALUES (?)",
            [(code,) for code in codes],
        )

    def _copy_count(self, table_name: str, where_sql: str = "") -> int:
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM {table_name} {where_sql}"
        ).fetchone()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stocks (
                    code TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    company_name_english TEXT,
                    market_code TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    sector_17_code TEXT NOT NULL,
                    sector_17_name TEXT NOT NULL,
                    sector_33_code TEXT NOT NULL,
                    sector_33_name TEXT NOT NULL,
                    scale_category TEXT,
                    listed_date TEXT NOT NULL,
                    created_at TEXT,
                    updated_at TEXT
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
                    adjustment_factor DOUBLE,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_master_daily (
                    date TEXT,
                    code TEXT,
                    company_name TEXT NOT NULL,
                    company_name_english TEXT,
                    market_code TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    sector_17_code TEXT NOT NULL,
                    sector_17_name TEXT NOT NULL,
                    sector_33_code TEXT NOT NULL,
                    sector_33_name TEXT NOT NULL,
                    scale_category TEXT,
                    listed_date TEXT,
                    created_at TEXT,
                    PRIMARY KEY (date, code)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_adjustment_bases (
                    code TEXT,
                    basis_id TEXT,
                    valid_from TEXT NOT NULL,
                    valid_to_exclusive TEXT,
                    adjustment_through_date TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    materialized_through_date TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('building', 'ready', 'invalid')),
                    created_at TEXT,
                    updated_at TEXT,
                    PRIMARY KEY (code, basis_id),
                    UNIQUE (code, valid_from)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_adjustment_basis_segments (
                    code TEXT,
                    basis_id TEXT,
                    source_date_from TEXT,
                    source_date_to_exclusive TEXT,
                    cumulative_factor DOUBLE NOT NULL,
                    PRIMARY KEY (code, basis_id, source_date_from),
                    FOREIGN KEY (code, basis_id)
                        REFERENCES stock_adjustment_bases (code, basis_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS statement_metrics_adjusted (
                    code TEXT,
                    disclosed_date TEXT,
                    period_end TEXT,
                    period_type TEXT,
                    price_basis_date TEXT,
                    raw_eps DOUBLE,
                    adjusted_eps DOUBLE,
                    raw_bps DOUBLE,
                    adjusted_bps DOUBLE,
                    raw_forecast_eps DOUBLE,
                    adjusted_forecast_eps DOUBLE,
                    raw_dividend_fy DOUBLE,
                    adjusted_dividend_fy DOUBLE,
                    raw_shares_outstanding DOUBLE,
                    adjusted_shares_outstanding DOUBLE,
                    raw_treasury_shares DOUBLE,
                    adjusted_treasury_shares DOUBLE,
                    adjustment_factor_cumulative DOUBLE,
                    basis_version TEXT,
                    created_at TEXT,
                    PRIMARY KEY (code, disclosed_date, period_end, period_type, basis_version),
                    FOREIGN KEY (code, basis_version)
                        REFERENCES stock_adjustment_bases (code, basis_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_valuation (
                    code TEXT,
                    date TEXT,
                    price_basis_date TEXT,
                    close DOUBLE,
                    eps DOUBLE,
                    bps DOUBLE,
                    forward_eps DOUBLE,
                    per DOUBLE,
                    forward_per DOUBLE,
                    sales DOUBLE,
                    forward_sales DOUBLE,
                    psr DOUBLE,
                    forward_psr DOUBLE,
                    p_op DOUBLE,
                    forward_p_op DOUBLE,
                    pbr DOUBLE,
                    market_cap DOUBLE,
                    free_float_market_cap DOUBLE,
                    statement_disclosed_date TEXT,
                    forward_eps_disclosed_date TEXT,
                    forward_eps_source TEXT,
                    forward_sales_disclosed_date TEXT,
                    forward_sales_source TEXT,
                    basis_version TEXT,
                    created_at TEXT,
                    PRIMARY KEY (code, date, basis_version),
                    FOREIGN KEY (code, basis_version)
                        REFERENCES stock_adjustment_bases (code, basis_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dataset_info (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT
                )
                """
            )

    def _query_scalar_int(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        row = self._conn.execute(sql, params).fetchone()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    def _query_distinct_values(self, sql: str, params: tuple[Any, ...] = ()) -> set[str]:
        rows = self._conn.execute(sql, params).fetchall()
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        values = [
            (
                row.get("code"),
                row.get("company_name"),
                row.get("company_name_english"),
                row.get("market_code"),
                row.get("market_name"),
                row.get("sector_17_code"),
                row.get("sector_17_name"),
                row.get("sector_33_code"),
                row.get("sector_33_name"),
                row.get("scale_category"),
                row.get("listed_date"),
                row.get("created_at"),
                row.get("updated_at"),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO stocks (
                    code,
                    company_name,
                    company_name_english,
                    market_code,
                    market_name,
                    sector_17_code,
                    sector_17_name,
                    sector_33_code,
                    sector_33_name,
                    scale_category,
                    listed_date,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (code) DO UPDATE
                SET company_name = excluded.company_name,
                    company_name_english = excluded.company_name_english,
                    market_code = excluded.market_code,
                    market_name = excluded.market_name,
                    sector_17_code = excluded.sector_17_code,
                    sector_17_name = excluded.sector_17_name,
                    sector_33_code = excluded.sector_33_code,
                    sector_33_name = excluded.sector_33_name,
                    scale_category = excluded.scale_category,
                    listed_date = excluded.listed_date,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                """,
                values,
            )
            self._dirty_tables.add("stocks")
        return len(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        values = [
            (
                row.get("code"),
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                row.get("adjustment_factor"),
                row.get("created_at"),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO stock_data (
                    code, date, open, high, low, close, volume, adjustment_factor, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (code, date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    adjustment_factor = excluded.adjustment_factor,
                    created_at = excluded.created_at
                """,
                values,
            )
            self._dirty_tables.add("stock_data")
        return len(rows)

    def copy_stock_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> StockDataCopyResult:
        if not normalized_codes:
            return StockDataCopyResult(inserted_rows=0, code_stats={})

        cutoff = self._validated_snapshot_date(date_to, field="date_to")
        if cutoff is None:
            raise DatasetSnapshotError("date_to is required for stock data copy")
        with self._lock:
            self._require_matching_snapshot_cutoff_if_present(cutoff)
            source_alias = self._attach_source_database(source_duckdb_path)
            self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, normalized_codes)
            self._conn.execute(f"DROP TABLE IF EXISTS {_TEMP_STOCK_DATA_TABLE}")

            normalized_code_sql = self._normalize_stock_code_expr("code")
            priority_sql = self._stock_code_priority_expr("code")
            self._conn.execute(
                f"""
                CREATE TEMP TABLE {_TEMP_STOCK_DATA_TABLE} AS
                WITH source_rows AS (
                    SELECT
                        {normalized_code_sql} AS code,
                        date,
                        {priority_sql} AS source_priority,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        adjustment_factor,
                        created_at
                    FROM {source_alias}.stock_data
                    WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                      AND date IS NOT NULL
                      AND date <> ''
                      AND date <= '{cutoff}'
                ),
                merged_rows AS (
                    SELECT
                        code,
                        date,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN open END), MAX(CASE WHEN source_priority = 1 THEN open END)) AS open,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN high END), MAX(CASE WHEN source_priority = 1 THEN high END)) AS high,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN low END), MAX(CASE WHEN source_priority = 1 THEN low END)) AS low,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN close END), MAX(CASE WHEN source_priority = 1 THEN close END)) AS close,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN volume END), MAX(CASE WHEN source_priority = 1 THEN volume END)) AS volume,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN adjustment_factor END), MAX(CASE WHEN source_priority = 1 THEN adjustment_factor END)) AS adjustment_factor,
                        COALESCE(MAX(CASE WHEN source_priority = 0 THEN created_at END), MAX(CASE WHEN source_priority = 1 THEN created_at END)) AS created_at
                    FROM source_rows
                    GROUP BY code, date
                )
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
                    (
                        open IS NOT NULL
                        AND high IS NOT NULL
                        AND low IS NOT NULL
                        AND close IS NOT NULL
                        AND volume IS NOT NULL
                    ) AS is_valid
                FROM merged_rows
                """
            )

            code_stats: dict[str, StockDataCopyCodeStats] = {
                code: StockDataCopyCodeStats() for code in normalized_codes
            }
            for code, total_rows, valid_rows, skipped_rows in self._conn.execute(
                f"""
                SELECT
                    code,
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_rows,
                    SUM(CASE WHEN is_valid THEN 0 ELSE 1 END) AS skipped_rows
                FROM {_TEMP_STOCK_DATA_TABLE}
                GROUP BY code
                """
            ).fetchall():
                code_stats[str(code)] = StockDataCopyCodeStats(
                    total_rows=int(total_rows or 0),
                    valid_rows=int(valid_rows or 0),
                    skipped_rows=int(skipped_rows or 0),
                )

            inserted_rows = self._copy_count(_TEMP_STOCK_DATA_TABLE, "WHERE is_valid")
            fallback_created_at = datetime.now(UTC).isoformat()
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
                SELECT
                    code,
                    date,
                    open,
                    high,
                    low,
                    close,
                    CAST(volume AS BIGINT),
                    adjustment_factor,
                    COALESCE(created_at, ?)
                FROM {_TEMP_STOCK_DATA_TABLE}
                WHERE is_valid
                ON CONFLICT (code, date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    adjustment_factor = excluded.adjustment_factor,
                    created_at = excluded.created_at
                """,
                [fallback_created_at],
            )
            if inserted_rows > 0:
                self._dirty_tables.add("stock_data")
            return StockDataCopyResult(inserted_rows=inserted_rows, code_stats=code_stats)

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
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
            self._dirty_tables.add("topix_data")
        return len(rows)

    def copy_topix_data_from_source(
        self, *, source_duckdb_path: str, date_to: str
    ) -> int:
        cutoff = self._validated_snapshot_date(date_to, field="date_to")
        if cutoff is None:
            raise DatasetSnapshotError("date_to is required for TOPIX copy")
        with self._lock:
            self._require_matching_snapshot_cutoff(cutoff)
            source_alias = self._attach_source_database(source_duckdb_path)
            inserted_rows = self._query_scalar_int(
                f"SELECT COUNT(*) FROM {source_alias}.topix_data "
                f"WHERE date IS NOT NULL AND date <> '' AND date <= '{cutoff}'"
            )
            self._conn.execute(
                f"""
                INSERT INTO topix_data (date, open, high, low, close, created_at)
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    created_at
                FROM {source_alias}.topix_data
                WHERE date IS NOT NULL
                  AND date <> ''
                  AND date <= '{cutoff}'
                ON CONFLICT (date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    created_at = excluded.created_at
                """
            )
            if inserted_rows > 0:
                self._dirty_tables.add("topix_data")
            return inserted_rows

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        values = [
            (
                row.get("code"),
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("sector_name"),
                row.get("created_at"),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO indices_data (
                    code, date, open, high, low, close, sector_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (code, date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    sector_name = excluded.sector_name,
                    created_at = excluded.created_at
                """,
                values,
            )
            self._dirty_tables.add("indices_data")
        return len(rows)

    def copy_indices_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        if not normalized_codes:
            return 0

        cutoff = self._validated_snapshot_date(date_to, field="date_to")
        if cutoff is None:
            raise DatasetSnapshotError("date_to is required for indices copy")
        with self._lock:
            self._require_matching_snapshot_cutoff(cutoff)
            source_alias = self._attach_source_database(source_duckdb_path)
            self._reset_temp_table(_TEMP_INDEX_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_INDEX_CODE_TABLE, normalized_codes)

            normalized_code_sql = self._normalize_index_code_expr("code")
            inserted_rows = self._query_scalar_int(
                f"""
                SELECT COUNT(*)
                FROM {source_alias}.indices_data
                WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_INDEX_CODE_TABLE})
                  AND date IS NOT NULL
                  AND date <> ''
                  AND date <= '{cutoff}'
                """
            )
            self._conn.execute(
                f"""
                INSERT INTO indices_data (
                    code,
                    date,
                    open,
                    high,
                    low,
                    close,
                    sector_name,
                    created_at
                )
                SELECT
                    {normalized_code_sql} AS code,
                    date,
                    open,
                    high,
                    low,
                    close,
                    sector_name,
                    created_at
                FROM {source_alias}.indices_data
                WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_INDEX_CODE_TABLE})
                  AND date IS NOT NULL
                  AND date <> ''
                  AND date <= '{cutoff}'
                ON CONFLICT (code, date) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    sector_name = excluded.sector_name,
                    created_at = excluded.created_at
                """
            )
            if inserted_rows > 0:
                self._dirty_tables.add("indices_data")
            return inserted_rows

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        values = [
            (
                row.get("code"),
                row.get("date"),
                row.get("long_margin_volume"),
                row.get("short_margin_volume"),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO margin_data (code, date, long_margin_volume, short_margin_volume)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (code, date) DO UPDATE
                SET long_margin_volume = excluded.long_margin_volume,
                    short_margin_volume = excluded.short_margin_volume
                """,
                values,
            )
            self._dirty_tables.add("margin_data")
        return len(rows)

    def copy_margin_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        if not normalized_codes:
            return 0

        cutoff = self._validated_snapshot_date(date_to, field="date_to")
        if cutoff is None:
            raise DatasetSnapshotError("date_to is required for margin copy")
        with self._lock:
            self._require_matching_snapshot_cutoff(cutoff)
            source_alias = self._attach_source_database(source_duckdb_path)
            self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, normalized_codes)
            self._conn.execute(f"DROP TABLE IF EXISTS {_TEMP_MARGIN_TABLE}")

            normalized_code_sql = self._normalize_stock_code_expr("code")
            priority_sql = self._stock_code_priority_expr("code")
            self._conn.execute(
                f"""
                CREATE TEMP TABLE {_TEMP_MARGIN_TABLE} AS
                WITH source_rows AS (
                    SELECT
                        {normalized_code_sql} AS code,
                        date,
                        {priority_sql} AS source_priority,
                        long_margin_volume,
                        short_margin_volume
                    FROM {source_alias}.margin_data
                    WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                      AND date IS NOT NULL
                      AND date <> ''
                      AND date <= '{cutoff}'
                )
                SELECT
                    code,
                    date,
                    COALESCE(MAX(CASE WHEN source_priority = 0 THEN long_margin_volume END), MAX(CASE WHEN source_priority = 1 THEN long_margin_volume END)) AS long_margin_volume,
                    COALESCE(MAX(CASE WHEN source_priority = 0 THEN short_margin_volume END), MAX(CASE WHEN source_priority = 1 THEN short_margin_volume END)) AS short_margin_volume
                FROM source_rows
                GROUP BY code, date
                """
            )
            inserted_rows = self._copy_count(_TEMP_MARGIN_TABLE)
            self._conn.execute(
                f"""
                INSERT INTO margin_data (
                    code,
                    date,
                    long_margin_volume,
                    short_margin_volume
                )
                SELECT
                    code,
                    date,
                    long_margin_volume,
                    short_margin_volume
                FROM {_TEMP_MARGIN_TABLE}
                ON CONFLICT (code, date) DO UPDATE
                SET long_margin_volume = excluded.long_margin_volume,
                    short_margin_volume = excluded.short_margin_volume
                """
            )
            if inserted_rows > 0:
                self._dirty_tables.add("margin_data")
            return inserted_rows

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        placeholders = ", ".join("?" for _ in self._STATEMENT_COLUMNS)
        update_columns = ", ".join(
            f"{column} = COALESCE(excluded.{column}, statements.{column})"
            for column in self._STATEMENT_COLUMNS[2:]
        )
        sql = (
            "INSERT INTO statements "
            f"({', '.join(self._STATEMENT_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            "ON CONFLICT (code, disclosed_date) DO UPDATE "
            f"SET {update_columns}"
        )
        values = [tuple(row.get(column) for column in self._STATEMENT_COLUMNS) for row in rows]
        with self._lock:
            self._conn.executemany(sql, values)
            self._dirty_tables.add("statements")
        return len(rows)

    def copy_statements_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        if not normalized_codes:
            return 0
        cutoff = self._validated_snapshot_date(date_to, field="date_to")
        if cutoff is None:
            raise DatasetSnapshotError("date_to is required for statement copy")

        with self._lock:
            self._require_matching_snapshot_cutoff(cutoff)
            source_alias = self._attach_source_database(source_duckdb_path)
            self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, normalized_codes)
            self._stage_normalized_statements(
                source_alias=source_alias,
                target_table=_TEMP_STATEMENTS_TABLE,
                disclosed_date_to=cutoff,
            )

            inserted_rows = self._copy_count(_TEMP_STATEMENTS_TABLE)
            update_columns = ", ".join(
                f"{column} = COALESCE(excluded.{column}, statements.{column})"
                for column in self._STATEMENT_COLUMNS[2:]
            )
            self._conn.execute(
                f"""
                INSERT INTO statements ({", ".join(self._STATEMENT_COLUMNS)})
                SELECT {", ".join(self._STATEMENT_COLUMNS)}
                FROM {_TEMP_STATEMENTS_TABLE}
                ON CONFLICT (code, disclosed_date) DO UPDATE
                SET {update_columns}
                """
            )
            if inserted_rows > 0:
                self._dirty_tables.add("statements")
            return inserted_rows

    def _stage_normalized_statements(
        self,
        *,
        source_alias: str,
        target_table: str,
        disclosed_date_to: str | None,
    ) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS {target_table}")
        normalized_code_sql = self._normalize_stock_code_expr("code")
        priority_sql = self._stock_code_priority_expr("code")
        source_columns = self._get_source_table_columns(source_alias, "statements")
        statement_select_columns = [
            self._statement_source_select_expr(column, source_columns)
            for column in self._STATEMENT_COLUMNS[2:]
        ]
        merged_columns = [
            "COALESCE(MAX(CASE WHEN source_priority = 0 THEN {column} END), "
            "MAX(CASE WHEN source_priority = 1 THEN {column} END)) AS {column}".format(
                column=column
            )
            for column in self._STATEMENT_COLUMNS[2:]
        ]
        upper_sql = (
            "TRUE"
            if disclosed_date_to is None
            else f"disclosed_date <= '{disclosed_date_to}'"
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE {target_table} AS
            WITH source_rows AS (
                SELECT
                    {normalized_code_sql} AS code,
                    disclosed_date,
                    {priority_sql} AS source_priority,
                    {", ".join(statement_select_columns)}
                FROM {source_alias}.statements
                WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND disclosed_date IS NOT NULL
                  AND disclosed_date <> ''
                  AND {upper_sql}
            )
            SELECT
                code,
                disclosed_date,
                {", ".join(merged_columns)}
            FROM source_rows
            GROUP BY code, disclosed_date
            """
        )

    @staticmethod
    def _normalize_requested_codes(codes: list[str]) -> list[str]:
        normalized: set[str] = set()
        for value in codes:
            code = str(value).strip()
            if len(code) in (5, 6) and code.endswith("0"):
                code = code[:-1]
            if code:
                normalized.add(code)
        return sorted(normalized)

    @staticmethod
    def _validated_snapshot_date(value: str | None, *, field: str) -> str | None:
        if value is None:
            return None
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise DatasetSnapshotError(f"{field} must be an ISO YYYY-MM-DD date") from exc
        if parsed.isoformat() != value:
            raise DatasetSnapshotError(f"{field} must be an ISO YYYY-MM-DD date")
        return value

    def copy_event_time_pit_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str | None,
        date_to: str | None,
    ) -> EventTimePitCopyResult:
        """Copy a complete Market v4 event-time basis graph atomically."""
        codes = self._normalize_requested_codes(normalized_codes)
        if not codes:
            raise DatasetSnapshotError("event-time PIT copy requires at least one stock code")
        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is not None and upper is not None and lower > upper:
            raise DatasetSnapshotError("date_from must be on or before date_to")

        with self._lock:
            source_alias = self._attach_source_database(source_duckdb_path)
            self._preflight_event_time_source(source_alias)
            try:
                self._stage_event_time_pit_copy(
                    source_alias=source_alias,
                    codes=codes,
                    date_from=lower,
                    date_to=upper,
                )
                cutoff_row = self._conn.execute(
                    "SELECT max(date) FROM _dataset_pit_stock_data_raw"
                ).fetchone()
                snapshot_date_to = upper or (
                    str(cutoff_row[0])
                    if cutoff_row is not None and cutoff_row[0] is not None
                    else None
                )
                snapshot_date_to = self._validated_snapshot_date(
                    snapshot_date_to, field="event-time PIT snapshot cutoff"
                )
                if snapshot_date_to is None:
                    raise DatasetSnapshotError(
                        "event-time PIT snapshot cutoff is unavailable"
                    )
                self._validate_staged_event_time_pit(
                    codes=codes,
                    cutoff=snapshot_date_to,
                )
            except DatasetSnapshotError:
                raise
            except Exception as exc:
                raise DatasetSnapshotError(
                    "Market v4 event-time source failed PIT preflight"
                ) from exc
            counts = [self._copy_count(stage) for stage, _ in _PIT_STAGE_TABLES]
            if self._destination_matches_staged_event_time_pit():
                self._require_matching_snapshot_cutoff(snapshot_date_to)
                return EventTimePitCopyResult(*counts)

            try:
                self._conn.execute("BEGIN TRANSACTION")
                for stage, target in _PIT_STAGE_TABLES:
                    columns = tuple(
                        str(row[1])
                        for row in self._conn.execute(
                            f"PRAGMA table_info('{target}')"
                        ).fetchall()
                    )
                    key_columns = _PIT_PRIMARY_KEYS[target]
                    update_sql = ", ".join(
                        f"{column} = excluded.{column}"
                        for column in columns
                        if column not in key_columns
                    )
                    conflict_action = (
                        "DO NOTHING"
                        if target == "stock_adjustment_bases"
                        else f"DO UPDATE SET {update_sql}"
                    )
                    self._conn.execute(
                        f"INSERT INTO {target} SELECT * FROM {stage} "
                        f"ON CONFLICT ({', '.join(key_columns)}) {conflict_action}"
                    )
                self._conn.execute(
                    """
                    INSERT INTO dataset_info (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    [
                        EVENT_TIME_PIT_DATE_TO_INFO_KEY,
                        snapshot_date_to,
                        datetime.now(UTC).isoformat(),
                    ],
                )
                self._conn.execute("COMMIT")
            except Exception as exc:
                self._conn.execute("ROLLBACK")
                raise DatasetSnapshotError(
                    "event-time PIT snapshot publish failed atomically"
                ) from exc

            self._dirty_tables.update(target for _, target in _PIT_STAGE_TABLES)
            return EventTimePitCopyResult(*counts)

    def _require_matching_snapshot_cutoff(self, expected: str) -> None:
        row = self._conn.execute(
            "SELECT value FROM dataset_info WHERE key = ?",
            [EVENT_TIME_PIT_DATE_TO_INFO_KEY],
        ).fetchone()
        if row is None or str(row[0]) != expected:
            raise DatasetSnapshotError(
                "immutable Dataset PIT cutoff differs from staged source"
            )

    def _require_matching_snapshot_cutoff_if_present(self, expected: str) -> None:
        row = self._conn.execute(
            "SELECT value FROM dataset_info WHERE key = ?",
            [EVENT_TIME_PIT_DATE_TO_INFO_KEY],
        ).fetchone()
        if row is not None and str(row[0]) != expected:
            raise DatasetSnapshotError(
                "immutable Dataset PIT cutoff differs from staged source"
            )

    def _preflight_event_time_source(self, source_alias: str) -> None:
        missing = sorted(
            table
            for table in MARKET_V4_EVENT_TIME_REQUIRED_TABLES
            if not self._source_table_exists(source_alias, table)
        )
        if missing:
            raise DatasetSnapshotError(
                "Market v4 event-time source is missing required tables: "
                + ", ".join(missing)
            )
        version = self._conn.execute(
            f"SELECT MAX(version) FROM {source_alias}.market_schema_version"
        ).fetchone()
        if version is None or version[0] != 4:
            raise DatasetSnapshotError("Dataset v3 snapshots require Market schema version 4")
        mode = self._conn.execute(
            f"SELECT value FROM {source_alias}.sync_metadata "
            "WHERE key = 'stock_price_adjustment_mode'"
        ).fetchone()
        if mode is None or mode[0] != "local_projection_v2_event_time":
            raise DatasetSnapshotError(
                "Dataset v3 snapshots require local_projection_v2_event_time"
            )

    def _stage_event_time_pit_copy(
        self,
        *,
        source_alias: str,
        codes: list[str],
        date_from: str | None,
        date_to: str | None,
    ) -> None:
        self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
        self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, codes)
        for stage, _ in _PIT_STAGE_TABLES:
            self._conn.execute(f"DROP TABLE IF EXISTS {stage}")
        self._conn.execute("DROP TABLE IF EXISTS _dataset_pit_provenance_errors")
        self._conn.execute(
            "DROP TABLE IF EXISTS _dataset_pit_expected_statement_metrics"
        )

        normalized = self._normalize_stock_code_expr("code")
        lower_raw = "TRUE" if date_from is None else f"date >= '{date_from}'"
        upper_raw = "TRUE" if date_to is None else f"date <= '{date_to}'"
        basis_lower = "TRUE" if date_from is None else f"(valid_to_exclusive IS NULL OR valid_to_exclusive > '{date_from}')"
        basis_upper = "TRUE" if date_to is None else f"valid_from <= '{date_to}'"
        statement_upper = "TRUE" if date_to is None else f"disclosed_date <= '{date_to}'"
        valuation_lower = "TRUE" if date_from is None else f"valuation.date >= '{date_from}'"
        valuation_upper = "TRUE" if date_to is None else f"valuation.date <= '{date_to}'"
        segment_lower = (
            "TRUE"
            if date_from is None
            else "(segment.source_date_to_exclusive IS NULL "
            f"OR segment.source_date_to_exclusive > '{date_from}')"
        )
        segment_upper = (
            "TRUE"
            if date_to is None
            else f"segment.source_date_from <= '{date_to}'"
        )

        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_stock_data_raw AS
            SELECT {normalized} AS code, date, open, high, low, close, volume,
                   adjustment_factor, created_at
            FROM {source_alias}.stock_data_raw
            WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {lower_raw} AND {upper_raw}
              AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
              AND close IS NOT NULL AND volume IS NOT NULL
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_stock_master_daily AS
            SELECT date, {normalized} AS code, company_name, company_name_english,
                   market_code, market_name, sector_17_code, sector_17_name,
                   sector_33_code, sector_33_name, scale_category, listed_date, created_at
            FROM {source_alias}.stock_master_daily
            WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {lower_raw} AND {upper_raw}
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_bases AS
            SELECT {normalized} AS code, basis_id, valid_from, valid_to_exclusive,
                   adjustment_through_date, source_fingerprint,
                   materialized_through_date, status, created_at, updated_at
            FROM {source_alias}.stock_adjustment_bases
            WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {basis_lower} AND {basis_upper}
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_provenance_errors AS
            SELECT 'statement_metrics_adjusted' AS source_table
            FROM {source_alias}.statement_metrics_adjusted AS metric
            LEFT JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('metric.code')} = basis.code
             AND metric.basis_version = basis.basis_id
            LEFT JOIN {source_alias}.stock_adjustment_bases AS catalog
              ON {self._normalize_stock_code_expr('metric.code')} =
                 {self._normalize_stock_code_expr('catalog.code')}
             AND metric.basis_version = catalog.basis_id
            WHERE {self._normalize_stock_code_expr('metric.code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {statement_upper}
              AND (
                  catalog.basis_id IS NULL
                  OR (
                      basis.basis_id IS NOT NULL
                      AND metric.price_basis_date IS DISTINCT FROM basis.adjustment_through_date
                  )
              )
            UNION ALL
            SELECT 'daily_valuation' AS source_table
            FROM {source_alias}.daily_valuation AS valuation
            LEFT JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('valuation.code')} = basis.code
             AND valuation.basis_version = basis.basis_id
            WHERE {self._normalize_stock_code_expr('valuation.code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {valuation_lower} AND {valuation_upper}
              AND (
                  basis.basis_id IS NULL
                  OR valuation.price_basis_date IS DISTINCT FROM basis.adjustment_through_date
              )
            UNION ALL
            SELECT 'stock_adjustment_basis_segments' AS source_table
            FROM {source_alias}.stock_adjustment_basis_segments AS segment
            LEFT JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('segment.code')} = basis.code
             AND segment.basis_id = basis.basis_id
            LEFT JOIN {source_alias}.stock_adjustment_bases AS catalog
              ON {self._normalize_stock_code_expr('segment.code')} =
                 {self._normalize_stock_code_expr('catalog.code')}
             AND segment.basis_id = catalog.basis_id
            WHERE {self._normalize_stock_code_expr('segment.code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {segment_lower} AND {segment_upper}
              AND (
                  catalog.basis_id IS NULL
                  OR (
                      basis.basis_id IS NOT NULL
                      AND (
                          NOT isfinite(segment.cumulative_factor)
                          OR segment.cumulative_factor <= 0
                          OR (
                              segment.source_date_to_exclusive IS NOT NULL
                              AND segment.source_date_from >=
                                  segment.source_date_to_exclusive
                          )
                      )
                  )
              )
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_segments AS
            SELECT {self._normalize_stock_code_expr('segment.code')} AS code,
                   segment.basis_id, segment.source_date_from,
                   segment.source_date_to_exclusive, segment.cumulative_factor
            FROM {source_alias}.stock_adjustment_basis_segments AS segment
            JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('segment.code')} = basis.code
             AND segment.basis_id = basis.basis_id
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_statement_metrics AS
            SELECT {self._normalize_stock_code_expr('metric.code')} AS code,
                   {", ".join(f'metric.{column}' for column in self._ADJUSTED_STATEMENT_COLUMNS[1:])}
            FROM {source_alias}.statement_metrics_adjusted AS metric
            JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('metric.code')} = basis.code
             AND metric.basis_version = basis.basis_id
            WHERE {self._normalize_stock_code_expr('metric.code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {statement_upper}
            """
        )
        self._stage_normalized_statements(
            source_alias=source_alias,
            target_table="_dataset_pit_normalized_statements",
            disclosed_date_to=date_to,
        )
        self._conn.execute(
            """
            CREATE TEMP TABLE _dataset_pit_expected_statement_metrics AS
            SELECT basis.code, basis.basis_id, source.disclosed_date,
                   source.disclosed_date AS period_end,
                   coalesce(source.type_of_current_period, '') AS period_type
            FROM _dataset_pit_normalized_statements AS source
            JOIN _dataset_pit_bases AS basis
              ON source.code = basis.code
             AND (basis.valid_to_exclusive IS NULL
                  OR source.disclosed_date < basis.valid_to_exclusive)
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_pit_daily_valuation AS
            SELECT {self._normalize_stock_code_expr('valuation.code')} AS code,
                   {", ".join(f'valuation.{column}' for column in self._DAILY_VALUATION_COLUMNS[1:])}
            FROM {source_alias}.daily_valuation AS valuation
            JOIN _dataset_pit_bases AS basis
              ON {self._normalize_stock_code_expr('valuation.code')} = basis.code
             AND valuation.basis_version = basis.basis_id
            WHERE {self._normalize_stock_code_expr('valuation.code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
              AND {valuation_lower} AND {valuation_upper}
            """
        )

    def _validate_staged_event_time_pit(
        self, *, codes: list[str], cutoff: str
    ) -> None:
        audit_error = find_dataset_pit_audit_error(
            self._conn,
            cutoff=cutoff,
            tables={
                "stock_data_raw": "_dataset_pit_stock_data_raw",
                "stock_master_daily": "_dataset_pit_stock_master_daily",
                "stock_adjustment_bases": "_dataset_pit_bases",
                "stock_adjustment_basis_segments": "_dataset_pit_segments",
                "statements": "_dataset_pit_normalized_statements",
                "statement_metrics_adjusted": "_dataset_pit_statement_metrics",
                "daily_valuation": "_dataset_pit_daily_valuation",
            },
        )
        if audit_error is not None:
            raise DatasetSnapshotError(audit_error)
        staged_codes = self._query_distinct_values(
            "SELECT DISTINCT code FROM _dataset_pit_stock_data_raw"
        )
        if staged_codes != set(codes):
            raise DatasetSnapshotError("ready event-time price coverage is missing for requested codes")
        if self._query_scalar_int(
            "SELECT COUNT(*) FROM _dataset_pit_bases WHERE status <> 'ready'"
        ):
            raise DatasetSnapshotError("intersecting event-time basis is not ready")
        if self._query_distinct_values(
            "SELECT DISTINCT code FROM _dataset_pit_bases"
        ) != set(codes):
            raise DatasetSnapshotError("ready intersecting basis is missing for requested codes")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT code, valid_from, valid_to_exclusive,
                       lead(valid_from) OVER (PARTITION BY code ORDER BY valid_from) AS next_from
                FROM _dataset_pit_bases
            ) AS ordered
            WHERE valid_to_exclusive IS NOT NULL
              AND (valid_from >= valid_to_exclusive
                   OR (next_from IS NOT NULL AND valid_to_exclusive <> next_from))
            """
        ):
            raise DatasetSnapshotError("event-time basis intervals are incomplete or overlapping")
        if self._query_scalar_int(
            """
            SELECT COUNT(*)
            FROM _dataset_pit_stock_data_raw AS raw
            LEFT JOIN _dataset_pit_bases AS basis
              ON raw.code = basis.code
             AND raw.date >= basis.valid_from
             AND (basis.valid_to_exclusive IS NULL OR raw.date < basis.valid_to_exclusive)
            GROUP BY raw.code, raw.date
            HAVING COUNT(basis.basis_id) <> 1
            """
        ):
            raise DatasetSnapshotError("raw price dates are not covered by exactly one ready basis")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT code, date FROM _dataset_pit_stock_data_raw
                WHERE open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                  AND close IS NOT NULL AND volume IS NOT NULL
                EXCEPT ALL
                SELECT code, date FROM _dataset_pit_stock_master_daily
            ) AS raw_without_daily_master
            """
        ):
            raise DatasetSnapshotError("raw price coverage is missing exact daily master rows")
        if self._query_scalar_int(
            """
            SELECT COUNT(*)
            FROM _dataset_pit_bases AS basis
            LEFT JOIN _dataset_pit_segments AS segment
              ON basis.code = segment.code AND basis.basis_id = segment.basis_id
            GROUP BY basis.code, basis.basis_id
            HAVING COUNT(segment.source_date_from) = 0
            """
        ):
            raise DatasetSnapshotError("event-time basis segments are missing")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT code, basis_id, source_date_from, source_date_to_exclusive,
                       lead(source_date_from) OVER (
                           PARTITION BY code, basis_id ORDER BY source_date_from
                       ) AS next_from,
                       cumulative_factor
                FROM _dataset_pit_segments
            ) AS ordered
            WHERE NOT isfinite(cumulative_factor) OR cumulative_factor <= 0
               OR (source_date_to_exclusive IS NOT NULL
                   AND (source_date_from >= source_date_to_exclusive
                        OR (next_from IS NOT NULL AND source_date_to_exclusive <> next_from)))
            """
        ):
            raise DatasetSnapshotError("event-time basis segment integrity failed")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, raw.date
                FROM _dataset_pit_bases AS basis
                JOIN _dataset_pit_stock_data_raw AS raw
                  ON basis.code = raw.code
                 AND raw.date <= basis.materialized_through_date
                LEFT JOIN _dataset_pit_segments AS segment
                  ON basis.code = segment.code
                 AND basis.basis_id = segment.basis_id
                 AND raw.date >= segment.source_date_from
                 AND (segment.source_date_to_exclusive IS NULL
                      OR raw.date < segment.source_date_to_exclusive)
                GROUP BY basis.code, basis.basis_id, raw.date
                HAVING COUNT(segment.source_date_from) <> 1
            ) AS invalid_segment_coverage
            """
        ):
            raise DatasetSnapshotError("event-time segment physical coverage is incomplete")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, basis.materialized_through_date,
                       max(raw.date) AS required_through
                FROM _dataset_pit_bases AS basis
                JOIN _dataset_pit_stock_data_raw AS raw
                  ON basis.code = raw.code
                 AND raw.date >= basis.valid_from
                 AND (basis.valid_to_exclusive IS NULL OR raw.date < basis.valid_to_exclusive)
                GROUP BY basis.code, basis.basis_id, basis.materialized_through_date
            ) AS coverage
            WHERE materialized_through_date < required_through
            """
        ):
            raise DatasetSnapshotError("event-time basis materialized coverage is incomplete")
        if self._query_scalar_int(
            "SELECT COUNT(*) FROM _dataset_pit_provenance_errors "
            "WHERE source_table = 'stock_adjustment_basis_segments'"
        ):
            raise DatasetSnapshotError("event-time segment provenance is inconsistent")
        if self._query_scalar_int(
            "SELECT COUNT(*) FROM _dataset_pit_provenance_errors"
        ):
            raise DatasetSnapshotError("event-time basis provenance is inconsistent")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM _dataset_pit_daily_valuation
            WHERE (statement_disclosed_date IS NOT NULL AND statement_disclosed_date > date)
               OR (forward_eps_disclosed_date IS NOT NULL AND forward_eps_disclosed_date > date)
               OR (forward_sales_disclosed_date IS NOT NULL AND forward_sales_disclosed_date > date)
            """
        ):
            raise DatasetSnapshotError("daily valuation provenance is inconsistent")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                (
                    SELECT basis.code, basis.basis_id, raw.date
                    FROM _dataset_pit_bases AS basis
                    JOIN _dataset_pit_stock_data_raw AS raw
                      ON basis.code = raw.code
                     AND raw.date <= basis.materialized_through_date
                     AND raw.open IS NOT NULL AND raw.high IS NOT NULL
                     AND raw.low IS NOT NULL AND raw.close IS NOT NULL
                     AND raw.volume IS NOT NULL
                    EXCEPT ALL
                    SELECT code, basis_version, date
                    FROM _dataset_pit_daily_valuation
                )
                UNION ALL
                (
                    SELECT code, basis_version, date
                    FROM _dataset_pit_daily_valuation
                    EXCEPT ALL
                    SELECT basis.code, basis.basis_id, raw.date
                    FROM _dataset_pit_bases AS basis
                    JOIN _dataset_pit_stock_data_raw AS raw
                      ON basis.code = raw.code
                     AND raw.date <= basis.materialized_through_date
                     AND raw.open IS NOT NULL AND raw.high IS NOT NULL
                     AND raw.low IS NOT NULL AND raw.close IS NOT NULL
                     AND raw.volume IS NOT NULL
                )
            ) AS valuation_difference
            """
        ):
            raise DatasetSnapshotError("daily valuation coverage is incomplete or gapped")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT code, basis_id, disclosed_date, period_end, period_type
                FROM _dataset_pit_expected_statement_metrics
                EXCEPT ALL
                SELECT code, basis_version, disclosed_date, period_end, period_type
                FROM _dataset_pit_statement_metrics
            ) AS missing_expected_metric
            """
        ):
            raise DatasetSnapshotError("adjusted metric coverage is incomplete or gapped")
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, identity.disclosed_date,
                       identity.period_end, identity.period_type
                FROM _dataset_pit_bases AS basis
                JOIN (
                    SELECT DISTINCT code, disclosed_date, period_end, period_type
                    FROM _dataset_pit_statement_metrics
                ) AS identity
                  ON basis.code = identity.code
                 AND (basis.valid_to_exclusive IS NULL
                      OR identity.disclosed_date < basis.valid_to_exclusive)
                EXCEPT ALL
                SELECT code, basis_version, disclosed_date, period_end, period_type
                FROM _dataset_pit_statement_metrics
            ) AS missing_metric_basis
            """
        ):
            raise DatasetSnapshotError("adjusted metric coverage is incomplete or gapped")

    def _destination_matches_staged_event_time_pit(self) -> bool:
        existing_rows = sum(
            self._query_scalar_int(
                f"SELECT COUNT(*) FROM {target} "
                f"WHERE code IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})"
            )
            for _, target in _PIT_STAGE_TABLES
        )
        if existing_rows == 0:
            return False
        for stage, target in _PIT_STAGE_TABLES:
            difference = self._query_scalar_int(
                f"""
                SELECT COUNT(*) FROM (
                    (SELECT * FROM {target}
                     WHERE code IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                     EXCEPT ALL SELECT * FROM {stage})
                    UNION ALL
                    (SELECT * FROM {stage} EXCEPT ALL
                     SELECT * FROM {target}
                     WHERE code IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE}))
                ) AS graph_difference
                """
            )
            if difference:
                raise DatasetSnapshotError(
                    "immutable Dataset PIT graph differs from staged source"
                )
        return True

    def _source_table_exists(self, source_alias: str, table_name: str) -> bool:
        try:
            self._conn.execute(f"SELECT 1 FROM {source_alias}.{table_name} LIMIT 0")
        except Exception:
            return False
        return True

    def set_dataset_info(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO dataset_info (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE
                SET value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                [key, value, datetime.now(UTC).isoformat()],
            )

    def get_stock_count(self) -> int:
        with self._lock:
            return self._query_scalar_int("SELECT COUNT(*) FROM stocks")

    def get_stock_data_count(self) -> int:
        with self._lock:
            return self._query_scalar_int("SELECT COUNT(*) FROM stock_data")

    def get_existing_stock_data_codes(self) -> set[str]:
        with self._lock:
            return self._query_distinct_values("SELECT DISTINCT code FROM stock_data")

    def has_topix_data(self) -> bool:
        with self._lock:
            return self._query_scalar_int("SELECT COUNT(*) FROM topix_data") > 0

    def get_existing_index_codes(self) -> set[str]:
        with self._lock:
            return self._query_distinct_values("SELECT DISTINCT code FROM indices_data")

    def get_existing_margin_codes(self) -> set[str]:
        with self._lock:
            return self._query_distinct_values("SELECT DISTINCT code FROM margin_data")

    def get_existing_statement_codes(self) -> set[str]:
        with self._lock:
            return self._query_distinct_values("SELECT DISTINCT code FROM statements")

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            for table_name, parquet_name, order_by in DATASET_V3_PARQUET_EXPORTS:
                output_path = self._parquet_dir / parquet_name
                if output_path.exists():
                    output_path.unlink()
                escaped = str(output_path).replace("'", "''")
                if order_by:
                    source_sql = f"(SELECT * FROM {table_name} ORDER BY {order_by})"
                else:
                    source_sql = table_name
                self._conn.execute(f"COPY {source_sql} TO '{escaped}' (FORMAT PARQUET)")
            self._dirty_tables.clear()
            self._conn.close()
            if self._source_copy_dir is not None:
                self._source_copy_dir.cleanup()
                self._source_copy_dir = None
            self._closed = True


class DatasetWriter:
    """Dataset snapshot writer backed only by DuckDB + parquet."""

    def __init__(self, path: str) -> None:
        self.snapshot_dir = snapshot_dir_for_path(path)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.duckdb_path = duckdb_path_for_path(path)
        self.parquet_dir = parquet_dir_for_path(path)
        self._duckdb_store = _DatasetDuckDbStore(
            duckdb_path=str(self.duckdb_path),
            parquet_dir=str(self.parquet_dir),
        )

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_stocks(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_stock_data(rows)

    def copy_stock_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> StockDataCopyResult:
        return self._duckdb_store.copy_stock_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_to=date_to,
        )

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_topix_data(rows)

    def copy_topix_data_from_source(
        self, *, source_duckdb_path: str, date_to: str
    ) -> int:
        return self._duckdb_store.copy_topix_data_from_source(
            source_duckdb_path=source_duckdb_path, date_to=date_to
        )

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_indices_data(rows)

    def copy_indices_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        return self._duckdb_store.copy_indices_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_to=date_to,
        )

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_margin_data(rows)

    def copy_margin_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        return self._duckdb_store.copy_margin_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_to=date_to,
        )

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_statements(rows)

    def copy_statements_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_to: str,
    ) -> int:
        return self._duckdb_store.copy_statements_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_to=date_to,
        )

    def copy_event_time_pit_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str | None,
        date_to: str | None,
    ) -> EventTimePitCopyResult:
        return self._duckdb_store.copy_event_time_pit_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_from=date_from,
            date_to=date_to,
        )

    def set_dataset_info(self, key: str, value: str) -> None:
        self._duckdb_store.set_dataset_info(key, value)

    def get_stock_count(self) -> int:
        return self._duckdb_store.get_stock_count()

    def get_stock_data_count(self) -> int:
        return self._duckdb_store.get_stock_data_count()

    def get_existing_stock_data_codes(self) -> set[str]:
        return self._duckdb_store.get_existing_stock_data_codes()

    def has_topix_data(self) -> bool:
        return self._duckdb_store.has_topix_data()

    def get_existing_index_codes(self) -> set[str]:
        return self._duckdb_store.get_existing_index_codes()

    def get_existing_margin_codes(self) -> set[str]:
        return self._duckdb_store.get_existing_margin_codes()

    def get_existing_statement_codes(self) -> set[str]:
        return self._duckdb_store.get_existing_statement_codes()

    def close(self) -> None:
        self._duckdb_store.close()
