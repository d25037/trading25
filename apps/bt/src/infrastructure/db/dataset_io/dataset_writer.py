"""DuckDB-only dataset snapshot writer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import importlib
import json
from pathlib import Path
import shutil
import tempfile
from threading import RLock
from typing import Any, cast

from src.infrastructure.db.dataset_io.snapshot_contract import (
    DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY,
    DATASET_PROVIDER_AS_OF_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_END_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_START_INFO_KEY,
    DATASET_PROVIDER_PLAN_INFO_KEY,
    DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY,
    DATASET_V4_PARQUET_EXPORTS,
    MARKET_V5_PROVIDER_REQUIRED_TABLES,
)
from src.infrastructure.db.dataset_io.pit_validation import (
    find_dataset_snapshot_audit_error,
)
from src.shared.provider_stock_window import provider_stock_source_fingerprint

_SOURCE_ALIAS = "market_source"
_TEMP_STOCK_CODE_TABLE = "_dataset_copy_target_stock_codes"
_TEMP_INDEX_CODE_TABLE = "_dataset_copy_target_index_codes"
_TEMP_STOCK_DATA_TABLE = "_dataset_copy_stock_data"
_TEMP_STATEMENTS_TABLE = "_dataset_copy_statements"
_TEMP_MARGIN_TABLE = "_dataset_copy_margin"
_PROVIDER_STAGE_TABLES: tuple[tuple[str, str], ...] = (
    ("_dataset_provider_stock_data_raw", "stock_data_raw"),
    ("_dataset_provider_stock_master_daily", "stock_master_daily"),
    ("_dataset_provider_statements", "statements"),
    ("_dataset_provider_statement_metrics", "statement_metrics_adjusted"),
    ("_dataset_provider_daily_valuation", "daily_valuation"),
)
_PROVIDER_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "stock_data_raw": ("code", "date"),
    "stock_master_daily": ("date", "code"),
    "statements": ("code", "statement_id"),
    "statement_metrics_adjusted": ("code", "statement_id"),
    "daily_valuation": ("code", "date"),
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
    """The source cannot produce a complete Dataset v4 provider snapshot."""


@dataclass(frozen=True)
class ProviderSnapshotCopyResult:
    raw_price_rows: int
    stock_master_rows: int
    statement_rows: int
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
        {
            "disclosure_number",
            "disclosed_date",
            "disclosed_at",
            "period_start",
            "period_end",
            "type_of_current_period",
            "type_of_document",
        }
    )
    _STATEMENT_COLUMNS: tuple[str, ...] = (
        "code",
        "statement_id",
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
        "statement_id",
        "disclosed_date",
        "disclosed_at",
        "period_end",
        "period_type",
        "fundamentals_adjustment_basis_date",
        "raw_eps",
        "adjusted_eps",
        "raw_diluted_eps",
        "adjusted_diluted_eps",
        "raw_bps",
        "adjusted_bps",
        "raw_forecast_eps",
        "adjusted_forecast_eps",
        "raw_dividend_fy",
        "adjusted_dividend_fy",
        "raw_forecast_dividend_fy",
        "adjusted_forecast_dividend_fy",
        "raw_shares_outstanding",
        "adjusted_shares_outstanding",
        "raw_treasury_shares",
        "adjusted_treasury_shares",
        "adjustment_factor_cumulative",
        "source_fingerprint",
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
        "statement_id",
        "statement_disclosed_at",
        "fundamentals_adjustment_basis_date",
        "source_fingerprint",
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
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_data_raw (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
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
                CREATE TABLE IF NOT EXISTS statement_metrics_adjusted (
                    code TEXT NOT NULL,
                    statement_id TEXT NOT NULL,
                    disclosed_date TEXT NOT NULL,
                    disclosed_at TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    fundamentals_adjustment_basis_date TEXT NOT NULL,
                    raw_eps DOUBLE,
                    adjusted_eps DOUBLE,
                    raw_diluted_eps DOUBLE,
                    adjusted_diluted_eps DOUBLE,
                    raw_bps DOUBLE,
                    adjusted_bps DOUBLE,
                    raw_forecast_eps DOUBLE,
                    adjusted_forecast_eps DOUBLE,
                    raw_dividend_fy DOUBLE,
                    adjusted_dividend_fy DOUBLE,
                    raw_forecast_dividend_fy DOUBLE,
                    adjusted_forecast_dividend_fy DOUBLE,
                    raw_shares_outstanding DOUBLE,
                    adjusted_shares_outstanding DOUBLE,
                    raw_treasury_shares DOUBLE,
                    adjusted_treasury_shares DOUBLE,
                    adjustment_factor_cumulative DOUBLE NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    created_at TEXT,
                    PRIMARY KEY (code, statement_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_valuation (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    price_basis_date TEXT NOT NULL,
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
                    statement_id TEXT,
                    statement_disclosed_at TEXT,
                    fundamentals_adjustment_basis_date TEXT,
                    source_fingerprint TEXT,
                    created_at TEXT,
                    PRIMARY KEY (code, date)
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
        date_from: str,
        date_to: str,
    ) -> StockDataCopyResult:
        if not normalized_codes:
            return StockDataCopyResult(inserted_rows=0, code_stats={})

        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is None or upper is None:
            raise DatasetSnapshotError("provider coverage bounds are required")
        if lower > upper:
            raise DatasetSnapshotError("date_from must be on or before date_to")
        with self._lock:
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
                      AND date >= '{lower}'
                      AND date <= '{upper}'
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
        self, *, source_duckdb_path: str, date_from: str, date_to: str
    ) -> int:
        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is None or upper is None:
            raise DatasetSnapshotError("provider coverage bounds are required")
        with self._lock:
            source_alias = self._attach_source_database(source_duckdb_path)
            inserted_rows = self._query_scalar_int(
                f"SELECT COUNT(*) FROM {source_alias}.topix_data "
                f"WHERE date BETWEEN '{lower}' AND '{upper}'"
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
                  AND date BETWEEN '{lower}' AND '{upper}'
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
        date_from: str,
        date_to: str,
    ) -> int:
        if not normalized_codes:
            return 0

        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is None or upper is None:
            raise DatasetSnapshotError("provider coverage bounds are required")
        with self._lock:
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
                  AND date BETWEEN '{lower}' AND '{upper}'
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
                  AND date BETWEEN '{lower}' AND '{upper}'
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
        date_from: str,
        date_to: str,
    ) -> int:
        if not normalized_codes:
            return 0

        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is None or upper is None:
            raise DatasetSnapshotError("provider coverage bounds are required")
        with self._lock:
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
                      AND date BETWEEN '{lower}' AND '{upper}'
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
            "ON CONFLICT (code, statement_id) DO UPDATE "
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
                ON CONFLICT (code, statement_id) DO UPDATE
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
                    statement_id,
                    {priority_sql} AS source_priority,
                    {", ".join(statement_select_columns)}
                FROM {source_alias}.statements
                WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND statement_id IS NOT NULL
                  AND statement_id <> ''
                  AND disclosed_date IS NOT NULL
                  AND disclosed_date <> ''
                  AND {upper_sql}
            )
            SELECT
                code,
                statement_id,
                {", ".join(merged_columns)}
            FROM source_rows
            GROUP BY code, statement_id
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

    def copy_provider_snapshot_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str,
        date_to: str,
    ) -> ProviderSnapshotCopyResult:
        """Copy one bounded Market v5 provider/current-basis snapshot atomically."""
        codes = self._normalize_requested_codes(normalized_codes)
        if not codes:
            raise DatasetSnapshotError("provider snapshot copy requires at least one stock code")
        lower = self._validated_snapshot_date(date_from, field="date_from")
        upper = self._validated_snapshot_date(date_to, field="date_to")
        if lower is None or upper is None:
            raise DatasetSnapshotError("provider coverage bounds are required")
        if lower > upper:
            raise DatasetSnapshotError("date_from must be on or before date_to")

        with self._lock:
            source_alias = self._attach_source_database(source_duckdb_path)
            self._preflight_provider_source(source_alias)
            self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, codes)
            vintage = self._load_provider_vintage(source_alias, codes)
            if (
                vintage[DATASET_PROVIDER_COVERAGE_START_INFO_KEY] != lower
                or vintage[DATASET_PROVIDER_COVERAGE_END_INFO_KEY] != upper
            ):
                raise DatasetSnapshotError(
                    "requested bounds differ from the pinned effective provider coverage"
                )
            try:
                self._stage_provider_snapshot_copy(
                    source_alias=source_alias,
                    date_from=lower,
                    date_to=upper,
                )
                self._validate_staged_provider_snapshot(
                    codes=codes,
                    coverage_start=lower,
                    coverage_end=upper,
                    fundamentals_basis_date=vintage[
                        DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY
                    ],
                )
            except DatasetSnapshotError:
                raise
            except Exception as exc:
                raise DatasetSnapshotError(
                    "Market v5 provider source failed Dataset v4 preflight"
                ) from exc

            counts = [
                self._copy_count(stage) for stage, _target in _PROVIDER_STAGE_TABLES
            ]
            if self._destination_matches_staged_provider_snapshot():
                self._require_matching_provider_vintage(vintage)
                return ProviderSnapshotCopyResult(*counts)

            try:
                self._conn.execute("BEGIN TRANSACTION")
                for stage, target in _PROVIDER_STAGE_TABLES:
                    self._conn.execute(f"INSERT INTO {target} SELECT * FROM {stage}")
                now = datetime.now(UTC).isoformat()
                self._conn.executemany(
                    """
                    INSERT INTO dataset_info (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value, updated_at = excluded.updated_at
                    """,
                    [(key, value, now) for key, value in vintage.items()],
                )
                self._conn.execute("COMMIT")
            except Exception as exc:
                self._conn.execute("ROLLBACK")
                raise DatasetSnapshotError(
                    "provider snapshot publish failed atomically"
                ) from exc

            self._dirty_tables.update(target for _stage, target in _PROVIDER_STAGE_TABLES)
            return ProviderSnapshotCopyResult(*counts)

    def _preflight_provider_source(self, source_alias: str) -> None:
        missing = sorted(
            table
            for table in MARKET_V5_PROVIDER_REQUIRED_TABLES
            if not self._source_table_exists(source_alias, table)
        )
        if missing:
            raise DatasetSnapshotError(
                "Market v5 provider source is missing required tables: "
                + ", ".join(missing)
            )
        version = self._conn.execute(
            f"SELECT MAX(version) FROM {source_alias}.market_schema_version"
        ).fetchone()
        if version is None or version[0] != 5:
            raise DatasetSnapshotError("Dataset v4 snapshots require Market schema version 5")
        mode = self._conn.execute(
            f"SELECT value FROM {source_alias}.sync_metadata "
            "WHERE key = 'stock_price_adjustment_mode'"
        ).fetchone()
        if mode is None or mode[0] != "provider_adjusted_v1":
            raise DatasetSnapshotError(
                "Dataset v4 snapshots require provider_adjusted_v1"
            )
        plan = self._conn.execute(
            f"SELECT value FROM {source_alias}.sync_metadata "
            "WHERE key = 'provider_plan'"
        ).fetchone()
        if plan is None or not str(plan[0]).strip():
            raise DatasetSnapshotError("Market v5 provider plan metadata is missing")

    def _load_provider_vintage(
        self,
        source_alias: str,
        codes: list[str],
    ) -> dict[str, str]:
        normalized_window_code = self._normalize_stock_code_expr("provider_window.code")
        normalized_state_code = self._normalize_stock_code_expr("basis_state.code")
        rows = self._conn.execute(
            f"""
            SELECT {normalized_window_code} AS code,
                   provider_window.coverage_start, provider_window.coverage_end,
                   provider_window.provider_as_of, provider_window.source_fingerprint,
                   basis_state.fundamentals_adjustment_basis_date,
                   basis_state.source_fingerprint, basis_state.statement_count
            FROM {source_alias}.stock_provider_windows AS provider_window
            LEFT JOIN {source_alias}.current_basis_fundamentals_state AS basis_state
              ON {normalized_state_code} = {normalized_window_code}
            WHERE {normalized_window_code}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
            ORDER BY code
            """
        ).fetchall()
        if len(rows) != len(codes) or {str(row[0]) for row in rows} != set(codes):
            raise DatasetSnapshotError(
                "selected stocks require exactly one provider window and current basis state"
            )
        if self._query_scalar_int(
            f"""
            SELECT COUNT(*) FROM {source_alias}.current_basis_recompute_pending
            WHERE {self._normalize_stock_code_expr('code')}
                  IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
            """
        ):
            raise DatasetSnapshotError(
                "selected stocks have unresolved current-basis recomputation"
            )

        starts: list[str] = []
        ends: list[str] = []
        provider_as_of_values: set[str] = set()
        basis_dates: set[str] = set()
        fingerprint_rows: list[dict[str, object]] = []
        for row in rows:
            code = str(row[0])
            coverage_start = self._validated_snapshot_date(
                str(row[1]), field=f"provider coverage start for {code}"
            )
            coverage_end = self._validated_snapshot_date(
                str(row[2]), field=f"provider coverage end for {code}"
            )
            provider_as_of = self._validated_snapshot_date(
                str(row[3]), field=f"provider as-of for {code}"
            )
            basis_date = self._validated_snapshot_date(
                str(row[5]), field=f"fundamentals adjustment basis date for {code}"
            )
            window_fingerprint = str(row[4] or "").strip()
            state_fingerprint = str(row[6] or "").strip()
            statement_count = int(row[7]) if row[7] is not None else -1
            if (
                coverage_start is None
                or coverage_end is None
                or provider_as_of is None
                or basis_date is None
                or coverage_start > coverage_end
                or coverage_end > provider_as_of
            ):
                raise DatasetSnapshotError(
                    f"provider vintage dates are incoherent for {code}"
                )
            if (
                basis_date != coverage_end
                or not window_fingerprint
                or not state_fingerprint
                or statement_count < 0
            ):
                raise DatasetSnapshotError(
                    f"current-basis provider state is incomplete for {code}"
                )
            raw_rows = self._conn.execute(
                f"""
                SELECT code, date, open, high, low, close, volume, turnover_value,
                       adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
                       adjusted_close, adjusted_volume
                FROM (
                    SELECT {self._normalize_stock_code_expr('code')} AS code,
                           date, open, high, low, close, volume, turnover_value,
                           adjustment_factor, adjusted_open, adjusted_high,
                           adjusted_low, adjusted_close, adjusted_volume,
                           row_number() OVER (
                               PARTITION BY {self._normalize_stock_code_expr('code')}, date
                               ORDER BY {self._stock_code_priority_expr('code')}, code
                           ) AS alias_rank
                    FROM {source_alias}.stock_data_raw
                    WHERE {self._normalize_stock_code_expr('code')} = ?
                      AND date BETWEEN ? AND ?
                ) ranked
                WHERE alias_rank = 1
                ORDER BY date
                """,
                (code, coverage_start, coverage_end),
            ).fetchall()
            raw_columns = (
                "code", "date", "open", "high", "low", "close", "volume",
                "turnover_value", "adjustment_factor", "adjusted_open",
                "adjusted_high", "adjusted_low", "adjusted_close",
                "adjusted_volume",
            )
            calculated_window_fingerprint = provider_stock_source_fingerprint(
                [dict(zip(raw_columns, raw_row, strict=True)) for raw_row in raw_rows]
            )
            if calculated_window_fingerprint != window_fingerprint:
                raise DatasetSnapshotError(
                    f"provider source fingerprint is stale or malformed for {code}"
                )
            state_error = self._query_scalar_int(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT
                        (SELECT COUNT(*) FROM {source_alias}.statements
                         WHERE {self._normalize_stock_code_expr('code')} = ?) AS raw_count,
                        (SELECT COUNT(*) FROM {source_alias}.statement_metrics_adjusted
                         WHERE {self._normalize_stock_code_expr('code')} = ?) AS metric_count,
                        (SELECT COUNT(*) FROM {source_alias}.statement_metrics_adjusted
                         WHERE {self._normalize_stock_code_expr('code')} = ?
                           AND (fundamentals_adjustment_basis_date <> ?
                                OR source_fingerprint <> ?)) AS stale_count
                ) state
                WHERE raw_count <> ? OR metric_count <> ? OR stale_count <> 0
                """,
                (
                    code,
                    code,
                    code,
                    basis_date,
                    state_fingerprint,
                    statement_count,
                    statement_count,
                ),
            )
            if state_error:
                raise DatasetSnapshotError(
                    f"current-basis statement state is inconsistent for {code}"
                )
            starts.append(coverage_start)
            ends.append(coverage_end)
            provider_as_of_values.add(provider_as_of)
            basis_dates.add(basis_date)
            fingerprint_rows.append(
                {
                    "code": code,
                    "coverageStart": coverage_start,
                    "coverageEnd": coverage_end,
                    "providerAsOf": provider_as_of,
                    "providerFingerprint": window_fingerprint,
                    "fundamentalsBasisDate": basis_date,
                    "fundamentalsFingerprint": state_fingerprint,
                    "statementCount": statement_count,
                }
            )

        effective_start = max(starts)
        effective_end = min(ends)
        if effective_start > effective_end:
            raise DatasetSnapshotError(
                "selected provider windows have no common effective coverage"
            )
        if len(provider_as_of_values) != 1 or len(basis_dates) != 1:
            raise DatasetSnapshotError(
                "selected stocks do not share one provider/current-basis vintage"
            )
        provider_as_of = next(iter(provider_as_of_values))
        fundamentals_basis_date = next(iter(basis_dates))
        if fundamentals_basis_date != effective_end:
            raise DatasetSnapshotError(
                "fundamentals adjustment basis date differs from effective coverage end"
            )
        plan_row = self._conn.execute(
            f"SELECT value FROM {source_alias}.sync_metadata "
            "WHERE key = 'provider_plan'"
        ).fetchone()
        plan = str(plan_row[0]).strip() if plan_row is not None else ""
        normalized = json.dumps(
            fingerprint_rows,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return {
            DATASET_PROVIDER_PLAN_INFO_KEY: plan,
            DATASET_PROVIDER_AS_OF_INFO_KEY: provider_as_of,
            DATASET_PROVIDER_COVERAGE_START_INFO_KEY: effective_start,
            DATASET_PROVIDER_COVERAGE_END_INFO_KEY: effective_end,
            DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY: hashlib.sha256(
                normalized.encode("utf-8")
            ).hexdigest(),
            DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY: fundamentals_basis_date,
        }

    def _stage_provider_snapshot_copy(
        self,
        *,
        source_alias: str,
        date_from: str,
        date_to: str,
    ) -> None:
        self._reject_conflicting_provider_aliases(
            source_alias=source_alias,
            date_from=date_from,
            date_to=date_to,
        )
        for stage, _target in _PROVIDER_STAGE_TABLES:
            self._conn.execute(f"DROP TABLE IF EXISTS {stage}")
        self._conn.execute("DROP TABLE IF EXISTS _dataset_provider_expected_sessions")
        normalized = self._normalize_stock_code_expr("code")
        priority = self._stock_code_priority_expr("code")

        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_provider_expected_sessions AS
            SELECT DISTINCT date FROM {source_alias}.topix_data
            WHERE date BETWEEN '{date_from}' AND '{date_to}'
            """
        )

        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_provider_stock_data_raw AS
            SELECT code, date, open, high, low, close, volume, turnover_value,
                   adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
                   adjusted_close, adjusted_volume, created_at
            FROM (
                SELECT {normalized} AS code, date, open, high, low, close, volume,
                       turnover_value, adjustment_factor, adjusted_open,
                       adjusted_high, adjusted_low, adjusted_close, adjusted_volume,
                       created_at,
                       row_number() OVER (
                           PARTITION BY {normalized}, date
                           ORDER BY {priority}, code
                       ) AS alias_rank
                FROM {source_alias}.stock_data_raw
                WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND date BETWEEN '{date_from}' AND '{date_to}'
            ) ranked WHERE alias_rank = 1
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_provider_stock_master_daily AS
            SELECT date, code, company_name, company_name_english, market_code,
                   market_name, sector_17_code, sector_17_name, sector_33_code,
                   sector_33_name, scale_category, listed_date, created_at
            FROM (
                SELECT date, {normalized} AS code,
                       coalesce(company_name, '') AS company_name,
                       company_name_english, coalesce(market_code, '') AS market_code,
                       coalesce(market_name, '') AS market_name,
                       coalesce(sector_17_code, '') AS sector_17_code,
                       coalesce(sector_17_name, '') AS sector_17_name,
                       coalesce(sector_33_code, '') AS sector_33_code,
                       coalesce(sector_33_name, '') AS sector_33_name,
                       scale_category, coalesce(listed_date, '') AS listed_date,
                       created_at,
                       row_number() OVER (
                           PARTITION BY date, {normalized}
                           ORDER BY {priority}, code
                       ) AS alias_rank
                FROM {source_alias}.stock_master_daily
                WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND date BETWEEN '{date_from}' AND '{date_to}'
            ) ranked WHERE alias_rank = 1
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_provider_statement_metrics AS
            SELECT {", ".join(self._ADJUSTED_STATEMENT_COLUMNS)}
            FROM (
                SELECT {normalized} AS code,
                       {", ".join(self._ADJUSTED_STATEMENT_COLUMNS[1:])},
                       row_number() OVER (
                           PARTITION BY {normalized}, statement_id
                           ORDER BY {priority}, code
                       ) AS alias_rank
                FROM {source_alias}.statement_metrics_adjusted
                WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND disclosed_date <= '{date_to}'
            ) ranked WHERE alias_rank = 1
            """
        )
        self._conn.execute(
            f"""
            CREATE TEMP TABLE _dataset_provider_daily_valuation AS
            SELECT {", ".join(self._DAILY_VALUATION_COLUMNS)}
            FROM (
                SELECT {normalized} AS code,
                       {", ".join(self._DAILY_VALUATION_COLUMNS[1:])},
                       row_number() OVER (
                           PARTITION BY {normalized}, date
                           ORDER BY {priority}, code
                       ) AS alias_rank
                FROM {source_alias}.daily_valuation
                WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                  AND date BETWEEN '{date_from}' AND '{date_to}'
            ) ranked WHERE alias_rank = 1
            """
        )
        self._stage_normalized_statements(
            source_alias=source_alias,
            target_table="_dataset_provider_statements",
            disclosed_date_to=date_to,
        )

    def _reject_conflicting_provider_aliases(
        self, *, source_alias: str, date_from: str, date_to: str
    ) -> None:
        normalized = self._normalize_stock_code_expr("code")
        checks = (
            (
                "stock_data_raw",
                "date",
                "open, high, low, close, volume, turnover_value, adjustment_factor, "
                "adjusted_open, adjusted_high, adjusted_low, adjusted_close, adjusted_volume",
                f"date BETWEEN '{date_from}' AND '{date_to}'",
            ),
            (
                "stock_master_daily",
                "date",
                "company_name, company_name_english, market_code, market_name, "
                "sector_17_code, sector_17_name, sector_33_code, sector_33_name, "
                "scale_category, listed_date",
                f"date BETWEEN '{date_from}' AND '{date_to}'",
            ),
            (
                "statement_metrics_adjusted",
                "statement_id",
                ", ".join(self._ADJUSTED_STATEMENT_COLUMNS[2:-1]),
                f"disclosed_date <= '{date_to}'",
            ),
            (
                "daily_valuation",
                "date",
                ", ".join(self._DAILY_VALUATION_COLUMNS[2:-1]),
                f"date BETWEEN '{date_from}' AND '{date_to}'",
            ),
        )
        for table, identity, payload, bounds in checks:
            conflicts = self._query_scalar_int(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {normalized} AS normalized_code, {identity}
                    FROM {source_alias}.{table}
                    WHERE {normalized} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                      AND {bounds}
                    GROUP BY normalized_code, {identity}
                    HAVING COUNT(DISTINCT code) > 1
                       AND COUNT(DISTINCT hash({payload})) > 1
                ) conflicts
                """
            )
            if conflicts:
                raise DatasetSnapshotError(
                    f"conflicting normalized stock-code aliases in {table}"
                )

    def _validate_staged_provider_snapshot(
        self,
        *,
        codes: list[str],
        coverage_start: str,
        coverage_end: str,
        fundamentals_basis_date: str,
    ) -> None:
        expected_session_count = self._copy_count(
            "_dataset_provider_expected_sessions"
        )
        expected_bounds = self._conn.execute(
            "SELECT min(date), max(date) FROM _dataset_provider_expected_sessions"
        ).fetchone()
        if (
            expected_session_count == 0
            or expected_bounds is None
            or expected_bounds[0] != coverage_start
            or expected_bounds[1] != coverage_end
        ):
            raise DatasetSnapshotError(
                "provider coverage lacks exact market sessions at both bounds"
            )
        expected_pairs_sql = (
            "SELECT codes.code, sessions.date "
            "FROM _dataset_copy_target_stock_codes AS codes "
            "CROSS JOIN _dataset_provider_expected_sessions AS sessions"
        )
        for table in (
            "_dataset_provider_stock_data_raw",
            "_dataset_provider_stock_master_daily",
            "stock_data",
            "_dataset_provider_daily_valuation",
        ):
            if self._query_scalar_int(
                f"""
                SELECT COUNT(*) FROM (
                    ({expected_pairs_sql} EXCEPT ALL SELECT code, date FROM {table})
                    UNION ALL
                    (SELECT code, date FROM {table}
                     WHERE code IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                     EXCEPT ALL {expected_pairs_sql})
                ) differences
                """
            ):
                raise DatasetSnapshotError(
                    "provider snapshot has an empty, gap, or bound mismatch: "
                    f"{table}"
                )
        audit_error = find_dataset_snapshot_audit_error(
            self._conn,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            fundamentals_basis_date=fundamentals_basis_date,
            tables={
                "stocks": "stocks",
                "stock_data": "stock_data",
                "stock_data_raw": "_dataset_provider_stock_data_raw",
                "stock_master_daily": "_dataset_provider_stock_master_daily",
                "statements": "_dataset_provider_statements",
                "statement_metrics_adjusted": "_dataset_provider_statement_metrics",
                "daily_valuation": "_dataset_provider_daily_valuation",
            },
        )
        if audit_error is not None:
            raise DatasetSnapshotError(audit_error)
        if self._query_distinct_values(
            "SELECT code FROM _dataset_provider_stock_data_raw"
        ) != set(codes):
            raise DatasetSnapshotError(
                "provider-adjusted price coverage is missing for requested codes"
            )
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                (SELECT code, company_name, company_name_english, market_code,
                        market_name, sector_17_code, sector_17_name,
                        sector_33_code, sector_33_name, scale_category, listed_date
                 FROM stocks
                 EXCEPT ALL
                 SELECT code, company_name, company_name_english, market_code,
                        market_name, sector_17_code, sector_17_name,
                        sector_33_code, sector_33_name, scale_category, listed_date
                 FROM _dataset_provider_stock_master_daily WHERE date = ?)
                UNION ALL
                (SELECT code, company_name, company_name_english, market_code,
                        market_name, sector_17_code, sector_17_name,
                        sector_33_code, sector_33_name, scale_category, listed_date
                 FROM _dataset_provider_stock_master_daily WHERE date = ?
                 EXCEPT ALL
                 SELECT code, company_name, company_name_english, market_code,
                        market_name, sector_17_code, sector_17_name,
                        sector_33_code, sector_33_name, scale_category, listed_date
                 FROM stocks)
            ) differences
            """,
            (coverage_end, coverage_end),
        ):
            raise DatasetSnapshotError(
                "Dataset stocks must exactly match coverage-end stock master rows"
            )
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                SELECT code, date FROM _dataset_provider_stock_data_raw
                EXCEPT ALL
                SELECT code, date FROM _dataset_provider_stock_master_daily
            ) missing_master
            """
        ):
            raise DatasetSnapshotError(
                "provider raw price coverage is missing exact daily master rows"
            )
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM (
                (SELECT code, date FROM stock_data
                 WHERE code IN (SELECT code FROM _dataset_copy_target_stock_codes)
                 EXCEPT ALL
                 SELECT code, date FROM _dataset_provider_daily_valuation)
                UNION ALL
                (SELECT code, date FROM _dataset_provider_daily_valuation
                 EXCEPT ALL
                 SELECT code, date FROM stock_data
                 WHERE code IN (SELECT code FROM _dataset_copy_target_stock_codes))
            ) differences
            """
        ):
            raise DatasetSnapshotError(
                "daily valuation coverage differs from provider-adjusted prices"
            )
        if self._query_scalar_int(
            """
            SELECT COUNT(*) FROM _dataset_provider_daily_valuation valuation
            JOIN stock_data price USING (code, date)
            WHERE valuation.close IS DISTINCT FROM price.close
            """
        ):
            raise DatasetSnapshotError(
                "daily valuation close differs from provider-adjusted price"
            )

    def _destination_matches_staged_provider_snapshot(self) -> bool:
        existing_rows = sum(
            self._query_scalar_int(
                f"SELECT COUNT(*) FROM {target} "
                f"WHERE code IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})"
            )
            for _stage, target in _PROVIDER_STAGE_TABLES
        )
        if existing_rows == 0:
            return False
        for stage, target in _PROVIDER_STAGE_TABLES:
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
                ) differences
                """
            )
            if difference:
                raise DatasetSnapshotError(
                    "immutable Dataset provider snapshot differs from staged source"
                )
        return True

    def _require_matching_provider_vintage(self, expected: dict[str, str]) -> None:
        actual = dict(
            self._conn.execute(
                "SELECT key, value FROM dataset_info WHERE key IN ("
                + ", ".join("?" for _key in expected)
                + ")",
                list(expected),
            ).fetchall()
        )
        if actual != expected:
            raise DatasetSnapshotError(
                "immutable Dataset provider vintage differs from staged source"
            )

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
            for table_name, parquet_name, order_by in DATASET_V4_PARQUET_EXPORTS:
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
        date_from: str,
        date_to: str,
    ) -> StockDataCopyResult:
        return self._duckdb_store.copy_stock_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_from=date_from,
            date_to=date_to,
        )

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_topix_data(rows)

    def copy_topix_data_from_source(
        self, *, source_duckdb_path: str, date_from: str, date_to: str
    ) -> int:
        return self._duckdb_store.copy_topix_data_from_source(
            source_duckdb_path=source_duckdb_path,
            date_from=date_from,
            date_to=date_to,
        )

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_indices_data(rows)

    def copy_indices_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str,
        date_to: str,
    ) -> int:
        return self._duckdb_store.copy_indices_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_from=date_from,
            date_to=date_to,
        )

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_margin_data(rows)

    def copy_margin_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str,
        date_to: str,
    ) -> int:
        return self._duckdb_store.copy_margin_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_from=date_from,
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

    def copy_provider_snapshot_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
        date_from: str,
        date_to: str,
    ) -> ProviderSnapshotCopyResult:
        return self._duckdb_store.copy_provider_snapshot_from_source(
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
