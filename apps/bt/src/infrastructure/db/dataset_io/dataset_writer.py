"""DuckDB-only dataset snapshot writer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import importlib
from pathlib import Path
import shutil
import tempfile
from threading import RLock
from typing import Any, cast

_PARQUET_EXPORTS: tuple[tuple[str, str, str | None], ...] = (
    ("stocks", "stocks.parquet", "code"),
    ("stock_data", "stock_data.parquet", None),
    ("topix_data", "topix_data.parquet", "date"),
    ("indices_data", "indices_data.parquet", None),
    ("margin_data", "margin_data.parquet", "code, date"),
    ("statements", "statements.parquet", "disclosed_date, code"),
)

_SOURCE_ALIAS = "market_source"
_TEMP_STOCK_CODE_TABLE = "_dataset_copy_target_stock_codes"
_TEMP_INDEX_CODE_TABLE = "_dataset_copy_target_index_codes"
_TEMP_STOCK_DATA_TABLE = "_dataset_copy_stock_data"
_TEMP_STATEMENTS_TABLE = "_dataset_copy_statements"
_TEMP_MARGIN_TABLE = "_dataset_copy_margin"


@dataclass(frozen=True)
class StockDataCopyCodeStats:
    total_rows: int = 0
    valid_rows: int = 0
    skipped_rows: int = 0


@dataclass(frozen=True)
class StockDataCopyResult:
    inserted_rows: int
    code_stats: dict[str, StockDataCopyCodeStats]


def snapshot_dir_for_path(path: str) -> Path:
    """Resolve snapshot directory from current or legacy dataset path representations."""
    source = Path(path)
    if source.name in {"dataset.duckdb", "dataset.db"}:
        return source.parent
    if source.suffix in {".db", ".duckdb"}:
        return source.with_suffix("")
    return source


def duckdb_path_for_path(path: str) -> Path:
    return snapshot_dir_for_path(path) / "dataset.duckdb"


def parquet_dir_for_path(path: str) -> Path:
    return snapshot_dir_for_path(path) / "parquet"


class _DatasetDuckDbStore:
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
    ) -> StockDataCopyResult:
        if not normalized_codes:
            return StockDataCopyResult(inserted_rows=0, code_stats={})

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

    def copy_topix_data_from_source(self, *, source_duckdb_path: str) -> int:
        with self._lock:
            source_alias = self._attach_source_database(source_duckdb_path)
            inserted_rows = self._query_scalar_int(
                f"SELECT COUNT(*) FROM {source_alias}.topix_data WHERE date IS NOT NULL AND date <> ''"
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
    ) -> int:
        if not normalized_codes:
            return 0

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
    ) -> int:
        if not normalized_codes:
            return 0

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
    ) -> int:
        if not normalized_codes:
            return 0

        with self._lock:
            source_alias = self._attach_source_database(source_duckdb_path)
            self._reset_temp_table(_TEMP_STOCK_CODE_TABLE, "code TEXT PRIMARY KEY")
            self._load_temp_codes(_TEMP_STOCK_CODE_TABLE, normalized_codes)
            self._conn.execute(f"DROP TABLE IF EXISTS {_TEMP_STATEMENTS_TABLE}")

            normalized_code_sql = self._normalize_stock_code_expr("code")
            priority_sql = self._stock_code_priority_expr("code")
            merged_columns = [
                "COALESCE(MAX(CASE WHEN source_priority = 0 THEN {column} END), "
                "MAX(CASE WHEN source_priority = 1 THEN {column} END)) AS {column}".format(column=column)
                for column in self._STATEMENT_COLUMNS[2:]
            ]
            self._conn.execute(
                f"""
                CREATE TEMP TABLE {_TEMP_STATEMENTS_TABLE} AS
                WITH source_rows AS (
                    SELECT
                        {normalized_code_sql} AS code,
                        disclosed_date,
                        {priority_sql} AS source_priority,
                        {", ".join(self._STATEMENT_COLUMNS[2:])}
                    FROM {source_alias}.statements
                    WHERE {normalized_code_sql} IN (SELECT code FROM {_TEMP_STOCK_CODE_TABLE})
                      AND disclosed_date IS NOT NULL
                      AND disclosed_date <> ''
                )
                SELECT
                    code,
                    disclosed_date,
                    {", ".join(merged_columns)}
                FROM source_rows
                GROUP BY code, disclosed_date
                """
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
            for table_name, parquet_name, order_by in _PARQUET_EXPORTS:
                if table_name not in self._dirty_tables:
                    continue
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
    ) -> StockDataCopyResult:
        return self._duckdb_store.copy_stock_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
        )

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_topix_data(rows)

    def copy_topix_data_from_source(self, *, source_duckdb_path: str) -> int:
        return self._duckdb_store.copy_topix_data_from_source(source_duckdb_path=source_duckdb_path)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_indices_data(rows)

    def copy_indices_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
    ) -> int:
        return self._duckdb_store.copy_indices_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
        )

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_margin_data(rows)

    def copy_margin_data_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
    ) -> int:
        return self._duckdb_store.copy_margin_data_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
        )

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        return self._duckdb_store.upsert_statements(rows)

    def copy_statements_from_source(
        self,
        *,
        source_duckdb_path: str,
        normalized_codes: list[str],
    ) -> int:
        return self._duckdb_store.copy_statements_from_source(
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
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
