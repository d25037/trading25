"""Market time-series storage (DuckDB + Parquet SoT)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, cast

from loguru import logger

class MarketTimeSeriesStore(Protocol):
    """時系列 publish/index インターフェース。"""

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> int: ...

    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...
    def index_indices_data(self) -> None: ...
    def index_statements(self) -> None: ...

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> "TimeSeriesInspection": ...

    def close(self) -> None: ...


@dataclass
class _TableSpec:
    table_name: str
    parquet_name: str
    order_by: str | None = None


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
    missing_stock_dates: list[str] = field(default_factory=list)
    missing_stock_dates_count: int = 0
    indices_count: int = 0
    indices_min: str | None = None
    indices_max: str | None = None
    indices_date_count: int = 0
    latest_indices_dates: dict[str, str] = field(default_factory=dict)
    statements_count: int = 0
    latest_statement_disclosed_date: str | None = None
    statement_codes: set[str] = field(default_factory=set)
    statement_non_null_counts: dict[str, int] = field(default_factory=dict)


class DuckDbParquetTimeSeriesStore:
    """DuckDB へ upsert し、Parquet を再生成する Data Plane store。"""

    _TABLE_SPECS = {
        "topix_data": _TableSpec("topix_data", "topix_data.parquet", "date"),
        # 高カーディナリティ表は export 時の全件 sort が支配的になりやすいため非ソートで出力する。
        "stock_data": _TableSpec("stock_data", "stock_data.parquet"),
        "indices_data": _TableSpec("indices_data", "indices_data.parquet"),
        "statements": _TableSpec("statements", "statements.parquet", "disclosed_date, code"),
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

    def __init__(
        self,
        *,
        duckdb_path: str,
        parquet_dir: str,
    ) -> None:
        self._duckdb_path = Path(duckdb_path)
        self._parquet_dir = Path(parquet_dir)
        self._duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self._parquet_dir.mkdir(parents=True, exist_ok=True)

        try:
            duckdb = __import__("duckdb")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDB backend requested but `duckdb` package is not installed. "
                "Install duckdb and retry."
            ) from exc

        self._conn = cast(Any, duckdb).connect(str(self._duckdb_path))
        # app state で共有されるため、sync 書き込みと stats/validate 読み取りを直列化する。
        self._lock = RLock()
        self._dirty_tables: set[str] = set()
        self._ensure_schema()

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

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
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
                INSERT INTO stock_data
                    (code, date, open, high, low, close, volume, adjustment_factor, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int:
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
                INSERT INTO indices_data
                    (code, date, open, high, low, close, sector_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

    def publish_statements(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        insert_columns = [
            "code",
            "disclosed_date",
            *self._STATEMENT_UPDATABLE_COLUMNS,
        ]
        placeholders = ", ".join("?" for _ in insert_columns)
        update_clause = ", ".join(
            f"{column} = COALESCE(excluded.{column}, statements.{column})"
            for column in self._STATEMENT_UPDATABLE_COLUMNS
        )

        sql = (
            f"INSERT INTO statements ({', '.join(insert_columns)}) "
            f"VALUES ({placeholders}) "
            "ON CONFLICT (code, disclosed_date) DO UPDATE "
            f"SET {update_clause}"
        )

        values = [
            tuple(row.get(column) for column in insert_columns)
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(sql, values)
            self._dirty_tables.add("statements")
        return len(rows)

    def index_topix_data(self) -> None:
        self._export_if_dirty("topix_data")

    def index_stock_data(self) -> None:
        self._export_if_dirty("stock_data")

    def index_indices_data(self) -> None:
        self._export_if_dirty("indices_data")

    def index_statements(self) -> None:
        self._export_if_dirty("statements")

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
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
            statement_codes_rows = self._conn.execute(
                "SELECT DISTINCT code FROM statements WHERE code IS NOT NULL"
            ).fetchall()
            topix_row = topix_row_raw if topix_row_raw is not None else (0, None, None)
            stock_row = stock_row_raw if stock_row_raw is not None else (0, None, None, 0)
            indices_row = indices_row_raw if indices_row_raw is not None else (0, None, None, 0)
            statements_row = statements_row_raw if statements_row_raw is not None else (0, None)
            missing_stock_dates_count = int(missing_count_row[0] or 0) if missing_count_row else 0

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
                missing_stock_dates = [str(row[0]) for row in missing_rows if row and row[0]]

            statement_non_null_counts = self._duckdb_statement_non_null_counts(
                statement_non_null_columns or []
            )

            latest_indices_dates = {
                str(row[0]): str(row[1])
                for row in indices_rows
                if row and row[0] and row[1]
            }
            statement_codes = {
                str(row[0])
                for row in statement_codes_rows
                if row and row[0]
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
                missing_stock_dates=missing_stock_dates,
                missing_stock_dates_count=missing_stock_dates_count,
                indices_count=int(indices_row[0] or 0),
                indices_min=cast(str | None, indices_row[1]),
                indices_max=cast(str | None, indices_row[2]),
                indices_date_count=int(indices_row[3] or 0),
                latest_indices_dates=latest_indices_dates,
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

    def _export_if_dirty(self, table_name: str) -> None:
        with self._lock:
            if table_name not in self._dirty_tables:
                return
            spec = self._TABLE_SPECS[table_name]
            output_path = self._parquet_dir / spec.parquet_name
            if output_path.exists():
                output_path.unlink()

            escaped = str(output_path).replace("'", "''")
            if spec.order_by:
                source_sql = f"(SELECT * FROM {spec.table_name} ORDER BY {spec.order_by})"
            else:
                source_sql = spec.table_name
            self._conn.execute(f"COPY {source_sql} TO '{escaped}' (FORMAT PARQUET)")
            self._dirty_tables.discard(table_name)

    def close(self) -> None:
        with self._lock:
            for table_name in list(self._dirty_tables):
                self._export_if_dirty(table_name)
            self._conn.close()


def create_time_series_store(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
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
        )
    except Exception as exc:  # noqa: BLE001 - backend初期化失敗を呼び出し側で扱う
        logger.warning("DuckDB backend is unavailable: {}", exc)
        return None
    logger.info("Market time-series backend enabled: duckdb-parquet")
    return store
