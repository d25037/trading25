"""Market time-series storage split (DuckDB + Parquet).

Phase 2 Data Plane:
- market 時系列: DuckDB + Parquet
- portfolio/jobs metadata: SQLite

互換期間中は SQLite mirror を有効化できる。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from loguru import logger

from src.infrastructure.db.market.market_db import MarketDb


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

    def close(self) -> None: ...


class SqliteMirrorTimeSeriesStore:
    """互換用 SQLite mirror。"""

    def __init__(self, market_db: MarketDb) -> None:
        self._market_db = market_db

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_topix_data(rows)

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_stock_data(rows)

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_indices_data(rows)

    def publish_statements(self, rows: list[dict[str, Any]]) -> int:
        return self._market_db.upsert_statements(rows)

    def index_topix_data(self) -> None:
        return None

    def index_stock_data(self) -> None:
        return None

    def index_indices_data(self) -> None:
        return None

    def index_statements(self) -> None:
        return None

    def close(self) -> None:
        return None


@dataclass
class _TableSpec:
    table_name: str
    parquet_name: str
    order_by: str


class DuckDbParquetTimeSeriesStore:
    """DuckDB へ upsert し、Parquet を再生成する Data Plane store。"""

    _TABLE_SPECS = {
        "topix_data": _TableSpec("topix_data", "topix_data.parquet", "date"),
        "stock_data": _TableSpec("stock_data", "stock_data.parquet", "date, code"),
        "indices_data": _TableSpec("indices_data", "indices_data.parquet", "date, code"),
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
            import duckdb  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDB backend requested but `duckdb` package is not installed. "
                "Install duckdb and retry."
            ) from exc

        self._conn = duckdb.connect(str(self._duckdb_path))
        self._dirty_tables: set[str] = set()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
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

    def _export_if_dirty(self, table_name: str) -> None:
        if table_name not in self._dirty_tables:
            return
        spec = self._TABLE_SPECS[table_name]
        output_path = self._parquet_dir / spec.parquet_name
        if output_path.exists():
            output_path.unlink()

        escaped = str(output_path).replace("'", "''")
        self._conn.execute(
            f"COPY (SELECT * FROM {spec.table_name} ORDER BY {spec.order_by}) "
            f"TO '{escaped}' (FORMAT PARQUET)"
        )
        self._dirty_tables.discard(table_name)

    def close(self) -> None:
        for table_name in list(self._dirty_tables):
            self._export_if_dirty(table_name)
        self._conn.close()


class CompositeTimeSeriesStore:
    """複数 backend へ同時 publish するストア。"""

    def __init__(self, stores: list[MarketTimeSeriesStore]) -> None:
        self._stores = stores

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_all("publish_topix_data", rows)

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_all("publish_stock_data", rows)

    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_all("publish_indices_data", rows)

    def publish_statements(self, rows: list[dict[str, Any]]) -> int:
        return self._publish_all("publish_statements", rows)

    def index_topix_data(self) -> None:
        self._index_all("index_topix_data")

    def index_stock_data(self) -> None:
        self._index_all("index_stock_data")

    def index_indices_data(self) -> None:
        self._index_all("index_indices_data")

    def index_statements(self) -> None:
        self._index_all("index_statements")

    def close(self) -> None:
        for store in self._stores:
            store.close()

    def _publish_all(self, method_name: str, rows: list[dict[str, Any]]) -> int:
        published = 0
        for store in self._stores:
            method = cast(Any, getattr(store, method_name))
            published = method(rows)
        return published

    def _index_all(self, method_name: str) -> None:
        for store in self._stores:
            method = cast(Any, getattr(store, method_name))
            method()


def create_time_series_store(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
    sqlite_mirror: bool,
    market_db: MarketDb | None,
) -> MarketTimeSeriesStore | None:
    """設定に応じて時系列ストアを組み立てる。"""
    stores: list[MarketTimeSeriesStore] = []

    normalized_backend = backend.strip().lower()
    if normalized_backend in {"duckdb", "duckdb-parquet", "dual"}:
        try:
            stores.append(
                DuckDbParquetTimeSeriesStore(
                    duckdb_path=duckdb_path,
                    parquet_dir=parquet_dir,
                )
            )
            logger.info("Market time-series backend enabled: duckdb-parquet")
        except RuntimeError as exc:
            logger.warning("DuckDB backend is unavailable: {}", exc)

    if sqlite_mirror and market_db is not None:
        stores.append(SqliteMirrorTimeSeriesStore(market_db))
        logger.info("Market time-series sqlite mirror enabled")

    if not stores and market_db is not None:
        # 最低限の後方互換として SQLite のみを利用
        logger.warning("Falling back to SQLite-only market time-series store")
        stores.append(SqliteMirrorTimeSeriesStore(market_db))

    if not stores:
        return None
    if len(stores) == 1:
        return stores[0]
    return CompositeTimeSeriesStore(stores)
