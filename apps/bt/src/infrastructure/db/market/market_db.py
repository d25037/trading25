"""
Market Metadata DB Access (DuckDB)

market time-series SoT (DuckDB/Parquet) と同じ DuckDB ファイル上で、
metadata / reference data（stocks, sync_metadata, index_master）と
補助クエリを扱う。
"""

from __future__ import annotations

import importlib
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, cast

# Hono 互換 metadata キー
METADATA_KEYS = {
    "INIT_COMPLETED": "init_completed",
    "LAST_SYNC_DATE": "last_sync_date",
    "LAST_STOCKS_REFRESH": "last_stocks_refresh",
    "FAILED_DATES": "failed_dates",
    "REFETCHED_STOCKS": "refetched_stocks",
    "FUNDAMENTALS_LAST_SYNC_DATE": "fundamentals_last_sync_date",
    "FUNDAMENTALS_LAST_DISCLOSED_DATE": "fundamentals_last_disclosed_date",
    "FUNDAMENTALS_FAILED_DATES": "fundamentals_failed_dates",
    "FUNDAMENTALS_FAILED_CODES": "fundamentals_failed_codes",
}

_STATS_TABLES: tuple[str, ...] = (
    "stocks",
    "stock_data",
    "topix_data",
    "indices_data",
    "statements",
    "sync_metadata",
    "index_master",
)

_STATEMENTS_UPDATABLE_COLUMNS: tuple[str, ...] = (
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

_STATEMENTS_ADDITIONAL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("forecast_dividend_fy", "DOUBLE"),
    ("next_year_forecast_dividend_fy", "DOUBLE"),
    ("payout_ratio", "DOUBLE"),
    ("forecast_payout_ratio", "DOUBLE"),
    ("next_year_forecast_payout_ratio", "DOUBLE"),
)


class MarketDb:
    """DuckDB ベースの market metadata / helper query アクセス。"""

    def __init__(self, db_path: str, *, read_only: bool = False) -> None:
        self._db_path = str(db_path)
        self._read_only = read_only
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        duckdb = importlib.import_module("duckdb")
        self._conn = cast(Any, duckdb).connect(self._db_path, read_only=read_only)
        self._lock = threading.RLock()
        if not read_only:
            self.ensure_schema()

    @property
    def db_path(self) -> str:
        return self._db_path

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _assert_writable(self) -> None:
        if self._read_only:
            raise PermissionError("market metadata database is read-only")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _execute(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        with self._lock:
            if params is None:
                return self._conn.execute(sql)
            return self._conn.execute(sql, params)

    def _fetchone(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        with self._lock:
            if params is None:
                return self._conn.execute(sql).fetchone()
            return self._conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[Any]:
        with self._lock:
            if params is None:
                return self._conn.execute(sql).fetchall()
            return self._conn.execute(sql, params).fetchall()

    def _executemany(self, sql: str, params_seq: list[tuple[Any, ...]]) -> None:
        with self._lock:
            self._conn.executemany(sql, params_seq)

    def _table_exists(self, table_name: str) -> bool:
        row = self._fetchone(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [table_name],
        )
        return row is not None

    def _count_rows(self, table_name: str) -> int:
        if not self._table_exists(table_name):
            return 0
        escaped = self._quote_identifier(table_name)
        row = self._fetchone(f"SELECT COUNT(*) FROM {escaped}")
        return int(row[0] or 0) if row else 0

    def ensure_schema(self) -> None:
        """不足テーブルを補完する（DuckDB SoT）。"""
        self._assert_writable()

        self._execute(
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
        self._execute(
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
        self._execute(
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
        self._execute(
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
        self._execute(
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
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sync_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS index_master (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                name_english TEXT,
                category TEXT NOT NULL,
                data_start_date TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        self._ensure_statements_columns()

    def _ensure_statements_columns(self) -> None:
        """既存 statements テーブルに不足カラムを追加する。"""
        existing_columns = {
            str(row[1])
            for row in self._fetchall("PRAGMA table_info('statements')")
            if row and len(row) > 1
        }
        for column_name, column_type in _STATEMENTS_ADDITIONAL_COLUMNS:
            if column_name in existing_columns:
                continue
            self._execute(
                f"ALTER TABLE statements ADD COLUMN {self._quote_identifier(column_name)} {column_type}"
            )

    # --- Read ---

    def get_stats(self) -> dict[str, Any]:
        """DB 統計情報を取得。"""
        return {table_name: self._count_rows(table_name) for table_name in _STATS_TABLES}

    def validate_schema(self) -> dict[str, Any]:
        """スキーマ検証: 必要なテーブルが存在するか確認。"""
        existing = {
            str(row[0])
            for row in self._fetchall("SELECT table_name FROM information_schema.tables")
            if row and row[0]
        }
        required = set(_STATS_TABLES)
        missing = required - existing
        return {
            "valid": len(missing) == 0,
            "required_tables": sorted(required),
            "existing_tables": sorted(existing & required),
            "missing_tables": sorted(missing),
        }

    def get_sync_metadata(self, key: str) -> str | None:
        """sync_metadata からキーの値を取得。"""
        if not self._table_exists("sync_metadata"):
            return None
        row = self._fetchone(
            "SELECT value FROM sync_metadata WHERE key = ?",
            [key],
        )
        return str(row[0]) if row and row[0] is not None else None

    def get_latest_trading_date(self) -> str | None:
        """topix_data の最新取引日を取得。"""
        if not self._table_exists("topix_data"):
            return None
        row = self._fetchone("SELECT MAX(date) FROM topix_data")
        return str(row[0]) if row and row[0] is not None else None

    def get_latest_stock_data_date(self) -> str | None:
        """stock_data の最新取引日を取得。"""
        if not self._table_exists("stock_data"):
            return None
        row = self._fetchone("SELECT MAX(date) FROM stock_data")
        return str(row[0]) if row and row[0] is not None else None

    def get_latest_indices_data_dates(self) -> dict[str, str]:
        """indices_data の銘柄コードごとの最新取引日を取得。"""
        if not self._table_exists("indices_data"):
            return {}
        rows = self._fetchall(
            """
            SELECT code, MAX(date) AS max_date
            FROM indices_data
            GROUP BY code
            """
        )
        return {
            str(row[0]): str(row[1])
            for row in rows
            if row and row[0] and row[1]
        }

    def get_index_master_codes(self) -> set[str]:
        """index_master に存在する指数コード一覧を取得。"""
        if not self._table_exists("index_master"):
            return set()
        rows = self._fetchall("SELECT code FROM index_master")
        return {
            str(row[0])
            for row in rows
            if row and row[0]
        }

    def get_latest_statement_disclosed_date(self) -> str | None:
        """statements の最新開示日を取得。"""
        if not self._table_exists("statements"):
            return None
        row = self._fetchone("SELECT MAX(disclosed_date) FROM statements")
        return str(row[0]) if row and row[0] is not None else None

    def get_statement_codes(self) -> set[str]:
        """statements に存在する銘柄コード一覧を取得。"""
        if not self._table_exists("statements"):
            return set()
        rows = self._fetchall("SELECT DISTINCT code FROM statements WHERE code IS NOT NULL")
        return {
            str(row[0])
            for row in rows
            if row and row[0]
        }

    def get_statement_non_null_counts(self, columns: list[str]) -> dict[str, int]:
        """statements 指定カラムの非NULL件数を返す。"""
        if not columns or not self._table_exists("statements"):
            return {column: 0 for column in columns}

        available = {
            str(row[1])
            for row in self._fetchall("PRAGMA table_info('statements')")
            if row and len(row) > 1
        }
        counts: dict[str, int] = {}
        for column in columns:
            if column not in available:
                counts[column] = 0
                continue
            escaped = self._quote_identifier(column)
            row = self._fetchone(
                f"SELECT COUNT(*) FROM statements WHERE {escaped} IS NOT NULL"
            )
            counts[column] = int(row[0] or 0) if row else 0
        return counts

    def get_prime_codes(self) -> set[str]:
        """stocks から Prime 銘柄コードを取得（legacy 表記も吸収）。"""
        if not self._table_exists("stocks"):
            return set()
        rows = self._fetchall(
            """
            SELECT code
            FROM stocks
            WHERE lower(trim(market_code)) IN ('0111', 'prime')
            """
        )
        return {
            str(row[0])
            for row in rows
            if row and row[0]
        }

    def get_prime_statement_coverage(
        self,
        *,
        limit_missing: int | None = 20,
    ) -> dict[str, Any]:
        """Prime 銘柄に対する statements カバレッジを集計。"""
        prime_codes = self.get_prime_codes()
        statement_codes = self.get_statement_codes()

        missing_codes = sorted(prime_codes - statement_codes)
        covered_count = len(prime_codes & statement_codes)
        prime_count = len(prime_codes)
        missing_count = len(missing_codes)
        coverage_ratio = covered_count / prime_count if prime_count > 0 else 0.0

        if limit_missing is None:
            missing_limited = missing_codes
        else:
            missing_limited = missing_codes[: max(limit_missing, 0)]

        return {
            "primeCount": prime_count,
            "coveredCount": covered_count,
            "missingCount": missing_count,
            "coverageRatio": round(coverage_ratio, 4),
            "missingCodes": missing_limited,
        }

    # --- Write ---

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> int:
        """stocks テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        now_iso = datetime.now().isoformat()  # noqa: DTZ005
        params = [
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
                now_iso,
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO stocks (
                code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            params,
        )
        return len(rows)

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        """stock_data テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
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
        self._executemany(
            """
            INSERT INTO stock_data (
                code, date, open, high, low, close, volume, adjustment_factor, created_at
            )
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
            params,
        )
        return len(rows)

    def upsert_topix_data(self, rows: list[dict[str, Any]]) -> int:
        """topix_data テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
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
        self._executemany(
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
            params,
        )
        return len(rows)

    def upsert_indices_data(self, rows: list[dict[str, Any]]) -> int:
        """indices_data テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
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
        self._executemany(
            """
            INSERT INTO indices_data (code, date, open, high, low, close, sector_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, date) DO UPDATE
            SET open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                sector_name = excluded.sector_name,
                created_at = excluded.created_at
            """,
            params,
        )
        return len(rows)

    def upsert_statements(self, rows: list[dict[str, Any]]) -> int:
        """statements テーブルに upsert（非NULL優先マージ）。"""
        if not rows:
            return 0
        self._assert_writable()

        insert_columns = [
            "code",
            "disclosed_date",
            *_STATEMENTS_UPDATABLE_COLUMNS,
        ]
        placeholders = ", ".join("?" for _ in insert_columns)
        update_clause = ", ".join(
            f"{column} = COALESCE(excluded.{column}, statements.{column})"
            for column in _STATEMENTS_UPDATABLE_COLUMNS
        )
        sql = (
            f"INSERT INTO statements ({', '.join(insert_columns)}) "
            f"VALUES ({placeholders}) "
            "ON CONFLICT (code, disclosed_date) DO UPDATE "
            f"SET {update_clause}"
        )
        params = [
            tuple(row.get(column) for column in insert_columns)
            for row in rows
        ]
        self._executemany(sql, params)
        return len(rows)

    def set_sync_metadata(self, key: str, value: str) -> None:
        """sync_metadata にキーバリューを設定（upsert）。"""
        self._assert_writable()
        self._execute(
            """
            INSERT INTO sync_metadata (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (key) DO UPDATE
            SET value = excluded.value,
                updated_at = excluded.updated_at
            """,
            [key, value, datetime.now().isoformat()],  # noqa: DTZ005
        )

    def upsert_index_master(self, rows: list[dict[str, Any]]) -> int:
        """index_master テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        now_iso = datetime.now().isoformat()  # noqa: DTZ005
        params = [
            (
                row.get("code"),
                row.get("name"),
                row.get("name_english"),
                row.get("category"),
                row.get("data_start_date"),
                row.get("created_at"),
                now_iso,
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO index_master (
                code, name, name_english, category, data_start_date, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (code) DO UPDATE
            SET name = excluded.name,
                name_english = excluded.name_english,
                category = excluded.category,
                data_start_date = COALESCE(excluded.data_start_date, index_master.data_start_date),
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            params,
        )
        return len(rows)

    # --- Stats (Phase 3D-2) ---

    def get_topix_date_range(self) -> dict[str, Any] | None:
        """TOPIX 日付範囲 + 件数。"""
        if not self._table_exists("topix_data"):
            return None
        row = self._fetchone(
            "SELECT COUNT(date), MIN(date), MAX(date) FROM topix_data"
        )
        if row is None or row[1] is None:
            return None
        return {"count": int(row[0] or 0), "min": str(row[1]), "max": str(row[2])}

    def get_stock_data_date_range(self) -> dict[str, Any] | None:
        """stock_data 日付範囲 + 統計。"""
        if not self._table_exists("stock_data"):
            return None
        row = self._fetchone(
            """
            SELECT
                COUNT(*),
                MIN(date),
                MAX(date),
                COUNT(DISTINCT date)
            FROM stock_data
            """
        )
        if row is None or row[1] is None:
            return None
        count = int(row[0] or 0)
        date_count = int(row[3] or 0)
        avg_per_day = count / date_count if date_count > 0 else 0.0
        return {
            "count": count,
            "min": str(row[1]),
            "max": str(row[2]),
            "dateCount": date_count,
            "averageStocksPerDay": round(avg_per_day, 1),
        }

    def get_stock_count_by_market(self) -> dict[str, int]:
        """市場別の銘柄数。"""
        if not self._table_exists("stocks"):
            return {}
        rows = self._fetchall(
            """
            SELECT market_name, COUNT(code)
            FROM stocks
            GROUP BY market_name
            """
        )
        return {
            str(row[0]): int(row[1] or 0)
            for row in rows
            if row and row[0]
        }

    def get_indices_data_range(self) -> dict[str, Any] | None:
        """indices_data 統計。"""
        master_count = self._count_rows("index_master")
        if not self._table_exists("indices_data"):
            by_category = self._load_index_master_category_counts()
            return {
                "masterCount": master_count,
                "dataCount": 0,
                "dateCount": 0,
                "dateRange": None,
                "byCategory": by_category,
            }

        row = self._fetchone(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT date),
                MIN(date),
                MAX(date)
            FROM indices_data
            """
        )
        by_category = self._load_index_master_category_counts()
        if row is None or row[2] is None:
            return {
                "masterCount": master_count,
                "dataCount": 0,
                "dateCount": 0,
                "dateRange": None,
                "byCategory": by_category,
            }
        return {
            "masterCount": master_count,
            "dataCount": int(row[0] or 0),
            "dateCount": int(row[1] or 0),
            "dateRange": {"min": str(row[2]), "max": str(row[3])},
            "byCategory": by_category,
        }

    def _load_index_master_category_counts(self) -> dict[str, int]:
        if not self._table_exists("index_master"):
            return {}
        rows = self._fetchall(
            """
            SELECT category, COUNT(code)
            FROM index_master
            GROUP BY category
            """
        )
        return {
            str(row[0]): int(row[1] or 0)
            for row in rows
            if row and row[0]
        }

    def is_initialized(self) -> bool:
        """DB 初期化済みかを判定する。

        優先判定:
        1. sync_metadata.init_completed が存在する場合はその値を使用
        2. metadata 欠落時は既存データ量から推定（移行済みDBの後方互換）
        """
        val = self.get_sync_metadata(METADATA_KEYS["INIT_COMPLETED"])
        if val is not None:
            return val.strip().lower() == "true"

        has_stocks = self._count_rows("stocks") > 0
        has_time_series = any(
            self._count_rows(table_name) > 0
            for table_name in ("stock_data", "topix_data", "indices_data")
        )
        return has_stocks and has_time_series

    def get_db_file_size(self) -> int:
        """DB ファイルサイズ。"""
        try:
            return int(os.path.getsize(self._db_path))
        except OSError:
            return 0

    # --- Validate (Phase 3D-2) ---

    def get_missing_stock_data_dates(self, *, limit: int = 100) -> list[str]:
        """TOPIX 日付のうち stock_data に存在しない日付（新しい順）。"""
        if limit <= 0:
            return []
        if not self._table_exists("topix_data") or not self._table_exists("stock_data"):
            return []

        rows = self._fetchall(
            """
            SELECT t.date
            FROM topix_data t
            LEFT JOIN (SELECT DISTINCT date FROM stock_data) s ON t.date = s.date
            WHERE s.date IS NULL
            ORDER BY t.date DESC
            LIMIT ?
            """,
            [int(limit)],
        )
        return [str(row[0]) for row in rows if row and row[0]]

    def get_missing_stock_data_dates_count(self) -> int:
        """TOPIX 日付のうち stock_data に存在しない日付の総数。"""
        if not self._table_exists("topix_data") or not self._table_exists("stock_data"):
            return 0
        row = self._fetchone(
            """
            SELECT COUNT(*)
            FROM topix_data t
            LEFT JOIN (SELECT DISTINCT date FROM stock_data) s ON t.date = s.date
            WHERE s.date IS NULL
            """
        )
        return int(row[0] or 0) if row else 0

    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """adjustment_factor != 1.0 のイベント。"""
        if limit <= 0 or not self._table_exists("stock_data"):
            return []
        rows = self._fetchall(
            """
            SELECT code, date, adjustment_factor, close
            FROM stock_data
            WHERE adjustment_factor IS NOT NULL
              AND adjustment_factor != 1.0
            ORDER BY date DESC
            LIMIT ?
            """,
            [int(limit)],
        )
        return [
            {
                "code": str(row[0]),
                "date": str(row[1]),
                "adjustmentFactor": float(row[2]),
                "close": float(row[3]) if row[3] is not None else None,
                "eventType": "stock_split" if float(row[2]) < 1.0 else "reverse_split",
            }
            for row in rows
            if row and row[0] and row[1] and row[2] is not None
        ]

    def get_stocks_needing_refresh(self, limit: int = 20) -> list[str]:
        """調整イベントがある銘柄のうち再取得が必要なもの。"""
        if limit <= 0 or not self._table_exists("stock_data"):
            return []
        rows = self._fetchall(
            """
            SELECT DISTINCT code
            FROM stock_data
            WHERE adjustment_factor IS NOT NULL
              AND adjustment_factor != 1.0
            LIMIT ?
            """,
            [int(limit)],
        )
        return [str(row[0]) for row in rows if row and row[0]]

    def get_stock_data_unique_date_count(self) -> int:
        """stock_data のユニーク日付数。"""
        if not self._table_exists("stock_data"):
            return 0
        row = self._fetchone("SELECT COUNT(DISTINCT date) FROM stock_data")
        return int(row[0] or 0) if row else 0
