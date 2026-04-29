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

import pandas as pd

from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.market_code_alias import expand_market_codes

# Hono 互換 metadata キー
METADATA_KEYS = {
    "INIT_COMPLETED": "init_completed",
    "LAST_SYNC_DATE": "last_sync_date",
    "LAST_STOCKS_REFRESH": "last_stocks_refresh",
    "STOCK_PRICE_ADJUSTMENT_MODE": "stock_price_adjustment_mode",
    "FAILED_DATES": "failed_dates",
    "REFETCHED_STOCKS": "refetched_stocks",
    "MARGIN_EMPTY_CODES": "margin_empty_codes",
    "FUNDAMENTALS_LAST_SYNC_DATE": "fundamentals_last_sync_date",
    "FUNDAMENTALS_LAST_DISCLOSED_DATE": "fundamentals_last_disclosed_date",
    "FUNDAMENTALS_FAILED_DATES": "fundamentals_failed_dates",
    "FUNDAMENTALS_FAILED_CODES": "fundamentals_failed_codes",
    "FUNDAMENTALS_EMPTY_CODES": "fundamentals_empty_codes",
    "ADJUSTMENT_REFRESH_STATE_INITIALIZED": "adjustment_refresh_state_initialized",
    "LAST_INTRADAY_SYNC": "last_intraday_sync",
}
LOCAL_STOCK_PRICE_ADJUSTMENT_MODE = "local_projection_v1"
MARKET_SCHEMA_VERSION = 3
INCOMPATIBLE_MARKET_SCHEMA_VERSION = 0

_STATS_TABLES: tuple[str, ...] = (
    "market_schema_version",
    "stocks",
    "stocks_latest",
    "stock_master_daily",
    "stock_master_intervals",
    "stock_data_raw",
    "stock_data",
    "stock_data_minute_raw",
    "topix_data",
    "indices_data",
    "options_225_data",
    "margin_data",
    "statements",
    "sync_metadata",
    "index_master",
    "index_membership_daily",
)

_CORE_MARKET_TABLES: tuple[str, ...] = (
    "stocks",
    "stock_data_raw",
    "stock_data",
    "topix_data",
    "indices_data",
    "options_225_data",
    "margin_data",
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

_PRIME_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["prime"]))
_FUNDAMENTALS_TARGET_MARKET_CODES: tuple[str, ...] = (
    "0111",
    "0112",
    "0113",
    "prime",
    "standard",
    "growth",
)

_STOCK_MASTER_DAILY_COLUMNS: tuple[str, ...] = (
    "date",
    "code",
    "company_name",
    "company_name_english",
    "market_code",
    "market_name",
    "sector_17_code",
    "sector_17_name",
    "sector_33_code",
    "sector_33_name",
    "scale_category",
    "listed_date",
    "created_at",
)
_STOCK_MASTER_DAILY_RELATION = "__tmp_stock_master_daily_publish"


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

    def _existing_table_names(self) -> set[str]:
        return {
            str(row[0])
            for row in self._fetchall("SELECT table_name FROM information_schema.tables")
            if row and row[0]
        }

    def ensure_schema(self) -> None:
        """不足テーブルを補完する（DuckDB SoT）。"""
        self._assert_writable()

        existing_before = self._existing_table_names()
        had_schema_version = "market_schema_version" in existing_before
        had_legacy_market_tables = any(
            table_name in existing_before for table_name in _CORE_MARKET_TABLES
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS market_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL,
                notes TEXT
            )
            """
        )
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
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS stock_master_intervals (
                code TEXT,
                valid_from TEXT,
                valid_to TEXT,
                fingerprint TEXT,
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
                PRIMARY KEY (code, valid_from, fingerprint)
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS stocks_latest (
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
                listed_date TEXT,
                source_date TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        self._execute(
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
        self._execute(
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
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS index_membership_daily (
                date TEXT,
                index_code TEXT,
                code TEXT,
                created_at TEXT,
                PRIMARY KEY (date, index_code, code)
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS stock_adjustment_refresh_state (
                code TEXT PRIMARY KEY,
                resolved_adjustment_date TEXT,
                updated_at TEXT
            )
            """
        )
        self._ensure_market_schema_version(
            had_schema_version=had_schema_version,
            had_legacy_market_tables=had_legacy_market_tables,
        )
        self._ensure_statements_columns()
        self._ensure_stock_price_adjustment_mode_for_empty_db()

    def _ensure_market_schema_version(
        self,
        *,
        had_schema_version: bool,
        had_legacy_market_tables: bool,
    ) -> None:
        """Record schema v3 for fresh DBs; mark pre-v3 DBs incompatible."""
        if had_schema_version:
            return
        version = (
            INCOMPATIBLE_MARKET_SCHEMA_VERSION
            if had_legacy_market_tables
            else MARKET_SCHEMA_VERSION
        )
        notes = (
            "pre-v3 market.duckdb detected; destructive initial sync reset is required"
            if version == INCOMPATIBLE_MARKET_SCHEMA_VERSION
            else "market.duckdb schema v3"
        )
        self._execute(
            """
            INSERT INTO market_schema_version (version, applied_at, notes)
            VALUES (?, ?, ?)
            ON CONFLICT (version) DO UPDATE
            SET applied_at = excluded.applied_at,
                notes = excluded.notes
            """,
            [version, datetime.now().isoformat(), notes],
        )

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

    def get_market_schema_version(self) -> int | None:
        if not self._table_exists("market_schema_version"):
            return None
        row = self._fetchone("SELECT MAX(version) FROM market_schema_version")
        return int(row[0]) if row and row[0] is not None else None

    def is_market_schema_current(self) -> bool:
        return self.get_market_schema_version() == MARKET_SCHEMA_VERSION

    def get_stock_master_coverage(self) -> dict[str, Any]:
        daily_count = self._count_rows("stock_master_daily")
        interval_count = self._count_rows("stock_master_intervals")
        latest_count = self._count_rows("stocks_latest")
        membership_count = self._count_rows("index_membership_daily")
        missing_dates_count = self.get_missing_stock_master_dates_count()
        row = None
        if self._table_exists("stock_master_daily"):
            row = self._fetchone(
                """
                SELECT MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(DISTINCT code)
                FROM stock_master_daily
                """
            )
        return {
            "dailyCount": daily_count,
            "intervalCount": interval_count,
            "latestCount": latest_count,
            "indexMembershipDailyCount": membership_count,
            "dateMin": str(row[0]) if row and row[0] is not None else None,
            "dateMax": str(row[1]) if row and row[1] is not None else None,
            "dateCount": int(row[2] or 0) if row else 0,
            "codeCount": int(row[3] or 0) if row else 0,
            "missingTopixDatesCount": missing_dates_count,
            "missingTopixDates": self.get_missing_stock_master_dates(limit=20),
        }

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

    def get_stock_price_adjustment_mode(self) -> str | None:
        return self.get_sync_metadata(METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"])

    def is_legacy_stock_price_snapshot(self) -> bool:
        stock_count = self._count_rows("stock_data") if self._table_exists("stock_data") else 0
        raw_count = (
            self._count_rows("stock_data_raw")
            if self._table_exists("stock_data_raw")
            else 0
        )
        if stock_count <= 0 and raw_count <= 0:
            return False

        adjustment_mode = self.get_stock_price_adjustment_mode()
        if adjustment_mode != LOCAL_STOCK_PRICE_ADJUSTMENT_MODE:
            return True
        return raw_count <= 0 and stock_count > 0

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

    def get_topix_dates(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """topix_data の取引日一覧を取得。"""
        if not self._table_exists("topix_data"):
            return []
        sql = "SELECT date FROM topix_data"
        params: list[Any] = []
        conditions: list[str] = []
        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY date"
        rows = self._fetchall(sql, params)
        return [str(row[0]) for row in rows if row and row[0]]

    def get_latest_stock_master_date(self) -> str | None:
        """stock_master_daily の最新スナップショット日を取得。"""
        if not self._table_exists("stock_master_daily"):
            return None
        row = self._fetchone("SELECT MAX(date) FROM stock_master_daily")
        return str(row[0]) if row and row[0] is not None else None

    def get_missing_stock_master_dates(self, *, limit: int | None = 20) -> list[str]:
        """TOPIX 取引日のうち daily master が存在しない日付を取得。"""
        if not self._table_exists("topix_data") or not self._table_exists("stock_master_daily"):
            return []
        sql = """
            SELECT t.date
            FROM topix_data t
            LEFT JOIN stock_master_daily m ON m.date = t.date
            GROUP BY t.date
            HAVING COUNT(m.code) = 0
            ORDER BY t.date
        """
        params: list[Any] = []
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(limit, 0))
        rows = self._fetchall(sql, params)
        return [str(row[0]) for row in rows if row and row[0]]

    def get_missing_stock_master_dates_count(self) -> int:
        """TOPIX 取引日のうち daily master が存在しない日付数。"""
        if not self._table_exists("topix_data") or not self._table_exists("stock_master_daily"):
            return 0
        row = self._fetchone(
            """
            SELECT COUNT(*)
            FROM (
                SELECT t.date
                FROM topix_data t
                LEFT JOIN stock_master_daily m ON m.date = t.date
                GROUP BY t.date
                HAVING COUNT(m.code) = 0
            ) missing
            """
        )
        return int(row[0] or 0) if row else 0

    def get_stock_master_rows_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """指定日の PIT 銘柄マスタ行を取得。latest fallback はしない。"""
        if not self._table_exists("stock_master_daily"):
            return []
        conditions = ["date = ?"]
        params: list[Any] = [as_of_date]
        if market_codes:
            placeholders = ", ".join("?" for _ in market_codes)
            conditions.append(f"market_code IN ({placeholders})")
            params.extend(market_codes)
        if scale_categories:
            placeholders = ", ".join("?" for _ in scale_categories)
            conditions.append(f"coalesce(scale_category, '') IN ({placeholders})")
            params.extend(scale_categories)
        rows = self._fetchall(
            f"""
            SELECT
                date, code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date
            FROM stock_master_daily
            WHERE {' AND '.join(conditions)}
            ORDER BY code
            """,
            params,
        )
        return [
            {
                "date": row[0],
                "code": row[1],
                "company_name": row[2],
                "company_name_english": row[3],
                "market_code": row[4],
                "market_name": row[5],
                "sector_17_code": row[6],
                "sector_17_name": row[7],
                "sector_33_code": row[8],
                "sector_33_name": row[9],
                "scale_category": row[10],
                "listed_date": row[11],
            }
            for row in rows
        ]

    def get_stock_master_codes_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
        exclude_scale_categories: list[str] | None = None,
    ) -> list[str]:
        """指定日の PIT 銘柄コードだけを取得。latest fallback はしない。"""
        if not self._table_exists("stock_master_daily"):
            return []
        conditions = ["date = ?"]
        params: list[Any] = [as_of_date]
        if market_codes:
            placeholders = ", ".join("?" for _ in market_codes)
            conditions.append(f"market_code IN ({placeholders})")
            params.extend(market_codes)
        if scale_categories:
            placeholders = ", ".join("?" for _ in scale_categories)
            conditions.append(f"coalesce(scale_category, '') IN ({placeholders})")
            params.extend(scale_categories)
        if exclude_scale_categories:
            placeholders = ", ".join("?" for _ in exclude_scale_categories)
            conditions.append(f"coalesce(scale_category, '') NOT IN ({placeholders})")
            params.extend(exclude_scale_categories)
        rows = self._fetchall(
            f"""
            SELECT code
            FROM stock_master_daily
            WHERE {' AND '.join(conditions)}
            ORDER BY code
            """,
            params,
        )
        return [code for row in rows if row and (code := normalize_stock_code(row[0]))]

    def get_index_membership_codes(self, as_of_date: str, index_code: str) -> set[str]:
        """指定日の指数 membership code set。latest fallback はしない。"""
        if not self._table_exists("index_membership_daily"):
            return set()
        rows = self._fetchall(
            """
            SELECT code
            FROM index_membership_daily
            WHERE date = ? AND index_code = ?
            ORDER BY code
            """,
            [as_of_date, index_code],
        )
        return {code for row in rows if row and (code := normalize_stock_code(row[0]))}

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

    def get_latest_options_225_date(self) -> str | None:
        """options_225_data の最新取引日を取得。"""
        if not self._table_exists("options_225_data"):
            return None
        row = self._fetchone("SELECT MAX(date) FROM options_225_data")
        return str(row[0]) if row and row[0] is not None else None

    def get_latest_margin_date(self) -> str | None:
        """margin_data の最新日付を取得。"""
        if not self._table_exists("margin_data"):
            return None
        row = self._fetchone("SELECT MAX(date) FROM margin_data")
        return str(row[0]) if row and row[0] is not None else None

    def get_margin_codes(self) -> set[str]:
        """margin_data に存在する銘柄コード一覧を取得。"""
        if not self._table_exists("margin_data"):
            return set()
        rows = self._fetchall("SELECT DISTINCT code FROM margin_data WHERE code IS NOT NULL")
        return {
            str(row[0])
            for row in rows
            if row and row[0]
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
        return self._get_codes_by_market_codes(_PRIME_MARKET_CODES)

    def get_fundamentals_target_codes(self) -> set[str]:
        """stocks から fundamentals sync 対象市場の銘柄コードを取得。"""
        return self._get_codes_by_market_codes(_FUNDAMENTALS_TARGET_MARKET_CODES)

    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]:
        """listed-market target universe の基礎情報を返す。"""
        if not self._table_exists("stocks"):
            return []
        placeholders = ", ".join("?" for _ in _FUNDAMENTALS_TARGET_MARKET_CODES)
        rows = self._fetchall(
            f"""
            SELECT code, company_name, market_code
            FROM stocks
            WHERE lower(trim(market_code)) IN ({placeholders})
            ORDER BY code
            """,
            list(_FUNDAMENTALS_TARGET_MARKET_CODES),
        )
        return [
            {
                "code": str(row[0]),
                "company_name": str(row[1] or ""),
                "market_code": str(row[2] or ""),
            }
            for row in rows
            if row and row[0]
        ]

    def _get_codes_by_market_codes(self, market_codes: tuple[str, ...]) -> set[str]:
        if not self._table_exists("stocks"):
            return set()
        placeholders = ", ".join("?" for _ in market_codes)
        rows = self._fetchall(
            f"""
            SELECT code
            FROM stocks
            WHERE lower(trim(market_code)) IN ({placeholders})
            """,
            list(market_codes),
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

    def upsert_stock_master_daily(self, snapshot_date: str, rows: list[dict[str, Any]]) -> int:
        """stock_master_daily に日次 PIT 銘柄マスタを upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
            (
                snapshot_date,
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
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO stock_master_daily (
                date, code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (date, code) DO UPDATE
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
                created_at = excluded.created_at
            """,
            params,
        )
        return len(rows)

    def upsert_stock_master_daily_rows(self, rows: list[dict[str, Any]]) -> int:
        """date を含む PIT 銘柄マスタ行を relation-based upsert する。"""
        if not rows:
            return 0
        self._assert_writable()
        deduped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            row_date = str(row.get("date") or "")
            code = str(row.get("code") or "")
            if not row_date or not code:
                continue
            deduped[(row_date, code)] = row
        if not deduped:
            return 0

        dataframe = pd.DataFrame.from_records(
            [
                {column: row.get(column) for column in _STOCK_MASTER_DAILY_COLUMNS}
                for row in deduped.values()
            ],
            columns=_STOCK_MASTER_DAILY_COLUMNS,
        )
        columns_sql = ", ".join(_STOCK_MASTER_DAILY_COLUMNS)
        update_columns = [
            column
            for column in _STOCK_MASTER_DAILY_COLUMNS
            if column not in {"date", "code"}
        ]
        update_clause = ", ".join(
            f"{column} = excluded.{column}" for column in update_columns
        )
        with self._lock:
            self._conn.register(_STOCK_MASTER_DAILY_RELATION, dataframe)
            try:
                self._conn.execute(
                    f"""
                    INSERT INTO stock_master_daily ({columns_sql})
                    SELECT {columns_sql} FROM {_STOCK_MASTER_DAILY_RELATION}
                    ON CONFLICT (date, code) DO UPDATE SET {update_clause}
                    """
                )
            finally:
                self._conn.unregister(_STOCK_MASTER_DAILY_RELATION)
        return len(deduped)

    def rebuild_stock_master_intervals(self) -> int:
        """daily master から同一属性が続く PIT interval を再構築。"""
        self._assert_writable()
        if not self._table_exists("stock_master_daily"):
            return 0
        self._execute("DELETE FROM stock_master_intervals")
        self._execute(
            """
            INSERT INTO stock_master_intervals (
                code, valid_from, valid_to, fingerprint, company_name, company_name_english,
                market_code, market_name, sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, scale_category, listed_date, created_at
            )
            WITH fingerprinted AS (
                SELECT
                    *,
                    md5(concat_ws('|',
                        coalesce(company_name, ''), coalesce(company_name_english, ''),
                        coalesce(market_code, ''), coalesce(market_name, ''),
                        coalesce(sector_17_code, ''), coalesce(sector_17_name, ''),
                        coalesce(sector_33_code, ''), coalesce(sector_33_name, ''),
                        coalesce(scale_category, ''), coalesce(listed_date, '')
                    )) AS fingerprint
                FROM stock_master_daily
            ), marked AS (
                SELECT
                    *,
                    CASE
                        WHEN lag(fingerprint) OVER (PARTITION BY code ORDER BY date) = fingerprint
                        THEN 0 ELSE 1
                    END AS starts_new_group
                FROM fingerprinted
            ), grouped AS (
                SELECT
                    *,
                    sum(starts_new_group) OVER (PARTITION BY code ORDER BY date) AS interval_group
                FROM marked
            )
            SELECT
                code,
                min(date) AS valid_from,
                max(date) AS valid_to,
                fingerprint,
                any_value(company_name),
                any_value(company_name_english),
                any_value(market_code),
                any_value(market_name),
                any_value(sector_17_code),
                any_value(sector_17_name),
                any_value(sector_33_code),
                any_value(sector_33_name),
                any_value(scale_category),
                any_value(listed_date),
                max(created_at)
            FROM grouped
            GROUP BY code, interval_group, fingerprint
            """
        )
        return self._count_rows("stock_master_intervals")

    def rebuild_stocks_latest(self) -> int:
        """最新 daily master から stocks_latest と legacy stocks を再構築。"""
        self._assert_writable()
        latest_date = self.get_latest_stock_master_date()
        if latest_date is None:
            return 0
        now_iso = datetime.now().isoformat()  # noqa: DTZ005
        self._execute("DELETE FROM stocks_latest")
        self._execute(
            """
            INSERT INTO stocks_latest (
                code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date, source_date, created_at, updated_at
            )
            SELECT
                code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date, date, created_at, ?
            FROM stock_master_daily
            WHERE date = ?
            """,
            [now_iso, latest_date],
        )
        rows = self._fetchall(
            """
            SELECT
                code, company_name, company_name_english, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                scale_category, listed_date, created_at
            FROM stocks_latest
            """
        )
        self.upsert_stocks(
            [
                {
                    "code": row[0],
                    "company_name": row[1],
                    "company_name_english": row[2],
                    "market_code": row[3],
                    "market_name": row[4],
                    "sector_17_code": row[5],
                    "sector_17_name": row[6],
                    "sector_33_code": row[7],
                    "sector_33_name": row[8],
                    "scale_category": row[9],
                    "listed_date": row[10],
                    "created_at": row[11],
                }
                for row in rows
            ]
        )
        return self._count_rows("stocks_latest")

    def upsert_stock_data(self, rows: list[dict[str, Any]]) -> int:
        """stock_data_raw と stock_data に upsert。"""
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
            INSERT INTO stock_data_raw (
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
        self.set_sync_metadata(
            METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"],
            LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
        )
        return len(rows)

    def upsert_stock_minute_data(self, rows: list[dict[str, Any]]) -> int:
        """stock_data_minute_raw に upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
            (
                row.get("code"),
                row.get("date"),
                row.get("time"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                row.get("turnover_value"),
                row.get("created_at"),
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO stock_data_minute_raw (
                code, date, time, open, high, low, close, volume, turnover_value, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, date, time) DO UPDATE
            SET open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                turnover_value = excluded.turnover_value,
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

    def upsert_options_225_data(self, rows: list[dict[str, Any]]) -> int:
        """options_225_data テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
            (
                row.get("code"),
                row.get("date"),
                row.get("whole_day_open"),
                row.get("whole_day_high"),
                row.get("whole_day_low"),
                row.get("whole_day_close"),
                row.get("night_session_open"),
                row.get("night_session_high"),
                row.get("night_session_low"),
                row.get("night_session_close"),
                row.get("day_session_open"),
                row.get("day_session_high"),
                row.get("day_session_low"),
                row.get("day_session_close"),
                row.get("volume"),
                row.get("open_interest"),
                row.get("turnover_value"),
                row.get("contract_month"),
                row.get("strike_price"),
                row.get("only_auction_volume"),
                row.get("emergency_margin_trigger_division"),
                row.get("put_call_division"),
                row.get("last_trading_day"),
                row.get("special_quotation_day"),
                row.get("settlement_price"),
                row.get("theoretical_price"),
                row.get("base_volatility"),
                row.get("underlying_price"),
                row.get("implied_volatility"),
                row.get("interest_rate"),
                row.get("created_at"),
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO options_225_data (
                code, date, whole_day_open, whole_day_high, whole_day_low, whole_day_close,
                night_session_open, night_session_high, night_session_low, night_session_close,
                day_session_open, day_session_high, day_session_low, day_session_close,
                volume, open_interest, turnover_value, contract_month, strike_price,
                only_auction_volume, emergency_margin_trigger_division, put_call_division,
                last_trading_day, special_quotation_day, settlement_price, theoretical_price,
                base_volatility, underlying_price, implied_volatility, interest_rate, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, date) DO UPDATE
            SET whole_day_open = excluded.whole_day_open,
                whole_day_high = excluded.whole_day_high,
                whole_day_low = excluded.whole_day_low,
                whole_day_close = excluded.whole_day_close,
                night_session_open = excluded.night_session_open,
                night_session_high = excluded.night_session_high,
                night_session_low = excluded.night_session_low,
                night_session_close = excluded.night_session_close,
                day_session_open = excluded.day_session_open,
                day_session_high = excluded.day_session_high,
                day_session_low = excluded.day_session_low,
                day_session_close = excluded.day_session_close,
                volume = excluded.volume,
                open_interest = excluded.open_interest,
                turnover_value = excluded.turnover_value,
                contract_month = excluded.contract_month,
                strike_price = excluded.strike_price,
                only_auction_volume = excluded.only_auction_volume,
                emergency_margin_trigger_division = excluded.emergency_margin_trigger_division,
                put_call_division = excluded.put_call_division,
                last_trading_day = excluded.last_trading_day,
                special_quotation_day = excluded.special_quotation_day,
                settlement_price = excluded.settlement_price,
                theoretical_price = excluded.theoretical_price,
                base_volatility = excluded.base_volatility,
                underlying_price = excluded.underlying_price,
                implied_volatility = excluded.implied_volatility,
                interest_rate = excluded.interest_rate,
                created_at = excluded.created_at
            """,
            params,
        )
        return len(rows)

    def upsert_margin_data(self, rows: list[dict[str, Any]]) -> int:
        """margin_data テーブルに upsert。"""
        if not rows:
            return 0
        self._assert_writable()
        params = [
            (
                row.get("code"),
                row.get("date"),
                row.get("long_margin_volume"),
                row.get("short_margin_volume"),
            )
            for row in rows
        ]
        self._executemany(
            """
            INSERT INTO margin_data (code, date, long_margin_volume, short_margin_volume)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (code, date) DO UPDATE
            SET long_margin_volume = excluded.long_margin_volume,
                short_margin_volume = excluded.short_margin_volume
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
                data_start_date = CASE
                    WHEN excluded.data_start_date IS NULL THEN index_master.data_start_date
                    WHEN index_master.data_start_date IS NULL THEN excluded.data_start_date
                    WHEN excluded.data_start_date < index_master.data_start_date THEN excluded.data_start_date
                    ELSE index_master.data_start_date
                END,
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
            by_category = self.get_index_master_category_counts()
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
        by_category = self.get_index_master_category_counts()
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

    def get_options_225_data_range(self) -> dict[str, Any] | None:
        """options_225_data 統計。"""
        if not self._table_exists("options_225_data"):
            return None
        row = self._fetchone(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT date),
                MIN(date),
                MAX(date)
            FROM options_225_data
            """
        )
        if row is None or row[2] is None:
            return {
                "count": 0,
                "dateCount": 0,
                "dateRange": None,
            }
        return {
            "count": int(row[0] or 0),
            "dateCount": int(row[1] or 0),
            "dateRange": {"min": str(row[2]), "max": str(row[3])},
        }

    def get_options_225_underlying_price_issue_dates(
        self,
        *,
        issue_type: str,
        limit: int = 20,
    ) -> list[str]:
        """UnderPx integrity issue dates for options_225_data."""
        if limit <= 0 or not self._table_exists("options_225_data"):
            return []
        predicate = self._options_225_underlying_issue_predicate(issue_type)
        rows = self._fetchall(
            f"""
            WITH per_date AS (
                SELECT
                    date,
                    COUNT(*) FILTER (WHERE underlying_price IS NOT NULL) AS non_null_count,
                    COUNT(DISTINCT underlying_price) FILTER (WHERE underlying_price IS NOT NULL) AS distinct_count
                FROM options_225_data
                GROUP BY date
            )
            SELECT date
            FROM per_date
            WHERE {predicate}
            ORDER BY date DESC
            LIMIT ?
            """,
            [int(limit)],
        )
        return [str(row[0]) for row in rows if row and row[0]]

    def get_options_225_underlying_price_issue_count(self, *, issue_type: str) -> int:
        """Count UnderPx integrity issues for options_225_data."""
        if not self._table_exists("options_225_data"):
            return 0
        predicate = self._options_225_underlying_issue_predicate(issue_type)
        row = self._fetchone(
            f"""
            WITH per_date AS (
                SELECT
                    date,
                    COUNT(*) FILTER (WHERE underlying_price IS NOT NULL) AS non_null_count,
                    COUNT(DISTINCT underlying_price) FILTER (WHERE underlying_price IS NOT NULL) AS distinct_count
                FROM options_225_data
                GROUP BY date
            )
            SELECT COUNT(*)
            FROM per_date
            WHERE {predicate}
            """
        )
        return int(row[0] or 0) if row else 0

    @staticmethod
    def _options_225_underlying_issue_predicate(issue_type: str) -> str:
        if issue_type == "missing":
            return "non_null_count = 0"
        if issue_type == "conflicting":
            return "distinct_count > 1"
        raise ValueError(f"Unsupported options_225 issue type: {issue_type}")

    def get_index_master_category_counts(self) -> dict[str, int]:
        return self._load_index_master_category_counts()

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
            for table_name in ("stock_data_raw", "topix_data", "indices_data")
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
        if limit <= 0 or not self._table_exists("stock_data_raw"):
            return []
        rows = self._fetchall(
            """
            SELECT code, date, adjustment_factor, close
            FROM stock_data_raw
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

    def get_adjustment_events_count(self) -> int:
        if not self._table_exists("stock_data_raw"):
            return 0
        row = self._fetchone(
            """
            SELECT COUNT(*)
            FROM stock_data_raw
            WHERE adjustment_factor IS NOT NULL
              AND adjustment_factor != 1.0
            """
        )
        return int(row[0] or 0) if row else 0

    def get_stocks_needing_refresh(self, limit: int | None = 20) -> list[str]:
        """local projection 移行後は常に空を返す。"""
        del limit
        return []

    def get_stocks_needing_refresh_count(self) -> int:
        """local projection 移行後は常に 0 を返す。"""
        return 0

    def mark_stock_adjustments_resolved(self, codes: list[str] | None = None) -> int:
        """Deprecated no-op kept for call-site compatibility."""
        del codes
        return 0

    def get_stock_data_unique_date_count(self) -> int:
        """stock_data のユニーク日付数。"""
        if not self._table_exists("stock_data"):
            return 0
        row = self._fetchone("SELECT COUNT(DISTINCT date) FROM stock_data")
        return int(row[0] or 0) if row else 0

    def _ensure_stock_price_adjustment_mode_for_empty_db(self) -> None:
        if self._read_only:
            return
        if self.get_stock_price_adjustment_mode() is not None:
            return
        stock_count = self._count_rows("stock_data") if self._table_exists("stock_data") else 0
        raw_count = (
            self._count_rows("stock_data_raw")
            if self._table_exists("stock_data_raw")
            else 0
        )
        if stock_count > 0 or raw_count > 0:
            return
        self.set_sync_metadata(
            METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"],
            LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
        )
