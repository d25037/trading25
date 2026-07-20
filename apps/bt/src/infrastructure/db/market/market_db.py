"""
Market Metadata DB Access (DuckDB)

market time-series SoT (DuckDB/Parquet) と同じ DuckDB ファイル上で、
metadata / reference data（stocks, sync_metadata, index_master）と
補助クエリを扱う。
"""

from __future__ import annotations

from collections.abc import Sequence
import os
import threading
from pathlib import Path
from typing import Any, cast

from src.infrastructure.db.market import adjustment_basis_queries as _adjustment_basis_queries
from src.infrastructure.db.market import metadata_writers as _metadata_writers
from src.infrastructure.db.market import stock_master_writers as _stock_master_writers
from src.infrastructure.db.market import technical_metric_writers as _technical_metric_writers
from src.infrastructure.db.market.duckdb_connection import (
    MarketWriterToken,
    connect_market_duckdb,
)
from src.infrastructure.db.market.market_schema import (
    IncompatibleMarketSchemaError,
    INCOMPATIBLE_MARKET_SCHEMA_VERSION,
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    STATS_TABLES as _STATS_TABLES,
    ensure_market_schema,
)
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.infrastructure.db.market.stock_master_writers import (
    StockMasterFrontierResult,
    StockMasterPublicationResult,
)
from src.infrastructure.db.market import stock_master_queries as _stock_master_queries
from src.infrastructure.db.market.valuation_queries import (
    get_adjusted_metrics_source_diagnostics as _get_adjusted_metrics_source_diagnostics,
    get_adjusted_metrics_snapshot as _get_adjusted_metrics_snapshot,
    get_adjusted_statement_metrics as _get_adjusted_statement_metrics,
    get_adjusted_statement_metrics_for_basis as _get_adjusted_statement_metrics_for_basis,
    get_daily_valuation as _get_daily_valuation,
    get_daily_valuation_for_basis as _get_daily_valuation_for_basis,
    get_daily_valuation_for_codes as _get_daily_valuation_for_codes,
)
from src.infrastructure.db.market.valuation_writers import (
    AdjustedBasisMaterializationPlan,
    AdjustedBasisPublishResult,
    BasisSnapshot,
    AdjustedMaterializationSource,
    AdjustedMarketSessions,
    CurrentBasisFundamentalsSource,
    AdjustedRelationPublishResult,
    load_current_basis_fundamentals_source as _load_current_basis_fundamentals_source,
    load_adjusted_materialization_source as _load_adjusted_materialization_source,
    load_adjusted_market_sessions as _load_adjusted_market_sessions,
    load_basis_snapshots as _load_basis_snapshots,
    publish_adjusted_basis_materialization as _publish_adjusted_basis_materialization,
    publish_current_basis_statement_metrics as _publish_current_basis_statement_metrics,
)
from src.shared.utils.market_code_alias import expand_market_codes

_PRIME_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["prime"]))
_FUNDAMENTALS_TARGET_MARKET_CODES: tuple[str, ...] = (
    "0111",
    "0112",
    "0113",
    "prime",
    "standard",
    "growth",
)

__all__ = [
    "IncompatibleMarketSchemaError",
    "INCOMPATIBLE_MARKET_SCHEMA_VERSION",
    "MARKET_SCHEMA_VERSION",
    "METADATA_KEYS",
    "PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE",
    "MarketDb",
]


class MarketDbInitializationFencedError(RuntimeError):
    """Schema initialization failed and the writable connection did not close."""


class MarketDb:
    """DuckDB ベースの market metadata / helper query アクセス。"""

    def __init__(
        self,
        db_path: str,
        *,
        read_only: bool = True,
        writer_token: MarketWriterToken | None = None,
    ) -> None:
        self._db_path = str(db_path)
        self._read_only = read_only
        if not read_only:
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = cast(
            Any,
            connect_market_duckdb(
                self._db_path,
                read_only=read_only,
                writer_token=writer_token,
            ),
        )
        self._lock = threading.RLock()
        if not read_only:
            try:
                self.ensure_schema()
            except BaseException as initialization_error:
                try:
                    self.close()
                except BaseException as close_error:
                    fenced = MarketDbInitializationFencedError(
                        "MarketDb initialization failed and connection close failed"
                    )
                    fenced.__cause__ = initialization_error
                    fenced.add_note(f"MarketDb connection close failure: {close_error}")
                    raise fenced
                raise

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

    def _fetchall_dicts(
        self,
        sql: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            result = self._conn.execute(sql) if params is None else self._conn.execute(sql, params)
            columns = [str(desc[0]) for desc in result.description]
            return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]

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

    def _fetch_date_stats(self, table_name: str) -> tuple[int, int, str, str] | None:
        if not self._table_exists(table_name):
            return None
        escaped = self._quote_identifier(table_name)
        row = self._fetchone(
            f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT date),
                MIN(date),
                MAX(date)
            FROM {escaped}
            """
        )
        if row is None or row[2] is None:
            return None
        return int(row[0] or 0), int(row[1] or 0), str(row[2]), str(row[3])

    def _existing_table_names(self) -> set[str]:
        return {
            str(row[0])
            for row in self._fetchall("SELECT table_name FROM information_schema.tables")
            if row and row[0]
        }

    def ensure_schema(self) -> None:
        """不足テーブルを補完する（DuckDB SoT）。"""
        ensure_market_schema(self)

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
        if adjustment_mode != PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE:
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

    def load_raw_adjustment_points(
        self,
        codes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Load normalized raw adjustment facts from ``stock_data_raw`` only."""
        return _adjustment_basis_queries.load_raw_adjustment_points(
            self._fetchall_dicts,
            codes,
        )

    def list_adjustment_materialization_codes(self) -> list[str]:
        """Enumerate normalized raw/catalog codes without loading raw rows."""
        return _adjustment_basis_queries.list_adjustment_materialization_codes(
            self._fetchall
        )

    def get_ready_adjustment_basis(
        self,
        code: str,
        effective_market_date: str,
    ) -> dict[str, Any] | None:
        """Resolve a ready basis by containing interval and coverage frontier."""
        return _adjustment_basis_queries.get_ready_adjustment_basis(
            self._fetchall_dicts,
            code,
            effective_market_date,
        )

    def get_adjustment_basis_segments(
        self,
        code: str,
        basis_id: str,
    ) -> list[dict[str, Any]]:
        return _adjustment_basis_queries.get_adjustment_basis_segments(
            self._fetchall_dicts,
            code,
            basis_id,
        )

    def get_basis_adjusted_stock_data(
        self,
        code: str,
        basis_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        """Project basis-adjusted OHLCV exclusively from raw prices and segments."""
        return _adjustment_basis_queries.get_basis_adjusted_stock_data(
            self._fetchall_dicts,
            code,
            basis_id,
            start=start,
            end=end,
        )

    def get_latest_trading_date(self) -> str | None:
        """topix_data の最新取引日を取得。"""
        return _stock_master_queries.get_latest_table_date(
            self._table_exists,
            self._fetchone,
            table_name="topix_data",
        )

    def get_latest_stock_data_date(self) -> str | None:
        """stock_data の最新取引日を取得。"""
        return _stock_master_queries.get_latest_table_date(
            self._table_exists,
            self._fetchone,
            table_name="stock_data",
        )

    def get_adjusted_statement_metrics(
        self,
        code: str,
        *,
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """Canonical adjusted statement metrics を code/as-of で取得。"""
        return _get_adjusted_statement_metrics(
            self._table_exists, self._fetchall_dicts, code, as_of_date
        )

    def get_adjusted_statement_metrics_for_basis(
        self,
        code: str,
        *,
        basis_id: str,
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return _get_adjusted_statement_metrics_for_basis(
            self._table_exists,
            self._fetchall_dicts,
            code,
            basis_id=basis_id,
            as_of_date=as_of_date,
        )

    def get_daily_valuation(
        self,
        code: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        """Canonical daily valuation metrics を code/date range で取得。"""
        return _get_daily_valuation(
            self._table_exists, self._fetchall_dicts, code, start, end
        )

    def get_daily_valuation_for_basis(
        self,
        code: str,
        *,
        basis_id: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        return _get_daily_valuation_for_basis(
            self._table_exists,
            self._fetchall_dicts,
            code,
            basis_id=basis_id,
            start=start,
            end=end,
        )

    def get_daily_valuation_for_codes(
        self,
        codes: list[str],
        date: str,
    ) -> list[dict[str, Any]]:
        """Canonical daily valuation metrics を同一日付の複数codeで取得。"""
        return _get_daily_valuation_for_codes(
            self._table_exists, self._fetchall_dicts, codes, date
        )

    def get_adjusted_metrics_snapshot(self) -> dict[str, Any]:
        """Adjusted metrics materialization freshness snapshot."""
        return _get_adjusted_metrics_snapshot(
            self._table_exists, self._count_rows, self._fetchone
        )

    def get_adjusted_metrics_source_diagnostics(self) -> dict[str, int]:
        """Compare adjusted metrics with exact raw source keys and provenance."""
        return _get_adjusted_metrics_source_diagnostics(
            self._table_exists,
            lambda sql, params: self._fetchone(
                sql,
                list(params) if params is not None else None,
            ),
        )

    def get_topix_dates(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """topix_data の取引日一覧を取得。"""
        return _stock_master_queries.get_topix_dates(
            self._table_exists,
            self._fetchall,
            start_date=start_date,
            end_date=end_date,
        )

    def get_latest_stock_master_date(self) -> str | None:
        """stock_master_daily の最新スナップショット日を取得。"""
        return _stock_master_queries.get_latest_table_date(
            self._table_exists,
            self._fetchone,
            table_name="stock_master_daily",
        )

    def get_missing_stock_master_dates(self, *, limit: int | None = 20) -> list[str]:
        """TOPIX 取引日のうち daily master が存在しない日付を取得。"""
        return _stock_master_queries.get_missing_stock_master_dates(
            self._table_exists,
            self._fetchall,
            limit=limit,
        )

    def get_missing_stock_master_dates_count(self) -> int:
        """TOPIX 取引日のうち daily master が存在しない日付数。"""
        return _stock_master_queries.get_missing_stock_master_dates_count(
            self._table_exists,
            self._fetchone,
        )

    def get_stock_master_rows_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """指定日の PIT 銘柄マスタ行を取得。latest fallback はしない。"""
        return _stock_master_queries.get_stock_master_rows_for_date(
            self._table_exists,
            self._fetchall,
            as_of_date,
            market_codes=market_codes,
            scale_categories=scale_categories,
        )

    def get_stock_master_codes_for_date(
        self,
        as_of_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
        exclude_scale_categories: list[str] | None = None,
    ) -> list[str]:
        """指定日の PIT 銘柄コードだけを取得。latest fallback はしない。"""
        return _stock_master_queries.get_stock_master_codes_for_date(
            self._table_exists,
            self._fetchall,
            as_of_date,
            market_codes=market_codes,
            scale_categories=scale_categories,
            exclude_scale_categories=exclude_scale_categories,
        )

    def get_stock_master_codes_for_date_range(
        self,
        start_date: str,
        end_date: str,
        *,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
        exclude_scale_categories: list[str] | None = None,
    ) -> list[str]:
        """指定期間内に条件を満たした PIT 銘柄コードの superset を取得する。"""
        return _stock_master_queries.get_stock_master_codes_for_date_range(
            self._table_exists,
            self._fetchall,
            start_date,
            end_date,
            market_codes=market_codes,
            scale_categories=scale_categories,
            exclude_scale_categories=exclude_scale_categories,
        )

    def get_stock_master_code_dates_for_date_range(
        self,
        start_date: str,
        end_date: str,
        *,
        codes: list[str] | None = None,
        market_codes: list[str] | None = None,
        scale_categories: list[str] | None = None,
        exclude_scale_categories: list[str] | None = None,
    ) -> list[tuple[str, str]]:
        """指定期間内に universe 条件を満たした (date, code) を取得する。"""
        return _stock_master_queries.get_stock_master_code_dates_for_date_range(
            self._table_exists,
            self._fetchall,
            start_date,
            end_date,
            codes=codes,
            market_codes=market_codes,
            scale_categories=scale_categories,
            exclude_scale_categories=exclude_scale_categories,
        )

    def get_index_membership_codes(self, as_of_date: str, index_code: str) -> set[str]:
        """指定日の指数 membership code set。latest fallback はしない。"""
        return _stock_master_queries.get_index_membership_codes(
            self._table_exists,
            self._fetchall,
            as_of_date,
            index_code,
        )

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

    def upsert_stocks(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        """stocks テーブルに upsert。"""
        self._assert_writable()
        return _stock_master_writers.upsert_stocks(self._conn, self._lock, rows)

    def publish_stock_master_daily_rows(
        self, rows: list[dict[str, Any]], *, derive: bool = True
    ) -> StockMasterPublicationResult:
        """Publish canonical dated stock-master rows and exact derived deltas."""
        self._assert_writable()
        return _stock_master_writers.publish_stock_master_daily_rows(
            self._conn,
            self._lock,
            rows,
            derive=derive,
        )

    def get_stock_master_pending_derivation_codes(self) -> set[str]:
        return _stock_master_writers.get_stock_master_pending_derivation_codes(
            self._conn, self._lock
        )

    def reconcile_stock_master_derived_codes(
        self, codes: set[str]
    ) -> StockMasterPublicationResult:
        self._assert_writable()
        return _stock_master_writers.reconcile_stock_master_derived_codes(
            self._conn, self._lock, frozenset(codes)
        )

    def get_stock_master_pending_frontier_dates(self) -> set[str]:
        return _stock_master_writers.get_stock_master_pending_frontier_dates(
            self._conn, self._lock
        )

    def reconcile_stock_master_frontier(
        self, snapshot_date: str
    ) -> StockMasterFrontierResult:
        """Reconcile a stock-master frontier proven complete by the sync stage."""
        self._assert_writable()
        return _stock_master_writers.reconcile_stock_master_frontier(
            self._conn, self._lock, snapshot_date
        )

    def load_basis_snapshots(self, code: str) -> dict[str, BasisSnapshot]:
        """Load exact persisted basis graphs for differential planning."""
        return _load_basis_snapshots(self._conn, self._lock, code)

    def load_adjusted_materialization_source(
        self,
        code: str,
        *,
        market_sessions: Sequence[str] | None = None,
        market_sessions_fingerprint: str | None = None,
    ) -> AdjustedMaterializationSource:
        return _load_adjusted_materialization_source(
            self._conn,
            self._lock,
            code,
            market_sessions=market_sessions,
            market_sessions_fingerprint=market_sessions_fingerprint,
        )

    def load_adjusted_market_sessions(self) -> AdjustedMarketSessions:
        return _load_adjusted_market_sessions(self._conn, self._lock)

    def load_current_basis_fundamentals_source(
        self, code: str
    ) -> CurrentBasisFundamentalsSource | None:
        return _load_current_basis_fundamentals_source(self._conn, self._lock, code)

    def list_current_basis_recompute_pending_codes(self) -> list[str]:
        return [
            str(row[0])
            for row in self._fetchall(
                "SELECT code FROM current_basis_recompute_pending ORDER BY code"
            )
        ]

    def publish_current_basis_statement_metrics(
        self,
        code: str,
        rows: list[dict[str, Any]],
        *,
        expected_source_fingerprint: str,
    ) -> AdjustedRelationPublishResult:
        self._assert_writable()
        return _publish_current_basis_statement_metrics(
            self._conn,
            self._lock,
            code,
            rows,
            expected_source_fingerprint=expected_source_fingerprint,
        )

    def rebuild_daily_technical_metrics_from_stock_data(
        self,
    ) -> _technical_metric_writers.TechnicalMetricRebuildResult:
        """Canonical daily technical metrics を stock_data から一括再生成する。"""
        self._assert_writable()
        return _technical_metric_writers.rebuild_daily_technical_metrics_from_stock_data(
            self._conn,
            self._lock,
            self._table_exists,
        )

    def publish_adjusted_basis_materialization(
        self, plan: AdjustedBasisMaterializationPlan
    ) -> AdjustedBasisPublishResult:
        """Publish lineage, adjusted statements, and valuation atomically."""
        self._assert_writable()
        return self._commit_basis_publish(plan)

    def _commit_basis_publish(
        self, plan: AdjustedBasisMaterializationPlan
    ) -> AdjustedBasisPublishResult:
        return _publish_adjusted_basis_materialization(self._conn, self._lock, plan)

    def set_sync_metadata(self, key: str, value: str) -> None:
        """sync_metadata にキーバリューを設定（upsert）。"""
        self._assert_writable()
        _metadata_writers.set_sync_metadata(self._execute, key, value)

    def upsert_index_master(self, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
        """index_master テーブルに upsert。"""
        self._assert_writable()
        return _metadata_writers.upsert_index_master(self._conn, self._lock, rows)

    # --- Stats (Phase 3D-2) ---

    def get_topix_date_range(self) -> dict[str, Any] | None:
        """TOPIX 日付範囲 + 件数。"""
        stats = self._fetch_date_stats("topix_data")
        if stats is None:
            return None
        count, _date_count, min_date, max_date = stats
        return {"count": count, "min": min_date, "max": max_date}

    def get_stock_data_date_range(self) -> dict[str, Any] | None:
        """stock_data 日付範囲 + 統計。"""
        stats = self._fetch_date_stats("stock_data")
        if stats is None:
            return None
        count, date_count, min_date, max_date = stats
        avg_per_day = count / date_count if date_count > 0 else 0.0
        return {
            "count": count,
            "min": min_date,
            "max": max_date,
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

        stats = self._fetch_date_stats("indices_data")
        by_category = self.get_index_master_category_counts()
        if stats is None:
            return {
                "masterCount": master_count,
                "dataCount": 0,
                "dateCount": 0,
                "dateRange": None,
                "byCategory": by_category,
            }
        count, date_count, min_date, max_date = stats
        return {
            "masterCount": master_count,
            "dataCount": count,
            "dateCount": date_count,
            "dateRange": {"min": min_date, "max": max_date},
            "byCategory": by_category,
        }

    def get_options_225_data_range(self) -> dict[str, Any] | None:
        """options_225_data 統計。"""
        stats = self._fetch_date_stats("options_225_data")
        if stats is None and not self._table_exists("options_225_data"):
            return None
        if stats is None:
            return {
                "count": 0,
                "dateCount": 0,
                "dateRange": None,
            }
        count, date_count, min_date, max_date = stats
        return {
            "count": count,
            "dateCount": date_count,
            "dateRange": {"min": min_date, "max": max_date},
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
        """Provider-adjusted price migration後は常に空を返す。"""
        del limit
        return []

    def get_stocks_needing_refresh_count(self) -> int:
        """Provider-adjusted price migration後は常に 0 を返す。"""
        return 0

    def get_stock_data_unique_date_count(self) -> int:
        """stock_data のユニーク日付数。"""
        if not self._table_exists("stock_data"):
            return 0
        row = self._fetchone("SELECT COUNT(DISTINCT date) FROM stock_data")
        return int(row[0] or 0) if row else 0
