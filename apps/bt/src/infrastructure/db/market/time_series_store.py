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
    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> int: ...

    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...
    def index_indices_data(self) -> None: ...
    def index_options_225_data(self) -> None: ...
    def index_margin_data(self) -> None: ...
    def index_statements(self) -> None: ...

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> "TimeSeriesInspection": ...

    def get_storage_stats(self) -> "TimeSeriesStorageStats": ...
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
    options_225_count: int = 0
    options_225_min: str | None = None
    options_225_max: str | None = None
    options_225_date_count: int = 0
    latest_options_225_date: str | None = None
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


class DuckDbParquetTimeSeriesStore:
    """DuckDB へ upsert し、Parquet を再生成する Data Plane store。"""

    _TABLE_SPECS = {
        "topix_data": _TableSpec("topix_data", "topix_data.parquet", "date"),
        # 高カーディナリティ表は export 時の全件 sort が支配的になりやすいため非ソートで出力する。
        "stock_data": _TableSpec("stock_data", "stock_data.parquet"),
        "indices_data": _TableSpec("indices_data", "indices_data.parquet"),
        "options_225_data": _TableSpec("options_225_data", "options_225_data.parquet"),
        "margin_data": _TableSpec("margin_data", "margin_data.parquet"),
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
        self._cleanup_invalid_topix_rows_on_startup()

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

    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        values = [
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
        with self._lock:
            self._conn.executemany(
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
                values,
            )
            self._dirty_tables.add("options_225_data")
        return len(rows)

    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int:
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
                INSERT INTO margin_data
                    (code, date, long_margin_volume, short_margin_volume)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (code, date) DO UPDATE
                SET long_margin_volume = excluded.long_margin_volume,
                    short_margin_volume = excluded.short_margin_volume
                """,
                values,
            )
            self._dirty_tables.add("margin_data")
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

    def index_options_225_data(self) -> None:
        self._export_if_dirty("options_225_data")

    def index_margin_data(self) -> None:
        self._export_if_dirty("margin_data")

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
            statement_codes_rows = self._conn.execute(
                "SELECT DISTINCT code FROM statements WHERE code IS NOT NULL"
            ).fetchall()
            topix_row = topix_row_raw if topix_row_raw is not None else (0, None, None)
            stock_row = stock_row_raw if stock_row_raw is not None else (0, None, None, 0)
            indices_row = indices_row_raw if indices_row_raw is not None else (0, None, None, 0)
            options_225_row = (
                options_225_row_raw if options_225_row_raw is not None else (0, None, None, 0)
            )
            margin_row = margin_row_raw if margin_row_raw is not None else (0, None, None, 0)
            statements_row = statements_row_raw if statements_row_raw is not None else (0, None)
            missing_stock_dates_count = int(missing_count_row[0] or 0) if missing_count_row else 0
            margin_codes = {
                str(row[0])
                for row in margin_codes_rows
                if row and row[0]
            }
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
                margin_orphan_count = int(margin_orphan_row[0] or 0) if margin_orphan_row else 0

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
                options_225_count=int(options_225_row[0] or 0),
                options_225_min=cast(str | None, options_225_row[1]),
                options_225_max=cast(str | None, options_225_row[2]),
                options_225_date_count=int(options_225_row[3] or 0),
                latest_options_225_date=cast(str | None, options_225_row[2]),
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
