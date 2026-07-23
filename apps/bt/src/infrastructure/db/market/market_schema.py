"""Market DuckDB schema constants and setup helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

# Market sync metadata keys
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
    "PROVIDER_PLAN": "provider_plan",
    "PROVIDER_AS_OF": "provider_as_of",
    "PROVIDER_COVERAGE_START": "provider_coverage_start",
    "PROVIDER_COVERAGE_END": "provider_coverage_end",
    "PROVIDER_SOURCE_FINGERPRINT": "provider_source_fingerprint",
    "FUNDAMENTALS_ADJUSTMENT_BASIS_DATE": "fundamentals_adjustment_basis_date",
}
PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE = "provider_adjusted_v1"
MARKET_SCHEMA_VERSION = 5
INCOMPATIBLE_MARKET_SCHEMA_VERSION = 0

ADJUSTED_DAILY_VOLUME_PHYSICAL_CONTRACT: tuple[tuple[str, str, str], ...] = (
    ("stock_data_raw", "adjusted_volume", "DOUBLE"),
    ("stock_data", "volume", "DOUBLE"),
)


class IncompatibleMarketSchemaError(RuntimeError):
    """Raised before a writable handle can mutate an older Market schema."""


def inspect_adjusted_daily_volume_physical_contract(
    connection: Any,
) -> list[str]:
    """Return read-only-safe Market v5 physical adjusted-volume mismatches."""
    issues: list[str] = []
    for table_name, column_name, expected_type in (
        ADJUSTED_DAILY_VOLUME_PHYSICAL_CONTRACT
    ):
        table_exists = connection.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        if table_exists is None:
            issues.append(f"{table_name}.{column_name}=missing (expected {expected_type})")
            continue
        column_types = {
            str(row[1]): str(row[2]).upper()
            for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            if row and len(row) > 2
        }
        observed_type = column_types.get(column_name)
        if observed_type != expected_type:
            issues.append(
                f"{table_name}.{column_name}={observed_type or 'missing'} "
                f"(expected {expected_type})"
            )
    return issues

STATS_TABLES: tuple[str, ...] = (
    "market_schema_version",
    "stocks",
    "stocks_latest",
    "stock_master_daily",
    "stock_master_intervals",
    "stock_data_raw",
    "stock_data",
    "stock_adjustment_events",
    "stock_provider_windows",
    "current_basis_fundamentals_state",
    "current_basis_recompute_pending",
    "stock_data_minute_raw",
    "topix_data",
    "indices_data",
    "options_225_data",
    "margin_data",
    "statements",
    "statement_metrics_adjusted",
    "daily_valuation",
    "daily_technical_metrics",
    "sync_metadata",
    "index_master",
    "index_membership_daily",
)

CORE_MARKET_TABLES: tuple[str, ...] = (
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

STATEMENTS_UPDATABLE_COLUMNS: tuple[str, ...] = (
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
    "forecast_sales",
    "next_year_forecast_sales",
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

STATEMENTS_ADDITIONAL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("forecast_dividend_fy", "DOUBLE"),
    ("next_year_forecast_dividend_fy", "DOUBLE"),
    ("payout_ratio", "DOUBLE"),
    ("forecast_payout_ratio", "DOUBLE"),
    ("next_year_forecast_payout_ratio", "DOUBLE"),
    ("forecast_operating_profit", "DOUBLE"),
    ("next_year_forecast_operating_profit", "DOUBLE"),
    ("forecast_sales", "DOUBLE"),
    ("next_year_forecast_sales", "DOUBLE"),
)

STATEMENT_METRICS_ADJUSTED_COLUMNS: tuple[str, ...] = (
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
STATEMENT_METRICS_ADJUSTED_RELATION = "__tmp_statement_metrics_adjusted_upsert"

DAILY_VALUATION_COLUMNS: tuple[str, ...] = (
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
DAILY_VALUATION_TABLE_DDL = """
    CREATE TABLE daily_valuation (
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

DAILY_TECHNICAL_METRICS_COLUMNS: tuple[str, ...] = (
    "code",
    "date",
    "close",
    "sma5",
    "sma5_sessions",
    "close_above_sma5_flag",
    "sma5_above_count_5d",
    "sma5_above_count_sessions",
    "sma5_above_count_group",
    "sma5_below_streak",
    "created_at",
)

DAILY_TECHNICAL_METRICS_ADDITIONAL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("sma5_below_streak", "INTEGER"),
)

STOCK_MASTER_DAILY_COLUMNS: tuple[str, ...] = (
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
STOCK_MASTER_DAILY_RELATION = "__tmp_stock_master_daily_publish"

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS market_schema_version (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL,
        notes TEXT
    )
    """,
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
    """,
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
    """,
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
    """,
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
    """,
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
        adjusted_volume DOUBLE,
        created_at TEXT,
        PRIMARY KEY (code, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_data (
        code TEXT NOT NULL,
        date TEXT NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume DOUBLE NOT NULL,
        adjustment_factor DOUBLE,
        created_at TEXT,
        PRIMARY KEY (code, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_adjustment_events (
        code TEXT NOT NULL,
        date TEXT NOT NULL,
        adjustment_factor DOUBLE NOT NULL
            CHECK (adjustment_factor > 0 AND adjustment_factor <> 1),
        source_fingerprint TEXT NOT NULL,
        created_at TEXT,
        PRIMARY KEY (code, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_provider_windows (
        code TEXT PRIMARY KEY,
        coverage_start TEXT NOT NULL,
        coverage_end TEXT NOT NULL,
        provider_plan TEXT NOT NULL,
        provider_as_of TEXT NOT NULL,
        source_fingerprint TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS current_basis_fundamentals_state (
        code TEXT PRIMARY KEY,
        fundamentals_adjustment_basis_date TEXT NOT NULL,
        source_fingerprint TEXT NOT NULL,
        statement_count BIGINT NOT NULL CHECK (statement_count >= 0),
        materialized_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS current_basis_recompute_pending (
        code TEXT PRIMARY KEY,
        reason TEXT NOT NULL,
        source_fingerprint TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS topix_data (
        date TEXT PRIMARY KEY,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        created_at TEXT
    )
    """,
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
    """,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS margin_data (
        code TEXT,
        date TEXT,
        long_margin_volume DOUBLE,
        short_margin_volume DOUBLE,
        PRIMARY KEY (code, date)
    )
    """,
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
        forecast_sales DOUBLE,
        next_year_forecast_sales DOUBLE,
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
    """,
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_statement_metrics_adjusted_code_disclosed
    ON statement_metrics_adjusted(code, disclosed_date)
    """,
    """
    CREATE OR REPLACE VIEW daily_valuation AS
    WITH statement_source AS (
        SELECT * EXCLUDE (alias_rank) FROM (
            SELECT statements.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           CASE
                               WHEN length(statements.code) = 5
                                AND right(statements.code, 1) = '0'
                               THEN left(statements.code, 4)
                               ELSE statements.code
                           END,
                           statements.statement_id
                       ORDER BY CASE WHEN length(statements.code) = 4 THEN 0 ELSE 1 END,
                                length(statements.code), statements.code
                   ) AS alias_rank
            FROM statements
        ) WHERE alias_rank = 1
    ), metric_source AS (
        SELECT metrics.*, statement_source.type_of_document
        FROM statement_metrics_adjusted AS metrics
        LEFT JOIN statement_source
          ON CASE
                 WHEN length(statement_source.code) = 5
                  AND right(statement_source.code, 1) = '0'
                 THEN left(statement_source.code, 4)
                 ELSE statement_source.code
             END = metrics.code
         AND statement_source.statement_id = metrics.statement_id
    ), eps_metrics AS (
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY code, disclosed_at ORDER BY statement_id DESC
            ) AS rn
            FROM metric_source WHERE adjusted_eps IS NOT NULL
        ) WHERE rn = 1
    ), bps_metrics AS (
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY code, disclosed_at ORDER BY statement_id DESC
            ) AS rn
            FROM metric_source WHERE adjusted_bps IS NOT NULL
        ) WHERE rn = 1
    ), forecast_metrics AS (
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY code, disclosed_at ORDER BY statement_id DESC
            ) AS rn
            FROM metric_source WHERE adjusted_forecast_eps IS NOT NULL
        ) WHERE rn = 1
    ), share_metrics AS (
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY code, disclosed_at ORDER BY statement_id DESC
            ) AS rn
            FROM metric_source WHERE adjusted_shares_outstanding IS NOT NULL
        ) WHERE rn = 1
    )
    SELECT
        prices.code,
        prices.date,
        prices.date AS price_basis_date,
        prices.close,
        eps_metric.adjusted_eps AS eps,
        bps_metric.adjusted_bps AS bps,
        forecast_metric.adjusted_forecast_eps AS forward_eps,
        prices.close / NULLIF(eps_metric.adjusted_eps, 0) AS per,
        prices.close / NULLIF(forecast_metric.adjusted_forecast_eps, 0) AS forward_per,
        CAST(NULL AS DOUBLE) AS sales,
        CAST(NULL AS DOUBLE) AS forward_sales,
        CAST(NULL AS DOUBLE) AS psr,
        CAST(NULL AS DOUBLE) AS forward_psr,
        CAST(NULL AS DOUBLE) AS p_op,
        CAST(NULL AS DOUBLE) AS forward_p_op,
        prices.close / NULLIF(bps_metric.adjusted_bps, 0) AS pbr,
        prices.close * share_metric.adjusted_shares_outstanding AS market_cap,
        CASE
            WHEN share_metric.adjusted_shares_outstanding IS NULL THEN NULL
            ELSE prices.close * GREATEST(
                share_metric.adjusted_shares_outstanding
                - COALESCE(share_metric.adjusted_treasury_shares, 0),
                0
            )
        END AS free_float_market_cap,
        eps_metric.disclosed_date AS statement_disclosed_date,
        forecast_metric.disclosed_date AS forward_eps_disclosed_date,
        CASE
            WHEN forecast_metric.adjusted_forecast_eps IS NULL THEN NULL
            WHEN contains(
                coalesce(forecast_metric.type_of_document, ''),
                'ForecastRevision'
            ) OR upper(coalesce(forecast_metric.period_type, '')) <> 'FY'
            THEN 'revised'
            ELSE 'fy'
        END AS forward_eps_source,
        CAST(NULL AS TEXT) AS forward_sales_disclosed_date,
        CAST(NULL AS TEXT) AS forward_sales_source,
        eps_metric.statement_id,
        eps_metric.disclosed_at AS statement_disclosed_at,
        COALESCE(
            eps_metric.fundamentals_adjustment_basis_date,
            bps_metric.fundamentals_adjustment_basis_date,
            forecast_metric.fundamentals_adjustment_basis_date,
            share_metric.fundamentals_adjustment_basis_date
        ) AS fundamentals_adjustment_basis_date,
        COALESCE(
            eps_metric.source_fingerprint,
            bps_metric.source_fingerprint,
            forecast_metric.source_fingerprint,
            share_metric.source_fingerprint
        ) AS source_fingerprint,
        prices.created_at
    FROM stock_data AS prices
    ASOF LEFT JOIN eps_metrics AS eps_metric
      ON CASE
             WHEN length(prices.code) = 5 AND right(prices.code, 1) = '0'
             THEN left(prices.code, 4)
             ELSE prices.code
         END = eps_metric.code
     AND prices.date || 'T23:59:59+09:00' >= eps_metric.disclosed_at
    ASOF LEFT JOIN bps_metrics AS bps_metric
      ON CASE
             WHEN length(prices.code) = 5 AND right(prices.code, 1) = '0'
             THEN left(prices.code, 4)
             ELSE prices.code
         END = bps_metric.code
     AND prices.date || 'T23:59:59+09:00' >= bps_metric.disclosed_at
    ASOF LEFT JOIN forecast_metrics AS forecast_metric
      ON CASE
             WHEN length(prices.code) = 5 AND right(prices.code, 1) = '0'
             THEN left(prices.code, 4)
             ELSE prices.code
         END = forecast_metric.code
     AND prices.date || 'T23:59:59+09:00' >= forecast_metric.disclosed_at
    ASOF LEFT JOIN share_metrics AS share_metric
      ON CASE
             WHEN length(prices.code) = 5 AND right(prices.code, 1) = '0'
             THEN left(prices.code, 4)
             ELSE prices.code
         END = share_metric.code
     AND prices.date || 'T23:59:59+09:00' >= share_metric.disclosed_at
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_technical_metrics (
        code TEXT,
        date TEXT,
        close DOUBLE,
        sma5 DOUBLE,
        sma5_sessions INTEGER,
        close_above_sma5_flag INTEGER,
        sma5_above_count_5d INTEGER,
        sma5_above_count_sessions INTEGER,
        sma5_above_count_group TEXT,
        sma5_below_streak INTEGER,
        created_at TEXT,
        PRIMARY KEY (code, date)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_daily_technical_metrics_date_code
    ON daily_technical_metrics(date, code)
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT
    )
    """,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS index_membership_daily (
        date TEXT,
        index_code TEXT,
        code TEXT,
        created_at TEXT,
        PRIMARY KEY (date, index_code, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_master_derivation_pending (
        code TEXT PRIMARY KEY,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_master_frontier_pending (
        date TEXT PRIMARY KEY,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_adjustment_refresh_state (
        code TEXT PRIMARY KEY,
        resolved_adjustment_date TEXT,
        updated_at TEXT
    )
    """,
)


def ensure_market_schema(store: Any) -> None:
    """Ensure market DuckDB tables and compatibility metadata exist."""
    store._assert_writable()

    existing_before = store._existing_table_names()
    had_schema_version = "market_schema_version" in existing_before
    had_legacy_market_tables = any(table_name in existing_before for table_name in CORE_MARKET_TABLES)

    if had_schema_version:
        row = store._fetchone("SELECT MAX(version) FROM market_schema_version")
        existing_version = int(row[0]) if row and row[0] is not None else None
        if existing_version != MARKET_SCHEMA_VERSION:
            raise IncompatibleMarketSchemaError(
                "Incompatible market schema version "
                f"{existing_version}; required version {MARKET_SCHEMA_VERSION}"
            )

    if not had_schema_version and had_legacy_market_tables:
        store._execute(
            """
            CREATE TABLE IF NOT EXISTS market_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL,
                notes TEXT
            )
            """
        )
        _ensure_market_schema_version(
            store,
            had_schema_version=False,
            had_legacy_market_tables=True,
        )
        return

    daily_valuation_relation = store._fetchone(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'daily_valuation'
        """
    )
    daily_valuation_is_table = (
        daily_valuation_relation is not None
        and str(daily_valuation_relation[0]).upper() == "BASE TABLE"
    )
    for statement in SCHEMA_STATEMENTS:
        if (
            daily_valuation_is_table
            and "CREATE OR REPLACE VIEW daily_valuation AS" in statement
        ):
            continue
        store._execute(statement)

    _ensure_market_schema_version(
        store,
        had_schema_version=had_schema_version,
        had_legacy_market_tables=had_legacy_market_tables,
    )
    _ensure_statements_columns(store)
    _ensure_daily_technical_metrics_columns(store)
    if "daily_valuation" not in existing_before:
        store._execute("DROP VIEW daily_valuation")
        store._execute(DAILY_VALUATION_TABLE_DDL)
        store._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_valuation_date_code
            ON daily_valuation(date, code)
            """
        )
    _ensure_stock_price_adjustment_mode_for_empty_db(store)


def _ensure_market_schema_version(
    store: Any,
    *,
    had_schema_version: bool,
    had_legacy_market_tables: bool,
) -> None:
    """Record schema v5 for fresh DBs; mark unversioned DBs incompatible."""
    if had_schema_version:
        return
    version = (
        INCOMPATIBLE_MARKET_SCHEMA_VERSION
        if had_legacy_market_tables
        else MARKET_SCHEMA_VERSION
    )
    notes = (
        "unversioned market.duckdb detected; destructive initial sync reset is required"
        if version == INCOMPATIBLE_MARKET_SCHEMA_VERSION
        else "market.duckdb schema v5"
    )
    store._execute(
        """
        INSERT INTO market_schema_version (version, applied_at, notes)
        VALUES (?, ?, ?)
        ON CONFLICT (version) DO UPDATE
        SET applied_at = excluded.applied_at,
            notes = excluded.notes
        """,
        [version, datetime.now().isoformat(), notes],
    )


def _ensure_statements_columns(store: Any) -> None:
    existing_columns = _table_columns(store, "statements")
    for column_name, column_type in STATEMENTS_ADDITIONAL_COLUMNS:
        if column_name in existing_columns:
            continue
        store._execute(
            f"ALTER TABLE statements ADD COLUMN {store._quote_identifier(column_name)} {column_type}"
        )


def _ensure_daily_technical_metrics_columns(store: Any) -> None:
    if not store._table_exists("daily_technical_metrics"):
        return
    existing_columns = _table_columns(store, "daily_technical_metrics")
    for column_name, column_type in DAILY_TECHNICAL_METRICS_ADDITIONAL_COLUMNS:
        if column_name in existing_columns:
            continue
        store._execute(
            f"ALTER TABLE daily_technical_metrics ADD COLUMN {store._quote_identifier(column_name)} {column_type}"
        )


def _ensure_stock_price_adjustment_mode_for_empty_db(store: Any) -> None:
    if store._read_only:
        return
    if store.get_stock_price_adjustment_mode() is not None:
        return
    stock_count = store._count_rows("stock_data") if store._table_exists("stock_data") else 0
    raw_count = store._count_rows("stock_data_raw") if store._table_exists("stock_data_raw") else 0
    if stock_count > 0 or raw_count > 0:
        return
    store.set_sync_metadata(
        METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"],
        PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    )


def _table_columns(store: Any, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in store._fetchall(f"PRAGMA table_info('{table_name}')")
        if row and len(row) > 1
    }
