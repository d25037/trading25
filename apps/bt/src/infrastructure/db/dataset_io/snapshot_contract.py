"""Canonical physical artifact contract for Dataset v4 bundles."""

from __future__ import annotations

DATASET_PROVIDER_COVERAGE_START_INFO_KEY = "provider_coverage_start"
DATASET_PROVIDER_COVERAGE_END_INFO_KEY = "provider_coverage_end"
DATASET_PROVIDER_AS_OF_INFO_KEY = "provider_as_of"
DATASET_PROVIDER_PLAN_INFO_KEY = "provider_plan"
DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY = "provider_source_fingerprint"
DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY = "fundamentals_adjustment_basis_date"

DATASET_V4_SOURCE_INFO_KEYS = frozenset(
    {
        DATASET_PROVIDER_COVERAGE_START_INFO_KEY,
        DATASET_PROVIDER_COVERAGE_END_INFO_KEY,
        DATASET_PROVIDER_AS_OF_INFO_KEY,
        DATASET_PROVIDER_PLAN_INFO_KEY,
        DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY,
        DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY,
    }
)

MARKET_V5_PROVIDER_REQUIRED_TABLES = frozenset(
    {
        "market_schema_version",
        "sync_metadata",
        "topix_data",
        "stock_data",
        "stock_data_raw",
        "stock_master_daily",
        "stock_provider_windows",
        "current_basis_fundamentals_state",
        "current_basis_recompute_pending",
        "statements",
        "statement_metrics_adjusted",
        "daily_valuation",
    }
)

DATASET_V4_REQUIRED_TABLES = frozenset(
    {
        "stocks",
        "stock_data",
        "topix_data",
        "indices_data",
        "margin_data",
        "statements",
        "stock_data_raw",
        "stock_master_daily",
        "statement_metrics_adjusted",
        "daily_valuation",
        "dataset_info",
    }
)

# Exact DuckDB schema emitted by DatasetWriter. Column tuples are
# (name, DuckDB type, NOT NULL); the second tuple is the ordered primary key.
DATASET_V4_PHYSICAL_SCHEMAS: dict[
    str, tuple[tuple[tuple[str, str, bool], ...], tuple[str, ...]]
] = {
    "daily_valuation": ((("code", "VARCHAR", True), ("date", "VARCHAR", True), ("price_basis_date", "VARCHAR", True), ("close", "DOUBLE", False), ("eps", "DOUBLE", False), ("bps", "DOUBLE", False), ("forward_eps", "DOUBLE", False), ("per", "DOUBLE", False), ("forward_per", "DOUBLE", False), ("sales", "DOUBLE", False), ("forward_sales", "DOUBLE", False), ("psr", "DOUBLE", False), ("forward_psr", "DOUBLE", False), ("p_op", "DOUBLE", False), ("forward_p_op", "DOUBLE", False), ("pbr", "DOUBLE", False), ("market_cap", "DOUBLE", False), ("free_float_market_cap", "DOUBLE", False), ("statement_disclosed_date", "VARCHAR", False), ("forward_eps_disclosed_date", "VARCHAR", False), ("forward_eps_source", "VARCHAR", False), ("forward_sales_disclosed_date", "VARCHAR", False), ("forward_sales_source", "VARCHAR", False), ("statement_id", "VARCHAR", False), ("statement_disclosed_at", "VARCHAR", False), ("fundamentals_adjustment_basis_date", "VARCHAR", False), ("source_fingerprint", "VARCHAR", False), ("created_at", "VARCHAR", False)), ("code", "date")),
    "dataset_info": ((("key", "VARCHAR", True), ("value", "VARCHAR", True), ("updated_at", "VARCHAR", False)), ("key",)),
    "indices_data": ((("code", "VARCHAR", True), ("date", "VARCHAR", True), ("open", "DOUBLE", False), ("high", "DOUBLE", False), ("low", "DOUBLE", False), ("close", "DOUBLE", False), ("sector_name", "VARCHAR", False), ("created_at", "VARCHAR", False)), ("code", "date")),
    "margin_data": ((("code", "VARCHAR", True), ("date", "VARCHAR", True), ("long_margin_volume", "DOUBLE", False), ("short_margin_volume", "DOUBLE", False)), ("code", "date")),
    "statement_metrics_adjusted": ((("code", "VARCHAR", True), ("statement_id", "VARCHAR", True), ("disclosed_date", "VARCHAR", True), ("disclosed_at", "VARCHAR", True), ("period_end", "VARCHAR", True), ("period_type", "VARCHAR", True), ("fundamentals_adjustment_basis_date", "VARCHAR", True), ("raw_eps", "DOUBLE", False), ("adjusted_eps", "DOUBLE", False), ("raw_diluted_eps", "DOUBLE", False), ("adjusted_diluted_eps", "DOUBLE", False), ("raw_bps", "DOUBLE", False), ("adjusted_bps", "DOUBLE", False), ("raw_forecast_eps", "DOUBLE", False), ("adjusted_forecast_eps", "DOUBLE", False), ("raw_dividend_fy", "DOUBLE", False), ("adjusted_dividend_fy", "DOUBLE", False), ("raw_forecast_dividend_fy", "DOUBLE", False), ("adjusted_forecast_dividend_fy", "DOUBLE", False), ("raw_shares_outstanding", "DOUBLE", False), ("adjusted_shares_outstanding", "DOUBLE", False), ("raw_treasury_shares", "DOUBLE", False), ("adjusted_treasury_shares", "DOUBLE", False), ("adjustment_factor_cumulative", "DOUBLE", True), ("source_fingerprint", "VARCHAR", True), ("created_at", "VARCHAR", False)), ("code", "statement_id")),
    "statements": ((("code", "VARCHAR", True), ("statement_id", "VARCHAR", True), ("disclosure_number", "VARCHAR", False), ("disclosed_date", "VARCHAR", True), ("disclosed_at", "VARCHAR", True), ("period_start", "VARCHAR", True), ("period_end", "VARCHAR", True), ("earnings_per_share", "DOUBLE", False), ("diluted_earnings_per_share", "DOUBLE", False), ("profit", "DOUBLE", False), ("equity", "DOUBLE", False), ("type_of_current_period", "VARCHAR", False), ("type_of_document", "VARCHAR", False), ("next_year_forecast_earnings_per_share", "DOUBLE", False), ("bps", "DOUBLE", False), ("sales", "DOUBLE", False), ("operating_profit", "DOUBLE", False), ("forecast_operating_profit", "DOUBLE", False), ("next_year_forecast_operating_profit", "DOUBLE", False), ("ordinary_profit", "DOUBLE", False), ("operating_cash_flow", "DOUBLE", False), ("dividend_fy", "DOUBLE", False), ("forecast_dividend_fy", "DOUBLE", False), ("next_year_forecast_dividend_fy", "DOUBLE", False), ("payout_ratio", "DOUBLE", False), ("forecast_payout_ratio", "DOUBLE", False), ("next_year_forecast_payout_ratio", "DOUBLE", False), ("forecast_eps", "DOUBLE", False), ("investing_cash_flow", "DOUBLE", False), ("financing_cash_flow", "DOUBLE", False), ("cash_and_equivalents", "DOUBLE", False), ("total_assets", "DOUBLE", False), ("shares_outstanding", "DOUBLE", False), ("treasury_shares", "DOUBLE", False)), ("code", "statement_id")),
    "stock_data": ((("code", "VARCHAR", True), ("date", "VARCHAR", True), ("open", "DOUBLE", False), ("high", "DOUBLE", False), ("low", "DOUBLE", False), ("close", "DOUBLE", False), ("volume", "DOUBLE", False), ("adjustment_factor", "DOUBLE", False), ("created_at", "VARCHAR", False)), ("code", "date")),
    "stock_data_raw": ((("code", "VARCHAR", True), ("date", "VARCHAR", True), ("open", "DOUBLE", True), ("high", "DOUBLE", True), ("low", "DOUBLE", True), ("close", "DOUBLE", True), ("volume", "BIGINT", True), ("turnover_value", "DOUBLE", False), ("adjustment_factor", "DOUBLE", False), ("adjusted_open", "DOUBLE", False), ("adjusted_high", "DOUBLE", False), ("adjusted_low", "DOUBLE", False), ("adjusted_close", "DOUBLE", False), ("adjusted_volume", "DOUBLE", False), ("created_at", "VARCHAR", False)), ("code", "date")),
    "stock_master_daily": ((("date", "VARCHAR", True), ("code", "VARCHAR", True), ("company_name", "VARCHAR", True), ("company_name_english", "VARCHAR", False), ("market_code", "VARCHAR", True), ("market_name", "VARCHAR", True), ("sector_17_code", "VARCHAR", True), ("sector_17_name", "VARCHAR", True), ("sector_33_code", "VARCHAR", True), ("sector_33_name", "VARCHAR", True), ("scale_category", "VARCHAR", False), ("listed_date", "VARCHAR", False), ("created_at", "VARCHAR", False)), ("date", "code")),
    "stocks": ((("code", "VARCHAR", True), ("company_name", "VARCHAR", True), ("company_name_english", "VARCHAR", False), ("market_code", "VARCHAR", True), ("market_name", "VARCHAR", True), ("sector_17_code", "VARCHAR", True), ("sector_17_name", "VARCHAR", True), ("sector_33_code", "VARCHAR", True), ("sector_33_name", "VARCHAR", True), ("scale_category", "VARCHAR", False), ("listed_date", "VARCHAR", True), ("created_at", "VARCHAR", False), ("updated_at", "VARCHAR", False)), ("code",)),
    "topix_data": ((("date", "VARCHAR", True), ("open", "DOUBLE", False), ("high", "DOUBLE", False), ("low", "DOUBLE", False), ("close", "DOUBLE", False), ("created_at", "VARCHAR", False)), ("date",)),
}

DATASET_V4_PARQUET_EXPORTS: tuple[tuple[str, str, str | None], ...] = (
    ("stocks", "stocks.parquet", "code"),
    ("stock_data", "stock_data.parquet", None),
    ("topix_data", "topix_data.parquet", "date"),
    ("indices_data", "indices_data.parquet", None),
    ("margin_data", "margin_data.parquet", "code, date"),
    ("statements", "statements.parquet", "disclosed_at, code, statement_id"),
    ("stock_data_raw", "stock_data_raw.parquet", None),
    ("stock_master_daily", "stock_master_daily.parquet", "date, code"),
    (
        "statement_metrics_adjusted",
        "statement_metrics_adjusted.parquet",
        "disclosed_at, code, statement_id",
    ),
    ("daily_valuation", "daily_valuation.parquet", "date, code"),
)

DATASET_V4_PARQUET_ARTIFACT_NAMES = frozenset(
    parquet_name for _, parquet_name, _ in DATASET_V4_PARQUET_EXPORTS
)
