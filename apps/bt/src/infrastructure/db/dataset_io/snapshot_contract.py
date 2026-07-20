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
