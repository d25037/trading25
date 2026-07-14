"""Canonical physical artifact contract for Dataset v3 bundles."""

from __future__ import annotations

DATASET_V3_PARQUET_EXPORTS: tuple[tuple[str, str, str | None], ...] = (
    ("stocks", "stocks.parquet", "code"),
    ("stock_data", "stock_data.parquet", None),
    ("topix_data", "topix_data.parquet", "date"),
    ("indices_data", "indices_data.parquet", None),
    ("margin_data", "margin_data.parquet", "code, date"),
    ("statements", "statements.parquet", "disclosed_date, code"),
    ("stock_data_raw", "stock_data_raw.parquet", None),
    ("stock_master_daily", "stock_master_daily.parquet", "date, code"),
    ("stock_adjustment_bases", "stock_adjustment_bases.parquet", "code, valid_from"),
    (
        "stock_adjustment_basis_segments",
        "stock_adjustment_basis_segments.parquet",
        "code, basis_id, source_date_from",
    ),
    (
        "statement_metrics_adjusted",
        "statement_metrics_adjusted.parquet",
        "disclosed_date, code, basis_version",
    ),
    ("daily_valuation", "daily_valuation.parquet", "date, code, basis_version"),
)

DATASET_V3_PARQUET_ARTIFACT_NAMES = frozenset(
    parquet_name for _, parquet_name, _ in DATASET_V3_PARQUET_EXPORTS
)
