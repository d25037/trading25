"""
SQLAlchemy Core Table Definitions

Drizzle スキーマ（apps/ts）を正（Single Source of Truth）として、
20 テーブルを 2 つの MetaData に分離定義する。

- market_meta: market DuckDB（14 relation 定義、daily_valuation view を含む）
- portfolio_meta: portfolio.db（6 テーブル）

銘柄コード: DB 内は 4桁統一（Drizzle stockCode() と同一ルール）。
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.types import REAL

# ---------------------------------------------------------------------------
# MetaData（DB ごとに分離）
# ---------------------------------------------------------------------------

market_meta = MetaData()
portfolio_meta = MetaData()

# ===========================================================================
# market DuckDB (14 relations)
# ===========================================================================

# --- stocks ---
stocks = Table(
    "stocks",
    market_meta,
    Column("code", Text, primary_key=True),  # 4桁 stockCode
    Column("company_name", Text, nullable=False),
    Column("company_name_english", Text),
    Column("market_code", Text, nullable=False),
    Column("market_name", Text, nullable=False),
    Column("sector_17_code", Text, nullable=False),
    Column("sector_17_name", Text, nullable=False),
    Column("sector_33_code", Text, nullable=False),
    Column("sector_33_name", Text, nullable=False),
    Column("scale_category", Text),
    Column("listed_date", Text, nullable=False),
    Column("created_at", Text),
    Column("updated_at", Text),
)
Index("idx_stocks_market", stocks.c.market_code)
Index("idx_stocks_sector", stocks.c.sector_33_code)

# --- stock_data_raw ---
stock_data_raw = Table(
    "stock_data_raw",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("turnover_value", REAL),
    Column("adjustment_factor", REAL),
    Column("adjusted_open", REAL),
    Column("adjusted_high", REAL),
    Column("adjusted_low", REAL),
    Column("adjusted_close", REAL),
    Column("adjusted_volume", Integer),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "date"),
)
Index("idx_stock_data_raw_date", stock_data_raw.c.date)
Index("idx_stock_data_raw_code", stock_data_raw.c.code)

# --- stock_data ---
stock_data = Table(
    "stock_data",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("adjustment_factor", REAL),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "date"),
)
Index("idx_stock_data_date", stock_data.c.date)
Index("idx_stock_data_code", stock_data.c.code)

# --- stock_adjustment_events ---
stock_adjustment_events = Table(
    "stock_adjustment_events",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("adjustment_factor", REAL, nullable=False),
    Column("source_fingerprint", Text, nullable=False),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "date"),
    CheckConstraint(
        "adjustment_factor > 0 AND adjustment_factor <> 1",
        name="ck_stock_adjustment_events_non_unit_positive_factor",
    ),
)
Index("idx_stock_adjustment_events_date", stock_adjustment_events.c.date)

# --- stock_provider_windows ---
stock_provider_windows = Table(
    "stock_provider_windows",
    market_meta,
    Column("code", Text, primary_key=True),
    Column("coverage_start", Text, nullable=False),
    Column("coverage_end", Text, nullable=False),
    Column("provider_as_of", Text, nullable=False),
    Column("source_fingerprint", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
)

# --- current_basis_fundamentals_state ---
current_basis_fundamentals_state = Table(
    "current_basis_fundamentals_state",
    market_meta,
    Column("code", Text, primary_key=True),
    Column("fundamentals_adjustment_basis_date", Text, nullable=False),
    Column("source_fingerprint", Text, nullable=False),
    Column("statement_count", Integer, nullable=False),
    Column("materialized_at", Text, nullable=False),
    CheckConstraint(
        "statement_count >= 0",
        name="ck_current_basis_fundamentals_state_statement_count",
    ),
)

# --- current_basis_recompute_pending ---
current_basis_recompute_pending = Table(
    "current_basis_recompute_pending",
    market_meta,
    Column("code", Text, primary_key=True),
    Column("reason", Text, nullable=False),
    Column("source_fingerprint", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
)

# --- statement_metrics_adjusted (current provider basis only) ---
statement_metrics_adjusted = Table(
    "statement_metrics_adjusted",
    market_meta,
    Column("code", Text, nullable=False),
    Column("statement_id", Text, nullable=False),
    Column("disclosed_date", Text, nullable=False),
    Column("disclosed_at", Text, nullable=False),
    Column("period_end", Text, nullable=False),
    Column("period_type", Text, nullable=False),
    Column("fundamentals_adjustment_basis_date", Text, nullable=False),
    Column("raw_eps", REAL),
    Column("adjusted_eps", REAL),
    Column("raw_diluted_eps", REAL),
    Column("adjusted_diluted_eps", REAL),
    Column("raw_bps", REAL),
    Column("adjusted_bps", REAL),
    Column("raw_forecast_eps", REAL),
    Column("adjusted_forecast_eps", REAL),
    Column("raw_dividend_fy", REAL),
    Column("adjusted_dividend_fy", REAL),
    Column("raw_forecast_dividend_fy", REAL),
    Column("adjusted_forecast_dividend_fy", REAL),
    Column("raw_shares_outstanding", REAL),
    Column("adjusted_shares_outstanding", REAL),
    Column("raw_treasury_shares", REAL),
    Column("adjusted_treasury_shares", REAL),
    Column("adjustment_factor_cumulative", REAL, nullable=False),
    Column("source_fingerprint", Text, nullable=False),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "statement_id"),
)
Index(
    "idx_statement_metrics_adjusted_code_disclosed",
    statement_metrics_adjusted.c.code,
    statement_metrics_adjusted.c.disclosed_date,
)

# --- stock_data_minute_raw ---
stock_data_minute_raw = Table(
    "stock_data_minute_raw",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("time", Text, nullable=False),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("turnover_value", REAL),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "date", "time"),
)
Index("idx_stock_data_minute_raw_date", stock_data_minute_raw.c.date)
Index("idx_stock_data_minute_raw_code", stock_data_minute_raw.c.code)

# --- topix_data ---
topix_data = Table(
    "topix_data",
    market_meta,
    Column("date", Text, primary_key=True),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("created_at", Text),
)
Index("idx_topix_date", topix_data.c.date)

# --- indices_data ---
indices_data = Table(
    "indices_data",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("open", REAL),
    Column("high", REAL),
    Column("low", REAL),
    Column("close", REAL),
    Column("sector_name", Text),
    Column("created_at", Text),
    PrimaryKeyConstraint("code", "date"),
)
Index("idx_indices_data_date", indices_data.c.date)
Index("idx_indices_data_code", indices_data.c.code)

# --- market: margin_data ---
market_margin_data = Table(
    "margin_data",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("long_margin_volume", REAL),
    Column("short_margin_volume", REAL),
    PrimaryKeyConstraint("code", "date"),
)
Index("idx_market_margin_data_date", market_margin_data.c.date)
Index("idx_market_margin_data_code", market_margin_data.c.code)

# --- statements ---
market_statements = Table(
    "statements",
    market_meta,
    Column("code", Text, nullable=False),
    Column("statement_id", Text, nullable=False),
    Column("disclosure_number", Text),
    Column("disclosed_date", Text, nullable=False),
    Column("disclosed_at", Text, nullable=False),
    Column("period_start", Text, nullable=False),
    Column("period_end", Text, nullable=False),
    Column("earnings_per_share", REAL),
    Column("diluted_earnings_per_share", REAL),
    Column("profit", REAL),
    Column("equity", REAL),
    Column("type_of_current_period", Text),
    Column("type_of_document", Text),
    Column("next_year_forecast_earnings_per_share", REAL),
    Column("bps", REAL),
    Column("sales", REAL),
    Column("forecast_sales", REAL),
    Column("next_year_forecast_sales", REAL),
    Column("operating_profit", REAL),
    Column("forecast_operating_profit", REAL),
    Column("next_year_forecast_operating_profit", REAL),
    Column("ordinary_profit", REAL),
    Column("operating_cash_flow", REAL),
    Column("dividend_fy", REAL),
    Column("forecast_dividend_fy", REAL),
    Column("next_year_forecast_dividend_fy", REAL),
    Column("payout_ratio", REAL),
    Column("forecast_payout_ratio", REAL),
    Column("next_year_forecast_payout_ratio", REAL),
    Column("forecast_eps", REAL),
    Column("investing_cash_flow", REAL),
    Column("financing_cash_flow", REAL),
    Column("cash_and_equivalents", REAL),
    Column("total_assets", REAL),
    Column("shares_outstanding", REAL),
    Column("treasury_shares", REAL),
    PrimaryKeyConstraint("code", "statement_id"),
)
Index("idx_market_statements_date", market_statements.c.disclosed_date)
Index("idx_market_statements_code", market_statements.c.code)

# --- daily_valuation (DuckDB ASOF view declaration) ---
daily_valuation = Table(
    "daily_valuation",
    market_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("price_basis_date", Text, nullable=False),
    Column("close", REAL),
    Column("eps", REAL),
    Column("bps", REAL),
    Column("forward_eps", REAL),
    Column("per", REAL),
    Column("forward_per", REAL),
    Column("sales", REAL),
    Column("forward_sales", REAL),
    Column("psr", REAL),
    Column("forward_psr", REAL),
    Column("p_op", REAL),
    Column("forward_p_op", REAL),
    Column("pbr", REAL),
    Column("market_cap", REAL),
    Column("free_float_market_cap", REAL),
    Column("statement_disclosed_date", Text),
    Column("forward_eps_disclosed_date", Text),
    Column("forward_eps_source", Text),
    Column("forward_sales_disclosed_date", Text),
    Column("forward_sales_source", Text),
    Column("statement_id", Text),
    Column("statement_disclosed_at", Text),
    Column("fundamentals_adjustment_basis_date", Text),
    Column("source_fingerprint", Text),
    Column("created_at", Text),
)

# --- sync_metadata ---
sync_metadata = Table(
    "sync_metadata",
    market_meta,
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("updated_at", Text),
)

# --- index_master ---
index_master = Table(
    "index_master",
    market_meta,
    Column("code", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("name_english", Text),
    Column("category", Text, nullable=False),
    Column("data_start_date", Text),
    Column("created_at", Text),
    Column("updated_at", Text),
)

# ===========================================================================
# portfolio.db (6 tables)
# ===========================================================================

# --- portfolio_metadata ---
portfolio_metadata = Table(
    "portfolio_metadata",
    portfolio_meta,
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("updated_at", Text),
)

# --- portfolios ---
portfolios = Table(
    "portfolios",
    portfolio_meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("created_at", Text),
    Column("updated_at", Text),
    UniqueConstraint("name", name="portfolios_name_unique"),
)

# --- portfolio_items ---
portfolio_items = Table(
    "portfolio_items",
    portfolio_meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "portfolio_id",
        Integer,
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("code", Text, nullable=False),  # 4桁 stockCode
    Column("company_name", Text, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("purchase_price", REAL, nullable=False),
    Column("purchase_date", Text, nullable=False),
    Column("account", Text),
    Column("notes", Text),
    Column("created_at", Text),
    Column("updated_at", Text),
    UniqueConstraint("portfolio_id", "code", name="portfolio_items_portfolio_code_unique"),
)
Index("idx_portfolio_items_portfolio_id", portfolio_items.c.portfolio_id)
Index("idx_portfolio_items_code", portfolio_items.c.code)
Index("idx_portfolio_items_purchase_date", portfolio_items.c.purchase_date)

# --- watchlists ---
watchlists = Table(
    "watchlists",
    portfolio_meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("created_at", Text),
    Column("updated_at", Text),
    UniqueConstraint("name", name="watchlists_name_unique"),
)

# --- watchlist_items ---
watchlist_items = Table(
    "watchlist_items",
    portfolio_meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "watchlist_id",
        Integer,
        ForeignKey("watchlists.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("code", Text, nullable=False),  # 4桁 stockCode
    Column("company_name", Text, nullable=False),
    Column("memo", Text),
    Column("created_at", Text),
    UniqueConstraint("watchlist_id", "code", name="watchlist_items_watchlist_code_unique"),
)
Index("idx_watchlist_items_watchlist_id", watchlist_items.c.watchlist_id)
Index("idx_watchlist_items_code", watchlist_items.c.code)

# --- jobs ---
jobs = Table(
    "jobs",
    portfolio_meta,
    Column("job_id", Text, primary_key=True),
    Column("job_type", Text, nullable=False),
    Column("strategy_name", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("progress", REAL),
    Column("message", Text),
    Column("error", Text),
    Column("created_at", Text, nullable=False),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("lease_owner", Text),
    Column("lease_expires_at", Text),
    Column("last_heartbeat_at", Text),
    Column("cancel_requested_at", Text),
    Column("cancel_reason", Text),
    Column("timeout_at", Text),
    Column("run_spec_json", Text),
    Column("run_metadata_json", Text),
    Column("result_json", Text),
    Column("raw_result_json", Text),
    Column("canonical_result_json", Text),
    Column("artifact_index_json", Text),
    Column("html_path", Text),
    Column("dataset_name", Text),
    Column("execution_time", REAL),
    Column("best_score", REAL),
    Column("best_params_json", Text),
    Column("worst_score", REAL),
    Column("worst_params_json", Text),
    Column("total_combinations", Integer),
    Column("updated_at", Text, nullable=False),
)
Index("idx_jobs_created_at", jobs.c.created_at)
Index("idx_jobs_status", jobs.c.status)
Index("idx_jobs_job_type", jobs.c.job_type)
