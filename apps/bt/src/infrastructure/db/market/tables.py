"""
SQLAlchemy Core Table Definitions

Drizzle スキーマ（apps/ts）を正（Single Source of Truth）として、
20 テーブルを 3 つの MetaData に分離定義する。

- market_meta: market.db（7 テーブル）
- dataset_meta: dataset.db（7 テーブル — stocks 等は market と共通定義を再利用）
- portfolio_meta: portfolio.db（6 テーブル）

銘柄コード: DB 内は 4桁統一（Drizzle stockCode() と同一ルール）。
"""

from __future__ import annotations

from sqlalchemy import (
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
dataset_meta = MetaData()
portfolio_meta = MetaData()

# ===========================================================================
# market.db (7 tables)
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

# --- statements ---
market_statements = Table(
    "statements",
    market_meta,
    Column("code", Text, nullable=False),
    Column("disclosed_date", Text, nullable=False),
    Column("earnings_per_share", REAL),
    Column("profit", REAL),
    Column("equity", REAL),
    Column("type_of_current_period", Text),
    Column("type_of_document", Text),
    Column("next_year_forecast_earnings_per_share", REAL),
    Column("bps", REAL),
    Column("sales", REAL),
    Column("operating_profit", REAL),
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
    PrimaryKeyConstraint("code", "disclosed_date"),
)
Index("idx_market_statements_date", market_statements.c.disclosed_date)
Index("idx_market_statements_code", market_statements.c.code)

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
# dataset.db (7 tables)
# 共通テーブル（stocks, stock_data, topix_data, indices_data）は
# dataset_meta に再定義する。
# ===========================================================================

# --- dataset: stocks ---
ds_stocks = Table(
    "stocks",
    dataset_meta,
    Column("code", Text, primary_key=True),
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
Index("ds_idx_stocks_market", ds_stocks.c.market_code)
Index("ds_idx_stocks_sector", ds_stocks.c.sector_33_code)

# --- dataset: stock_data ---
ds_stock_data = Table(
    "stock_data",
    dataset_meta,
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
Index("ds_idx_stock_data_date", ds_stock_data.c.date)
Index("ds_idx_stock_data_code", ds_stock_data.c.code)

# --- dataset: topix_data ---
ds_topix_data = Table(
    "topix_data",
    dataset_meta,
    Column("date", Text, primary_key=True),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("created_at", Text),
)
Index("ds_idx_topix_date", ds_topix_data.c.date)

# --- dataset: indices_data ---
ds_indices_data = Table(
    "indices_data",
    dataset_meta,
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
Index("ds_idx_indices_data_date", ds_indices_data.c.date)
Index("ds_idx_indices_data_code", ds_indices_data.c.code)

# --- dataset: dataset_info ---
dataset_info = Table(
    "dataset_info",
    dataset_meta,
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("updated_at", Text),
)

# --- dataset: margin_data ---
margin_data = Table(
    "margin_data",
    dataset_meta,
    Column("code", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("long_margin_volume", REAL),
    Column("short_margin_volume", REAL),
    PrimaryKeyConstraint("code", "date"),
)
Index("idx_margin_data_date", margin_data.c.date)
Index("idx_margin_data_code", margin_data.c.code)

# --- dataset: statements ---
statements = Table(
    "statements",
    dataset_meta,
    Column("code", Text, nullable=False),
    Column("disclosed_date", Text, nullable=False),
    Column("earnings_per_share", REAL),
    Column("profit", REAL),
    Column("equity", REAL),
    Column("type_of_current_period", Text),
    Column("type_of_document", Text),
    Column("next_year_forecast_earnings_per_share", REAL),
    Column("bps", REAL),
    Column("sales", REAL),
    Column("operating_profit", REAL),
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
    PrimaryKeyConstraint("code", "disclosed_date"),
)
Index("idx_statements_date", statements.c.disclosed_date)
Index("idx_statements_code", statements.c.code)

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
    Column("result_json", Text),
    Column("raw_result_json", Text),
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
