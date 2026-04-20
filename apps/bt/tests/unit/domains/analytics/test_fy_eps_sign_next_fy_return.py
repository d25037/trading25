from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.fy_eps_sign_next_fy_return import (
    DEFAULT_FORECAST_RATIO_THRESHOLDS,
    FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID,
    get_fy_eps_sign_next_fy_return_bundle_path_for_run_id,
    get_fy_eps_sign_next_fy_return_latest_bundle_path,
    load_fy_eps_sign_next_fy_return_bundle,
    run_fy_eps_sign_next_fy_return,
    write_fy_eps_sign_next_fy_return_bundle,
)


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
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
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
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
    conn.execute(
        """
        CREATE TABLE statements (
            code TEXT NOT NULL,
            disclosed_date TEXT NOT NULL,
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

    stocks = [
        ("1111", "Std Positive", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("11110", "Std Positive Duplicate", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("2222", "Growth Negative", None, "0113", "Growth", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("3333", "Std Zero", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("4444", "Growth No Next FY", None, "0113", "Growth", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("5555", "Prime Ex TOPIX500", None, "0111", "Prime", "1", "A", "1", "A", "TOPIX Small 1", "2000-01-01", None, None),
        ("6666", "Prime TOPIX500", None, "0111", "Prime", "1", "A", "1", "A", "TOPIX Mid400", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    stock_rows = [
        ("1111", "2024-05-13", 10.0, 11.0, 9.9, 11.0, 1000, 1.0, None),
        ("11110", "2024-05-13", 1.0, 2.0, 1.0, 2.0, 1000, 1.0, None),
        ("1111", "2024-05-14", 11.0, 12.0, 10.9, 12.0, 1000, 1.0, None),
        ("11110", "2024-05-14", 2.0, 4.0, 2.0, 4.0, 1000, 1.0, None),
        ("1111", "2025-05-09", 14.0, 15.0, 13.9, 15.0, 1000, 1.0, None),
        ("11110", "2025-05-09", 4.0, 8.0, 4.0, 8.0, 1000, 1.0, None),
        ("1111", "2025-05-12", 16.0, 17.0, 15.9, 17.0, 1000, 1.0, None),
        ("11110", "2025-05-12", 8.0, 16.0, 8.0, 16.0, 1000, 1.0, None),
        ("2222", "2024-05-13", 20.0, 20.0, 17.9, 18.0, 1000, 1.0, None),
        ("2222", "2024-05-14", 18.0, 18.1, 15.9, 16.0, 1000, 1.0, None),
        ("2222", "2025-05-09", 11.0, 11.1, 9.9, 10.0, 1000, 1.0, None),
        ("2222", "2025-05-12", 9.0, 9.1, 7.9, 8.0, 1000, 1.0, None),
        ("3333", "2024-05-13", 30.0, 31.0, 29.9, 31.0, 1000, 1.0, None),
        ("3333", "2025-05-09", 40.0, 41.0, 39.9, 41.0, 1000, 1.0, None),
        ("4444", "2024-05-13", 50.0, 55.0, 49.9, 55.0, 1000, 1.0, None),
        ("5555", "2024-05-13", 10.0, 10.5, 9.8, 10.5, 1000, 1.0, None),
        ("5555", "2024-05-14", 10.5, 11.5, 10.4, 11.0, 1000, 1.0, None),
        ("5555", "2025-05-09", 11.5, 12.5, 11.4, 12.0, 1000, 1.0, None),
        ("5555", "2025-05-12", 12.0, 12.2, 11.9, 12.1, 1000, 1.0, None),
        ("6666", "2024-05-13", 20.0, 20.2, 19.8, 20.0, 1000, 1.0, None),
        ("6666", "2024-05-14", 20.0, 20.5, 19.9, 20.5, 1000, 1.0, None),
        ("6666", "2025-05-09", 21.0, 21.5, 20.9, 21.5, 1000, 1.0, None),
        ("6666", "2025-05-12", 21.5, 21.6, 21.4, 21.6, 1000, 1.0, None),
    ]
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)

    statement_rows = [
        ("1111", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 200.0, None),
        ("1111", "2024-05-10", 50.0, None, None, "FY", None, 70.0, None, None, None, None, 30.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("11110", "2024-05-10", 999.0, None, None, "FY", None, 999.0, None, None, None, None, 999.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("1111", "2024-06-01", 55.0, None, None, "FY", None, 72.0, None, None, None, None, 32.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("1111", "2025-05-12", 60.0, None, None, "FY", None, 75.0, None, None, None, None, 35.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("2222", "2024-05-10", -40.0, None, None, "FY", None, -20.0, None, None, None, None, -40.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("2222", "2025-05-12", -20.0, None, None, "FY", None, -10.0, None, None, None, None, -20.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("3333", "2024-05-10", 0.0, None, None, "FY", None, 10.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("3333", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("4444", "2024-05-10", 30.0, None, None, "FY", None, 40.0, None, None, None, None, 10.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("5555", "2024-05-10", 20.0, None, None, "FY", None, 28.0, None, None, None, None, -10.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("5555", "2025-05-12", 24.0, None, None, "FY", None, 30.0, None, None, None, None, -8.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("6666", "2024-05-10", 15.0, None, None, "FY", None, 18.0, None, None, None, None, 20.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("6666", "2025-05-12", 18.0, None, None, "FY", None, 20.0, None, None, None, None, 22.0, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
    ]
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        statement_rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def _event_row(result, code: str, disclosed_date: str) -> pd.Series:
    row = result.event_ledger_df[
        (result.event_ledger_df["code"] == code)
        & (result.event_ledger_df["disclosed_date"] == disclosed_date)
    ]
    assert len(row) == 1
    return row.iloc[0]


def _summary_row(
    result,
    market_scope: str,
    eps_sign: str,
    forecast_filter: str = "all_signed",
) -> pd.Series:
    row = result.event_summary_df[
        (result.event_summary_df["market_scope"].astype(str) == market_scope)
        & (result.event_summary_df["eps_sign"].astype(str) == eps_sign)
        & (result.event_summary_df["forecast_filter"].astype(str) == forecast_filter)
    ]
    assert len(row) == 1
    return row.iloc[0]


def _portfolio_row(
    result,
    market_scope: str,
    eps_sign: str,
    forecast_filter: str = "all_signed",
) -> pd.Series:
    row = result.portfolio_summary_df[
        (result.portfolio_summary_df["market_scope"].astype(str) == market_scope)
        & (result.portfolio_summary_df["eps_sign"].astype(str) == eps_sign)
        & (result.portfolio_summary_df["forecast_filter"].astype(str) == forecast_filter)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_run_study_uses_adjusted_eps_and_previous_close_before_next_fy(
    analytics_db_path: str,
) -> None:
    result = run_fy_eps_sign_next_fy_return(analytics_db_path)

    positive_event = _event_row(result, "1111", "2024-05-10")
    negative_event = _event_row(result, "2222", "2024-05-10")
    zero_event = _event_row(result, "3333", "2024-05-10")
    no_next_event = _event_row(result, "4444", "2024-05-10")

    assert positive_event["classification"] == "positive"
    assert positive_event["status"] == "realized"
    assert positive_event["actual_eps"] == pytest.approx(25.0)
    assert positive_event["next_fy_disclosed_date"] == "2025-05-12"
    assert positive_event["entry_date"] == "2024-05-13"
    assert positive_event["exit_date"] == "2025-05-09"
    assert positive_event["event_return"] == pytest.approx(0.5)
    assert positive_event["event_return_pct"] == pytest.approx(50.0)
    assert positive_event["forecast_eps"] == pytest.approx(35.0)
    assert positive_event["forecast_sign"] == "forecast_positive"
    assert positive_event["cfo_sign"] == "cfo_positive"
    assert bool(positive_event["forecast_above_actual"]) is True
    assert positive_event["forecast_vs_actual_ratio"] == pytest.approx(1.4)

    assert negative_event["classification"] == "negative"
    assert negative_event["status"] == "realized"
    assert negative_event["event_return"] == pytest.approx(-0.5)
    assert negative_event["forecast_eps"] == pytest.approx(-20.0)
    assert negative_event["forecast_sign"] == "forecast_non_positive"
    assert negative_event["cfo_sign"] == "cfo_non_positive"
    assert bool(negative_event["forecast_above_actual"]) is True
    assert pd.isna(negative_event["forecast_vs_actual_ratio"])

    assert zero_event["classification"] == "zero_eps"
    assert zero_event["status"] == "excluded_zero_eps"

    assert no_next_event["classification"] == "positive"
    assert no_next_event["status"] == "no_next_fy"


def test_summary_and_portfolio_views_include_all_and_market_scopes(
    analytics_db_path: str,
) -> None:
    result = run_fy_eps_sign_next_fy_return(analytics_db_path)

    all_positive = _summary_row(result, "all", "positive")
    growth_negative = _summary_row(result, "growth", "negative")
    all_positive_12 = _summary_row(result, "all", "positive", "forecast_ge_1_2x")
    all_positive_14 = _summary_row(result, "all", "positive", "forecast_ge_1_4x")
    standard_positive_portfolio = _portfolio_row(result, "standard", "positive")
    growth_negative_portfolio = _portfolio_row(result, "growth", "negative")
    standard_positive_portfolio_12 = _portfolio_row(
        result,
        "standard",
        "positive",
        "forecast_ge_1_2x",
    )

    assert all_positive["signed_event_count"] == 4
    assert all_positive["realized_event_count"] == 1
    assert all_positive["no_next_fy_count"] == 3
    assert all_positive["mean_return_pct"] == pytest.approx(50.0)
    assert all_positive["win_rate_pct"] == pytest.approx(100.0)

    assert all_positive_12["signed_event_count"] == 4
    assert all_positive_12["realized_event_count"] == 1
    assert all_positive_12["no_next_fy_count"] == 3
    assert all_positive_12["mean_forecast_vs_actual_ratio"] == pytest.approx(1.4)

    assert all_positive_14["signed_event_count"] == 1
    assert all_positive_14["realized_event_count"] == 1

    assert growth_negative["signed_event_count"] == 2
    assert growth_negative["realized_event_count"] == 1
    assert growth_negative["no_next_fy_count"] == 1
    assert growth_negative["mean_return_pct"] == pytest.approx(-50.0)
    assert growth_negative["win_rate_pct"] == pytest.approx(0.0)

    assert standard_positive_portfolio["realized_event_count"] == 1
    assert standard_positive_portfolio["total_return_pct"] == pytest.approx(50.0)
    assert standard_positive_portfolio["max_drawdown_pct"] == pytest.approx(0.0)
    assert standard_positive_portfolio_12["realized_event_count"] == 1
    assert standard_positive_portfolio_12["total_return_pct"] == pytest.approx(50.0)

    assert growth_negative_portfolio["realized_event_count"] == 1
    assert growth_negative_portfolio["total_return_pct"] == pytest.approx(-50.0)
    assert growth_negative_portfolio["max_drawdown_pct"] < 0.0

    all_positive_daily = result.portfolio_daily_df[
        (result.portfolio_daily_df["market_scope"].astype(str) == "all")
        & (result.portfolio_daily_df["eps_sign"].astype(str) == "positive")
        & (result.portfolio_daily_df["forecast_filter"].astype(str) == "all_signed")
    ].reset_index(drop=True)
    assert list(all_positive_daily["date"]) == ["2024-05-13", "2024-05-14", "2025-05-09"]
    assert all_positive_daily.loc[0, "mean_daily_return"] == pytest.approx(0.1)
    assert all_positive_daily.loc[0, "portfolio_value"] == pytest.approx(1.1)

    assert tuple(DEFAULT_FORECAST_RATIO_THRESHOLDS) == (1.2, 1.4)


def test_prime_ex_topix500_scope_uses_latest_scale_category_proxy(
    analytics_db_path: str,
) -> None:
    result = run_fy_eps_sign_next_fy_return(
        analytics_db_path,
        markets=("primeExTopix500",),
    )

    assert result.selected_markets == ("primeExTopix500",)
    assert result.uses_current_scale_category_proxy is True
    assert set(result.event_ledger_df["code"]) == {"5555"}
    assert set(result.event_ledger_df["market"]) == {"primeExTopix500"}

    prime_ex_positive = _summary_row(result, "primeExTopix500", "positive")
    prime_ex_positive_14 = _summary_row(
        result,
        "primeExTopix500",
        "positive",
        "forecast_ge_1_4x",
    )
    prime_ex_portfolio = _portfolio_row(result, "primeExTopix500", "positive")

    assert prime_ex_positive["signed_event_count"] == 2
    assert prime_ex_positive["realized_event_count"] == 1
    assert prime_ex_positive["no_next_fy_count"] == 1
    assert prime_ex_positive["mean_return_pct"] == pytest.approx(20.0)

    assert prime_ex_positive_14["signed_event_count"] == 1
    assert prime_ex_positive_14["realized_event_count"] == 1

    assert prime_ex_portfolio["realized_event_count"] == 1
    assert prime_ex_portfolio["total_return_pct"] == pytest.approx(20.0)


def test_topix500_scope_and_eps_forecast_cfo_cross_summary(
    analytics_db_path: str,
) -> None:
    result = run_fy_eps_sign_next_fy_return(
        analytics_db_path,
        markets=("topix500",),
    )

    assert result.selected_markets == ("topix500",)
    assert result.uses_current_scale_category_proxy is True
    assert set(result.event_ledger_df["code"]) == {"6666"}
    assert set(result.event_ledger_df["market"]) == {"topix500"}

    topix_positive = _summary_row(result, "topix500", "positive")
    assert topix_positive["signed_event_count"] == 2
    assert topix_positive["realized_event_count"] == 1
    assert topix_positive["no_next_fy_count"] == 1
    assert topix_positive["mean_return_pct"] == pytest.approx(7.5)

    cross_row = result.cross_summary_df[
        (result.cross_summary_df["market_scope"].astype(str) == "topix500")
        & (result.cross_summary_df["eps_sign"].astype(str) == "positive")
        & (result.cross_summary_df["forecast_sign"].astype(str) == "forecast_positive")
        & (result.cross_summary_df["cfo_sign"].astype(str) == "cfo_positive")
    ]
    assert len(cross_row) == 1
    cross = cross_row.iloc[0]
    assert cross["signed_event_count"] == 2
    assert cross["realized_event_count"] == 1
    assert cross["no_next_fy_count"] == 1
    assert cross["mean_return_pct"] == pytest.approx(7.5)


def test_bundle_roundtrip_for_study(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_fy_eps_sign_next_fy_return(analytics_db_path)
    bundle = write_fy_eps_sign_next_fy_return_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )
    loaded = load_fy_eps_sign_next_fy_return_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID
    assert get_fy_eps_sign_next_fy_return_bundle_path_for_run_id(
        "test-run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert get_fy_eps_sign_next_fy_return_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.event_summary_df.reset_index(drop=True),
        result.event_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.cross_summary_df.reset_index(drop=True),
        result.cross_summary_df.reset_index(drop=True),
        check_dtype=False,
    )


def test_bundle_preserves_custom_thresholds_even_without_ratio_rows(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_fy_eps_sign_next_fy_return(
        analytics_db_path,
        markets=("growth",),
        forecast_ratio_thresholds=(2.0, 3.0),
    )
    bundle = write_fy_eps_sign_next_fy_return_bundle(
        result,
        output_root=tmp_path,
        run_id="custom-thresholds",
    )

    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))

    assert result.forecast_ratio_thresholds == (2.0, 3.0)
    assert manifest["params"]["forecast_ratio_thresholds"] == [2.0, 3.0]

    loaded = load_fy_eps_sign_next_fy_return_bundle(bundle.bundle_dir)
    assert loaded.forecast_ratio_thresholds == (2.0, 3.0)
