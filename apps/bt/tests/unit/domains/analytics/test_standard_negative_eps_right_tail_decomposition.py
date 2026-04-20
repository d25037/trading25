from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.standard_negative_eps_right_tail_decomposition import (
    STANDARD_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID,
    get_standard_negative_eps_right_tail_bundle_path_for_run_id,
    get_standard_negative_eps_right_tail_latest_bundle_path,
    load_standard_negative_eps_right_tail_bundle,
    run_standard_negative_eps_right_tail_decomposition,
    write_standard_negative_eps_right_tail_bundle,
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
        ("1111", "Std F+ C+", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("2222", "Std F+ C-", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("3333", "Std F- C+", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("4444", "Std F- C-", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("5555", "Std Missing", None, "0112", "Standard", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("9999", "Prime Ignore", None, "0111", "Prime", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    stock_rows = [
        ("1111", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 100, 1.0, None),
        ("1111", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 100, 1.0, None),
        ("1111", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("1111", "2025-05-09", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("2222", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 90, 1.0, None),
        ("2222", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 90, 1.0, None),
        ("2222", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 90, 1.0, None),
        ("2222", "2025-05-09", 8.0, 8.1, 7.9, 8.0, 90, 1.0, None),
        ("3333", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 10, 1.0, None),
        ("3333", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 10, 1.0, None),
        ("3333", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 10, 1.0, None),
        ("3333", "2025-05-09", 15.0, 15.1, 14.9, 15.0, 10, 1.0, None),
        ("4444", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 5, 1.0, None),
        ("4444", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 5, 1.0, None),
        ("4444", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 5, 1.0, None),
        ("4444", "2025-05-09", 40.0, 40.1, 39.9, 40.0, 5, 1.0, None),
        ("5555", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 20, 1.0, None),
        ("5555", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 20, 1.0, None),
        ("5555", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 20, 1.0, None),
        ("9999", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 1000, 1.0, None),
        ("9999", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 1000, 1.0, None),
        ("9999", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 1000, 1.0, None),
        ("9999", "2025-05-09", 50.0, 50.1, 49.9, 50.0, 1000, 1.0, None),
    ]
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)

    statement_rows = [
        ("1111", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("1111", "2024-05-10", -10.0, None, None, "FY", None, 5.0, None, None, None, None, 100.0, None, None, None, None, None, None, 5.0, None, None, None, None, 100.0, None),
        ("1111", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, 120.0, None, None, None, None, None, None, 12.0, None, None, None, None, 100.0, None),
        ("2222", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("2222", "2024-05-10", -10.0, None, None, "FY", None, 5.0, None, None, None, None, -100.0, None, None, None, None, None, None, 5.0, None, None, None, None, 100.0, None),
        ("2222", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, -120.0, None, None, None, None, None, None, 12.0, None, None, None, None, 100.0, None),
        ("3333", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("3333", "2024-05-10", -10.0, None, None, "FY", None, -5.0, None, None, None, None, 100.0, None, None, None, None, None, None, -5.0, None, None, None, None, 100.0, None),
        ("3333", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, 120.0, None, None, None, None, None, None, 12.0, None, None, None, None, 100.0, None),
        ("4444", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("4444", "2024-05-10", -10.0, None, None, "FY", None, -5.0, None, None, None, None, -100.0, None, None, None, None, None, None, -5.0, None, None, None, None, 100.0, None),
        ("4444", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, -120.0, None, None, None, None, None, None, 12.0, None, None, None, None, 100.0, None),
        ("5555", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("5555", "2024-05-10", -10.0, None, None, "FY", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("9999", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 100.0, None),
        ("9999", "2024-05-10", -10.0, None, None, "FY", None, 30.0, None, None, None, None, 200.0, None, None, None, None, None, None, 30.0, None, None, None, None, 100.0, None),
        ("9999", "2025-05-12", 10.0, None, None, "FY", None, 12.0, None, None, None, None, 220.0, None, None, None, None, None, None, 12.0, None, None, None, None, 100.0, None),
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


def _summary_row(result, group_key: str, liquidity_filter: str = "all_liquidity") -> pd.Series:
    row = result.event_summary_df[
        (result.event_summary_df["group_key"].astype(str) == group_key)
        & (result.event_summary_df["liquidity_filter"].astype(str) == liquidity_filter)
    ]
    assert len(row) == 1
    return row.iloc[0]


def _portfolio_row(result, group_key: str, liquidity_filter: str = "all_liquidity") -> pd.Series:
    row = result.portfolio_summary_df[
        (result.portfolio_summary_df["group_key"].astype(str) == group_key)
        & (result.portfolio_summary_df["liquidity_filter"].astype(str) == liquidity_filter)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_run_study_decomposes_forecast_cfo_and_liquidity(analytics_db_path: str) -> None:
    result = run_standard_negative_eps_right_tail_decomposition(
        analytics_db_path,
        adv_window=2,
    )

    assert result.selected_market == "standard"
    assert result.scope_name == "standard / FY actual EPS < 0"
    assert result.adv_window == 2
    assert set(result.event_ledger_df["code"]) == {"1111", "2222", "3333", "4444", "5555"}
    assert set(result.event_ledger_df["market"]) == {"standard"}

    code_1111 = result.event_ledger_df[result.event_ledger_df["code"] == "1111"].iloc[0]
    code_4444 = result.event_ledger_df[result.event_ledger_df["code"] == "4444"].iloc[0]
    code_5555 = result.event_ledger_df[result.event_ledger_df["code"] == "5555"].iloc[0]

    assert code_1111["forecast_sign"] == "forecast_positive"
    assert code_1111["cfo_sign"] == "cfo_positive"
    assert code_1111["group_key"] == "forecast_positive__cfo_positive"
    assert code_1111["entry_adv"] == pytest.approx(850.0)
    assert code_1111["liquidity_state"] == "high_liquidity"
    assert code_1111["event_return_pct"] == pytest.approx(100.0)

    assert code_4444["group_key"] == "forecast_non_positive__cfo_non_positive"
    assert code_4444["liquidity_state"] == "low_liquidity"
    assert code_4444["event_return_pct"] == pytest.approx(300.0)

    assert code_5555["group_key"] == "forecast_missing__cfo_missing"
    assert code_5555["status"] == "no_next_fy"

    all_negative = _summary_row(result, "all_negative")
    low_liquidity = _summary_row(result, "all_negative", "low_liquidity")
    fpos_cpos = _summary_row(result, "forecast_positive__cfo_positive")

    assert all_negative["signed_event_count"] == 5
    assert all_negative["realized_event_count"] == 4
    assert all_negative["no_next_fy_count"] == 1
    assert all_negative["mean_return_pct"] == pytest.approx(107.5)
    assert all_negative["median_return_pct"] == pytest.approx(75.0)

    assert low_liquidity["signed_event_count"] == 2
    assert low_liquidity["realized_event_count"] == 2
    assert low_liquidity["mean_return_pct"] == pytest.approx(175.0)
    assert low_liquidity["win_rate_pct"] == pytest.approx(100.0)

    assert fpos_cpos["signed_event_count"] == 1
    assert fpos_cpos["realized_event_count"] == 1
    assert fpos_cpos["mean_return_pct"] == pytest.approx(100.0)

    all_negative_tail = result.tail_concentration_df[
        (result.tail_concentration_df["group_key"].astype(str) == "all_negative")
        & (result.tail_concentration_df["liquidity_filter"].astype(str) == "all_liquidity")
    ].iloc[0]
    assert all_negative_tail["positive_event_count"] == 3
    assert all_negative_tail["max_return_pct"] == pytest.approx(300.0)
    assert all_negative_tail["top_1_gross_gain_share_pct"] == pytest.approx(66.6666667)

    all_negative_portfolio = _portfolio_row(result, "all_negative")
    low_liquidity_portfolio = _portfolio_row(result, "all_negative", "low_liquidity")
    assert all_negative_portfolio["realized_event_count"] == 4
    assert all_negative_portfolio["total_return_pct"] == pytest.approx(107.5)
    assert low_liquidity_portfolio["total_return_pct"] == pytest.approx(175.0)

    top_event = result.top_winner_events_df.iloc[0]
    assert top_event["code"] == "4444"
    assert top_event["event_return_pct"] == pytest.approx(300.0)


def test_run_study_supports_prime_market(analytics_db_path: str) -> None:
    result = run_standard_negative_eps_right_tail_decomposition(
        analytics_db_path,
        market="prime",
        adv_window=2,
    )

    assert result.selected_market == "prime"
    assert result.scope_name == "prime / FY actual EPS < 0"
    assert set(result.event_ledger_df["code"]) == {"9999"}
    assert set(result.event_ledger_df["market"]) == {"prime"}

    prime_event = result.event_ledger_df.iloc[0]
    assert prime_event["group_key"] == "forecast_positive__cfo_positive"
    assert prime_event["entry_adv"] == pytest.approx(8500.0)
    assert prime_event["liquidity_state"] == "high_liquidity"
    assert prime_event["event_return_pct"] == pytest.approx(400.0)

    all_negative = _summary_row(result, "all_negative")
    fpos_cpos = _summary_row(result, "forecast_positive__cfo_positive")
    assert all_negative["signed_event_count"] == 1
    assert all_negative["realized_event_count"] == 1
    assert all_negative["mean_return_pct"] == pytest.approx(400.0)
    assert fpos_cpos["mean_return_pct"] == pytest.approx(400.0)

    portfolio_row = _portfolio_row(result, "all_negative")
    assert portfolio_row["realized_event_count"] == 1
    assert portfolio_row["total_return_pct"] == pytest.approx(400.0)


def test_bundle_roundtrip_for_study(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_standard_negative_eps_right_tail_decomposition(
        analytics_db_path,
        adv_window=2,
    )
    bundle = write_standard_negative_eps_right_tail_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )
    loaded = load_standard_negative_eps_right_tail_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == STANDARD_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID
    assert loaded.selected_market == "standard"
    assert get_standard_negative_eps_right_tail_bundle_path_for_run_id(
        "test-run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert get_standard_negative_eps_right_tail_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    assert "prime-negative-eps-right-tail-decomposition" in str(
        get_standard_negative_eps_right_tail_bundle_path_for_run_id(
            "test-run",
            market="prime",
            output_root=tmp_path,
        )
    )
    pd.testing.assert_frame_equal(
        loaded.event_summary_df.reset_index(drop=True),
        result.event_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
