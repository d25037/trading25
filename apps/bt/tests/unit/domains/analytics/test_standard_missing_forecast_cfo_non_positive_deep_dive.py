from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.standard_missing_forecast_cfo_non_positive_deep_dive import (
    STANDARD_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
    get_standard_missing_forecast_cfo_non_positive_deep_dive_bundle_path_for_run_id,
    get_standard_missing_forecast_cfo_non_positive_deep_dive_latest_bundle_path,
    load_standard_missing_forecast_cfo_non_positive_deep_dive_bundle,
    run_standard_missing_forecast_cfo_non_positive_deep_dive,
    write_standard_missing_forecast_cfo_non_positive_deep_dive_bundle,
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
        ("1001", "Turns Positive", None, "0112", "Standard", "1", "A", "1", "Tech", "-", "2000-01-01", None, None),
        ("1002", "Turns Non Positive", None, "0112", "Standard", "1", "A", "2", "Retail", "-", "2000-01-01", None, None),
        ("1003", "Stays Missing", None, "0112", "Standard", "1", "A", "3", "Services", "-", "2000-01-01", None, None),
        ("9001", "Prime Topix500", None, "0111", "Prime", "1", "A", "4", "Industrial", "TOPIX Mid400", "2000-01-01", None, None),
        ("9002", "Prime Ex Topix500", None, "0111", "Prime", "1", "A", "5", "Finance", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    stock_rows = [
        ("1001", "2024-05-09", 8.0, 8.1, 7.9, 8.0, 100, 1.0, None),
        ("1001", "2024-05-10", 9.0, 9.1, 8.9, 9.0, 100, 1.0, None),
        ("1001", "2024-05-13", 10.0, 10.1, 8.9, 9.0, 100, 1.0, None),
        ("1001", "2024-05-14", 8.0, 8.1, 7.9, 8.0, 100, 1.0, None),
        ("1001", "2025-05-09", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("1002", "2024-05-09", 40.0, 40.1, 39.9, 40.0, 100, 1.0, None),
        ("1002", "2024-05-10", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("1002", "2024-05-13", 10.0, 10.1, 7.9, 8.0, 100, 1.0, None),
        ("1002", "2024-05-14", 7.0, 7.1, 6.9, 7.0, 100, 1.0, None),
        ("1002", "2025-05-09", 6.0, 6.1, 5.9, 6.0, 100, 1.0, None),
        ("1003", "2024-05-09", 50.0, 50.1, 49.9, 50.0, 100, 1.0, None),
        ("1003", "2024-05-10", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("1003", "2024-05-13", 5.0, 5.1, 3.9, 4.0, 100, 1.0, None),
        ("1003", "2024-05-14", 4.0, 4.1, 3.9, 4.0, 100, 1.0, None),
        ("1003", "2025-05-09", 200.0, 200.1, 199.9, 200.0, 100, 1.0, None),
        ("9001", "2024-05-09", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("9001", "2024-05-10", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("9001", "2024-05-13", 5.0, 5.1, 4.9, 5.0, 100, 1.0, None),
        ("9001", "2024-05-14", 6.0, 6.1, 5.9, 6.0, 100, 1.0, None),
        ("9001", "2025-05-09", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("9002", "2024-05-09", 30.0, 30.1, 29.9, 30.0, 100, 1.0, None),
        ("9002", "2024-05-10", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("9002", "2024-05-13", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("9002", "2024-05-14", 11.0, 11.1, 10.9, 11.0, 100, 1.0, None),
        ("9002", "2025-05-09", 15.0, 15.1, 14.9, 15.0, 100, 1.0, None),
    ]
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)

    shares = 100_000_000.0
    statement_rows = [
        ("1001", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("1001", "2024-05-10", -10.0, -1000.0, 5000.0, "FY", None, None, None, 10000.0, -900.0, -950.0, -100.0, None, None, None, None, None, None, None, None, None, None, 12000.0, shares, None),
        ("1001", "2024-08-09", None, None, None, "1Q", None, None, None, None, None, None, -50.0, None, None, None, None, None, None, 6.0, None, None, None, None, shares, None),
        ("1001", "2025-05-12", 8.0, 800.0, 5600.0, "FY", None, 10.0, None, 10800.0, 920.0, 900.0, 50.0, None, None, None, None, None, None, 10.0, None, None, None, 12400.0, shares, None),
        ("1002", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("1002", "2024-05-10", -10.0, -5000.0, 4000.0, "FY", None, None, None, 9000.0, -4500.0, -4600.0, -100.0, None, None, None, None, None, None, None, None, None, None, 15000.0, shares, None),
        ("1002", "2024-08-09", None, None, None, "1Q", None, None, None, None, None, None, -80.0, None, None, None, None, None, None, -2.0, None, None, None, None, shares, None),
        ("1002", "2025-05-12", 5.0, 500.0, 4200.0, "FY", None, 4.0, None, 9300.0, 520.0, 480.0, -10.0, None, None, None, None, None, None, 4.0, None, None, None, 15200.0, shares, None),
        ("1003", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("1003", "2024-05-10", -10.0, -8000.0, 3000.0, "FY", None, None, None, 7000.0, -7800.0, -7900.0, -100.0, None, None, None, None, None, None, None, None, None, None, 18000.0, shares, None),
        ("1003", "2025-05-12", 5.0, 600.0, 3500.0, "FY", None, 6.0, None, 7600.0, 640.0, 630.0, 10.0, None, None, None, None, None, None, 6.0, None, None, None, 18200.0, shares, None),
        ("9001", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("9001", "2024-05-10", -10.0, None, None, "FY", None, None, None, None, None, None, -100.0, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("9001", "2024-08-09", None, None, None, "1Q", None, None, None, None, None, None, -40.0, None, None, None, None, None, None, 3.0, None, None, None, None, shares, None),
        ("9001", "2025-05-12", 5.0, None, None, "FY", None, 6.0, None, None, None, None, 10.0, None, None, None, None, None, None, 6.0, None, None, None, None, shares, None),
        ("9002", "2024-02-10", None, None, None, "3Q", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("9002", "2024-05-10", -10.0, None, None, "FY", None, None, None, None, None, None, -100.0, None, None, None, None, None, None, None, None, None, None, None, shares, None),
        ("9002", "2024-08-09", None, None, None, "1Q", None, None, None, None, None, None, -20.0, None, None, None, None, None, None, -1.0, None, None, None, None, shares, None),
        ("9002", "2025-05-12", 5.0, None, None, "FY", None, 4.0, None, None, None, None, -10.0, None, None, None, None, None, None, 4.0, None, None, None, None, shares, None),
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


def test_run_deep_dive_builds_expected_followup_and_horizon_views(analytics_db_path: str) -> None:
    result = run_standard_missing_forecast_cfo_non_positive_deep_dive(
        analytics_db_path,
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
        recent_year_window=10,
    )

    assert result.base_scope_name == "standard / FY actual EPS < 0"
    assert result.subgroup_key == "forecast_missing__cfo_non_positive"
    assert result.signed_event_count == 3
    assert result.realized_event_count == 3
    assert result.recent_year_window == 10

    followup_state_by_code = (
        result.subgroup_event_df.set_index("code")["followup_forecast_state"].to_dict()
    )
    assert followup_state_by_code == {
        "1001": "turned_positive_before_next_fy",
        "1002": "turned_non_positive_before_next_fy",
        "1003": "still_missing_until_next_fy",
    }

    prior_bucket_by_code = (
        result.subgroup_event_df.set_index("code")["prior_return_bucket"].to_dict()
    )
    assert prior_bucket_by_code == {
        "1001": ">-20%",
        "1002": "-80% to -50%",
        "1003": "<=-80%",
    }

    resume_group_counts = (
        result.forecast_resume_summary_df.set_index("forecast_resume_group")["signed_event_count"].to_dict()
    )
    assert resume_group_counts == {
        "forecast_resumed_before_next_fy": 2,
        "forecast_stayed_missing_until_next_fy": 1,
    }

    year_counts = result.recent_year_count_df.set_index("disclosed_year")
    assert len(year_counts) == 10
    assert year_counts.loc["2024", "signed_code_count"] == 3
    stats_row = result.recent_year_count_stats_df.iloc[0]
    assert stats_row["window_start_year"] == "2015"
    assert stats_row["window_end_year"] == "2024"
    assert stats_row["average_signed_code_count"] == pytest.approx(0.3)
    assert stats_row["max_signed_code_count"] == 3
    assert stats_row["min_signed_code_count"] == 0

    top_exclusion = result.top_exclusion_summary_df.set_index("exclude_top_n")
    assert top_exclusion.loc[0, "mean_return_pct"] == pytest.approx(1320.0)
    assert top_exclusion.loc[1, "mean_return_pct"] == pytest.approx(30.0)

    horizon_df = result.horizon_summary_df.set_index("horizon_label")
    assert horizon_df.loc["1d", "available_event_count"] == 3
    assert horizon_df.loc["1d", "mean_return_pct"] == pytest.approx(-16.6666667)
    assert horizon_df.loc["2d", "mean_return_pct"] == pytest.approx(-23.3333333)
    assert horizon_df.loc["next_fy", "mean_return_pct"] == pytest.approx(1320.0)

    top_winner = result.top_winner_profile_df.iloc[0]
    assert top_winner["code"] == "1003"
    assert top_winner["event_return_pct"] == pytest.approx(3900.0)

    feature_effects = result.feature_effect_summary_df.set_index("feature_key")
    assert "prior_return_pct" in feature_effects.index
    assert "profit_margin_pct" in feature_effects.index


def test_run_deep_dive_supports_prime_market(analytics_db_path: str) -> None:
    result = run_standard_missing_forecast_cfo_non_positive_deep_dive(
        analytics_db_path,
        market="prime",
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
    )

    assert result.selected_market == "prime"
    assert result.base_scope_name == "prime / FY actual EPS < 0"
    assert result.signed_event_count == 2
    assert result.realized_event_count == 2
    assert set(result.subgroup_event_df["code"]) == {"9001", "9002"}

    prime_event = result.subgroup_event_df[result.subgroup_event_df["code"] == "9001"].iloc[0]
    assert prime_event["followup_forecast_state"] == "turned_positive_before_next_fy"
    assert prime_event["prior_return_bucket"] == "-80% to -50%"
    assert prime_event["event_return_pct"] == pytest.approx(100.0)

    top_exclusion = result.top_exclusion_summary_df.set_index("exclude_top_n")
    assert top_exclusion.loc[0, "mean_return_pct"] == pytest.approx(75.0)
    assert top_exclusion.loc[1, "remaining_event_count"] == 1
    assert top_exclusion.loc[1, "mean_return_pct"] == pytest.approx(50.0)


def test_run_deep_dive_supports_topix500_and_prime_ex_topix500_scopes(
    analytics_db_path: str,
) -> None:
    topix500_result = run_standard_missing_forecast_cfo_non_positive_deep_dive(
        analytics_db_path,
        market="topix500",
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
    )
    prime_ex_result = run_standard_missing_forecast_cfo_non_positive_deep_dive(
        analytics_db_path,
        market="primeExTopix500",
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
    )

    assert topix500_result.selected_market == "topix500"
    assert topix500_result.base_scope_name == "TOPIX500 / FY actual EPS < 0"
    assert set(topix500_result.subgroup_event_df["code"]) == {"9001"}
    assert set(topix500_result.subgroup_event_df["market"]) == {"topix500"}
    assert topix500_result.subgroup_event_df.iloc[0]["followup_forecast_state"] == (
        "turned_positive_before_next_fy"
    )

    assert prime_ex_result.selected_market == "primeExTopix500"
    assert prime_ex_result.base_scope_name == "primeExTopix500 / FY actual EPS < 0"
    assert set(prime_ex_result.subgroup_event_df["code"]) == {"9002"}
    assert set(prime_ex_result.subgroup_event_df["market"]) == {"primeExTopix500"}
    assert prime_ex_result.subgroup_event_df.iloc[0]["followup_forecast_state"] == (
        "turned_non_positive_before_next_fy"
    )


def test_bundle_roundtrip_for_deep_dive(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_standard_missing_forecast_cfo_non_positive_deep_dive(
        analytics_db_path,
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
    )
    bundle = write_standard_missing_forecast_cfo_non_positive_deep_dive_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )
    loaded = load_standard_missing_forecast_cfo_non_positive_deep_dive_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id
        == STANDARD_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID
    )
    assert loaded.selected_market == "standard"
    assert get_standard_missing_forecast_cfo_non_positive_deep_dive_bundle_path_for_run_id(
        "test-run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert get_standard_missing_forecast_cfo_non_positive_deep_dive_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    assert "prime-missing-forecast-cfo-non-positive-deep-dive" in str(
        get_standard_missing_forecast_cfo_non_positive_deep_dive_bundle_path_for_run_id(
            "test-run",
            market="prime",
            output_root=tmp_path,
        )
    )
    pd.testing.assert_frame_equal(
        loaded.horizon_summary_df.reset_index(drop=True),
        result.horizon_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.recent_year_count_df.reset_index(drop=True),
        result.recent_year_count_df.reset_index(drop=True),
        check_dtype=False,
    )
