from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix500_positive_eps_missing_forecast_cfo_positive_deep_dive import (
    TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
    get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle_path_for_run_id,
    get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_latest_bundle_path,
    load_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle,
    run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive,
    write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle,
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
        ("9001", "Target Co", None, "0111", "Prime", "1", "A", "1", "情報･通信業", "TOPIX Mid400", "2000-01-01", None, None),
        ("9002", "Baseline Co", None, "0111", "Prime", "1", "A", "2", "機械", "TOPIX Mid400", "2000-01-01", None, None),
        ("9003", "Sibling Co", None, "0111", "Prime", "1", "A", "3", "電気･ガス業", "TOPIX Mid400", "2000-01-01", None, None),
        ("9004", "Prime Ex", None, "0111", "Prime", "1", "A", "4", "サービス業", "TOPIX Small 1", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    stock_rows = [
        ("9001", "2024-05-08", 10.0, 10.1, 9.9, 10.0, 100, 1.0, None),
        ("9001", "2024-05-09", 11.0, 11.1, 10.9, 11.0, 120, 1.0, None),
        ("9001", "2024-05-13", 12.0, 12.1, 11.9, 12.0, 150, 1.0, None),
        ("9001", "2024-05-14", 13.0, 13.1, 12.9, 13.0, 160, 1.0, None),
        ("9001", "2025-05-09", 18.0, 18.1, 17.9, 18.0, 180, 1.0, None),
        ("9002", "2024-05-08", 20.0, 20.1, 19.9, 20.0, 100, 1.0, None),
        ("9002", "2024-05-09", 20.5, 20.6, 20.4, 20.5, 120, 1.0, None),
        ("9002", "2024-05-13", 21.0, 21.1, 20.9, 21.0, 140, 1.0, None),
        ("9002", "2025-05-09", 25.2, 25.3, 25.1, 25.2, 150, 1.0, None),
        ("9003", "2024-05-08", 30.0, 30.1, 29.9, 30.0, 100, 1.0, None),
        ("9003", "2024-05-09", 29.5, 29.6, 29.4, 29.5, 120, 1.0, None),
        ("9003", "2024-05-13", 29.0, 29.1, 28.9, 29.0, 140, 1.0, None),
        ("9003", "2025-05-09", 26.1, 26.2, 26.0, 26.1, 150, 1.0, None),
        ("9004", "2024-05-08", 40.0, 40.1, 39.9, 40.0, 100, 1.0, None),
        ("9004", "2024-05-09", 40.5, 40.6, 40.4, 40.5, 120, 1.0, None),
        ("9004", "2024-05-13", 41.0, 41.1, 40.9, 41.0, 140, 1.0, None),
        ("9004", "2025-05-09", 42.0, 42.1, 41.9, 42.0, 150, 1.0, None),
    ]
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)

    shares = 100_000_000.0
    statement_rows = [
        ("9001", "2024-05-10", 20.0, 2000.0, 8000.0, "FY", None, None, None, 10000.0, 2100.0, 2050.0, 100.0, None, None, None, None, None, None, None, None, None, None, 12000.0, shares, None),
        ("9001", "2024-08-09", None, None, None, "1Q", None, None, None, None, None, None, 80.0, None, None, None, None, None, None, 30.0, None, None, None, None, shares, None),
        ("9001", "2025-05-12", 22.0, 2200.0, 8500.0, "FY", None, 24.0, None, 10400.0, 2250.0, 2200.0, 110.0, None, None, None, None, None, None, 24.0, None, None, None, 12300.0, shares, None),
        ("9002", "2024-05-10", 25.0, 2500.0, 9000.0, "FY", None, 35.0, None, 11000.0, 2600.0, 2550.0, 120.0, None, None, None, None, None, None, 35.0, None, None, None, 13000.0, shares, None),
        ("9002", "2025-05-12", 26.0, 2600.0, 9200.0, "FY", None, 36.0, None, 11300.0, 2650.0, 2600.0, 130.0, None, None, None, None, None, None, 36.0, None, None, None, 13200.0, shares, None),
        ("9003", "2024-05-10", 18.0, 1800.0, 7000.0, "FY", None, None, None, 9500.0, 1700.0, 1650.0, -50.0, None, None, None, None, None, None, None, None, None, None, 11500.0, shares, None),
        ("9003", "2025-05-12", 17.0, 1700.0, 6800.0, "FY", None, None, None, 9300.0, 1600.0, 1550.0, -40.0, None, None, None, None, None, None, None, None, None, None, 11300.0, shares, None),
        ("9004", "2024-05-10", 30.0, 3000.0, 9500.0, "FY", None, None, None, 12000.0, 3100.0, 3050.0, 140.0, None, None, None, None, None, None, None, None, None, None, 14000.0, shares, None),
        ("9004", "2025-05-12", 31.0, 3100.0, 9600.0, "FY", None, None, None, 12100.0, 3150.0, 3100.0, 145.0, None, None, None, None, None, None, None, None, None, None, 14100.0, shares, None),
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


def test_run_deep_dive_builds_target_and_baseline_views(analytics_db_path: str) -> None:
    result = run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive(
        analytics_db_path,
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
        recent_year_window=10,
    )

    assert result.selected_market == "topix500"
    assert result.base_scope_name == "TOPIX500 / FY actual EPS > 0"
    assert result.subgroup_name == "EPS > 0 / forecast missing / CFO > 0"
    assert result.signed_event_count == 1
    assert result.realized_event_count == 1
    assert set(result.subgroup_event_df["code"]) == {"9001"}

    target_event = result.subgroup_event_df.iloc[0]
    assert target_event["followup_forecast_state"] == "turned_positive_before_next_fy"
    assert target_event["forecast_resume_group"] == "forecast_resumed_before_next_fy"
    assert target_event["prior_return_bucket"] == ">-20%"
    assert target_event["event_return_pct"] == pytest.approx(50.0)
    assert target_event["entry_adv"] == pytest.approx(((10.0 * 100) + (11.0 * 120)) / 2)

    benchmark_summary = result.benchmark_cell_summary_df.set_index("comparison_key")
    assert benchmark_summary.loc["target", "signed_event_count"] == 1
    assert benchmark_summary.loc["target", "mean_return_pct"] == pytest.approx(50.0)
    assert benchmark_summary.loc["forecast_positive_cfo_positive", "signed_event_count"] == 3
    assert benchmark_summary.loc["forecast_missing_cfo_non_positive", "signed_event_count"] == 2

    year_counts = result.recent_year_count_df.set_index("disclosed_year")
    assert year_counts.loc["2024", "signed_code_count"] == 1
    assert result.recent_year_count_stats_df.iloc[0]["average_signed_code_count"] == pytest.approx(0.1)

    top_exclusion = result.top_exclusion_summary_df.set_index("exclude_top_n")
    assert top_exclusion.loc[0, "mean_return_pct"] == pytest.approx(50.0)

    top_winner = result.top_winner_profile_df.iloc[0]
    assert top_winner["code"] == "9001"
    assert top_winner["event_return_pct"] == pytest.approx(50.0)


def test_run_deep_dive_rejects_non_positive_adv_window(analytics_db_path: str) -> None:
    with pytest.raises(ValueError, match="adv_window must be positive"):
        run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive(
            analytics_db_path,
            adv_window=0,
        )


def test_bundle_roundtrip_for_deep_dive(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive(
        analytics_db_path,
        adv_window=2,
        prior_sessions=2,
        horizons=(1, 2),
    )
    bundle = write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )
    loaded = load_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID
    )
    assert get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle_path_for_run_id(
        "test-run",
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.benchmark_cell_summary_df.reset_index(drop=True),
        result.benchmark_cell_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
