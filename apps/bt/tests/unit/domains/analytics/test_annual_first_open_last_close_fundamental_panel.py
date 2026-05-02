from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
    get_annual_first_open_last_close_fundamental_panel_bundle_path_for_run_id,
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
    load_annual_first_open_last_close_fundamental_panel_bundle,
    run_annual_first_open_last_close_fundamental_panel,
    write_annual_first_open_last_close_fundamental_panel_bundle,
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
        CREATE TABLE stock_master_daily (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
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
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                "Split Adjusted Standard",
                None,
                "0112",
                "Standard",
                "1",
                "A",
                "1",
                "A",
                "-",
                "2000-01-01",
                None,
                None,
            ),
            (
                "2222",
                "Growth Negative",
                None,
                "0113",
                "Growth",
                "1",
                "A",
                "1",
                "A",
                "-",
                "2000-01-01",
                None,
                None,
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "2024-01-04",
                "1111",
                "Split Adjusted Standard",
                None,
                "0112",
                "Standard",
                "1",
                "A",
                "1",
                "A",
                "-",
                "2000-01-01",
                None,
            ),
            (
                "2024-01-04",
                "2222",
                "Growth Negative",
                None,
                "0113",
                "Growth",
                "1",
                "A",
                "1",
                "A",
                "-",
                "2000-01-01",
                None,
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2023-12-29", 50.0, 51.0, 49.0, 50.0, 1000, 1.0, None),
            ("2222", "2023-12-29", 100.0, 101.0, 99.0, 100.0, 1000, 1.0, None),
            ("1111", "2024-01-04", 50.0, 56.0, 49.0, 55.0, 1000, 1.0, None),
            ("2222", "2024-01-04", 100.0, 101.0, 89.0, 90.0, 1000, 1.0, None),
            ("1111", "2024-01-05", 55.0, 61.0, 54.0, 60.0, 1000, 1.0, None),
            ("2222", "2024-01-05", 90.0, 91.0, 84.0, 85.0, 1000, 1.0, None),
            ("1111", "2024-12-30", 60.0, 76.0, 59.0, 75.0, 1000, 1.0, None),
            ("2222", "2024-12-30", 85.0, 86.0, 79.0, 80.0, 1000, 1.0, None),
        ],
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                "2023-05-10",
                100.0,
                100.0,
                1000.0,
                "FY",
                "FYFinancialStatements_Consolidated_JP",
                120.0,
                1000.0,
                500.0,
                50.0,
                None,
                80.0,
                10.0,
                None,
                14.0,
                0.1,
                None,
                0.2,
                None,
                -20.0,
                None,
                None,
                2000.0,
                100.0,
                0.0,
            ),
            (
                "1111",
                "2023-08-10",
                None,
                None,
                None,
                "1Q",
                "1QFinancialStatements_Consolidated_JP",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                70.0,
                None,
                None,
                None,
                None,
                200.0,
                20.0,
            ),
            (
                "1111",
                "2023-12-20",
                None,
                None,
                None,
                "FY",
                "EarnForecastRevision",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                90.0,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "2222",
                "2023-05-10",
                -20.0,
                -20.0,
                500.0,
                "FY",
                "FYFinancialStatements_Consolidated_JP",
                -10.0,
                500.0,
                300.0,
                -10.0,
                None,
                -30.0,
                0.0,
                None,
                0.0,
                0.0,
                None,
                0.0,
                None,
                -5.0,
                None,
                None,
                700.0,
                100.0,
                0.0,
            ),
        ],
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def _event_row(result, code: str, year: str) -> pd.Series:
    row = result.event_ledger_df[
        (result.event_ledger_df["code"] == code)
        & (result.event_ledger_df["year"] == year)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_run_study_adjusts_per_share_metrics_to_entry_share_baseline(
    analytics_db_path: str,
) -> None:
    result = run_annual_first_open_last_close_fundamental_panel(
        analytics_db_path,
        markets=("standard", "growth"),
        bucket_count=2,
    )

    assert list(result.calendar_df["year"]) == ["2024"]
    assert result.current_market_snapshot_only is False
    split_event = _event_row(result, "1111", "2024")
    negative_event = _event_row(result, "2222", "2024")

    assert split_event["status"] == "realized"
    assert bool(split_event["share_adjustment_applied"]) is True
    assert split_event["fy_shares_outstanding"] == pytest.approx(100.0)
    assert split_event["baseline_shares"] == pytest.approx(200.0)
    assert split_event["baseline_shares_source_date"] == "2023-08-10"
    assert split_event["share_adjustment_ratio"] == pytest.approx(0.5)
    assert split_event["eps"] == pytest.approx(50.0)
    assert split_event["bps"] == pytest.approx(500.0)
    assert split_event["forward_eps"] == pytest.approx(90.0)
    assert split_event["forward_eps_to_actual_eps"] == pytest.approx(1.8)
    assert split_event["forward_eps_source"] == "revised"
    assert split_event["forward_eps_period_type"] == "FY"
    assert split_event["per"] == pytest.approx(1.0)
    assert split_event["pbr"] == pytest.approx(0.1)
    assert split_event["market_cap_bil_jpy"] == pytest.approx(0.00001)
    assert split_event["free_float_market_cap_bil_jpy"] == pytest.approx(0.000009)
    assert split_event["free_float_ratio_pct"] == pytest.approx(90.0)
    assert split_event["event_return"] == pytest.approx(0.5)
    assert split_event["event_return_pct"] == pytest.approx(50.0)
    assert split_event["max_drawdown_pct"] == pytest.approx(0.0)

    assert negative_event["status"] == "realized"
    assert bool(negative_event["share_adjustment_applied"]) is False
    assert negative_event["event_return"] == pytest.approx(-0.2)
    assert negative_event["eps"] == pytest.approx(-20.0)
    assert negative_event["forward_eps_to_actual_eps"] == pytest.approx(0.5)


def test_run_study_uses_entry_date_stock_master_for_market_scope(
    analytics_db_path: str,
) -> None:
    conn = duckdb.connect(analytics_db_path)
    conn.execute(
        """
        UPDATE stock_master_daily
        SET market_code = '0102', market_name = '東証二部'
        WHERE code = '2222'
        """
    )
    conn.close()

    standard_result = run_annual_first_open_last_close_fundamental_panel(
        analytics_db_path,
        markets=("standard",),
        bucket_count=2,
    )
    growth_result = run_annual_first_open_last_close_fundamental_panel(
        analytics_db_path,
        markets=("growth",),
        bucket_count=2,
    )

    assert standard_result.current_market_snapshot_only is False
    assert set(standard_result.event_ledger_df["code"].astype(str)) == {"1111", "2222"}
    assert set(standard_result.event_ledger_df["market"].astype(str)) == {"standard"}
    assert growth_result.event_ledger_df.empty


def test_feature_buckets_and_annual_portfolio_summary_are_built(
    analytics_db_path: str,
) -> None:
    result = run_annual_first_open_last_close_fundamental_panel(
        analytics_db_path,
        markets=("standard", "growth"),
        bucket_count=2,
    )

    all_portfolio = result.annual_portfolio_summary_df[
        (result.annual_portfolio_summary_df["market_scope"].astype(str) == "all")
        & (
            result.annual_portfolio_summary_df["portfolio_scope"].astype(str)
            == "all_years"
        )
    ]
    assert len(all_portfolio) == 1
    assert all_portfolio.iloc[0]["realized_event_count"] == 2
    assert all_portfolio.iloc[0]["active_days"] == 3
    assert all_portfolio.iloc[0]["sharpe_ratio"] is not None

    eps_buckets = result.feature_bucket_summary_df[
        (result.feature_bucket_summary_df["market_scope"].astype(str) == "all")
        & (result.feature_bucket_summary_df["feature_name"] == "eps")
    ].sort_values("bucket")
    assert list(eps_buckets["bucket"]) == [1, 2]
    assert eps_buckets.iloc[0]["mean_return_pct"] == pytest.approx(-20.0)
    assert eps_buckets.iloc[1]["mean_return_pct"] == pytest.approx(50.0)

    eps_spread = result.factor_spread_summary_df[
        (result.factor_spread_summary_df["market_scope"].astype(str) == "all")
        & (result.factor_spread_summary_df["feature_name"] == "eps")
    ]
    assert len(eps_spread) == 1
    assert eps_spread.iloc[0]["high_minus_low_mean_return_pct"] == pytest.approx(70.0)
    assert eps_spread.iloc[0][
        "preferred_minus_opposite_mean_return_pct"
    ] == pytest.approx(70.0)


def test_bundle_roundtrip_for_study(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_annual_first_open_last_close_fundamental_panel(
        analytics_db_path,
        markets=("standard", "growth"),
        bucket_count=2,
    )
    bundle = write_annual_first_open_last_close_fundamental_panel_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )
    loaded = load_annual_first_open_last_close_fundamental_panel_bundle(
        bundle.bundle_dir
    )
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))

    assert (
        bundle.experiment_id
        == ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID
    )
    assert (
        get_annual_first_open_last_close_fundamental_panel_bundle_path_for_run_id(
            "test-run",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_annual_first_open_last_close_fundamental_panel_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    assert manifest["params"]["bucket_count"] == 2
    pd.testing.assert_frame_equal(
        loaded.event_ledger_df.reset_index(drop=True),
        result.event_ledger_df.reset_index(drop=True),
        check_dtype=False,
    )
