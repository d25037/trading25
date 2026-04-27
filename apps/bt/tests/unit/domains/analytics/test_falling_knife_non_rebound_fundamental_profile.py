from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.falling_knife_non_rebound_fundamental_profile import (
    FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID,
    get_falling_knife_non_rebound_fundamental_profile_bundle_path_for_run_id,
    get_falling_knife_non_rebound_fundamental_profile_latest_bundle_path,
    load_falling_knife_non_rebound_fundamental_profile_bundle,
    run_falling_knife_non_rebound_fundamental_profile,
    write_falling_knife_non_rebound_fundamental_profile_bundle,
)
from src.domains.analytics.falling_knife_reversal_study import (
    FallingKnifeReversalStudyResult,
    write_falling_knife_reversal_study_bundle,
)


def _create_statement_db(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE statements (
                code TEXT NOT NULL,
                disclosed_date TEXT NOT NULL,
                type_of_current_period TEXT,
                type_of_document TEXT,
                earnings_per_share DOUBLE,
                next_year_forecast_earnings_per_share DOUBLE,
                forecast_eps DOUBLE,
                profit DOUBLE,
                sales DOUBLE,
                operating_cash_flow DOUBLE,
                investing_cash_flow DOUBLE,
                equity DOUBLE,
                total_assets DOUBLE,
                bps DOUBLE
            )
            """
        )
        rows = [
            ("1001", "2025-12-20", "FY", "FY", 10, 15, 14, 100, 1000, 120, -20, 500, 900, 100),
            ("1001", "2026-01-05", "1Q", "1Q", 2, None, None, 20, 200, 30, -5, 520, 930, None),
            ("1001", "2026-01-20", "FY", "FY", -5, -2, -3, -50, 800, -60, -70, 100, 1000, 20),
            ("1002", "2025-12-25", "FY", "FY", -1, -4, -4, -30, 600, -40, -50, 80, 1000, 10),
            ("1003", "2025-12-25", "FY", "FY", 3, 3, 3, 20, 500, -10, -30, 120, 1000, 80),
            ("1004", "2025-12-25", "FY", "FY", 5, 6, 6, 30, 700, 40, 5, 600, 1000, 200),
            ("1005", "2025-12-25", "FY", "FY", -2, -1, -1, -10, 400, 5, -20, 60, 1000, 40),
        ]
        conn.executemany(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        conn.close()


def _build_input_result(db_path: str) -> FallingKnifeReversalStudyResult:
    event_df = pd.DataFrame(
        {
            "signal_date": [
                "2026-01-10",
                "2026-01-10",
                "2026-01-10",
                "2026-01-10",
                "2026-01-10",
            ],
            "code": ["1001", "1002", "1003", "1004", "1005"],
            "market_name": ["プライム", "グロース", "グロース", "スタンダード", "グロース"],
            "close": [100.0, 120.0, 60.0, 180.0, 80.0],
            "risk_adjusted_bucket": ["Q2", "Q5_highest", "Q3", "Q5_highest", "Q2"],
            "condition_count": [2, 4, 3, 2, 2],
            "catch_return_20d": [0.05, -0.02, -0.12, 0.03, -0.01],
        }
    )
    empty_trade_summary = pd.DataFrame(columns=["strategy_family"])
    empty_paired_delta = pd.DataFrame(columns=["horizon_days"])
    empty_condition_profile = pd.DataFrame(columns=["condition_name"])
    return FallingKnifeReversalStudyResult(
        db_path=db_path,
        source_mode="live",
        source_detail="test db",
        available_start_date="2026-01-01",
        available_end_date="2026-01-31",
        analysis_start_date="2026-01-10",
        analysis_end_date="2026-01-10",
        market_codes=("0111", "0112", "0113"),
        forward_horizons=(20,),
        risk_adjusted_lookback=60,
        condition_ratio_type="sortino",
        five_day_drop_threshold=-0.10,
        twenty_day_drop_threshold=-0.20,
        sixty_day_drawdown_threshold=-0.25,
        risk_adjusted_threshold=0.0,
        min_condition_count=2,
        max_wait_days=10,
        signal_cooldown_days=20,
        severe_loss_threshold=-0.10,
        source_row_count=5,
        event_count=5,
        wait_candidate_count=0,
        research_note="test",
        event_df=event_df,
        trade_summary_df=empty_trade_summary,
        paired_delta_df=empty_paired_delta,
        condition_profile_df=empty_condition_profile,
    )


def test_non_rebound_profile_labels_and_scores_features(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_statement_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260427_140000_inputabcd",
    )

    result = run_falling_knife_non_rebound_fundamental_profile(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
    )

    assert result.baseline_count == 5
    assert result.non_rebound_count == 3
    assert result.rebound_count == 2
    assert result.non_rebound_rate_pct == 60.0
    row_1001 = result.enriched_event_df[result.enriched_event_df["code"] == "1001"].iloc[0]
    assert row_1001["disclosed_date"] == "2026-01-05"
    assert not bool(row_1001["non_rebound"])
    assert float(row_1001["pbr"]) == 1.0
    assert float(row_1001["per"]) == 10.0
    assert float(row_1001["forward_per"]) == 100.0 / 15.0
    row_1002 = result.enriched_event_df[result.enriched_event_df["code"] == "1002"].iloc[0]
    assert row_1002["per_bucket"] == "non_positive_eps"
    assert row_1002["forward_per_bucket"] == "non_positive_forecast_eps"
    growth_row = result.fundamental_profile_summary_df[
        (result.fundamental_profile_summary_df["feature_name"] == "market_name")
        & (result.fundamental_profile_summary_df["feature_value"] == "グロース")
    ].iloc[0]
    assert float(growth_row["non_rebound_rate_pct"]) == 100.0
    low_quality = result.feature_lift_summary_df[
        result.feature_lift_summary_df["feature_name"] == "low_quality"
    ].iloc[0]
    assert int(low_quality["non_rebound_count"]) == 3
    assert float(low_quality["prevalence_in_non_rebound_pct"]) == 100.0
    pbr_ge3 = result.feature_lift_summary_df[
        result.feature_lift_summary_df["feature_name"] == "pbr_ge3"
    ].iloc[0]
    assert int(pbr_ge3["non_rebound_count"]) == 1


def test_non_rebound_profile_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_statement_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260427_140000_inputabcd",
    )
    result = run_falling_knife_non_rebound_fundamental_profile(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
    )

    bundle = write_falling_knife_non_rebound_fundamental_profile_bundle(
        result,
        output_root=tmp_path,
        run_id="20260427_140500_testabcd",
    )
    reloaded = load_falling_knife_non_rebound_fundamental_profile_bundle(
        bundle.bundle_dir,
    )

    assert bundle.experiment_id == FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID
    assert (
        get_falling_knife_non_rebound_fundamental_profile_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_falling_knife_non_rebound_fundamental_profile_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pdt.assert_frame_equal(
        reloaded.feature_lift_summary_df,
        result.feature_lift_summary_df,
        check_dtype=False,
    )
