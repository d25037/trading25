from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.falling_knife_fundamental_quality_pruning import (
    FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID,
    get_falling_knife_fundamental_quality_pruning_bundle_path_for_run_id,
    get_falling_knife_fundamental_quality_pruning_latest_bundle_path,
    load_falling_knife_fundamental_quality_pruning_bundle,
    run_falling_knife_fundamental_quality_pruning,
    write_falling_knife_fundamental_quality_pruning_bundle,
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
        rows = [
            ("1001", "2025-12-20", 10, 100, 500, "FY", "FY", 15, None, 1000, None, None, 120, None, None, None, None, None, None, 14, -20, None, None, 900, None, None),
            ("1001", "2026-01-20", -5, -50, 100, "FY", "FY", -2, None, 800, None, None, -60, None, None, None, None, None, None, -3, -70, None, None, 1000, None, None),
            ("1002", "2025-12-25", -1, -30, 80, "FY", "FY", -4, None, 600, None, None, -40, None, None, None, None, None, None, -4, -50, None, None, 1000, None, None),
            ("1003", "2025-12-25", 3, 20, 120, "FY", "FY", 3, None, 500, None, None, -10, None, None, None, None, None, None, 3, -30, None, None, 1000, None, None),
            ("1004", "2025-12-25", 5, 30, 600, "FY", "FY", 6, None, 700, None, None, 40, None, None, None, None, None, None, 6, 5, None, None, 1000, None, None),
        ]
        conn.executemany(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            "code": ["1001", "1002", "1003", "1004", "1999"],
            "market_name": ["プライム", "グロース", "グロース", "スタンダード", "グロース"],
            "risk_adjusted_bucket": ["Q2", "Q5_highest", "Q3", "Q5_highest", "Q2"],
            "condition_count": [2, 4, 3, 2, 2],
            "catch_return_20d": [0.05, -0.25, -0.12, 0.03, -0.11],
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


def test_fundamental_quality_pruning_uses_as_of_statement_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_statement_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260427_130000_inputabcd",
    )

    result = run_falling_knife_fundamental_quality_pruning(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
        min_quality_score=3,
    )

    assert result.baseline_count == 5
    assert result.statement_coverage_pct == 80.0
    row_1001 = result.enriched_event_df[result.enriched_event_df["code"] == "1001"].iloc[0]
    assert row_1001["disclosed_date"] == "2025-12-20"
    assert row_1001["profit_sign"] == "profit_positive"
    assert row_1001["quality_bucket"] == "high_quality"
    row_1002 = result.enriched_event_df[result.enriched_event_df["code"] == "1002"].iloc[0]
    assert row_1002["market_name"] == "グロース"
    assert row_1002["quality_bucket"] == "low_quality"
    assert not result.quality_rule_summary_df.empty
    growth_low = result.quality_rule_summary_df[
        result.quality_rule_summary_df["rule_name"] == "exclude_growth_low_quality"
    ].iloc[0]
    assert int(growth_low["removed_count"]) == 2
    assert float(growth_low["kept_severe_loss_rate_pct"]) < float(
        growth_low["baseline_severe_loss_rate_pct"]
    )


def test_fundamental_quality_pruning_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_statement_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260427_130000_inputabcd",
    )
    result = run_falling_knife_fundamental_quality_pruning(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
    )

    bundle = write_falling_knife_fundamental_quality_pruning_bundle(
        result,
        output_root=tmp_path,
        run_id="20260427_130500_testabcd",
    )
    reloaded = load_falling_knife_fundamental_quality_pruning_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID
    assert (
        get_falling_knife_fundamental_quality_pruning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_falling_knife_fundamental_quality_pruning_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pdt.assert_frame_equal(
        reloaded.quality_rule_summary_df,
        result.quality_rule_summary_df,
        check_dtype=False,
    )
