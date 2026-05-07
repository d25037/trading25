from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.falling_knife_long_horizon_technical_profile import (
    FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID,
    _median_side,
    _return_stats,
    get_falling_knife_long_horizon_technical_profile_bundle_path_for_run_id,
    get_falling_knife_long_horizon_technical_profile_latest_bundle_path,
    load_falling_knife_long_horizon_technical_profile_bundle,
    run_falling_knife_long_horizon_technical_profile,
    write_falling_knife_long_horizon_technical_profile_bundle,
)
from src.domains.analytics.falling_knife_reversal_study import (
    FallingKnifeReversalStudyResult,
    write_falling_knife_reversal_study_bundle,
)


def _create_price_db(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
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
                created_at TEXT
            )
            """
        )
        dates = pd.bdate_range("2024-12-30", periods=270).strftime("%Y-%m-%d")
        rows: list[tuple[object, ...]] = []
        for idx, date in enumerate(dates):
            rows.extend(
                [
                    ("1001", date, 100 + idx * 0.10, 101 + idx * 0.10, 99 + idx * 0.10, 100 + idx * 0.10, 100_000, 1.0, None),
                    ("1002", date, 150 - idx * 0.12, 151 - idx * 0.12, 149 - idx * 0.12, 150 - idx * 0.12, 80_000, 1.0, None),
                    ("1003", date, 80 + idx * 0.05, 81 + idx * 0.05, 79 + idx * 0.05, 80 + idx * 0.05, 50_000, 1.0, None),
                ]
            )
        rows.append(("1001", "2026-01-15", 500.0, 510.0, 490.0, 505.0, 200_000, 1.0, None))
        conn.executemany(
            """
            INSERT INTO stock_data (
                code, date, open, high, low, close, volume, adjustment_factor, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        conn.close()


def _build_input_result(db_path: str) -> FallingKnifeReversalStudyResult:
    event_df = pd.DataFrame(
        {
            "signal_date": ["2026-01-09", "2026-01-09", "2026-01-09"],
            "code": ["1001", "1002", "1003"],
            "market_name": ["スタンダード", "スタンダード", "グロース"],
            "risk_adjusted_bucket": ["Q2", "Q5_highest", "Q1_lowest"],
            "condition_count": [2, 3, 2],
            "catch_return_20d": [0.08, -0.16, -0.11],
            "catch_return_5d": [0.03, -0.04, -0.02],
            "catch_return_60d": [0.18, -0.24, -0.08],
        }
    )
    empty_trade_summary = pd.DataFrame(columns=["strategy_family"])
    empty_paired_delta = pd.DataFrame(columns=["horizon_days"])
    empty_condition_profile = pd.DataFrame(columns=["condition_name"])
    return FallingKnifeReversalStudyResult(
        db_path=db_path,
        source_mode="live",
        source_detail="test db",
        available_start_date="2024-12-30",
        available_end_date="2026-01-15",
        analysis_start_date="2026-01-09",
        analysis_end_date="2026-01-09",
        market_codes=("0112", "0113"),
        forward_horizons=(5, 20, 60),
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
        source_row_count=3,
        event_count=3,
        wait_candidate_count=0,
        research_note="test",
        event_df=event_df,
        trade_summary_df=empty_trade_summary,
        paired_delta_df=empty_paired_delta,
        condition_profile_df=empty_condition_profile,
    )


def test_long_horizon_technical_profile_uses_signal_date_features(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_price_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260506_input",
    )

    result = run_falling_knife_long_horizon_technical_profile(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        bucket_count=3,
    )

    assert result.baseline_count == 3
    assert result.technical_feature_count == 3
    assert {
        "return_252d_pct",
        "rebound_from_252d_low_pct",
        "drawdown_from_252d_high_pct",
        "range_position_252d",
        "price_to_sma250",
        "history_class",
    }.issubset(result.enriched_event_df.columns)
    row_1001 = result.enriched_event_df[result.enriched_event_df["code"] == "1001"].iloc[0]
    assert row_1001["feature_date"] == "2026-01-09"
    assert float(row_1001["close"]) < 200.0
    assert row_1001["history_class"] == "has_250_prior_closes"
    assert not result.technical_bucket_summary_df.empty
    assert not result.technical_rule_summary_df.empty
    assert not result.feature_rank_df.empty


def test_return_stats_respects_precomputed_labels_and_missing_technical_sides() -> None:
    frame = pd.DataFrame(
        {
            "catch_return": [0.08, -0.16, -0.11],
            "non_rebound": [True, True, True],
            "severe_loss": [False, True, False],
        }
    )

    stats = _return_stats(frame, severe_loss_threshold=-0.15)
    sides = _median_side(
        pd.Series([float("nan"), 0.2, 0.8]),
        low_label="low",
        high_label="high",
        missing_label="missing",
    )

    assert stats["non_rebound_rate_pct"] == pytest.approx(100.0)
    assert stats["severe_loss_rate_pct"] == pytest.approx(100.0 / 3.0)
    assert sides.tolist() == ["missing", "low", "high"]


def test_long_horizon_technical_profile_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_price_db(db_path)
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(str(db_path)),
        output_root=tmp_path,
        run_id="20260506_input",
    )
    result = run_falling_knife_long_horizon_technical_profile(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        bucket_count=3,
    )

    bundle = write_falling_knife_long_horizon_technical_profile_bundle(
        result,
        output_root=tmp_path,
        run_id="20260506_long_tech",
    )
    loaded = load_falling_knife_long_horizon_technical_profile_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID
    assert (
        get_falling_knife_long_horizon_technical_profile_bundle_path_for_run_id(
            "20260506_long_tech",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_falling_knife_long_horizon_technical_profile_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pdt.assert_frame_equal(
        loaded.feature_rank_df,
        result.feature_rank_df,
        check_dtype=False,
    )
