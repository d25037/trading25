from __future__ import annotations

import math
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    ResearchVariant,
    build_decision_gate_df,
    build_evidence_tables,
    build_position_signal_frames,
    classify_score_ring,
    entry_rule_matches,
    execute_variant,
    exit_rule_matches,
    holm_adjust,
    moving_block_bootstrap_delta_ci,
    prepare_position_signal_panel,
    run_ranking_sma5_score_ring_hard_filter_research,
)
from tests.unit.domains.analytics.test_ranking_sma5_position_state_evidence import (
    _build_sma5_position_state_db,
)


@pytest.mark.parametrize(
    ("value", "leadership", "expected"),
    [
        (0.80, 0.80, "core_high_high"),
        (0.79, 0.80, "near_high_high_1"),
        (0.70, 0.70, "near_high_high_1"),
        (0.69, 0.70, "near_high_high_2"),
        (0.60, 0.60, "near_high_high_2"),
        (0.59, 0.90, "outside"),
        (None, 0.90, "missing"),
        (math.nan, 0.90, "missing"),
        ("not-a-score", 0.90, "missing"),
    ],
)
def test_classify_score_ring(value: object, leadership: object, expected: str) -> None:
    assert classify_score_ring(value, leadership) == expected


def test_frozen_entry_and_exit_predicates() -> None:
    row = {
        "close": 101.0,
        "sma5": 100.0,
        "sma5_above_count_5d": 2,
        "sma5_below_streak": 0,
        "sma5_atr20_deviation": 0.75,
    }
    assert entry_rule_matches(row, "E0_no_sma5_filter")
    assert entry_rule_matches(row, "E1_close_above_sma5")
    assert entry_rule_matches(row, "E2_count_ge_2")
    assert entry_rule_matches(row, "E3_avoid_atr20_chase")
    assert entry_rule_matches(row, "E4_count_ge_2_and_avoid_chase")
    assert not exit_rule_matches(row, "X0_no_sma5_exit")
    assert not exit_rule_matches(row, "X1_close_below_sma5")
    assert not exit_rule_matches(row, "X2_count_le_1")
    assert not exit_rule_matches(row, "X3_below_streak_ge_3")
    assert not exit_rule_matches(row, "X4_atr20_below_le_neg1")


@pytest.mark.parametrize(
    ("rule_id", "row"),
    [
        ("X1_close_below_sma5", {"close": 99.0, "sma5": 100.0}),
        ("X2_count_le_1", {"sma5_above_count_5d": 1}),
        ("X3_below_streak_ge_3", {"sma5_below_streak": 3}),
        ("X4_atr20_below_le_neg1", {"sma5_atr20_deviation": -1.0}),
    ],
)
def test_active_exit_rules_match_their_frozen_boundaries(
    rule_id: str,
    row: dict[str, object],
) -> None:
    assert exit_rule_matches(row, rule_id)


@pytest.mark.parametrize(
    ("rule_id", "row", "expected"),
    [
        ("E1_close_above_sma5", {"close": None, "sma5": 100.0}, False),
        ("E2_count_ge_2", {"sma5_above_count_5d": None}, False),
        ("E3_avoid_atr20_chase", {"sma5_atr20_deviation": None}, False),
        (
            "E4_count_ge_2_and_avoid_chase",
            {"sma5_above_count_5d": 2, "sma5_atr20_deviation": None},
            False,
        ),
        ("X1_close_below_sma5", {"close": None, "sma5": 100.0}, False),
        ("X2_count_le_1", {"sma5_above_count_5d": None}, False),
        ("X3_below_streak_ge_3", {"sma5_below_streak": None}, False),
        ("X4_atr20_below_le_neg1", {"sma5_atr20_deviation": None}, False),
    ],
)
def test_active_rules_fail_closed_for_missing_numeric_inputs(
    rule_id: str,
    row: dict[str, object],
    expected: bool,
) -> None:
    matcher = entry_rule_matches if rule_id.startswith("E") else exit_rule_matches
    assert matcher(row, rule_id) is expected


def test_unknown_rule_ids_fail_loudly() -> None:
    with pytest.raises(ValueError, match="unknown entry rule"):
        entry_rule_matches({}, "E99_unknown")
    with pytest.raises(ValueError, match="unknown exit rule"):
        exit_rule_matches({}, "X99_unknown")


def test_position_state_enters_on_false_to_true_and_does_not_same_day_reenter() -> None:
    frames = build_position_signal_frames(
        _synthetic_feature_frame(),
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )
    events = frames.state_events
    assert events.loc[events["event_type"].eq("entry"), "date"].tolist() == [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-07"),
    ]
    assert not (
        events.groupby(["date", "code"])["event_type"]
        .agg(list)
        .map(lambda values: "entry" in values and "exit" in values)
        .any()
    )


def test_prepared_signal_panel_preserves_public_position_state_semantics() -> None:
    feature_df = _synthetic_feature_frame()
    direct = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )
    prepared = prepare_position_signal_panel(feature_df)
    reused = build_position_signal_frames(
        prepared,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )

    assert_frame_equal(reused.close, direct.close)
    assert_frame_equal(reused.entries, direct.entries)
    assert_frame_equal(reused.exits, direct.exits)
    assert_frame_equal(reused.held_intervals, direct.held_intervals)
    assert_frame_equal(reused.state_events, direct.state_events)


def test_prepared_signal_panel_builds_variant_frames_without_dataframe_row_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_position_signal_panel(_synthetic_feature_frame())

    def fail_dataframe_row_iteration(*args: object, **kwargs: object) -> object:
        raise AssertionError("variant generation must not iterate DataFrame rows")

    monkeypatch.setattr(pd.DataFrame, "iterrows", fail_dataframe_row_iteration)
    monkeypatch.setattr(pd.DataFrame, "to_dict", fail_dataframe_row_iteration)

    frames = build_position_signal_frames(
        prepared,
        ring_id="near_high_high_1",
        entry_rule_id="E4_count_ge_2_and_avoid_chase",
        exit_rule_id="X3_below_streak_ge_3",
        max_holding_sessions=20,
    )

    assert not frames.close.empty


def test_position_frames_use_globally_sorted_codes_for_staggered_listings() -> None:
    later_codes = [
        _single_code_frame(
            [
                ("2025-01-02", 0.8, 0.8, 2),
                ("2025-01-03", 0.8, 0.8, 2),
            ],
            code=code,
        )
        for code in ("1000", "2000")
    ]
    earlier_code = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 0),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
        ],
        code="3000",
    )
    feature_df = pd.concat([earlier_code, *later_codes], ignore_index=True)

    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )

    expected_codes = ["1000", "2000", "3000"]
    assert frames.close.columns.tolist() == expected_codes
    assert frames.entries.columns.tolist() == expected_codes
    assert frames.exits.columns.tolist() == expected_codes
    assert frames.held_intervals.columns.tolist() == expected_codes
    assert frames.state_events.loc[
        frames.state_events["event_type"].eq("entry"), "code"
    ].tolist() == expected_codes
    assert frames.state_events.loc[
        frames.state_events["event_type"].eq("exit"), "code"
    ].tolist() == expected_codes


def test_entry_day_is_excluded_and_exit_day_return_is_included_once() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 1),
            ("2025-01-07", 0.5, 0.5, 1),
        ],
        closes=[100.0, 110.0, 121.0, 133.1, 146.41],
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )

    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-02"), "1001"]
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-03"), "1001"]
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]
    returns = frames.close["1001"].pct_change(fill_method=None)
    included_returns = returns.loc[frames.held_intervals["1001"]]
    assert included_returns.tolist() == pytest.approx([0.10, 0.10])
    assert included_returns.sum() == pytest.approx(0.20)


def test_wider_rings_include_rows_classified_into_more_selective_rings() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
        ]
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="near_high_high_1",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )

    assert classify_score_ring(0.8, 0.8) == "core_high_high"
    assert frames.entries.loc[pd.Timestamp("2025-01-02"), "1001"]


def test_ring_exit_has_precedence_over_sma5_exit() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.7, 0.8, 1),
            ("2025-01-06", 0.7, 0.8, 1),
        ]
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )

    exit_event = frames.state_events.loc[
        frames.state_events["event_type"].eq("exit")
    ].iloc[0]
    assert exit_event["date"] == pd.Timestamp("2025-01-03")
    assert exit_event["exit_reason"] == "ring_exit"


@pytest.mark.parametrize("max_holding_sessions", [20, 60])
def test_time_exit_uses_the_requested_session_cap(max_holding_sessions: int) -> None:
    dates = pd.bdate_range("2025-01-01", periods=max_holding_sessions + 3)
    rows = [
        (str(date.date()), 0.5 if index == 0 else 0.8, 0.5 if index == 0 else 0.8, 2)
        for index, date in enumerate(dates)
    ]
    frames = build_position_signal_frames(
        _single_code_frame(rows),
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=max_holding_sessions,
    )

    exit_event = frames.state_events.loc[
        frames.state_events["event_type"].eq("exit")
    ].iloc[0]
    assert exit_event["date"] == dates[max_holding_sessions + 1]
    assert exit_event["exit_reason"] == "time_exit"
    assert int(frames.held_intervals["1001"].sum()) == max_holding_sessions


def test_terminal_open_position_includes_return_into_last_finite_close() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 2),
        ]
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )

    terminal_exit = frames.state_events.loc[
        frames.state_events["event_type"].eq("exit")
    ].iloc[0]
    assert terminal_exit["date"] == pd.Timestamp("2025-01-06")
    assert terminal_exit["exit_reason"] == "terminal_exit"
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]


def test_middle_missing_close_is_not_forward_filled() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 2),
            ("2025-01-07", 0.8, 0.8, 2),
        ],
        closes=[100.0, 101.0, None, 104.0, 105.0],
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )

    missing_date = pd.Timestamp("2025-01-03")
    assert pd.isna(frames.close.loc[missing_date, "1001"])
    assert not frames.held_intervals.loc[missing_date, "1001"]
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]


def test_exit_rearms_only_after_entry_eligibility_becomes_false() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 1),
            ("2025-01-06", 0.8, 0.8, 2),
            ("2025-01-07", 0.5, 0.5, 2),
            ("2025-01-08", 0.8, 0.8, 2),
            ("2025-01-09", 0.8, 0.8, 2),
        ]
    )
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E0_no_sma5_filter",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )

    entry_dates = frames.state_events.loc[
        frames.state_events["event_type"].eq("entry"), "date"
    ].tolist()
    assert entry_dates == [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-08"),
    ]


def test_market_v5_panel_contains_frozen_scores_and_sma_features(
    tmp_path: Path,
) -> None:
    db_path = _build_hard_filter_market_v5_db(tmp_path / "market.duckdb")

    result = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date="2024-01-01",
        end_date="2024-12-31",
        bootstrap_resamples=100,
        min_trades=1,
        min_signal_dates=1,
    )

    assert result.pit_lineage.market_schema_version == 5
    assert result.pit_lineage.stock_price_adjustment_mode == "provider_adjusted_v1"
    assert result.pit_lineage.market_source
    assert result.pit_lineage.source_mode == result.source_mode
    assert {
        "value_composite_equal_score",
        "long_hybrid_leadership_score",
        "sma5",
        "sma5_above_count_5d",
        "sma5_below_streak",
        "sma5_atr20_deviation",
    }.issubset(result.observation_sample_df.columns)
    panel = result.feature_df
    assert result.analysis_start_date == panel["date"].min().date().isoformat()
    assert result.analysis_end_date == panel["date"].max().date().isoformat()
    assert result.analysis_end_date != "2024-12-31"
    expected_below_streak = pd.Series(0, index=panel.index, dtype="int64")
    expected_below_streak.loc[panel["close_below_sma5_flag"].eq(1)] = 1
    expected_below_streak.loc[panel["below_sma5_streak_ge3_flag"].eq(True)] = 3
    assert panel["sma5_below_streak"].fillna(-1).astype(int).equals(
        expected_below_streak
    )
    default_end_result = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date="2024-01-01",
        bootstrap_resamples=100,
        min_trades=1,
        min_signal_dates=1,
    )
    assert default_end_result.analysis_start_date is not None
    assert default_end_result.analysis_end_date is not None
    assert default_end_result.analysis_end_date == (
        default_end_result.feature_df["date"].max().date().isoformat()
    )


def test_market_v5_panel_excludes_code_dates_without_usable_value_inputs(
    tmp_path: Path,
) -> None:
    db_path = _build_hard_filter_market_v5_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE daily_valuation
        SET forward_per = NULL,
            pbr = NULL,
            fundamentals_adjustment_basis_date = NULL,
            source_fingerprint = NULL
        WHERE code = '1111'
        """
    )
    conn.close()

    result = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date="2024-01-01",
        end_date="2024-12-31",
        bootstrap_resamples=100,
        min_trades=1,
        min_signal_dates=1,
    )

    assert "1111" not in set(result.feature_df["code"].astype(str))


@pytest.mark.parametrize(
    ("schema_version", "adjustment_mode"),
    [
        (4, "provider_adjusted_v1"),
        (5, "legacy_adjusted_v1"),
    ],
)
def test_market_v5_panel_fails_closed_on_incompatible_provenance(
    tmp_path: Path,
    schema_version: int,
    adjustment_mode: str,
) -> None:
    db_path = _build_hard_filter_market_v5_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute("DELETE FROM market_schema_version")
    conn.execute("INSERT INTO market_schema_version VALUES (?, NULL, NULL)", [schema_version])
    conn.execute(
        "UPDATE sync_metadata SET value = ? "
        "WHERE key = 'stock_price_adjustment_mode'",
        [adjustment_mode],
    )
    conn.close()

    with pytest.raises(RuntimeError, match="Incompatible market.duckdb metadata"):
        run_ranking_sma5_score_ring_hard_filter_research(db_path)


def test_execute_variant_uses_vectorbt_same_close_fills_and_fee_ledger() -> None:
    feature_df = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 1),
            ("2025-01-07", 0.5, 0.5, 1),
        ],
        closes=[100.0, 100.0, 110.0, 110.0, 110.0],
    )

    execution = execute_variant(
        feature_df,
        ResearchVariant(
            ring_id="core_high_high",
            entry_rule_id="E2_count_ge_2",
            exit_rule_id="X2_count_le_1",
            max_holding_sessions=60,
        ),
        fee_bps=10.0,
    )

    trades = execution.trade_records_df
    assert len(trades) == 1
    assert pd.Timestamp(str(trades.loc[0, "Entry Timestamp"])) == pd.Timestamp("2025-01-02")
    assert pd.Timestamp(str(trades.loc[0, "Exit Timestamp"])) == pd.Timestamp("2025-01-06")
    # VectorBT normalizes net PnL by gross entry value; fees do not compound.
    assert float(str(trades.loc[0, "Return"])) == pytest.approx(
        0.10 - 0.0005 - (1.10 * 0.0005)
    )
    assert execution.daily_portfolio_returns.loc[pd.Timestamp("2025-01-02")] == pytest.approx(
        -0.0005 / 1.0005
    )
    assert execution.daily_portfolio_returns.loc[pd.Timestamp("2025-01-03")] == pytest.approx(
        0.10
    )
    assert execution.daily_portfolio_returns.loc[pd.Timestamp("2025-01-06")] == pytest.approx(
        -0.0005
    )
    assert execution.state_events["event_type"].tolist() == ["entry", "exit"]
    assert not hasattr(execution, "portfolio")
    assert not hasattr(execution, "signal_frames")


def test_execute_variant_does_not_dilute_held_return_with_new_entry_fee() -> None:
    held_code = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.5, 0.5, 1),
        ],
        code="1001",
        closes=[100.0, 100.0, 110.0, 110.0],
    )
    entering_code = _single_code_frame(
        [
            ("2025-01-01", 0.5, 0.5, 1),
            ("2025-01-02", 0.5, 0.5, 1),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 2),
        ],
        code="1002",
        closes=[100.0, 100.0, 100.0, 100.0],
    )

    execution = execute_variant(
        pd.concat([held_code, entering_code], ignore_index=True),
        ResearchVariant(
            ring_id="core_high_high",
            entry_rule_id="E2_count_ge_2",
            exit_rule_id="X2_count_le_1",
            max_holding_sessions=60,
        ),
        fee_bps=10.0,
    )

    return_on_shared_date = execution.daily_portfolio_returns.loc[
        pd.Timestamp("2025-01-03")
    ]
    assert return_on_shared_date == pytest.approx(0.10 - ((0.0005 / 1.0005) / 2.0))
    assert return_on_shared_date > 0.09


def test_moving_block_bootstrap_is_paired_and_reproducible() -> None:
    baseline = pd.Series([0.0, -0.01, 0.0, -0.01] * 20)
    candidate = baseline + 0.002

    first = moving_block_bootstrap_delta_ci(
        candidate,
        baseline,
        block_length=4,
        resamples=500,
        seed=20260724,
    )
    second = moving_block_bootstrap_delta_ci(
        candidate,
        baseline,
        block_length=4,
        resamples=500,
        seed=20260724,
    )

    assert first == second
    assert first.lower > 0.0


def test_moving_block_bootstrap_aligns_union_dates_with_inactive_cash_returns() -> None:
    candidate = pd.Series(
        [0.03, 0.01],
        index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
    )
    baseline = pd.Series(
        [0.02, -0.02],
        index=pd.to_datetime(["2025-01-03", "2025-01-06"]),
    )

    interval = moving_block_bootstrap_delta_ci(
        candidate,
        baseline,
        block_length=1,
        resamples=100,
        seed=20260724,
    )

    assert interval.estimate == pytest.approx((0.03 - 0.01 + 0.02) / 3.0)


def test_holm_adjustment_preserves_original_order() -> None:
    adjusted = holm_adjust([0.01, 0.04, 0.03, None])

    assert adjusted == pytest.approx([0.03, 0.06, 0.06, None])


@pytest.mark.parametrize(
    ("target", "column", "value", "failed_gate"),
    [
        ("primary", "ci_lower", 0.0, "passes_bootstrap_ci"),
        ("primary", "adjusted_p_value", 0.05, "passes_adjusted_p"),
        ("primary", "trade_count", 199, "passes_trade_count"),
        ("primary", "signal_date_count", 99, "passes_signal_date_count"),
        ("primary", "annualized_ir_delta", 0.149, "passes_ir_lift"),
        ("primary", "tail_improvement_ratio", 0.0999, "passes_tail_improvement"),
        ("primary", "turnover_ratio", 1.501, "passes_turnover"),
        ("stress_cost", "net_mean_return_delta", -0.0001, "passes_cost_sensitivity"),
        ("annual", "net_mean_return_delta", -0.001, "passes_annual_stability"),
        ("holdout", "net_mean_return_delta", -0.001, "passes_holdout_direction"),
        ("near", "net_mean_return_delta", -0.001, "passes_robustness_sign"),
    ],
)
def test_decision_gate_independently_fails_each_frozen_boundary(
    target: str,
    column: str,
    value: object,
    failed_gate: str,
) -> None:
    evidence, annual, costs = _passing_decision_gate_inputs()
    if target == "primary":
        evidence.loc[evidence["period"].eq("oos") & evidence["is_primary"], column] = value
    elif target == "holdout":
        evidence.loc[evidence["period"].eq("holdout"), column] = value
    elif target == "near":
        evidence.loc[evidence["ring_id"].eq("near_high_high_1"), column] = value
    elif target == "annual":
        annual.loc[annual["year"].isin([2023, 2024]), column] = value
    else:
        costs.loc[costs["cost_bps"].eq(20.0), column] = value

    decision = build_decision_gate_df(evidence, annual, costs)

    variant = decision.loc[decision["row_type"].eq("variant")].iloc[0]
    assert not bool(variant[failed_gate])
    assert variant["decision"] == "insufficient_evidence"


def test_decision_gate_accepts_exact_inclusive_operational_boundaries() -> None:
    evidence, annual, costs = _passing_decision_gate_inputs()
    primary_mask = evidence["period"].eq("oos") & evidence["is_primary"]
    evidence.loc[primary_mask, "trade_count"] = 200
    evidence.loc[primary_mask, "signal_date_count"] = 100
    evidence.loc[primary_mask, "annualized_ir_delta"] = 0.15
    evidence.loc[primary_mask, "tail_improvement_ratio"] = 0.10
    evidence.loc[primary_mask, "turnover_ratio"] = 1.5

    decision = build_decision_gate_df(evidence, annual, costs)

    variant = decision.loc[decision["row_type"].eq("variant")].iloc[0]
    assert variant["decision"] == "production_candidate"
    assert bool(variant["all_required_gates"])
    entry = decision.loc[
        decision["row_type"].eq("family") & decision["family"].eq("entry")
    ].iloc[0]
    assert entry["decision"] == "production_candidate"


def test_decision_gate_keeps_holm_families_and_combined_outcome_independent() -> None:
    entry_evidence, entry_annual, entry_costs = _passing_decision_gate_inputs()
    exit_evidence = entry_evidence.assign(
        family="exit",
        variant_id="X1_close_below_sma5",
    )
    exit_annual = entry_annual.assign(
        family="exit",
        variant_id="X1_close_below_sma5",
    )
    exit_costs = entry_costs.assign(
        family="exit",
        variant_id="X1_close_below_sma5",
    )
    evidence = pd.concat([entry_evidence, exit_evidence], ignore_index=True)
    annual = pd.concat([entry_annual, exit_annual], ignore_index=True)
    costs = pd.concat([entry_costs, exit_costs], ignore_index=True)

    decision = build_decision_gate_df(evidence, annual, costs)

    family = decision.loc[decision["row_type"].eq("family")].set_index("family")
    assert family.loc["entry", "decision"] == "production_candidate"
    assert family.loc["exit", "decision"] == "production_candidate"
    assert family.loc["combined", "decision"] == "not_evaluated"

    evidence.loc[evidence["family"].eq("exit") & evidence["period"].eq("holdout"), "net_mean_return_delta"] = -0.001
    decision = build_decision_gate_df(evidence, annual, costs)
    family = decision.loc[decision["row_type"].eq("family")].set_index("family")
    assert family.loc["exit", "decision"] == "insufficient_evidence"
    assert family.loc["combined", "decision"] == "not_evaluated"


def test_decision_gate_leaves_combined_not_evaluated_without_pre_holdout_passes() -> None:
    evidence, annual, costs = _passing_decision_gate_inputs()
    evidence.loc[
        evidence["period"].eq("oos") & evidence["is_primary"], "adjusted_p_value"
    ] = 0.05

    decision = build_decision_gate_df(evidence, annual, costs)

    combined = decision.loc[
        decision["row_type"].eq("family") & decision["family"].eq("combined")
    ].iloc[0]
    assert combined["decision"] == "not_evaluated"


def test_annual_gate_uses_distinct_core_60_oos_years_only() -> None:
    evidence, annual, costs = _passing_decision_gate_inputs()
    annual = pd.concat(
        [
            annual,
            annual.loc[annual["year"].eq(2022)],
            annual.assign(ring_id="near_high_high_1", net_mean_return_delta=-100.0),
            annual.assign(max_holding_sessions=20, net_mean_return_delta=-100.0),
            annual.assign(period="holdout", net_mean_return_delta=-100.0),
        ],
        ignore_index=True,
    )

    decision = build_decision_gate_df(evidence, annual, costs)

    variant = decision.loc[decision["row_type"].eq("variant")].iloc[0]
    assert variant["distinct_annual_year_count"] == 3
    assert variant["positive_annual_year_count"] == 3
    assert bool(variant["passes_positive_year_majority"])
    assert bool(variant["passes_not_single_year_dependent"])


def test_annual_gate_separately_rejects_single_best_year_dependence() -> None:
    evidence, annual, costs = _passing_decision_gate_inputs()
    annual["net_mean_return_delta"] = annual["year"].map(
        {2022: 0.30, 2023: 0.01, 2024: -0.02}
    )

    decision = build_decision_gate_df(evidence, annual, costs)

    variant = decision.loc[decision["row_type"].eq("variant")].iloc[0]
    assert bool(variant["passes_positive_year_majority"])
    assert not bool(variant["passes_not_single_year_dependent"])
    assert not bool(variant["passes_annual_stability"])


def test_combined_variant_requires_its_exact_entry_and_exit_components() -> None:
    entry_evidence, entry_annual, entry_costs = _passing_decision_gate_inputs()
    exit_evidence, exit_annual, exit_costs = _decision_inputs_for(
        "exit", "X1_close_below_sma5"
    )
    combined_evidence, combined_annual, combined_costs = _decision_inputs_for(
        "combined", "E2_count_ge_2__X2_count_le_1"
    )

    decision = build_decision_gate_df(
        pd.concat(
            [entry_evidence, exit_evidence, combined_evidence], ignore_index=True
        ),
        pd.concat([entry_annual, exit_annual, combined_annual], ignore_index=True),
        pd.concat([entry_costs, exit_costs, combined_costs], ignore_index=True),
    )

    combined_variant = decision.loc[
        decision["row_type"].eq("variant")
        & decision["family"].eq("combined")
    ].iloc[0]
    combined_family = decision.loc[
        decision["row_type"].eq("family")
        & decision["family"].eq("combined")
    ].iloc[0]
    assert combined_variant["decision"] == "not_evaluated"
    assert combined_family["decision"] == "not_evaluated"


def test_combined_variant_is_evaluated_when_exact_components_pass_pre_holdout() -> None:
    entry_evidence, entry_annual, entry_costs = _passing_decision_gate_inputs()
    exit_evidence, exit_annual, exit_costs = _decision_inputs_for(
        "exit", "X1_close_below_sma5"
    )
    combined_evidence, combined_annual, combined_costs = _decision_inputs_for(
        "combined", "E2_count_ge_2__X1_close_below_sma5"
    )

    decision = build_decision_gate_df(
        pd.concat(
            [entry_evidence, exit_evidence, combined_evidence], ignore_index=True
        ),
        pd.concat([entry_annual, exit_annual, combined_annual], ignore_index=True),
        pd.concat([entry_costs, exit_costs, combined_costs], ignore_index=True),
    )

    combined_variant = decision.loc[
        decision["row_type"].eq("variant")
        & decision["family"].eq("combined")
    ].iloc[0]
    assert combined_variant["decision"] == "production_candidate"


def test_e4_is_not_evaluated_until_e2_and_e3_pass_pre_holdout() -> None:
    e4_evidence, e4_annual, e4_costs = _decision_inputs_for(
        "entry", "E4_count_ge_2_and_avoid_chase"
    )

    decision = build_decision_gate_df(e4_evidence, e4_annual, e4_costs)

    e4 = decision.loc[decision["row_type"].eq("variant")].iloc[0]
    assert e4["decision"] == "not_evaluated"
    assert not bool(e4["passes_confirmatory_prerequisite"])


def test_e4_is_confirmatory_when_e2_and_e3_pass_pre_holdout() -> None:
    inputs = [
        _decision_inputs_for("entry", variant_id)
        for variant_id in (
            "E2_count_ge_2",
            "E3_avoid_atr20_chase",
            "E4_count_ge_2_and_avoid_chase",
        )
    ]
    decision = build_decision_gate_df(
        pd.concat([item[0] for item in inputs], ignore_index=True),
        pd.concat([item[1] for item in inputs], ignore_index=True),
        pd.concat([item[2] for item in inputs], ignore_index=True),
    )

    e4 = decision.loc[
        decision["row_type"].eq("variant")
        & decision["variant_id"].eq("E4_count_ge_2_and_avoid_chase")
    ].iloc[0]
    assert bool(e4["passes_confirmatory_prerequisite"])
    assert e4["decision"] == "production_candidate"


def test_evidence_tables_report_frozen_metrics_and_correct_entry_baseline() -> None:
    feature_df = _single_code_frame(
        [
            ("2024-01-02", 0.5, 0.5, 1),
            ("2024-01-03", 0.8, 0.8, 2),
            ("2024-01-04", 0.8, 0.8, 2),
            ("2024-01-05", 0.5, 0.5, 1),
        ],
        closes=[100.0, 100.0, 110.0, 110.0],
    )
    baseline = ResearchVariant(
        ring_id="core_high_high",
        entry_rule_id="E0_no_sma5_filter",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )
    candidate = ResearchVariant(
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X0_no_sma5_exit",
        max_holding_sessions=60,
    )
    executions = [
        execute_variant(feature_df, variant, fee_bps=fee_bps)
        for fee_bps in (0.0, 10.0, 20.0)
        for variant in (baseline, candidate)
    ]

    tables = build_evidence_tables(
        executions,
        block_length=2,
        resamples=100,
        seed=20260724,
    )

    row = tables.entry_rule_evidence_df.loc[
        tables.entry_rule_evidence_df["variant_id"].eq("E2_count_ge_2")
        & tables.entry_rule_evidence_df["period"].eq("oos")
    ].iloc[0]
    assert row["baseline_variant_id"] == "E0_no_sma5_filter"
    assert row["trade_count"] == 1
    assert row["signal_date_count"] == 1
    assert row["turnover"] == pytest.approx(0.5)
    assert {
        "gross_mean_return",
        "gross_median_return",
        "net_mean_return",
        "net_median_return",
        "annualized_ir",
        "max_drawdown",
        "expected_shortfall_5pct",
        "turnover",
        "net_mean_return_delta",
        "annualized_ir_delta",
        "tail_improvement_ratio",
        "turnover_ratio",
        "adjusted_p_value",
    }.issubset(tables.entry_rule_evidence_df.columns)
    assert not tables.bootstrap_effect_ci_df.empty
    assert set(tables.cost_sensitivity_df["cost_bps"]) == {10.0, 20.0}


def test_evidence_ir_uses_topix_excess_returns() -> None:
    feature_df = _single_code_frame(
        [
            ("2024-01-02", 0.5, 0.5, 1),
            ("2024-01-03", 0.8, 0.8, 2),
            ("2024-01-04", 0.8, 0.8, 2),
            ("2024-01-05", 0.5, 0.5, 1),
        ],
        closes=[100.0, 100.0, 110.0, 110.0],
        topix_closes=[100.0, 102.0, 101.0, 103.0],
    )
    baseline = ResearchVariant(
        "core_high_high", "E0_no_sma5_filter", "X0_no_sma5_exit", 60
    )
    candidate = ResearchVariant(
        "core_high_high", "E2_count_ge_2", "X0_no_sma5_exit", 60
    )
    executions = [
        execute_variant(feature_df, variant, fee_bps=fee_bps)
        for fee_bps in (0.0, 10.0, 20.0)
        for variant in (baseline, candidate)
    ]

    tables = build_evidence_tables(
        executions, block_length=2, resamples=100, seed=20260724
    )

    execution = next(
        item
        for item in executions
        if item.variant == candidate and item.fee_bps == 10.0
    )
    excess = (
        execution.daily_portfolio_returns - execution.benchmark_daily_returns
    )
    expected_ir = excess.mean() / excess.std(ddof=1) * math.sqrt(252.0)
    row = tables.entry_rule_evidence_df.loc[
        tables.entry_rule_evidence_df["period"].eq("oos")
    ].iloc[0]
    assert row["annualized_ir"] == pytest.approx(expected_ir)
    assert "benchmark_return" in tables.portfolio_daily_df.columns
    assert "topix_excess_return" in tables.portfolio_daily_df.columns


@pytest.mark.parametrize("invalid_topix_close", [math.inf, 0.0, -1.0])
def test_execute_variant_rejects_non_finite_or_non_positive_topix_close(
    invalid_topix_close: float,
) -> None:
    feature_df = _single_code_frame(
        [
            ("2024-01-02", 0.5, 0.5, 1),
            ("2024-01-03", 0.8, 0.8, 2),
            ("2024-01-04", 0.8, 0.8, 2),
            ("2024-01-05", 0.5, 0.5, 1),
        ],
        topix_closes=[100.0, invalid_topix_close, 101.0, 102.0],
    )

    with pytest.raises(ValueError, match="finite and strictly positive"):
        execute_variant(
            feature_df,
            ResearchVariant(
                "core_high_high",
                "E2_count_ge_2",
                "X0_no_sma5_exit",
                60,
            ),
            fee_bps=10.0,
        )


def test_period_trade_metrics_exclude_trades_closed_after_period_end() -> None:
    feature_df = _single_code_frame(
        [
            ("2024-12-27", 0.5, 0.5, 1),
            ("2024-12-30", 0.8, 0.8, 2),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.5, 0.5, 1),
        ],
        closes=[100.0, 100.0, 110.0, 110.0],
    )
    baseline = ResearchVariant(
        "core_high_high", "E0_no_sma5_filter", "X0_no_sma5_exit", 60
    )
    candidate = ResearchVariant(
        "core_high_high", "E2_count_ge_2", "X0_no_sma5_exit", 60
    )
    executions = [
        execute_variant(feature_df, variant, fee_bps=fee_bps)
        for fee_bps in (0.0, 10.0, 20.0)
        for variant in (baseline, candidate)
    ]

    tables = build_evidence_tables(
        executions, block_length=2, resamples=100, seed=20260724
    )

    oos = tables.entry_rule_evidence_df.loc[
        tables.entry_rule_evidence_df["period"].eq("oos")
    ].iloc[0]
    assert oos["trade_count"] == 0
    assert pd.isna(oos["net_mean_return"])


def _build_hard_filter_market_v5_db(db_path: Path) -> Path:
    return _build_sma5_position_state_db(db_path)


def _passing_decision_gate_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    common = {
        "family": "entry",
        "variant_id": "E2_count_ge_2",
        "ci_lower": 0.0001,
        "ci_upper": 0.002,
        "adjusted_p_value": 0.049,
        "trade_count": 200,
        "signal_date_count": 100,
        "annualized_ir_delta": 0.15,
        "tail_improvement_ratio": 0.10,
        "turnover_ratio": 1.5,
        "net_mean_return_delta": 0.001,
    }
    evidence = pd.DataFrame(
        [
            {
                **common,
                "ring_id": "core_high_high",
                "max_holding_sessions": 60,
                "period": "oos",
                "is_primary": True,
            },
            {
                **common,
                "ring_id": "core_high_high",
                "max_holding_sessions": 60,
                "period": "holdout",
                "is_primary": True,
            },
            {
                **common,
                "ring_id": "near_high_high_1",
                "max_holding_sessions": 20,
                "period": "oos",
                "is_primary": False,
            },
            {
                **common,
                "ring_id": "near_high_high_2",
                "max_holding_sessions": 20,
                "period": "oos",
                "is_primary": False,
            },
        ]
    )
    annual = pd.DataFrame(
        [
            {
                "family": "entry",
                "variant_id": "E2_count_ge_2",
                "ring_id": "core_high_high",
                "max_holding_sessions": 60,
                "period": "oos",
                "year": year,
                "net_mean_return_delta": 0.001,
            }
            for year in (2022, 2023, 2024)
        ]
    )
    costs = pd.DataFrame(
        [
            {
                "family": "entry",
                "variant_id": "E2_count_ge_2",
                "ring_id": "core_high_high",
                "max_holding_sessions": 60,
                "period": "oos",
                "cost_bps": cost_bps,
                "net_mean_return_delta": 0.001,
            }
            for cost_bps in (10.0, 20.0)
        ]
    )
    return evidence, annual, costs


def _decision_inputs_for(
    family: str,
    variant_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    evidence, annual, costs = _passing_decision_gate_inputs()
    return (
        evidence.assign(family=family, variant_id=variant_id),
        annual.assign(family=family, variant_id=variant_id),
        costs.assign(family=family, variant_id=variant_id),
    )


def _synthetic_feature_frame() -> pd.DataFrame:
    first_code = _single_code_frame(
        [
            ("2025-01-01", 0.8, 0.8, 1),
            ("2025-01-02", 0.8, 0.8, 2),
            ("2025-01-03", 0.8, 0.8, 2),
            ("2025-01-06", 0.8, 0.8, 1),
            ("2025-01-07", 0.8, 0.8, 2),
            ("2025-01-08", 0.8, 0.8, 2),
        ],
        code="1001",
    )
    second_code = _single_code_frame(
        [
            ("2025-01-01", 0.79, 0.8, 2),
            ("2025-01-02", 0.79, 0.8, 2),
            ("2025-01-03", 0.79, 0.8, 2),
            ("2025-01-06", 0.79, 0.8, 2),
            ("2025-01-07", 0.79, 0.8, 2),
            ("2025-01-08", 0.79, 0.8, 2),
        ],
        code="1002",
    )
    return pd.concat([first_code, second_code], ignore_index=True)


def _single_code_frame(
    rows: list[tuple[str, float, float, int]],
    *,
    code: str = "1001",
    closes: list[float | None] | None = None,
    topix_closes: list[float] | None = None,
) -> pd.DataFrame:
    if closes is None:
        closes = [100.0 + index for index in range(len(rows))]
    if topix_closes is None:
        topix_closes = [100.0] * len(rows)
    return pd.DataFrame(
        {
            "date": [date for date, _, _, _ in rows],
            "code": code,
            "close": closes,
            "value_composite_equal_score": [value for _, value, _, _ in rows],
            "long_hybrid_leadership_score": [leadership for _, _, leadership, _ in rows],
            "sma5": [99.0] * len(rows),
            "sma5_above_count_5d": [count for _, _, _, count in rows],
            "sma5_below_streak": [0] * len(rows),
            "sma5_atr20_deviation": [0.0] * len(rows),
            "topix_close": topix_closes,
        }
    )
