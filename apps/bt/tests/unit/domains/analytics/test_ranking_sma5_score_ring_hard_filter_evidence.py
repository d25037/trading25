from __future__ import annotations

import math

import pandas as pd
import pytest

from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    build_position_signal_frames,
    classify_score_ring,
    entry_rule_matches,
    exit_rule_matches,
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


def test_entry_day_return_is_not_booked_and_exit_day_closes_exposure() -> None:
    frames = build_position_signal_frames(
        _synthetic_feature_frame(),
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )

    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-02"), "1001"]
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-03"), "1001"]
    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]


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
    assert exit_event["date"] == dates[max_holding_sessions + 2]
    assert exit_event["exit_reason"] == "time_exit"
    assert int(frames.held_intervals["1001"].sum()) == max_holding_sessions


def test_terminal_open_position_closes_at_last_finite_close() -> None:
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
    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]


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
) -> pd.DataFrame:
    if closes is None:
        closes = [100.0 + index for index in range(len(rows))]
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
        }
    )
