"""Pure score-ring membership and same-Close position-state semantics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, cast

import pandas as pd

from src.shared.utils.pandas_type_guards import finite_float_or_none


SCORE_RING_THRESHOLDS: Mapping[str, float] = MappingProxyType(
    {
        "core_high_high": 0.80,
        "near_high_high_1": 0.70,
        "near_high_high_2": 0.60,
    }
)
ENTRY_RULE_IDS = (
    "E0_no_sma5_filter",
    "E1_close_above_sma5",
    "E2_count_ge_2",
    "E3_avoid_atr20_chase",
    "E4_count_ge_2_and_avoid_chase",
)
EXIT_RULE_IDS = (
    "X0_no_sma5_exit",
    "X1_close_below_sma5",
    "X2_count_le_1",
    "X3_below_streak_ge_3",
    "X4_atr20_below_le_neg1",
)
EXIT_PRECEDENCE = ("ring_exit", "sma5_exit", "time_exit", "terminal_exit")

_VALUE_SCORE_COLUMN = "value_composite_equal_score"
_LEADERSHIP_SCORE_COLUMN = "long_hybrid_leadership_score"
_REQUIRED_FEATURE_COLUMNS = frozenset(
    {
        "date",
        "code",
        "close",
        _VALUE_SCORE_COLUMN,
        _LEADERSHIP_SCORE_COLUMN,
    }
)


@dataclass(frozen=True)
class PositionSignalFrames:
    close: pd.DataFrame
    entries: pd.DataFrame
    exits: pd.DataFrame
    held_intervals: pd.DataFrame
    state_events: pd.DataFrame


def classify_score_ring(value_score: object, leadership_score: object) -> str:
    """Return the most selective score-ring label satisfied by two scores."""
    value = _safe_finite_float_or_none(value_score)
    leadership = _safe_finite_float_or_none(leadership_score)
    if value is None or leadership is None:
        return "missing"
    for ring_id, threshold in SCORE_RING_THRESHOLDS.items():
        if value >= threshold and leadership >= threshold:
            return ring_id
    return "outside"


def entry_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen entry rule, failing closed for missing numeric inputs."""
    if rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {rule_id}")
    if rule_id == "E0_no_sma5_filter":
        return True
    if rule_id == "E1_close_above_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close >= sma5
    if rule_id == "E2_count_ge_2":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count >= 2.0
    if rule_id == "E3_avoid_atr20_chase":
        deviation = _numeric_value(row, "sma5_atr20_deviation")
        return deviation is not None and deviation < 1.0
    count = _numeric_value(row, "sma5_above_count_5d")
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return count is not None and count >= 2.0 and deviation is not None and deviation < 1.0


def exit_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen exit rule, failing closed for missing numeric inputs."""
    if rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {rule_id}")
    if rule_id == "X0_no_sma5_exit":
        return False
    if rule_id == "X1_close_below_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close < sma5
    if rule_id == "X2_count_le_1":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count <= 1.0
    if rule_id == "X3_below_streak_ge_3":
        streak = _numeric_value(row, "sma5_below_streak")
        return streak is not None and streak >= 3.0
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return deviation is not None and deviation <= -1.0


def build_position_signal_frames(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> PositionSignalFrames:
    """Build aligned price, signal, exposure, and event frames for one variant.

    Membership is threshold based, so a wider ring contains every qualifying row,
    including those classified into a more selective label.
    """
    _validate_arguments(
        feature_df,
        ring_id=ring_id,
        entry_rule_id=entry_rule_id,
        exit_rule_id=exit_rule_id,
        max_holding_sessions=max_holding_sessions,
    )
    prepared = feature_df.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="raise")
    prepared["code"] = prepared["code"].astype(str)
    if prepared.duplicated(["date", "code"]).any():
        raise ValueError("feature_df must contain at most one row per date and code")
    prepared = prepared.sort_values(["code", "date"], kind="stable")

    dates = pd.DatetimeIndex(sorted(prepared["date"].unique()), name="date")
    codes = pd.Index(sorted(prepared["code"].unique()), name="code")
    close = (
        prepared.assign(close=pd.to_numeric(prepared["close"], errors="coerce"))
        .pivot(index="date", columns="code", values="close")
        .reindex(index=dates, columns=codes)
    )
    entries = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    exits = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    held_intervals = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    events: list[dict[str, object]] = []

    for code, code_frame in prepared.groupby("code", sort=False):
        _build_code_position_state(
            code_frame,
            code=str(code),
            ring_id=ring_id,
            entry_rule_id=entry_rule_id,
            exit_rule_id=exit_rule_id,
            max_holding_sessions=max_holding_sessions,
            entries=entries,
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )

    state_events = pd.DataFrame(
        events,
        columns=["date", "code", "event_type", "exit_reason"],
    )
    if not state_events.empty:
        event_order = {"exit": 0, "entry": 1}
        state_events["_event_order"] = state_events["event_type"].map(event_order)
        state_events = (
            state_events.sort_values(["date", "code", "_event_order"], kind="stable")
            .drop(columns="_event_order")
            .reset_index(drop=True)
        )
    return PositionSignalFrames(
        close=close,
        entries=entries,
        exits=exits,
        held_intervals=held_intervals,
        state_events=state_events,
    )


def _build_code_position_state(
    code_frame: pd.DataFrame,
    *,
    code: str,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    rows = cast(list[dict[str, Any]], code_frame.to_dict(orient="records"))
    finite_close_dates = [
        pd.Timestamp(row["date"])
        for row in rows
        if _numeric_value(row, "close") is not None
    ]
    if not finite_close_dates:
        return
    last_finite_close_date = finite_close_dates[-1]
    active = False
    held_sessions = 0
    previous_entry_eligibility = False

    for row in rows:
        date = pd.Timestamp(row["date"])
        has_close = _numeric_value(row, "close") is not None
        ring_member = _row_is_in_ring(row, ring_id)
        entry_eligible = (
            has_close and ring_member and entry_rule_matches(row, entry_rule_id)
        )

        if active:
            exit_reason = _exit_reason(
                row,
                ring_member=ring_member,
                exit_rule_id=exit_rule_id,
                held_sessions=held_sessions,
                max_holding_sessions=max_holding_sessions,
            )
            if has_close and exit_reason is not None:
                _emit_exit(
                    date,
                    code,
                    exit_reason,
                    exits=exits,
                    held_intervals=held_intervals,
                    events=events,
                )
                active = False
                held_sessions = 0
            elif has_close:
                held_intervals.loc[date, code] = True
                held_sessions += 1
        elif entry_eligible and not previous_entry_eligibility:
            if date != last_finite_close_date:
                entries.loc[date, code] = True
                events.append(
                    {
                        "date": date,
                        "code": code,
                        "event_type": "entry",
                        "exit_reason": None,
                    }
                )
                active = True
                held_sessions = 0

        previous_entry_eligibility = entry_eligible

    if active:
        _emit_exit(
            last_finite_close_date,
            code,
            "terminal_exit",
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )


def _exit_reason(
    row: Mapping[str, object],
    *,
    ring_member: bool,
    exit_rule_id: str,
    held_sessions: int,
    max_holding_sessions: int,
) -> str | None:
    if not ring_member:
        return "ring_exit"
    if exit_rule_matches(row, exit_rule_id):
        return "sma5_exit"
    if held_sessions >= max_holding_sessions:
        return "time_exit"
    return None


def _emit_exit(
    date: pd.Timestamp,
    code: str,
    exit_reason: str,
    *,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    exits.loc[date, code] = True
    held_intervals.loc[date, code] = False
    events.append(
        {
            "date": date,
            "code": code,
            "event_type": "exit",
            "exit_reason": exit_reason,
        }
    )


def _row_is_in_ring(row: Mapping[str, object], ring_id: str) -> bool:
    threshold = SCORE_RING_THRESHOLDS[ring_id]
    value = _numeric_value(row, _VALUE_SCORE_COLUMN)
    leadership = _numeric_value(row, _LEADERSHIP_SCORE_COLUMN)
    return value is not None and leadership is not None and value >= threshold and leadership >= threshold


def _numeric_value(row: Mapping[str, object], column: str) -> float | None:
    return _safe_finite_float_or_none(row.get(column))


def _safe_finite_float_or_none(value: object) -> float | None:
    try:
        return finite_float_or_none(value)
    except (TypeError, ValueError):
        return None


def _validate_arguments(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> None:
    if ring_id not in SCORE_RING_THRESHOLDS:
        raise ValueError(f"unknown score ring: {ring_id}")
    if entry_rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {entry_rule_id}")
    if exit_rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {exit_rule_id}")
    if (
        isinstance(max_holding_sessions, bool)
        or not isinstance(max_holding_sessions, int)
        or max_holding_sessions <= 0
    ):
        raise ValueError("max_holding_sessions must be a positive integer")
    missing_columns = sorted(_REQUIRED_FEATURE_COLUMNS.difference(feature_df.columns))
    if missing_columns:
        raise ValueError(f"feature_df missing required columns: {', '.join(missing_columns)}")
