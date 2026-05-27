"""Pure execution helpers for BacktestExecutorMixin."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.domains.backtest.vectorbt_adapter import ROUND_TRIP_DIRECTION_MAP
from src.domains.strategy.runtime.compiler import resolve_round_trip_execution_mode_name


def build_strategy_shared_config_payload(strategy: Any) -> dict[str, Any]:
    return {
        "data_source": getattr(strategy, "data_source", "market"),
        "universe_preset": getattr(strategy, "universe_preset", None),
        "universe_filters": getattr(strategy, "universe_filters", {}),
        "universe_as_of_date": getattr(strategy, "universe_as_of_date", None),
        "static_universe": getattr(strategy, "static_universe", False),
        "start_date": getattr(strategy, "start_date", None),
        "end_date": getattr(strategy, "end_date", None),
    }


def normalize_signal_frame(frame: pd.DataFrame) -> pd.DataFrame:
    with pd.option_context("future.no_silent_downcasting", True):
        return frame.fillna(False).infer_objects(copy=False).astype(bool)


def build_empty_exit_frame(entries_data: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        False,
        index=entries_data.index,
        columns=entries_data.columns,
        dtype=bool,
    )


def resolve_round_trip_direction(direction: str) -> int:
    return int(ROUND_TRIP_DIRECTION_MAP.get(direction, ROUND_TRIP_DIRECTION_MAP["longonly"]))


def resolve_strategy_round_trip_mode_name(strategy: Any) -> str | None:
    compiled_strategy = getattr(strategy, "compiled_strategy", None)
    if compiled_strategy is not None:
        mode_name = resolve_round_trip_execution_mode_name(compiled_strategy)
        if mode_name is not None:
            return mode_name
    if getattr(strategy, "next_session_round_trip", False):
        return "next_session_round_trip"
    if getattr(strategy, "current_session_round_trip", False):
        return "current_session_round_trip"
    if getattr(strategy, "overnight_round_trip", False):
        return "overnight_round_trip"
    return None


def prepare_round_trip_signals(
    *,
    stock_code: str,
    entries: pd.Series,
    execution_data: pd.DataFrame,
    mode_name: str | None,
) -> tuple[pd.Series, pd.Series, list[tuple[str, str]]]:
    log_events: list[tuple[str, str]] = []
    if mode_name is None:
        return entries, pd.Series(False, index=entries.index, dtype=bool), log_events

    required_columns = {"Open", "Close"}
    missing_columns = required_columns - set(execution_data.columns)
    if missing_columns:
        raise ValueError(f"{stock_code}: {mode_name} requires columns {sorted(required_columns)}")

    normalized_entries = entries.fillna(False).infer_objects(copy=False).astype(bool)
    if normalized_entries.empty:
        empty = normalized_entries.copy()
        return empty, empty, log_events

    scheduled_entries = normalized_entries
    if mode_name == "next_session_round_trip":
        scheduled_entries = normalized_entries.shift(1, fill_value=False)

    if mode_name == "overnight_round_trip":
        executable_days = execution_data["Close"].notna() & execution_data["Open"].shift(-1).notna()
    else:
        executable_days = execution_data["Open"].notna() & execution_data["Close"].notna()
    execution_entries = (scheduled_entries & executable_days).astype(bool)
    execution_exits = pd.Series(False, index=execution_entries.index, dtype=bool)

    skipped_missing = int((scheduled_entries & ~executable_days).sum())
    if skipped_missing > 0:
        log_events.append(
            (
                "warning",
                f"{stock_code}: {mode_name} skipped {skipped_missing} signals because "
                "Open/Close was missing on the execution session",
            )
        )

    if mode_name in ("next_session_round_trip", "overnight_round_trip") and bool(normalized_entries.iloc[-1]):
        log_events.append(
            (
                "debug",
                f"{stock_code}: {mode_name} dropped the last-bar signal "
                "because the next session is unavailable",
            )
        )

    return execution_entries, execution_exits, log_events
