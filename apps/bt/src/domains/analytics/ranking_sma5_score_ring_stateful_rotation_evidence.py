"""Exploratory one-hop stateful rotation evidence for SMA5 exit states."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    SCORE_RING_THRESHOLDS,
    build_position_signal_frames,
    prepare_position_signal_panel,
    run_ranking_sma5_score_ring_hard_filter_research,
)
from src.domains.analytics.ranking_sma5_score_ring_rotation_evidence import (
    DEFAULT_COST_LEVELS_BPS,
    DEFAULT_RING_IDS,
    TRIGGER_IDS,
    _first_trigger_events,
    _ring_mask,
    attach_next_session_returns,
)


@dataclass
class RankingSma5ScoreRingStatefulRotationResult:
    stateful_rotation_summary_df: pd.DataFrame
    stateful_rotation_annual_df: pd.DataFrame
    stateful_rotation_exit_reason_df: pd.DataFrame
    stateful_rotation_decision_df: pd.DataFrame
    stateful_rotation_event_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    db_path: str = ""
    analysis_start_date: str | None = None
    analysis_end_date: str | None = None
    market_schema_version: int | None = None
    stock_price_adjustment_mode: str | None = None


def build_stateful_rotation_evidence(
    feature_df: pd.DataFrame,
    *,
    ring_ids: tuple[str, ...] = DEFAULT_RING_IDS,
    cost_levels_bps: tuple[int, ...] = DEFAULT_COST_LEVELS_BPS,
    holding_cap: int = 60,
) -> RankingSma5ScoreRingStatefulRotationResult:
    """Compare one-hop target episodes with source returns on matching horizons."""

    unknown_rings = sorted(set(ring_ids).difference(SCORE_RING_THRESHOLDS))
    if unknown_rings:
        raise ValueError(f"unknown score rings: {unknown_rings}")
    if holding_cap < 1:
        raise ValueError("holding_cap must be at least one session")

    frame = attach_next_session_returns(feature_df)
    frame = frame.sort_values(["date", "code"], kind="stable").reset_index(drop=True)
    prepared_panel = prepare_position_signal_panel(frame)
    sessions = pd.Index(sorted(frame["date"].unique()))
    code_rows = {
        str(code): group.set_index("date", drop=False)
        for code, group in frame.groupby("code", sort=False)
    }
    event_rows: list[dict[str, object]] = []
    exit_rows: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []

    for ring_id in ring_ids:
        threshold = SCORE_RING_THRESHOLDS[ring_id]
        frames = build_position_signal_frames(
            prepared_panel,
            ring_id=ring_id,
            entry_rule_id="E0_no_sma5_filter",
            exit_rule_id="X0_no_sma5_exit",
            max_holding_sessions=holding_cap,
        )
        sources = _first_trigger_events(frame, ring_id, frames)
        healthy = _healthy_mask(frame, threshold)
        healthy_by_date = {
            date: group
            for date, group in frame.loc[healthy].groupby("date", sort=False)
        }
        target_episodes = _precompute_target_episodes(
            frame,
            healthy=healthy,
            sessions=sessions,
            threshold=threshold,
            holding_cap=holding_cap,
        )
        for trigger_id in TRIGGER_IDS:
            trigger_sources = sources.loc[sources["trigger_id"].eq(trigger_id)]
            valid_event_count = 0
            valid_target_count = 0
            invalid_target_count = 0
            for source in trigger_sources.itertuples(index=False):
                source_date = pd.Timestamp(cast(Any, source.date))
                candidates = healthy_by_date.get(source_date)
                if candidates is None:
                    continue
                candidates = candidates.loc[
                    candidates["code"].astype(str).ne(str(source.source_code))
                ]
                pairs: list[dict[str, object]] = []
                for target in candidates.itertuples(index=False):
                    episode = _build_target_pair(
                        target=target,
                        source=source,
                        code_rows=code_rows,
                        target_episode=target_episodes.get(
                            (source_date, str(target.code))
                        ),
                    )
                    if episode is None:
                        invalid_target_count += 1
                        continue
                    pairs.append(episode)
                if not pairs:
                    continue
                valid_event_count += 1
                valid_target_count += len(pairs)
                event = _aggregate_source_event(
                    ring_id=ring_id,
                    source_trigger_id=trigger_id,
                    source=source,
                    pairs=pairs,
                )
                event_rows.append(event)
                for pair in pairs:
                    exit_rows.append(
                        {
                            "ring_id": ring_id,
                            "source_trigger_id": trigger_id,
                            "target_exit_reason": pair["target_exit_reason"],
                            "holding_sessions": pair["holding_sessions"],
                        }
                    )
            coverage_rows.append(
                {
                    "ring_id": ring_id,
                    "source_trigger_id": trigger_id,
                    "source_event_count": len(trigger_sources),
                    "paired_event_count": valid_event_count,
                    "valid_target_episode_count": valid_target_count,
                    "invalid_target_episode_count": invalid_target_count,
                    "target_availability_rate": (
                        valid_event_count / len(trigger_sources)
                        if len(trigger_sources)
                        else None
                    ),
                }
            )

    event_df = pd.DataFrame(event_rows, columns=_EVENT_COLUMNS)
    raw_exit_df = pd.DataFrame(exit_rows)
    exit_reason_df = _aggregate_exit_reasons(raw_exit_df)
    summary_df, annual_df = _aggregate_evidence(event_df, cost_levels_bps)
    decision_df = _build_decisions(summary_df, annual_df)
    return RankingSma5ScoreRingStatefulRotationResult(
        stateful_rotation_summary_df=summary_df,
        stateful_rotation_annual_df=annual_df,
        stateful_rotation_exit_reason_df=exit_reason_df,
        stateful_rotation_decision_df=decision_df,
        stateful_rotation_event_df=event_df,
        coverage_diagnostics_df=pd.DataFrame(
            coverage_rows,
            columns=_COVERAGE_COLUMNS,
        ),
    )


def run_ranking_sma5_score_ring_stateful_rotation_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> RankingSma5ScoreRingStatefulRotationResult:
    """Build the Market v5 feature panel and run stateful rotation evidence."""

    research = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
    )
    result = build_stateful_rotation_evidence(research.feature_df)
    result.db_path = research.db_path
    result.analysis_start_date = research.analysis_start_date
    result.analysis_end_date = research.analysis_end_date
    result.market_schema_version = research.pit_lineage.market_schema_version
    result.stock_price_adjustment_mode = (
        research.pit_lineage.stock_price_adjustment_mode
    )
    return result


def _healthy_mask(frame: pd.DataFrame, threshold: float) -> pd.Series:
    return (
        _ring_mask(frame, threshold)
        & pd.to_numeric(frame["sma5_above_count_5d"], errors="coerce").gt(1.0)
        & pd.to_numeric(frame["sma5_below_streak"], errors="coerce").lt(3.0)
        & pd.to_numeric(frame["sma5_atr20_deviation"], errors="coerce").gt(-1.0)
        & pd.to_numeric(frame["close"], errors="coerce").map(np.isfinite)
    )


def _build_target_pair(
    *,
    target: Any,
    source: Any,
    code_rows: dict[str, pd.DataFrame],
    target_episode: dict[str, object] | None,
) -> dict[str, object] | None:
    if target_episode is None:
        return None
    target_code = str(target.code)
    source_code = str(source.source_code)
    source_rows = code_rows[source_code]
    exit_date = pd.Timestamp(cast(Any, target_episode["target_exit_date"]))
    if exit_date not in source_rows.index:
        return None
    source_row = source_rows.loc[exit_date]
    if isinstance(source_row, pd.DataFrame):
        return None
    target_start_close = float(target.close)
    source_start_close = float(source.close)
    target_exit_close = float(cast(Any, target_episode["target_exit_close"]))
    source_exit_close = float(source_row["close"])
    if not all(
        np.isfinite(value) and value > 0
        for value in (
            target_start_close,
            source_start_close,
            target_exit_close,
            source_exit_close,
        )
    ):
        return None
    target_return = target_exit_close / target_start_close - 1.0
    source_return = source_exit_close / source_start_close - 1.0
    return {
        "target_code": target_code,
        "target_exit_date": exit_date,
        "target_exit_reason": target_episode["target_exit_reason"],
        "holding_sessions": target_episode["holding_sessions"],
        "target_cumulative_return": target_return,
        "matched_source_cumulative_return": source_return,
        "gross_pair_delta": target_return - source_return,
    }


def _precompute_target_episodes(
    frame: pd.DataFrame,
    *,
    healthy: pd.Series,
    sessions: pd.Index,
    threshold: float,
    holding_cap: int,
) -> dict[tuple[pd.Timestamp, str], dict[str, object] | None]:
    """Resolve each healthy code-date's next exit with one backward pass."""

    session_position = {
        pd.Timestamp(date): position for position, date in enumerate(sessions)
    }
    last_global_position = len(sessions) - 1
    episodes: dict[tuple[pd.Timestamp, str], dict[str, object] | None] = {}
    candidates = frame.assign(_healthy=healthy)
    for code, group in candidates.groupby("code", sort=False):
        ordered = group.sort_values("date", kind="stable").reset_index(drop=True)
        positions = [
            session_position[pd.Timestamp(date)] for date in ordered["date"]
        ]
        reasons = [
            _target_exit_reason(row, threshold)
            for _, row in ordered.iterrows()
        ]
        next_exit_index: int | None = None
        segment_end = len(ordered) - 1
        for index in range(len(ordered) - 1, -1, -1):
            if (
                index == len(ordered) - 1
                or positions[index + 1] != positions[index] + 1
            ):
                segment_end = index
                next_exit_index = None
            if bool(ordered.at[index, "_healthy"]):
                choices: list[tuple[int, str]] = []
                if next_exit_index is not None:
                    choices.append(
                        (next_exit_index, str(reasons[next_exit_index]))
                    )
                cap_index = index + holding_cap
                if cap_index <= segment_end:
                    choices.append((cap_index, "holding_cap"))
                if positions[segment_end] == last_global_position:
                    choices.append((segment_end, "terminal_exit"))
                choices = [choice for choice in choices if choice[0] > index]
                key = (
                    pd.Timestamp(cast(Any, ordered.at[index, "date"])),
                    str(code),
                )
                if not choices:
                    episodes[key] = None
                else:
                    exit_index, reason = min(choices, key=lambda choice: choice[0])
                    exit_close = float(cast(Any, ordered.at[exit_index, "close"]))
                    episodes[key] = {
                        "target_exit_date": pd.Timestamp(
                            cast(Any, ordered.at[exit_index, "date"])
                        ),
                        "target_exit_reason": reason,
                        "holding_sessions": exit_index - index,
                        "target_exit_close": exit_close,
                    }
            if reasons[index] is not None:
                next_exit_index = index
    return episodes


def _target_exit_reason(row: pd.Series, threshold: float) -> str | None:
    row_frame = row.to_frame().T
    if not bool(_ring_mask(row_frame, threshold).iloc[0]):
        return "ring_exit"
    deviation = pd.to_numeric(
        pd.Series([row["sma5_atr20_deviation"]]), errors="coerce"
    ).iloc[0]
    streak = pd.to_numeric(
        pd.Series([row["sma5_below_streak"]]), errors="coerce"
    ).iloc[0]
    count = pd.to_numeric(
        pd.Series([row["sma5_above_count_5d"]]), errors="coerce"
    ).iloc[0]
    if deviation <= -1.0:
        return "X4_atr20_below_le_neg1"
    if streak >= 3.0:
        return "X3_below_streak_ge_3"
    if count <= 1.0:
        return "X2_count_le_1"
    return None


def _aggregate_source_event(
    *,
    ring_id: str,
    source_trigger_id: str,
    source: Any,
    pairs: list[dict[str, object]],
) -> dict[str, object]:
    pair_df = pd.DataFrame(pairs)
    return {
        "ring_id": ring_id,
        "source_trigger_id": source_trigger_id,
        "source_date": pd.Timestamp(source.date),
        "year": int(pd.Timestamp(source.date).year),
        "source_code": str(source.source_code),
        "source_trade_id": int(source.trade_id),
        "target_count": len(pair_df),
        "target_codes": ",".join(pair_df["target_code"].astype(str)),
        "mean_holding_sessions": float(pair_df["holding_sessions"].mean()),
        "median_holding_sessions": float(pair_df["holding_sessions"].median()),
        "mean_target_cumulative_return": float(
            pair_df["target_cumulative_return"].mean()
        ),
        "mean_matched_source_cumulative_return": float(
            pair_df["matched_source_cumulative_return"].mean()
        ),
        "gross_event_paired_delta": float(pair_df["gross_pair_delta"].mean()),
    }


def _aggregate_exit_reasons(raw_exit_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "ring_id",
        "source_trigger_id",
        "target_exit_reason",
        "target_episode_count",
        "exit_reason_rate",
        "median_holding_sessions",
    ]
    if raw_exit_df.empty:
        return pd.DataFrame(columns=columns)
    grouped = (
        raw_exit_df.groupby(
            ["ring_id", "source_trigger_id", "target_exit_reason"],
            sort=False,
        )
        .agg(
            target_episode_count=("target_exit_reason", "size"),
            median_holding_sessions=("holding_sessions", "median"),
        )
        .reset_index()
    )
    totals = grouped.groupby(["ring_id", "source_trigger_id"])[
        "target_episode_count"
    ].transform("sum")
    grouped["exit_reason_rate"] = grouped["target_episode_count"] / totals
    return grouped[columns]


def _aggregate_evidence(
    event_df: pd.DataFrame,
    cost_levels_bps: tuple[int, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for fee_bps in cost_levels_bps:
        cost = fee_bps / 10_000.0
        for (ring_id, trigger_id), group in event_df.groupby(
            ["ring_id", "source_trigger_id"],
            sort=False,
        ):
            net = group["gross_event_paired_delta"] - cost
            summary_rows.append(
                {
                    "ring_id": ring_id,
                    "source_trigger_id": trigger_id,
                    "fee_bps": fee_bps,
                    "event_count": len(group),
                    "target_episode_count": int(group["target_count"].sum()),
                    "median_target_count": float(group["target_count"].median()),
                    "median_holding_sessions": float(
                        group["median_holding_sessions"].median()
                    ),
                    "mean_source_cumulative_return": float(
                        group["mean_matched_source_cumulative_return"].mean()
                    ),
                    "median_source_cumulative_return": float(
                        group["mean_matched_source_cumulative_return"].median()
                    ),
                    "mean_target_cumulative_return": float(
                        group["mean_target_cumulative_return"].mean()
                    ),
                    "median_target_cumulative_return": float(
                        group["mean_target_cumulative_return"].median()
                    ),
                    "mean_event_paired_delta": float(net.mean()),
                    "median_event_paired_delta": float(net.median()),
                    "positive_event_rate": float(net.gt(0).mean()),
                }
            )
            for year, annual_group in group.groupby("year", sort=True):
                annual_net = annual_group["gross_event_paired_delta"] - cost
                annual_rows.append(
                    {
                        "ring_id": ring_id,
                        "source_trigger_id": trigger_id,
                        "fee_bps": fee_bps,
                        "year": int(str(year)),
                        "event_count": len(annual_group),
                        "mean_event_paired_delta": float(annual_net.mean()),
                        "median_event_paired_delta": float(annual_net.median()),
                        "positive_event_rate": float(annual_net.gt(0).mean()),
                    }
                )
    return (
        pd.DataFrame(summary_rows, columns=_SUMMARY_COLUMNS),
        pd.DataFrame(annual_rows, columns=_ANNUAL_COLUMNS),
    )


def _build_decisions(
    summary_df: pd.DataFrame,
    annual_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for trigger_id in TRIGGER_IDS:
        core10 = _summary_row(summary_df, "core_high_high", trigger_id, 10)
        core20 = _summary_row(summary_df, "core_high_high", trigger_id, 20)
        near1 = _summary_row(summary_df, "near_high_high_1", trigger_id, 10)
        near2 = _summary_row(summary_df, "near_high_high_2", trigger_id, 10)
        annual = annual_df.loc[
            annual_df["ring_id"].eq("core_high_high")
            & annual_df["source_trigger_id"].eq(trigger_id)
            & annual_df["fee_bps"].eq(10)
        ]
        positive_years = int(annual["median_event_paired_delta"].gt(0).sum())
        total_years = len(annual)
        passed = (
            core10 is not None
            and core20 is not None
            and near1 is not None
            and near2 is not None
            and core10["median_event_paired_delta"] > 0
            and core10["positive_event_rate"] > 0.5
            and positive_years > total_years / 2
            and near1["median_event_paired_delta"] >= 0
            and near2["median_event_paired_delta"] >= 0
            and core20["median_event_paired_delta"] >= 0
        )
        rows.append(
            {
                "source_trigger_id": trigger_id,
                "decision": (
                    "stateful_rotation_candidate"
                    if passed
                    else "insufficient_evidence"
                ),
                "positive_years": positive_years,
                "total_years": total_years,
            }
        )
    return pd.DataFrame(rows)


def _summary_row(
    summary_df: pd.DataFrame,
    ring_id: str,
    trigger_id: str,
    fee_bps: int,
) -> pd.Series | None:
    selected = summary_df.loc[
        summary_df["ring_id"].eq(ring_id)
        & summary_df["source_trigger_id"].eq(trigger_id)
        & summary_df["fee_bps"].eq(fee_bps)
    ]
    return None if selected.empty else selected.iloc[0]


_EVENT_COLUMNS = [
    "ring_id",
    "source_trigger_id",
    "source_date",
    "year",
    "source_code",
    "source_trade_id",
    "target_count",
    "target_codes",
    "mean_holding_sessions",
    "median_holding_sessions",
    "mean_target_cumulative_return",
    "mean_matched_source_cumulative_return",
    "gross_event_paired_delta",
]
_SUMMARY_COLUMNS = [
    "ring_id",
    "source_trigger_id",
    "fee_bps",
    "event_count",
    "target_episode_count",
    "median_target_count",
    "median_holding_sessions",
    "mean_source_cumulative_return",
    "median_source_cumulative_return",
    "mean_target_cumulative_return",
    "median_target_cumulative_return",
    "mean_event_paired_delta",
    "median_event_paired_delta",
    "positive_event_rate",
]
_ANNUAL_COLUMNS = [
    "ring_id",
    "source_trigger_id",
    "fee_bps",
    "year",
    "event_count",
    "mean_event_paired_delta",
    "median_event_paired_delta",
    "positive_event_rate",
]
_COVERAGE_COLUMNS = [
    "ring_id",
    "source_trigger_id",
    "source_event_count",
    "paired_event_count",
    "valid_target_episode_count",
    "invalid_target_episode_count",
    "target_availability_rate",
]
