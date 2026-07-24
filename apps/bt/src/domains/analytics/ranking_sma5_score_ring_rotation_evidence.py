"""Exploratory same-ring rotation evidence for SMA5 exit states."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    PositionSignalFrames,
    SCORE_RING_THRESHOLDS,
    build_position_signal_frames,
    prepare_position_signal_panel,
    run_ranking_sma5_score_ring_hard_filter_research,
)


DEFAULT_RING_IDS = (
    "core_high_high",
    "near_high_high_1",
    "near_high_high_2",
)
TRIGGER_IDS = (
    "X2_count_le_1",
    "X3_below_streak_ge_3",
    "X4_atr20_below_le_neg1",
)
DEFAULT_COST_LEVELS_BPS = (0, 10, 20)


@dataclass
class RankingSma5ScoreRingRotationResult:
    rotation_summary_df: pd.DataFrame
    rotation_annual_df: pd.DataFrame
    rotation_decision_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    rotation_event_df: pd.DataFrame
    db_path: str = ""
    analysis_start_date: str | None = None
    analysis_end_date: str | None = None
    market_schema_version: int | None = None
    stock_price_adjustment_mode: str | None = None


def attach_next_session_returns(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Attach Close-to-next-market-session Close returns without gap filling."""

    frame = feature_df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["code"] = frame["code"].astype(str)
    frame = frame.sort_values(["code", "date"], kind="stable")
    sessions = pd.Index(sorted(frame["date"].unique()))
    next_session = pd.Series(sessions[1:], index=sessions[:-1])
    frame["next_date"] = frame.groupby("code")["date"].shift(-1)
    frame["next_close"] = frame.groupby("code")["close"].shift(-1)
    expected = frame["date"].map(next_session)
    valid = frame["next_date"].eq(expected)
    frame["next_session_return"] = (
        pd.to_numeric(frame["next_close"], errors="coerce")
        / pd.to_numeric(frame["close"], errors="coerce")
        - 1.0
    ).where(valid)
    return frame


def build_rotation_evidence(
    feature_df: pd.DataFrame,
    *,
    ring_ids: tuple[str, ...] = DEFAULT_RING_IDS,
    cost_levels_bps: tuple[int, ...] = DEFAULT_COST_LEVELS_BPS,
    holding_cap: int = 60,
) -> RankingSma5ScoreRingRotationResult:
    """Compare first held X2/X3/X4 events with a healthy same-ring basket."""

    unknown_rings = sorted(set(ring_ids).difference(SCORE_RING_THRESHOLDS))
    if unknown_rings:
        raise ValueError(f"unknown score rings: {unknown_rings}")
    frame = attach_next_session_returns(feature_df)
    prepared_panel = prepare_position_signal_panel(frame)
    all_events: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, object]] = []

    for ring_id in ring_ids:
        threshold = SCORE_RING_THRESHOLDS[ring_id]
        frame_ring = _ring_mask(frame, threshold)
        frames = build_position_signal_frames(
            prepared_panel,
            ring_id=ring_id,
            entry_rule_id="E0_no_sma5_filter",
            exit_rule_id="X0_no_sma5_exit",
            max_holding_sessions=holding_cap,
        )
        sources = _first_trigger_events(frame, ring_id, frames)
        paired_sources = _attach_healthy_targets(
            frame,
            frame_ring,
            sources,
            ring_id=ring_id,
        )
        for trigger_id in TRIGGER_IDS:
            trigger_sources = sources.loc[sources["trigger_id"].eq(trigger_id)].copy()
            paired = paired_sources.loc[
                paired_sources["trigger_id"].eq(trigger_id)
            ].copy()
            valid_source_count = int(
                trigger_sources["source_return"].notna().sum()
            )
            paired_count = len(paired)
            coverage_rows.append(
                {
                    "ring_id": ring_id,
                    "trigger_id": trigger_id,
                    "source_event_count": len(trigger_sources),
                    "source_outcome_count": valid_source_count,
                    "paired_event_count": paired_count,
                    "events_without_target": valid_source_count - paired_count,
                    "target_availability_rate": (
                        paired_count / valid_source_count if valid_source_count else None
                    ),
                    "median_target_candidate_count": (
                        float(paired["target_candidate_count"].median())
                        if paired_count
                        else None
                    ),
                }
            )
            if not paired.empty:
                all_events.append(paired)

    event_df = (
        pd.concat(all_events, ignore_index=True)
        if all_events
        else _empty_event_frame()
    )
    summary_df, annual_df = _aggregate_events(event_df, cost_levels_bps)
    decision_df = _build_decisions(summary_df, annual_df)
    return RankingSma5ScoreRingRotationResult(
        rotation_summary_df=summary_df,
        rotation_annual_df=annual_df,
        rotation_decision_df=decision_df,
        coverage_diagnostics_df=pd.DataFrame(coverage_rows),
        rotation_event_df=event_df,
    )


def run_ranking_sma5_score_ring_rotation_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> RankingSma5ScoreRingRotationResult:
    """Build the existing Market v5 panel and run the small rotation comparison."""

    research = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
    )
    result = build_rotation_evidence(research.feature_df)
    result.db_path = research.db_path
    result.analysis_start_date = research.analysis_start_date
    result.analysis_end_date = research.analysis_end_date
    result.market_schema_version = research.pit_lineage.market_schema_version
    result.stock_price_adjustment_mode = (
        research.pit_lineage.stock_price_adjustment_mode
    )
    return result


def _ring_mask(frame: pd.DataFrame, threshold: float) -> pd.Series:
    return pd.to_numeric(
        frame["value_composite_equal_score"], errors="coerce"
    ).ge(threshold) & pd.to_numeric(
        frame["long_hybrid_leadership_score"], errors="coerce"
    ).ge(threshold)


def _first_trigger_events(
    frame: pd.DataFrame,
    ring_id: str,
    frames: PositionSignalFrames,
) -> pd.DataFrame:
    indexed = frame.set_index(["date", "code"], drop=False)
    held = frames.held_intervals.stack(future_stack=True).rename("held")
    entries = frames.entries.stack(future_stack=True).rename("entry")
    state = pd.concat([held, entries], axis=1).reset_index()
    state["trade_id"] = state.groupby("code")["entry"].cumsum()
    state = state.loc[state["held"] & state["trade_id"].gt(0)]
    candidates = indexed.join(
        state.set_index(["date", "code"])[["trade_id"]],
        how="inner",
    ).reset_index(drop=True)
    candidate_ring = _ring_mask(candidates, SCORE_RING_THRESHOLDS[ring_id])
    candidates = candidates.loc[candidate_ring].copy()
    count = pd.to_numeric(candidates["sma5_above_count_5d"], errors="coerce")
    streak = pd.to_numeric(candidates["sma5_below_streak"], errors="coerce")
    deviation = pd.to_numeric(candidates["sma5_atr20_deviation"], errors="coerce")
    candidates["trigger_id"] = pd.Series(
        pd.NA,
        index=candidates.index,
        dtype="string",
    )
    candidates.loc[count.le(1.0), "trigger_id"] = "X2_count_le_1"
    candidates.loc[streak.ge(3.0), "trigger_id"] = "X3_below_streak_ge_3"
    candidates.loc[deviation.le(-1.0), "trigger_id"] = "X4_atr20_below_le_neg1"
    candidates = candidates.loc[candidates["trigger_id"].notna()]
    if candidates.empty:
        return _empty_source_frame()
    candidates = candidates.sort_values(["code", "trade_id", "date"], kind="stable")
    sources = candidates.drop_duplicates(["code", "trade_id"], keep="first")
    return sources.rename(
        columns={
            "code": "source_code",
            "next_session_return": "source_return",
        }
    )

def _attach_healthy_targets(
    frame: pd.DataFrame,
    in_ring: pd.Series,
    sources: pd.DataFrame,
    *,
    ring_id: str,
) -> pd.DataFrame:
    if sources.empty:
        return _empty_event_frame()
    healthy = (
        in_ring
        & pd.to_numeric(frame["sma5_above_count_5d"], errors="coerce").gt(1.0)
        & pd.to_numeric(frame["sma5_below_streak"], errors="coerce").lt(3.0)
        & pd.to_numeric(frame["sma5_atr20_deviation"], errors="coerce").gt(-1.0)
        & frame["next_session_return"].notna()
    )
    target_rows = frame.loc[healthy, ["date", "code", "next_session_return"]]
    if target_rows.empty:
        return _empty_event_frame()
    target_rows = target_rows.sort_values(["date", "code"], kind="stable")
    targets_by_date = (
        target_rows.groupby("date", as_index=False)
        .agg(
            rotation_return=("next_session_return", "mean"),
            target_candidate_count=("code", "size"),
            target_codes=("code", lambda codes: ",".join(codes.astype(str))),
        )
    )
    paired = sources.loc[sources["source_return"].notna()].merge(
        targets_by_date,
        on="date",
        how="inner",
        validate="many_to_one",
    )
    if paired.empty:
        return _empty_event_frame()
    paired["ring_id"] = ring_id
    paired["year"] = paired["date"].dt.year.astype(int)
    paired["gross_paired_delta"] = (
        paired["rotation_return"] - paired["source_return"]
    )
    return paired[
        [
            "ring_id",
            "trigger_id",
            "date",
            "year",
            "source_code",
            "source_return",
            "rotation_return",
            "gross_paired_delta",
            "target_candidate_count",
            "target_codes",
        ]
    ]


def _aggregate_events(
    event_df: pd.DataFrame,
    cost_levels_bps: tuple[int, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for fee_bps in cost_levels_bps:
        cost = fee_bps / 10_000.0
        for (ring_id, trigger_id), group in event_df.groupby(
            ["ring_id", "trigger_id"], sort=False
        ):
            net = group["gross_paired_delta"] - cost
            summary_rows.append(
                {
                    "ring_id": ring_id,
                    "trigger_id": trigger_id,
                    "fee_bps": fee_bps,
                    "event_count": len(group),
                    "mean_source_return": float(group["source_return"].mean()),
                    "median_source_return": float(group["source_return"].median()),
                    "mean_rotation_return": float(group["rotation_return"].mean()),
                    "median_rotation_return": float(group["rotation_return"].median()),
                    "mean_paired_delta": float(net.mean()),
                    "median_paired_delta": float(net.median()),
                    "rotation_outperform_rate": float(net.gt(0).mean()),
                }
            )
            for year, annual_group in group.groupby("year", sort=True):
                annual_net = annual_group["gross_paired_delta"] - cost
                annual_rows.append(
                    {
                        "ring_id": ring_id,
                        "trigger_id": trigger_id,
                        "fee_bps": fee_bps,
                        "year": int(year),
                        "event_count": len(annual_group),
                        "mean_paired_delta": float(annual_net.mean()),
                        "median_paired_delta": float(annual_net.median()),
                        "rotation_outperform_rate": float(annual_net.gt(0).mean()),
                    }
                )
    summary_columns = [
        "ring_id",
        "trigger_id",
        "fee_bps",
        "event_count",
        "mean_source_return",
        "median_source_return",
        "mean_rotation_return",
        "median_rotation_return",
        "mean_paired_delta",
        "median_paired_delta",
        "rotation_outperform_rate",
    ]
    annual_columns = [
        "ring_id",
        "trigger_id",
        "fee_bps",
        "year",
        "event_count",
        "mean_paired_delta",
        "median_paired_delta",
        "rotation_outperform_rate",
    ]
    return (
        pd.DataFrame(summary_rows, columns=summary_columns),
        pd.DataFrame(annual_rows, columns=annual_columns),
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
            & annual_df["trigger_id"].eq(trigger_id)
            & annual_df["fee_bps"].eq(10)
        ]
        positive_years = int(annual["median_paired_delta"].gt(0).sum())
        total_years = len(annual)
        passed = (
            core10 is not None
            and core20 is not None
            and near1 is not None
            and near2 is not None
            and core10["median_paired_delta"] > 0
            and core10["rotation_outperform_rate"] > 0.5
            and positive_years > total_years / 2
            and near1["median_paired_delta"] >= 0
            and near2["median_paired_delta"] >= 0
            and core20["median_paired_delta"] >= 0
        )
        rows.append(
            {
                "trigger_id": trigger_id,
                "decision": (
                    "rotation_candidate" if passed else "insufficient_evidence"
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
        & summary_df["trigger_id"].eq(trigger_id)
        & summary_df["fee_bps"].eq(fee_bps)
    ]
    return None if selected.empty else selected.iloc[0]


def _empty_source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["date", "source_code", "trade_id", "source_return", "trigger_id"]
    )


def _empty_event_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ring_id",
            "trigger_id",
            "date",
            "year",
            "source_code",
            "source_return",
            "rotation_return",
            "gross_paired_delta",
            "target_candidate_count",
            "target_codes",
        ]
    )
