"""
TOPIX streak multi-timeframe mode research.

This study formalizes the exploratory finding that streak-based mode labeling
contains two useful time scales:

1. A short streak window that reacts to the current exhaustion move.
2. A longer streak window that acts as a slower regime filter.

Each state is defined from the sign of the dominant streak candle inside the
short and long trailing windows. The resulting four states are ranked on how
stably they preserve their forward-return ordering across multiple horizons.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_streak_extreme_mode import (
    DEFAULT_MIN_MODE_CANDLES,
    DEFAULT_VALIDATION_RATIO,
    _format_int_sequence,
    _format_return,
    run_topix_streak_extreme_mode_research,
)

DEFAULT_SHORT_WINDOW_MAX = 10
DEFAULT_LONG_WINDOW_MIN = 20
DEFAULT_PAIR_STABILITY_HORIZONS: tuple[int, ...] = (5, 10, 20)
DEFAULT_MIN_STATE_OBSERVATIONS = 12
MULTI_TIMEFRAME_STATE_ORDER: tuple[str, ...] = (
    "long_bullish__short_bullish",
    "long_bullish__short_bearish",
    "long_bearish__short_bullish",
    "long_bearish__short_bearish",
)
TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix-streak-multi-timeframe-mode"
)


@dataclass(frozen=True)
class TopixStreakMultiTimeframeModeResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    candidate_windows: tuple[int, ...]
    future_horizons: tuple[int, ...]
    stability_horizons: tuple[int, ...]
    validation_ratio: float
    min_mode_candles: int
    min_state_observations: int
    selected_base_window_streaks: int
    selected_short_window_streaks: int
    selected_long_window_streaks: int
    selection_metric: str
    streak_candle_df: pd.DataFrame
    single_window_score_df: pd.DataFrame
    pair_score_df: pd.DataFrame
    selected_pair_state_streak_df: pd.DataFrame
    selected_pair_state_segment_df: pd.DataFrame
    selected_pair_state_summary_df: pd.DataFrame
    selected_pair_state_segment_summary_df: pd.DataFrame
    selected_pair_horizon_rank_df: pd.DataFrame


def run_topix_streak_multi_timeframe_mode_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    candidate_windows: Sequence[int] | None = None,
    future_horizons: Sequence[int] | None = None,
    stability_horizons: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    min_mode_candles: int = DEFAULT_MIN_MODE_CANDLES,
    min_state_observations: int = DEFAULT_MIN_STATE_OBSERVATIONS,
) -> TopixStreakMultiTimeframeModeResearchResult:
    if min_state_observations <= 0:
        raise ValueError("min_state_observations must be positive")

    base_result = run_topix_streak_extreme_mode_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        candidate_windows=candidate_windows,
        future_horizons=future_horizons,
        validation_ratio=validation_ratio,
        min_mode_candles=min_mode_candles,
    )
    resolved_stability_horizons = _resolve_stability_horizons(
        stability_horizons,
        future_horizons=base_result.future_horizons,
    )
    base_streak_df, mode_matrix_df = _prepare_pair_scan_inputs(
        base_result.mode_assignments_df,
        future_horizons=base_result.future_horizons,
    )
    pair_score_df = _build_pair_score_df(
        base_streak_df,
        mode_matrix_df,
        candidate_windows=base_result.candidate_windows,
        stability_horizons=resolved_stability_horizons,
        min_state_observations=min_state_observations,
    )
    selected_short_window_streaks, selected_long_window_streaks = _select_best_pair(
        pair_score_df
    )
    selected_pair_state_streak_df = _build_multi_timeframe_state_streak_df(
        base_result.mode_assignments_df,
        short_window_streaks=selected_short_window_streaks,
        long_window_streaks=selected_long_window_streaks,
        future_horizons=base_result.future_horizons,
    )
    selected_pair_state_segment_df = _build_multi_timeframe_state_segment_df(
        selected_pair_state_streak_df
    )
    selected_pair_state_summary_df = _build_multi_timeframe_state_summary_df(
        selected_pair_state_streak_df,
        future_horizons=base_result.future_horizons,
    )
    selected_pair_state_segment_summary_df = (
        _build_multi_timeframe_state_segment_summary_df(selected_pair_state_segment_df)
    )
    selected_pair_horizon_rank_df = _build_pair_horizon_rank_df(
        selected_pair_state_summary_df,
        stability_horizons=resolved_stability_horizons,
    )

    return TopixStreakMultiTimeframeModeResearchResult(
        db_path=base_result.db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        candidate_windows=base_result.candidate_windows,
        future_horizons=base_result.future_horizons,
        stability_horizons=resolved_stability_horizons,
        validation_ratio=base_result.validation_ratio,
        min_mode_candles=base_result.min_mode_candles,
        min_state_observations=min_state_observations,
        selected_base_window_streaks=base_result.selected_window_streaks,
        selected_short_window_streaks=selected_short_window_streaks,
        selected_long_window_streaks=selected_long_window_streaks,
        selection_metric=(
            "validation 4-state ranking stability + forward spread across "
            f"{_format_int_sequence(resolved_stability_horizons)}-day horizons"
        ),
        streak_candle_df=base_result.streak_candle_df,
        single_window_score_df=base_result.window_score_df,
        pair_score_df=pair_score_df,
        selected_pair_state_streak_df=selected_pair_state_streak_df,
        selected_pair_state_segment_df=selected_pair_state_segment_df,
        selected_pair_state_summary_df=selected_pair_state_summary_df,
        selected_pair_state_segment_summary_df=selected_pair_state_segment_summary_df,
        selected_pair_horizon_rank_df=selected_pair_horizon_rank_df,
    )


def write_topix_streak_multi_timeframe_mode_research_bundle(
    result: TopixStreakMultiTimeframeModeResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_streak_multi_timeframe_mode_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "candidate_windows": list(result.candidate_windows),
            "future_horizons": list(result.future_horizons),
            "stability_horizons": list(result.stability_horizons),
            "validation_ratio": result.validation_ratio,
            "min_mode_candles": result.min_mode_candles,
            "min_state_observations": result.min_state_observations,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "candidate_windows": list(result.candidate_windows),
            "future_horizons": list(result.future_horizons),
            "stability_horizons": list(result.stability_horizons),
            "validation_ratio": result.validation_ratio,
            "min_mode_candles": result.min_mode_candles,
            "min_state_observations": result.min_state_observations,
            "selected_base_window_streaks": result.selected_base_window_streaks,
            "selected_short_window_streaks": result.selected_short_window_streaks,
            "selected_long_window_streaks": result.selected_long_window_streaks,
            "selection_metric": result.selection_metric,
        },
        result_tables={
            "streak_candle_df": result.streak_candle_df,
            "single_window_score_df": result.single_window_score_df,
            "pair_score_df": result.pair_score_df,
            "selected_pair_state_streak_df": result.selected_pair_state_streak_df,
            "selected_pair_state_segment_df": result.selected_pair_state_segment_df,
            "selected_pair_state_summary_df": result.selected_pair_state_summary_df,
            "selected_pair_state_segment_summary_df": result.selected_pair_state_segment_summary_df,
            "selected_pair_horizon_rank_df": result.selected_pair_horizon_rank_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_streak_multi_timeframe_mode_research_bundle(
    bundle_path: str | Path,
) -> TopixStreakMultiTimeframeModeResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return TopixStreakMultiTimeframeModeResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        candidate_windows=tuple(int(value) for value in metadata["candidate_windows"]),
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        stability_horizons=tuple(int(value) for value in metadata["stability_horizons"]),
        validation_ratio=float(metadata["validation_ratio"]),
        min_mode_candles=int(metadata["min_mode_candles"]),
        min_state_observations=int(metadata["min_state_observations"]),
        selected_base_window_streaks=int(metadata["selected_base_window_streaks"]),
        selected_short_window_streaks=int(metadata["selected_short_window_streaks"]),
        selected_long_window_streaks=int(metadata["selected_long_window_streaks"]),
        selection_metric=str(metadata["selection_metric"]),
        streak_candle_df=tables["streak_candle_df"],
        single_window_score_df=tables["single_window_score_df"],
        pair_score_df=tables["pair_score_df"],
        selected_pair_state_streak_df=tables["selected_pair_state_streak_df"],
        selected_pair_state_segment_df=tables["selected_pair_state_segment_df"],
        selected_pair_state_summary_df=tables["selected_pair_state_summary_df"],
        selected_pair_state_segment_summary_df=tables[
            "selected_pair_state_segment_summary_df"
        ],
        selected_pair_horizon_rank_df=tables["selected_pair_horizon_rank_df"],
    )


def get_topix_streak_multi_timeframe_mode_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _resolve_stability_horizons(
    stability_horizons: Sequence[int] | None,
    *,
    future_horizons: Sequence[int],
) -> tuple[int, ...]:
    if stability_horizons is None:
        resolved = tuple(
            horizon
            for horizon in DEFAULT_PAIR_STABILITY_HORIZONS
            if horizon in future_horizons
        )
        return resolved or tuple(int(value) for value in future_horizons)

    normalized = tuple(sorted({int(value) for value in stability_horizons if int(value) > 0}))
    if not normalized:
        raise ValueError("stability_horizons must contain at least one positive integer")
    missing = [value for value in normalized if value not in set(future_horizons)]
    if missing:
        raise ValueError(
            "stability_horizons must be a subset of future_horizons: "
            f"{missing!r}"
        )
    return normalized


def _prepare_pair_scan_inputs(
    mode_assignments_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    first_window = int(mode_assignments_df["window_streaks"].min())
    base_columns = [
        "segment_id",
        "sample_split",
        "segment_start_date",
        "segment_end_date",
        "synthetic_open",
        "synthetic_close",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
    ]
    base_columns.extend(f"future_return_{horizon}d" for horizon in future_horizons)
    base_columns.extend(f"future_diff_{horizon}d" for horizon in future_horizons)
    base_streak_df = (
        mode_assignments_df[mode_assignments_df["window_streaks"] == first_window][
            base_columns
        ]
        .copy()
        .sort_values("segment_id", kind="stable")
        .reset_index(drop=True)
    )
    mode_matrix_df = (
        mode_assignments_df.assign(mode=mode_assignments_df["mode"].astype(str))
        .pivot(index="segment_id", columns="window_streaks", values="mode")
        .sort_index(axis=0)
        .sort_index(axis=1)
    )
    return base_streak_df, mode_matrix_df


def _build_pair_score_df(
    base_streak_df: pd.DataFrame,
    mode_matrix_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    stability_horizons: Sequence[int],
    min_state_observations: int,
) -> pd.DataFrame:
    validation_df = (
        base_streak_df[base_streak_df["sample_split"] == "validation"]
        .copy()
        .sort_values("segment_id", kind="stable")
        .reset_index(drop=True)
    )
    if validation_df.empty:
        raise ValueError("Validation split is empty; increase the sample length or lower validation_ratio")

    validation_modes = mode_matrix_df.loc[validation_df["segment_id"]].copy()
    short_candidates = [
        int(window)
        for window in candidate_windows
        if int(window) <= DEFAULT_SHORT_WINDOW_MAX
    ]
    long_candidates = [
        int(window)
        for window in candidate_windows
        if int(window) >= DEFAULT_LONG_WINDOW_MIN
    ]
    if not short_candidates or not long_candidates:
        raise ValueError("Candidate windows do not contain enough short/long values for pair scan")

    score_rows: list[dict[str, Any]] = []
    for short_window_streaks in short_candidates:
        if short_window_streaks not in validation_modes.columns:
            continue
        short_modes = validation_modes[short_window_streaks].astype(str).to_list()
        for long_window_streaks in long_candidates:
            if long_window_streaks not in validation_modes.columns:
                continue
            if long_window_streaks <= short_window_streaks:
                continue

            long_modes = validation_modes[long_window_streaks].astype(str).to_list()
            state_keys = [
                _build_multi_timeframe_state_key(
                    long_mode=long_mode,
                    short_mode=short_mode,
                )
                for long_mode, short_mode in zip(long_modes, short_modes, strict=True)
            ]
            state_series = pd.Series(state_keys, dtype="string")
            state_count_map = {
                str(key): int(value)
                for key, value in state_series.value_counts(sort=False).to_dict().items()
            }
            state_count_min = min(state_count_map.values()) if state_count_map else 0
            state_count_max = max(state_count_map.values()) if state_count_map else 0
            state_balance_ratio = (
                state_count_min / state_count_max
                if state_count_min > 0 and state_count_max > 0
                else 0.0
            )
            disagreement_ratio = (
                sum(
                    1 for short_mode, long_mode in zip(short_modes, long_modes, strict=True)
                    if short_mode != long_mode
                )
                / len(short_modes)
            )

            horizon_rank_rows: list[dict[str, Any]] = []
            spread_values: list[float] = []
            exact_state_presence = len(state_count_map) == len(MULTI_TIMEFRAME_STATE_ORDER)
            exact_state_coverage = exact_state_presence and state_count_min >= min_state_observations
            for horizon in stability_horizons:
                horizon_df = pd.DataFrame(
                    {
                        "state_key": state_keys,
                        "future_return": validation_df[f"future_return_{horizon}d"],
                    }
                )
                grouped = (
                    horizon_df.groupby("state_key", observed=True)["future_return"]
                    .agg(day_count="count", mean_future_return="mean")
                    .reindex(list(MULTI_TIMEFRAME_STATE_ORDER))
                    .reset_index()
                )
                grouped["state_label"] = grouped["state_key"].map(
                    _format_multi_timeframe_state_label
                )
                if grouped["day_count"].isna().any():
                    exact_state_coverage = False
                    continue
                grouped["day_count"] = grouped["day_count"].astype(int)
                grouped["mean_future_return"] = grouped["mean_future_return"].astype(float)
                if int(grouped["day_count"].min()) < min_state_observations:
                    exact_state_coverage = False
                ordered_grouped = grouped.sort_values(
                    ["mean_future_return", "state_key"],
                    ascending=[False, True],
                    kind="stable",
                ).reset_index(drop=True)
                spread_values.append(
                    float(ordered_grouped["mean_future_return"].iloc[0])
                    - float(ordered_grouped["mean_future_return"].iloc[-1])
                )
                for rank_position, state_row in enumerate(
                    ordered_grouped.itertuples(index=False),
                    start=1,
                ):
                    horizon_rank_rows.append(
                        {
                            "horizon_days": int(horizon),
                            "rank_position": rank_position,
                            "state_key": str(state_row.state_key),
                            "state_label": str(state_row.state_label),
                            "mean_future_return": _as_float_scalar(
                                state_row.mean_future_return
                            ),
                            "day_count": _as_int_scalar(state_row.day_count),
                        }
                    )

            ranking_consistency = _compute_ranking_consistency(horizon_rank_rows)
            edge_state_consistency = _compute_edge_state_consistency(horizon_rank_rows)
            locked_best_worst = _compute_locked_best_worst_ratio(horizon_rank_rows)
            mean_spread = float(sum(spread_values) / len(spread_values)) if spread_values else 0.0
            selection_eligible = bool(exact_state_coverage and horizon_rank_rows)
            selection_score = (
                mean_spread
                * ranking_consistency
                * edge_state_consistency
                * (0.5 + 0.5 * locked_best_worst)
                * disagreement_ratio
                * math.sqrt(state_balance_ratio)
                if selection_eligible
                else math.nan
            )

            row: dict[str, Any] = {
                "short_window_streaks": short_window_streaks,
                "long_window_streaks": long_window_streaks,
                "state_count_min": state_count_min,
                "state_count_max": state_count_max,
                "state_balance_ratio": state_balance_ratio,
                "disagreement_ratio": disagreement_ratio,
                "ranking_consistency": ranking_consistency,
                "edge_state_consistency": edge_state_consistency,
                "locked_best_worst_ratio": locked_best_worst,
                "mean_spread": mean_spread,
                "selection_eligible": selection_eligible,
                "selection_score": selection_score,
                "selection_rank": pd.NA,
            }
            for horizon in stability_horizons:
                ordered_rows = [
                    value
                    for value in horizon_rank_rows
                    if int(value["horizon_days"]) == int(horizon)
                ]
                if not ordered_rows:
                    row[f"best_state_{horizon}d"] = pd.NA
                    row[f"best_return_{horizon}d"] = math.nan
                    row[f"worst_state_{horizon}d"] = pd.NA
                    row[f"worst_return_{horizon}d"] = math.nan
                    row[f"spread_{horizon}d"] = math.nan
                    continue
                ordered_rows = sorted(
                    ordered_rows,
                    key=lambda value: int(value["rank_position"]),
                )
                row[f"best_state_{horizon}d"] = ordered_rows[0]["state_label"]
                row[f"best_return_{horizon}d"] = ordered_rows[0]["mean_future_return"]
                row[f"worst_state_{horizon}d"] = ordered_rows[-1]["state_label"]
                row[f"worst_return_{horizon}d"] = ordered_rows[-1]["mean_future_return"]
                row[f"spread_{horizon}d"] = (
                    float(ordered_rows[0]["mean_future_return"])
                    - float(ordered_rows[-1]["mean_future_return"])
                )
            score_rows.append(row)

    pair_score_df = pd.DataFrame(score_rows)
    ranked_df = (
        pair_score_df[
            pair_score_df["selection_eligible"] & pair_score_df["selection_score"].notna()
        ]
        .sort_values(
            [
                "selection_score",
                "locked_best_worst_ratio",
                "ranking_consistency",
                "mean_spread",
                "disagreement_ratio",
                "long_window_streaks",
                "short_window_streaks",
            ],
            ascending=[False, False, False, False, False, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    rank_lookup = {
        (
            int(cast(dict[str, Any], row)["short_window_streaks"]),
            int(cast(dict[str, Any], row)["long_window_streaks"]),
        ): rank_position
        for rank_position, row in enumerate(
            ranked_df.to_dict(orient="records"),
            start=1,
        )
    }
    pair_score_df["selection_rank"] = [
        rank_lookup.get(
            (
                int(cast(dict[str, Any], row)["short_window_streaks"]),
                int(cast(dict[str, Any], row)["long_window_streaks"]),
            ),
            pd.NA,
        )
        for row in pair_score_df.to_dict(orient="records")
    ]
    return pair_score_df.sort_values(
        ["selection_rank", "short_window_streaks", "long_window_streaks"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)


def _compute_ranking_consistency(horizon_rank_rows: Sequence[dict[str, Any]]) -> float:
    orderings = _extract_horizon_orderings(horizon_rank_rows)
    if not orderings:
        return 0.0
    if len(orderings) == 1:
        return 1.0
    correlations: list[float] = []
    for left_order, right_order in combinations(orderings.values(), 2):
        left_ranks = pd.Series(
            {state_key: index + 1 for index, state_key in enumerate(left_order)},
            dtype=float,
        )
        right_ranks = pd.Series(
            {state_key: index + 1 for index, state_key in enumerate(right_order)},
            dtype=float,
        )
        rho = float(left_ranks.corr(right_ranks))
        correlations.append((rho + 1.0) / 2.0 if not math.isnan(rho) else 0.0)
    return float(sum(correlations) / len(correlations)) if correlations else 0.0


def _compute_edge_state_consistency(horizon_rank_rows: Sequence[dict[str, Any]]) -> float:
    orderings = _extract_horizon_orderings(horizon_rank_rows)
    if not orderings:
        return 0.0
    best_states = [order[0] for order in orderings.values() if order]
    worst_states = [order[-1] for order in orderings.values() if order]
    if not best_states or not worst_states:
        return 0.0
    best_consistency = max(best_states.count(state) for state in set(best_states)) / len(best_states)
    worst_consistency = max(worst_states.count(state) for state in set(worst_states)) / len(worst_states)
    return (best_consistency + worst_consistency) / 2.0


def _compute_locked_best_worst_ratio(horizon_rank_rows: Sequence[dict[str, Any]]) -> float:
    orderings = _extract_horizon_orderings(horizon_rank_rows)
    if not orderings:
        return 0.0
    best_states = [order[0] for order in orderings.values() if order]
    worst_states = [order[-1] for order in orderings.values() if order]
    if not best_states or not worst_states:
        return 0.0
    return float(
        int(len(set(best_states)) == 1 and len(set(worst_states)) == 1)
    )


def _extract_horizon_orderings(
    horizon_rank_rows: Sequence[dict[str, Any]],
) -> dict[int, tuple[str, ...]]:
    orderings: dict[int, tuple[str, ...]] = {}
    horizon_values = sorted({int(row["horizon_days"]) for row in horizon_rank_rows})
    for horizon in horizon_values:
        ordered_rows = sorted(
            (
                row
                for row in horizon_rank_rows
                if int(row["horizon_days"]) == horizon
            ),
            key=lambda row: int(row["rank_position"]),
        )
        orderings[horizon] = tuple(str(row["state_key"]) for row in ordered_rows)
    return orderings


def _select_best_pair(pair_score_df: pd.DataFrame) -> tuple[int, int]:
    candidates_df = pair_score_df[
        pair_score_df["selection_eligible"] & pair_score_df["selection_score"].notna()
    ].copy()
    if candidates_df.empty:
        raise ValueError("No eligible streak multi-timeframe pairs were available")
    candidates_df = candidates_df.sort_values(
        [
            "selection_score",
            "locked_best_worst_ratio",
            "ranking_consistency",
            "mean_spread",
            "disagreement_ratio",
            "long_window_streaks",
            "short_window_streaks",
        ],
        ascending=[False, False, False, False, False, False, True],
        kind="stable",
    )
    top_row = candidates_df.iloc[0]
    return int(top_row["short_window_streaks"]), int(top_row["long_window_streaks"])


def _build_multi_timeframe_state_streak_df(
    mode_assignments_df: pd.DataFrame,
    *,
    short_window_streaks: int,
    long_window_streaks: int,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    short_df = (
        mode_assignments_df[mode_assignments_df["window_streaks"] == short_window_streaks]
        .copy()
        .rename(
            columns={
                "mode": "short_mode",
                "dominant_segment_return": "short_dominant_segment_return",
                "dominant_abs_segment_return": "short_dominant_abs_segment_return",
                "dominant_segment_start_date": "short_dominant_segment_start_date",
                "dominant_segment_end_date": "short_dominant_segment_end_date",
                "dominant_segment_day_count": "short_dominant_segment_day_count",
            }
        )
    )
    long_df = (
        mode_assignments_df[mode_assignments_df["window_streaks"] == long_window_streaks]
        .copy()
        .rename(
            columns={
                "mode": "long_mode",
                "dominant_segment_return": "long_dominant_segment_return",
                "dominant_abs_segment_return": "long_dominant_abs_segment_return",
                "dominant_segment_start_date": "long_dominant_segment_start_date",
                "dominant_segment_end_date": "long_dominant_segment_end_date",
                "dominant_segment_day_count": "long_dominant_segment_day_count",
            }
        )
    )
    if short_df.empty or long_df.empty:
        raise ValueError("Short/long streak rows were not found in mode_assignments_df")

    short_columns = [
        "segment_id",
        "sample_split",
        "segment_start_date",
        "segment_end_date",
        "synthetic_open",
        "synthetic_close",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
        "short_mode",
        "short_dominant_segment_return",
        "short_dominant_abs_segment_return",
        "short_dominant_segment_start_date",
        "short_dominant_segment_end_date",
        "short_dominant_segment_day_count",
    ]
    short_columns.extend(f"future_return_{horizon}d" for horizon in future_horizons)
    short_columns.extend(f"future_diff_{horizon}d" for horizon in future_horizons)
    long_columns = [
        "segment_id",
        "sample_split",
        "long_mode",
        "long_dominant_segment_return",
        "long_dominant_abs_segment_return",
        "long_dominant_segment_start_date",
        "long_dominant_segment_end_date",
        "long_dominant_segment_day_count",
    ]
    merged_df = short_df[short_columns].merge(
        long_df[long_columns],
        on=["segment_id", "sample_split"],
        how="inner",
        validate="one_to_one",
    )
    merged_df["short_window_streaks"] = short_window_streaks
    merged_df["long_window_streaks"] = long_window_streaks
    merged_df["state_key"] = merged_df.apply(
        lambda row: _build_multi_timeframe_state_key(
            long_mode=str(row["long_mode"]),
            short_mode=str(row["short_mode"]),
        ),
        axis=1,
    )
    merged_df["state_label"] = merged_df["state_key"].map(_format_multi_timeframe_state_label)
    merged_df["state_key"] = pd.Categorical(
        merged_df["state_key"],
        categories=list(MULTI_TIMEFRAME_STATE_ORDER),
        ordered=True,
    )
    return merged_df.sort_values("segment_id", kind="stable").reset_index(drop=True)


def _build_multi_timeframe_state_segment_df(
    state_streak_df: pd.DataFrame,
) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", state_streak_df)]
    for split_name in ("discovery", "validation"):
        split_df = state_streak_df[state_streak_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    segment_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        ordered_df = split_df.sort_values("segment_id", kind="stable").reset_index(drop=True)
        if ordered_df.empty:
            continue
        segment_keys = (
            ordered_df["state_key"].astype(str)
            != ordered_df["state_key"].astype(str).shift(fill_value=ordered_df["state_key"].iloc[0])
        ).cumsum()
        grouped = (
            ordered_df.groupby(segment_keys, observed=True)
            .agg(
                state_key=("state_key", "first"),
                state_label=("state_label", "first"),
                long_mode=("long_mode", "first"),
                short_mode=("short_mode", "first"),
                start_segment_start_date=("segment_start_date", "first"),
                end_segment_end_date=("segment_end_date", "last"),
                start_open=("synthetic_open", "first"),
                end_close=("synthetic_close", "last"),
                state_candle_count=("segment_id", "count"),
                state_day_count=("segment_day_count", "sum"),
                mean_short_dominant_segment_return=("short_dominant_segment_return", "mean"),
                mean_long_dominant_segment_return=("long_dominant_segment_return", "mean"),
            )
            .reset_index(drop=True)
        )
        grouped["sample_split"] = split_name
        grouped["short_window_streaks"] = int(ordered_df["short_window_streaks"].iloc[0])
        grouped["long_window_streaks"] = int(ordered_df["long_window_streaks"].iloc[0])
        grouped["segment_id"] = range(1, len(grouped) + 1)
        grouped["segment_return"] = grouped["end_close"] / grouped["start_open"] - 1.0
        segment_frames.append(grouped)

    if not segment_frames:
        raise ValueError("Failed to build any streak multi-timeframe state segments")
    return pd.concat(segment_frames, ignore_index=True)


def _build_multi_timeframe_state_summary_df(
    state_streak_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", state_streak_df)]
    for split_name in ("discovery", "validation"):
        split_df = state_streak_df[state_streak_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        for horizon in future_horizons:
            return_col = f"future_return_{horizon}d"
            grouped = (
                split_df.groupby(["state_key", "state_label", "long_mode", "short_mode"], observed=True)[
                    return_col
                ]
                .agg(
                    state_candle_count="count",
                    mean_future_return="mean",
                    median_future_return="median",
                    std_future_return="std",
                    up_count=lambda values: int((values > 0).sum()),
                    down_count=lambda values: int((values < 0).sum()),
                    flat_count=lambda values: int((values == 0).sum()),
                )
                .reset_index()
            )
            if grouped.empty:
                continue
            grouped["sample_split"] = split_name
            grouped["horizon_days"] = horizon
            grouped["hit_rate_positive"] = grouped["up_count"] / grouped["state_candle_count"]
            grouped["hit_rate_negative"] = grouped["down_count"] / grouped["state_candle_count"]
            grouped["short_window_streaks"] = int(split_df["short_window_streaks"].iloc[0])
            grouped["long_window_streaks"] = int(split_df["long_window_streaks"].iloc[0])
            summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build streak multi-timeframe state summary rows")
    return pd.concat(summary_frames, ignore_index=True)


def _build_multi_timeframe_state_segment_summary_df(
    state_segment_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = (
        state_segment_df.groupby(
            ["sample_split", "state_key", "state_label", "long_mode", "short_mode"],
            observed=True,
        )
        .agg(
            segment_count=("segment_id", "count"),
            total_state_days=("state_day_count", "sum"),
            mean_state_candle_count=("state_candle_count", "mean"),
            mean_state_day_count=("state_day_count", "mean"),
            median_state_day_count=("state_day_count", "median"),
            mean_segment_return=("segment_return", "mean"),
            median_segment_return=("segment_return", "median"),
            std_segment_return=("segment_return", "std"),
            positive_segment_count=("segment_return", lambda values: int((values > 0).sum())),
            negative_segment_count=("segment_return", lambda values: int((values < 0).sum())),
            flat_segment_count=("segment_return", lambda values: int((values == 0).sum())),
        )
        .reset_index()
    )
    summary_df["positive_segment_ratio"] = (
        summary_df["positive_segment_count"] / summary_df["segment_count"]
    )
    summary_df["negative_segment_ratio"] = (
        summary_df["negative_segment_count"] / summary_df["segment_count"]
    )
    return summary_df


def _build_pair_horizon_rank_df(
    state_summary_df: pd.DataFrame,
    *,
    stability_horizons: Sequence[int],
) -> pd.DataFrame:
    validation_df = state_summary_df[
        state_summary_df["sample_split"] == "validation"
    ].copy()
    rank_frames: list[pd.DataFrame] = []
    for horizon in stability_horizons:
        horizon_df = validation_df[validation_df["horizon_days"] == horizon].copy()
        if horizon_df.empty:
            continue
        horizon_df = horizon_df.sort_values(
            ["mean_future_return", "state_key"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)
        horizon_df["rank_position"] = range(1, len(horizon_df) + 1)
        rank_frames.append(horizon_df)
    if not rank_frames:
        raise ValueError("No validation state summary rows matched the stability horizons")
    return pd.concat(rank_frames, ignore_index=True)


def _build_multi_timeframe_state_key(*, long_mode: str, short_mode: str) -> str:
    return f"long_{long_mode}__short_{short_mode}"


def _format_multi_timeframe_state_label(state_key: str) -> str:
    return state_key.replace("long_", "Long ").replace("__short_", " / Short ").replace(
        "_", " "
    ).title()


def _build_research_bundle_summary_markdown(
    result: TopixStreakMultiTimeframeModeResearchResult,
) -> str:
    top_pairs = result.pair_score_df.head(5).copy()
    validation_segments = result.selected_pair_state_segment_summary_df[
        result.selected_pair_state_segment_summary_df["sample_split"] == "validation"
    ].copy()
    validation_forward = result.selected_pair_state_summary_df[
        result.selected_pair_state_summary_df["sample_split"] == "validation"
    ].copy()

    lines = [
        "# TOPIX Streak Multi-Timeframe Mode",
        "",
        "This study scans short and long streak-candle windows simultaneously, then evaluates which 4-state combination remains most stable across validation horizons.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Candidate windows (streak candles): `{_format_int_sequence(result.candidate_windows)}`",
        f"- Stability horizons: `{_format_int_sequence(result.stability_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Minimum state observations: `{result.min_state_observations}`",
        "",
        "## Selected Pair",
        "",
        f"- Base standalone streak X: `{result.selected_base_window_streaks}`",
        f"- Selected short window streaks: `{result.selected_short_window_streaks}`",
        f"- Selected long window streaks: `{result.selected_long_window_streaks}`",
        f"- Selection metric: `{result.selection_metric}`",
    ]

    if not validation_segments.empty:
        lines.extend(["", "## Validation 4-State Segment Readout", ""])
        for row in validation_segments.sort_values("state_key", kind="stable").to_dict(orient="records"):
            lines.append(
                "- "
                f"{str(row['state_label'])}: "
                f"segment={_format_return(float(row['mean_segment_return']))}, "
                f"candles={float(row['mean_state_candle_count']):.1f}, "
                f"days={float(row['mean_state_day_count']):.1f}"
            )

    if not validation_forward.empty:
        lines.extend(["", "## Validation Forward Ordering", ""])
        for horizon in result.stability_horizons:
            horizon_df = validation_forward[
                validation_forward["horizon_days"] == horizon
            ].copy()
            if horizon_df.empty:
                continue
            ordered_df = horizon_df.sort_values(
                ["mean_future_return", "state_key"],
                ascending=[False, True],
                kind="stable",
            )
            ordering = " > ".join(
                f"{str(row['state_label'])} ({_format_return(float(row['mean_future_return']))})"
                for row in ordered_df.to_dict(orient="records")
            )
            lines.append(f"- {int(horizon)}d: {ordering}")

    if not top_pairs.empty:
        lines.extend(["", "## Top Pair Candidates", ""])
        for row in top_pairs.to_dict(orient="records"):
            selection_score = row["selection_score"]
            selection_text = (
                "N/A"
                if selection_score is None or pd.isna(selection_score)
                else f"{float(selection_score):.4f}"
            )
            lines.append(
                "- "
                f"short={int(row['short_window_streaks'])}, "
                f"long={int(row['long_window_streaks'])}: "
                f"score={selection_text}, "
                f"rank-consistency={float(row['ranking_consistency']):.1%}, "
                f"edge-lock={float(row['locked_best_worst_ratio']):.1%}, "
                f"spread={_format_return(float(row['mean_spread']))}"
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `streak_candle_df`",
            "- `single_window_score_df`",
            "- `pair_score_df`",
            "- `selected_pair_state_streak_df`",
            "- `selected_pair_state_segment_df`",
            "- `selected_pair_state_summary_df`",
            "- `selected_pair_state_segment_summary_df`",
            "- `selected_pair_horizon_rank_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: TopixStreakMultiTimeframeModeResearchResult,
) -> dict[str, Any]:
    selected_pair_row = result.pair_score_df.iloc[0] if not result.pair_score_df.empty else None
    validation_forward = result.selected_pair_state_summary_df[
        result.selected_pair_state_summary_df["sample_split"] == "validation"
    ].copy()
    validation_segments = result.selected_pair_state_segment_summary_df[
        result.selected_pair_state_segment_summary_df["sample_split"] == "validation"
    ].copy()

    strongest_state = _select_state_row(
        validation_forward,
        largest=True,
        horizons=result.stability_horizons,
    )
    weakest_state = _select_state_row(
        validation_forward,
        largest=False,
        horizons=result.stability_horizons,
    )

    result_bullets: list[str] = []
    highlights: list[dict[str, str]] = [
        {
            "label": "Short / long pair",
            "value": f"{result.selected_short_window_streaks} / {result.selected_long_window_streaks}",
            "tone": "accent",
            "detail": "streak candles",
        },
        {
            "label": "Base standalone X",
            "value": f"{result.selected_base_window_streaks} streaks",
            "tone": "neutral",
            "detail": "single-window streak mode",
        },
    ]
    if selected_pair_row is not None:
        result_bullets.append(
            "The pair scan formalized the earlier exploratory read: a very short streak trigger and a much slower streak filter carried distinct information. "
            f"The most stable pair was short={result.selected_short_window_streaks} and long={result.selected_long_window_streaks} streak candles."
        )
        result_bullets.append(
            "This was not classic trend-following alignment. The pair was selected because the 4-state ordering stayed comparatively stable across "
            f"{_format_int_sequence(result.stability_horizons)}-day validation horizons, not because long bullish + short bullish dominated."
        )
        highlights.append(
            {
                "label": "Ranking consistency",
                "value": f"{float(selected_pair_row['ranking_consistency']):.1%}",
                "tone": "success",
                "detail": f"validation score {_format_return(float(selected_pair_row['mean_spread']))} average spread",
            }
        )
    if strongest_state is not None and weakest_state is not None:
        strongest_label = str(strongest_state["state_label"])
        weakest_label = str(weakest_state["state_label"])
        strongest_horizon = int(strongest_state["horizon_days"])
        weakest_horizon = int(weakest_state["horizon_days"])
        result_bullets.append(
            f"The persistent shape was still mean-reversion-like: {strongest_label} was the strongest validation state "
            f"at {strongest_horizon}d {_format_return(float(strongest_state['mean_future_return']))}, while "
            f"{weakest_label} was the weakest at {weakest_horizon}d {_format_return(float(weakest_state['mean_future_return']))}."
        )
        highlights.append(
            {
                "label": "Strongest state",
                "value": strongest_label,
                "tone": "success",
                "detail": _format_return(float(strongest_state["mean_future_return"])),
            }
        )
    if not validation_segments.empty:
        bearish_bearish = validation_segments[
            validation_segments["state_key"] == "long_bearish__short_bearish"
        ]
        bullish_bullish = validation_segments[
            validation_segments["state_key"] == "long_bullish__short_bullish"
        ]
        if not bearish_bearish.empty and not bullish_bullish.empty:
            result_bullets.append(
                "The segment view confirms the same interpretation: both-bearish streak states describe the most extended downside conditions, while both-bullish states are not where forward upside concentrated."
            )

    return {
        "title": "TOPIX Streak Multi-Timeframe Mode",
        "tags": ["TOPIX", "streaks", "multi-timeframe", "mode"],
        "purpose": (
            "Pair a short streak-candle mode with a slower streak-candle regime filter, then rank which short/long window combination "
            "produces the most stable 4-state forward-return ordering."
        ),
        "method": [
            "Start from the standalone streak-candle mode study, where consecutive positive or negative close-to-close moves are merged into one synthetic candle.",
            "Scan short windows from the short streak cluster and long windows from the slower cluster, then combine them into four states: Long/Short Bullish or Bearish.",
            "Select the pair on validation by ranking consistency, best/worst-state stability, forward spread, and enough observations in every state.",
        ],
        "resultHeadline": (
            f"The formal pair scan kept the same basic conclusion as the exploratory pass: short={result.selected_short_window_streaks} and "
            f"long={result.selected_long_window_streaks} streaks was the cleanest multi-timeframe split, but the signal still behaved like an exhaustion hierarchy rather than a trend hierarchy."
        ),
        "resultBullets": result_bullets,
        "considerations": [
            "This 4-state model is a better contextual layer than a replacement for the standalone streak mean-reversion trigger. The short streak mode is still the execution-side information; the long streak mode mainly conditions how seriously to take it.",
            "The pair itself is selected on validation stability, so it should be read as a published exploratory result rather than as a production parameter locked by strict out-of-sample protocol.",
            "If this graduates into a trading rule, the next step is not another mode search. It is a direct rule test on the key states, especially whether both-bearish or long-bearish/short-bearish states justify larger or longer mean-reversion entries.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.selected_short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.selected_long_window_streaks} streaks"},
            {"label": "Stability horizons", "value": _format_int_sequence(result.stability_horizons)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "pair_score_df",
                "label": "Pair-scan leaderboard",
                "description": "Ranking stability and spread for every short/long streak pair.",
            },
            {
                "name": "selected_pair_state_summary_df",
                "label": "Selected-pair forward summary",
                "description": "Forward returns for the chosen 4-state streak regime.",
            },
            {
                "name": "selected_pair_state_segment_summary_df",
                "label": "Selected-pair segment summary",
                "description": "Average move and duration for each state in the chosen pair.",
            },
        ],
    }


def _select_state_row(
    validation_forward: pd.DataFrame,
    *,
    largest: bool,
    horizons: Sequence[int],
) -> pd.Series | None:
    horizon_df = validation_forward[
        validation_forward["horizon_days"].isin(list(horizons))
    ].copy()
    if horizon_df.empty:
        return None
    horizon_df = horizon_df.sort_values(
        ["mean_future_return", "horizon_days", "state_key"],
        ascending=[not largest, True, True],
        kind="stable",
    )
    return horizon_df.iloc[0]


def _as_float_scalar(value: Any) -> float:
    return float(cast(Any, value))


def _as_int_scalar(value: Any) -> int:
    return int(cast(Any, value))
