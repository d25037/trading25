"""
TOPIX100 Q10 bounce conditioned by the fixed streak 3/53 state model.

This study fuses two existing results without re-optimizing either side:

1. The TOPIX100 `Q10 vs middle` bounce bucket lens
   (`price_vs_sma_50_gap x volume_sma_5_20`).
2. The fixed multi-timeframe streak state learned from TOPIX and transferred to
   TOPIX100 constituents (`short=3`, `long=53`).

The question is not whether either component works alone. It is whether the
bucket edge becomes materially stronger or weaker once we condition on the
stock's own streak state.
"""

from __future__ import annotations

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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    Q10_LOW_HYPOTHESIS_LABELS,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    run_topix100_price_vs_sma_q10_bounce_research,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    PRICE_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    _build_price_volume_horizon_panel,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix_close_return_streaks import DEFAULT_VALIDATION_RATIO
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_rank_future_close_core import (
    _holm_adjust,
    _safe_paired_t_test,
    _safe_wilcoxon,
)
from src.domains.analytics.topix_streak_extreme_mode import _format_return
from src.domains.analytics.topix_streak_multi_timeframe_mode import (
    MULTI_TIMEFRAME_STATE_ORDER,
)

DEFAULT_FUSION_HORIZONS: tuple[int, ...] = (1, 5, 10)
DEFAULT_MIN_CONSTITUENTS_PER_BUCKET_STATE_DATE = 3
TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID = (
    "market-behavior/topix100-q10-bounce-streak-3-53-conditioning"
)
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")


@dataclass(frozen=True)
class Topix100Q10BounceStreak353ConditioningResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    price_feature: str
    price_feature_label: str
    volume_feature: str
    volume_feature_label: str
    short_window_streaks: int
    long_window_streaks: int
    validation_ratio: float
    min_constituents_per_bucket_state_date: int
    universe_constituent_count: int
    covered_constituent_count: int
    joined_event_count: int
    valid_date_count: int
    state_bucket_horizon_panel_df: pd.DataFrame
    state_bucket_daily_means_df: pd.DataFrame
    state_bucket_summary_df: pd.DataFrame
    state_bucket_pairwise_significance_df: pd.DataFrame
    state_hypothesis_df: pd.DataFrame
    state_scorecard_df: pd.DataFrame
    validation_q10_state_matrix_df: pd.DataFrame


def run_topix100_q10_bounce_streak_353_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    min_constituents_per_bucket_state_date: int = DEFAULT_MIN_CONSTITUENTS_PER_BUCKET_STATE_DATE,
) -> Topix100Q10BounceStreak353ConditioningResearchResult:
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if min_constituents_per_bucket_state_date <= 0:
        raise ValueError("min_constituents_per_bucket_state_date must be positive")

    bounce_result = run_topix100_price_vs_sma_q10_bounce_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        price_features=(price_feature,),
        volume_features=(volume_feature,),
    )
    state_result = run_topix100_streak_353_transfer_research(
        db_path,
        start_date=None,
        end_date=end_date,
        future_horizons=DEFAULT_FUSION_HORIZONS,
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )

    q10_horizon_panel_df = _build_price_volume_horizon_panel(
        bounce_result.q10_middle_volume_split_panel_df,
        price_feature_order=(price_feature,),
    )
    q10_horizon_panel_df = q10_horizon_panel_df[
        (q10_horizon_panel_df["price_feature"] == price_feature)
        & (q10_horizon_panel_df["volume_feature"] == volume_feature)
        & q10_horizon_panel_df["combined_bucket"].isin(Q10_MIDDLE_COMBINED_BUCKET_ORDER)
    ].copy()
    if q10_horizon_panel_df.empty:
        raise ValueError("No Q10 bounce horizon rows were available for the selected feature pair")

    state_horizon_event_df = state_result.state_horizon_event_df[
        state_result.state_horizon_event_df["horizon_days"].isin(DEFAULT_FUSION_HORIZONS)
    ].copy()
    if state_horizon_event_df.empty:
        raise ValueError("No streak state horizon rows were available for the selected pair")

    state_bucket_horizon_panel_df = _build_state_bucket_horizon_panel(
        q10_horizon_panel_df=q10_horizon_panel_df,
        state_horizon_event_df=state_horizon_event_df,
    )
    state_bucket_daily_means_df = _build_state_bucket_daily_means_df(
        state_bucket_horizon_panel_df,
        min_constituents_per_bucket_state_date=min_constituents_per_bucket_state_date,
    )
    state_bucket_summary_df = _build_state_bucket_summary_df(state_bucket_daily_means_df)
    state_bucket_pairwise_significance_df = _build_state_bucket_pairwise_significance_df(
        state_bucket_daily_means_df
    )
    state_hypothesis_df = _build_state_hypothesis_df(
        state_bucket_pairwise_significance_df
    )
    state_scorecard_df = _build_state_scorecard_df(
        state_bucket_summary_df,
        state_hypothesis_df,
    )
    validation_q10_state_matrix_df = _build_validation_q10_state_matrix_df(
        state_bucket_summary_df
    )

    analysis_start_date = (
        str(state_bucket_horizon_panel_df["date"].min())
        if not state_bucket_horizon_panel_df.empty
        else None
    )
    analysis_end_date = (
        str(state_bucket_horizon_panel_df["date"].max())
        if not state_bucket_horizon_panel_df.empty
        else None
    )

    return Topix100Q10BounceStreak353ConditioningResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, bounce_result.base_result.source_mode),
        source_detail=str(bounce_result.base_result.source_detail),
        available_start_date=bounce_result.base_result.available_start_date,
        available_end_date=bounce_result.base_result.available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        min_constituents_per_bucket_state_date=min_constituents_per_bucket_state_date,
        universe_constituent_count=int(bounce_result.base_result.topix100_constituent_count),
        covered_constituent_count=int(state_bucket_horizon_panel_df["code"].nunique()),
        joined_event_count=int(len(state_bucket_horizon_panel_df)),
        valid_date_count=int(state_bucket_horizon_panel_df["date"].nunique()),
        state_bucket_horizon_panel_df=state_bucket_horizon_panel_df,
        state_bucket_daily_means_df=state_bucket_daily_means_df,
        state_bucket_summary_df=state_bucket_summary_df,
        state_bucket_pairwise_significance_df=state_bucket_pairwise_significance_df,
        state_hypothesis_df=state_hypothesis_df,
        state_scorecard_df=state_scorecard_df,
        validation_q10_state_matrix_df=validation_q10_state_matrix_df,
    )


def write_topix100_q10_bounce_streak_353_conditioning_research_bundle(
    result: Topix100Q10BounceStreak353ConditioningResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_q10_bounce_streak_353_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "validation_ratio": result.validation_ratio,
            "min_constituents_per_bucket_state_date": result.min_constituents_per_bucket_state_date,
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
            "price_feature": result.price_feature,
            "price_feature_label": result.price_feature_label,
            "volume_feature": result.volume_feature,
            "volume_feature_label": result.volume_feature_label,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "validation_ratio": result.validation_ratio,
            "min_constituents_per_bucket_state_date": result.min_constituents_per_bucket_state_date,
            "universe_constituent_count": result.universe_constituent_count,
            "covered_constituent_count": result.covered_constituent_count,
            "joined_event_count": result.joined_event_count,
            "valid_date_count": result.valid_date_count,
        },
        result_tables={
            "state_bucket_horizon_panel_df": result.state_bucket_horizon_panel_df,
            "state_bucket_daily_means_df": result.state_bucket_daily_means_df,
            "state_bucket_summary_df": result.state_bucket_summary_df,
            "state_bucket_pairwise_significance_df": result.state_bucket_pairwise_significance_df,
            "state_hypothesis_df": result.state_hypothesis_df,
            "state_scorecard_df": result.state_scorecard_df,
            "validation_q10_state_matrix_df": result.validation_q10_state_matrix_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_q10_bounce_streak_353_conditioning_research_bundle(
    bundle_path: str | Path,
) -> Topix100Q10BounceStreak353ConditioningResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return Topix100Q10BounceStreak353ConditioningResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        price_feature=str(metadata["price_feature"]),
        price_feature_label=str(metadata["price_feature_label"]),
        volume_feature=str(metadata["volume_feature"]),
        volume_feature_label=str(metadata["volume_feature_label"]),
        short_window_streaks=int(metadata["short_window_streaks"]),
        long_window_streaks=int(metadata["long_window_streaks"]),
        validation_ratio=float(metadata["validation_ratio"]),
        min_constituents_per_bucket_state_date=int(
            metadata["min_constituents_per_bucket_state_date"]
        ),
        universe_constituent_count=int(metadata["universe_constituent_count"]),
        covered_constituent_count=int(metadata["covered_constituent_count"]),
        joined_event_count=int(metadata["joined_event_count"]),
        valid_date_count=int(metadata["valid_date_count"]),
        state_bucket_horizon_panel_df=tables["state_bucket_horizon_panel_df"],
        state_bucket_daily_means_df=tables["state_bucket_daily_means_df"],
        state_bucket_summary_df=tables["state_bucket_summary_df"],
        state_bucket_pairwise_significance_df=tables[
            "state_bucket_pairwise_significance_df"
        ],
        state_hypothesis_df=tables["state_hypothesis_df"],
        state_scorecard_df=tables["state_scorecard_df"],
        validation_q10_state_matrix_df=tables["validation_q10_state_matrix_df"],
    )


def get_topix100_q10_bounce_streak_353_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_q10_bounce_streak_353_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_state_bucket_horizon_panel(
    *,
    q10_horizon_panel_df: pd.DataFrame,
    state_horizon_event_df: pd.DataFrame,
) -> pd.DataFrame:
    bounce_df = q10_horizon_panel_df.copy()
    bounce_df["date"] = bounce_df["date"].astype(str)
    state_df = state_horizon_event_df.copy()
    state_df["date"] = state_df["date"].astype(str)

    join_columns = ["code", "date", "horizon_days"]
    state_columns = [
        "code",
        "date",
        "horizon_days",
        "sample_split",
        "state_key",
        "state_label",
        "long_mode",
        "short_mode",
        "future_return",
    ]
    merged_df = bounce_df.merge(
        state_df[state_columns],
        on=join_columns,
        how="inner",
        validate="one_to_one",
        suffixes=("", "_state"),
    )
    if merged_df.empty:
        raise ValueError("Joining Q10 bounce rows with streak state rows produced no overlap")

    future_diff = (
        merged_df["future_return"].astype(float)
        - merged_df["future_return_state"].astype(float)
    ).abs()
    if not future_diff.le(1e-10).all():
        raise ValueError("Future-return mismatch detected between Q10 and streak panels")

    merged_df = merged_df.drop(columns=["future_return_state"])
    return _sort_interaction_frame(merged_df)


def _build_state_bucket_daily_means_df(
    state_bucket_horizon_panel_df: pd.DataFrame,
    *,
    min_constituents_per_bucket_state_date: int,
) -> pd.DataFrame:
    grouped = (
        state_bucket_horizon_panel_df.groupby(
            [
                "sample_split",
                "date",
                "horizon_key",
                "horizon_days",
                "state_key",
                "state_label",
                "long_mode",
                "short_mode",
                "combined_bucket",
                "combined_bucket_label",
            ],
            observed=True,
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
            positive_share=("future_return", lambda values: float((values > 0).mean())),
        )
    )
    grouped = grouped[
        grouped["group_sample_count"] >= min_constituents_per_bucket_state_date
    ].copy()
    if grouped.empty:
        raise ValueError(
            "No state/bucket/date rows satisfied min_constituents_per_bucket_state_date"
        )
    return _sort_interaction_frame(grouped)


def _build_state_bucket_summary_df(
    state_bucket_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(state_bucket_daily_means_df):
        grouped = (
            split_df.groupby(
                [
                    "horizon_key",
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                    "combined_bucket",
                    "combined_bucket_label",
                ],
                observed=True,
                as_index=False,
            )
            .agg(
                date_count=("date", "nunique"),
                mean_group_size=("group_sample_count", "mean"),
                mean_future_return=("group_mean_future_return", "mean"),
                median_future_return=("group_median_future_return", "median"),
                std_future_return=("group_mean_future_return", "std"),
                positive_date_count=(
                    "group_mean_future_return",
                    lambda values: int((values > 0).sum()),
                ),
                mean_positive_share=("positive_share", "mean"),
            )
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        grouped["hit_rate_positive"] = grouped["positive_date_count"] / grouped["date_count"]
        frames.append(grouped)

    if not frames:
        raise ValueError("Failed to build any state/bucket summary rows")
    return _sort_interaction_frame(pd.concat(frames, ignore_index=True))


def _build_state_bucket_pairwise_significance_df(
    state_bucket_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for split_name, split_df in _iter_split_frames(state_bucket_daily_means_df):
        for state_key in MULTI_TIMEFRAME_STATE_ORDER:
            state_df = split_df[split_df["state_key"].astype(str) == state_key].copy()
            if state_df.empty:
                continue
            state_label = str(state_df["state_label"].iloc[0])
            long_mode = str(state_df["long_mode"].iloc[0])
            short_mode = str(state_df["short_mode"].iloc[0])
            for horizon_key, horizon_df in state_df.groupby("horizon_key", observed=True):
                horizon_days = int(horizon_df["horizon_days"].iloc[0])
                for left_bucket, right_bucket in combinations(
                    Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                ):
                    pair_df = horizon_df[
                        horizon_df["combined_bucket"].isin((left_bucket, right_bucket))
                    ].copy()
                    if pair_df.empty:
                        pivot_df = pd.DataFrame()
                    else:
                        pivot_df = (
                            pair_df.pivot(
                                index="date",
                                columns="combined_bucket",
                                values="group_mean_future_return",
                            )
                            .reindex(columns=[left_bucket, right_bucket])
                            .dropna()
                        )
                    if pivot_df.empty:
                        records.append(
                            {
                                "sample_split": split_name,
                                "state_key": state_key,
                                "state_label": state_label,
                                "long_mode": long_mode,
                                "short_mode": short_mode,
                                "horizon_key": str(horizon_key),
                                "horizon_days": horizon_days,
                                "left_combined_bucket": left_bucket,
                                "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                                "right_combined_bucket": right_bucket,
                                "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                                "n_dates": 0,
                                "mean_difference": None,
                                "paired_t_statistic": None,
                                "paired_t_p_value": None,
                                "wilcoxon_statistic": None,
                                "wilcoxon_p_value": None,
                            }
                        )
                        continue

                    left = pivot_df[left_bucket].to_numpy(dtype=float)
                    right = pivot_df[right_bucket].to_numpy(dtype=float)
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                    records.append(
                        {
                            "sample_split": split_name,
                            "state_key": state_key,
                            "state_label": state_label,
                            "long_mode": long_mode,
                            "short_mode": short_mode,
                            "horizon_key": str(horizon_key),
                            "horizon_days": horizon_days,
                            "left_combined_bucket": left_bucket,
                            "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                            "right_combined_bucket": right_bucket,
                            "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": float((left - right).mean()),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )

    if not records:
        raise ValueError("Failed to build any state/bucket pairwise rows")
    pairwise_df = pd.DataFrame.from_records(records)
    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None

    for split_name in _SPLIT_ORDER:
        split_df = pairwise_df[pairwise_df["sample_split"] == split_name]
        if split_df.empty:
            continue
        for state_key in MULTI_TIMEFRAME_STATE_ORDER:
            state_df = split_df[split_df["state_key"].astype(str) == state_key]
            if state_df.empty:
                continue
            for horizon_key in sorted(state_df["horizon_key"].astype(str).unique()):
                mask = (
                    (pairwise_df["sample_split"] == split_name)
                    & (pairwise_df["state_key"].astype(str) == state_key)
                    & (pairwise_df["horizon_key"].astype(str) == horizon_key)
                )
                pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                    pairwise_df.loc[mask, "paired_t_p_value"].tolist()
                )
                pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                    pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
                )

    return _sort_interaction_frame(pairwise_df)


def _build_state_hypothesis_df(
    state_bucket_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    hypothesis_map = {
        label: (left_bucket, right_bucket)
        for left_bucket, right_bucket, label in Q10_LOW_HYPOTHESIS_LABELS
    }
    for split_name, split_df in _iter_split_frames(state_bucket_pairwise_significance_df):
        for state_key in MULTI_TIMEFRAME_STATE_ORDER:
            state_df = split_df[split_df["state_key"].astype(str) == state_key]
            if state_df.empty:
                continue
            state_label = str(state_df["state_label"].iloc[0])
            long_mode = str(state_df["long_mode"].iloc[0])
            short_mode = str(state_df["short_mode"].iloc[0])
            for horizon_key, horizon_df in state_df.groupby("horizon_key", observed=True):
                horizon_days = int(horizon_df["horizon_days"].iloc[0])
                for hypothesis_label, (left_bucket, right_bucket) in hypothesis_map.items():
                    row = horizon_df[
                        (horizon_df["left_combined_bucket"] == left_bucket)
                        & (horizon_df["right_combined_bucket"] == right_bucket)
                    ]
                    sign = 1.0
                    if row.empty:
                        row = horizon_df[
                            (horizon_df["left_combined_bucket"] == right_bucket)
                            & (horizon_df["right_combined_bucket"] == left_bucket)
                        ]
                        sign = -1.0
                    if row.empty:
                        records.append(
                            {
                                "sample_split": split_name,
                                "state_key": state_key,
                                "state_label": state_label,
                                "long_mode": long_mode,
                                "short_mode": short_mode,
                                "horizon_key": str(horizon_key),
                                "horizon_days": horizon_days,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": None,
                                "paired_t_p_value_holm": None,
                                "wilcoxon_p_value_holm": None,
                            }
                        )
                        continue
                    pairwise_row = row.iloc[0]
                    mean_difference = pairwise_row["mean_difference"]
                    records.append(
                        {
                            "sample_split": split_name,
                            "state_key": state_key,
                            "state_label": state_label,
                            "long_mode": long_mode,
                            "short_mode": short_mode,
                            "horizon_key": str(horizon_key),
                            "horizon_days": horizon_days,
                            "hypothesis_label": hypothesis_label,
                            "left_combined_bucket": left_bucket,
                            "right_combined_bucket": right_bucket,
                            "mean_difference": (
                                None
                                if mean_difference is None or pd.isna(mean_difference)
                                else sign * float(mean_difference)
                            ),
                            "paired_t_p_value_holm": pairwise_row["paired_t_p_value_holm"],
                            "wilcoxon_p_value_holm": pairwise_row["wilcoxon_p_value_holm"],
                        }
                    )

    if not records:
        raise ValueError("Failed to build any state-conditioned hypothesis rows")
    return _sort_interaction_frame(pd.DataFrame.from_records(records))


def _build_state_scorecard_df(
    state_bucket_summary_df: pd.DataFrame,
    state_hypothesis_df: pd.DataFrame,
) -> pd.DataFrame:
    validation_summary_df = state_bucket_summary_df.copy()
    validation_summary_df["bucket_rank"] = validation_summary_df.groupby(
        ["sample_split", "state_key", "horizon_key"],
        observed=True,
    )["mean_future_return"].rank(method="dense", ascending=False)

    q10_low_summary_df = validation_summary_df[
        validation_summary_df["combined_bucket"] == "q10_volume_low"
    ][
        [
            "sample_split",
            "state_key",
            "state_label",
            "long_mode",
            "short_mode",
            "horizon_key",
            "horizon_days",
            "date_count",
            "mean_future_return",
            "hit_rate_positive",
            "mean_positive_share",
            "bucket_rank",
        ]
    ].rename(
        columns={
            "date_count": "q10_low_date_count",
            "mean_future_return": "q10_low_mean_future_return",
            "hit_rate_positive": "q10_low_hit_rate_positive",
            "mean_positive_share": "q10_low_mean_positive_share",
            "bucket_rank": "q10_low_rank",
        }
    )

    strongest_bucket_df = validation_summary_df.sort_values(
        ["sample_split", "state_key", "horizon_key", "mean_future_return", "combined_bucket"],
        ascending=[True, True, True, False, True],
        kind="stable",
    ).drop_duplicates(
        subset=["sample_split", "state_key", "horizon_key"],
        keep="first",
    )[
        [
            "sample_split",
            "state_key",
            "horizon_key",
            "combined_bucket",
            "combined_bucket_label",
            "mean_future_return",
        ]
    ].rename(
        columns={
            "combined_bucket": "best_bucket",
            "combined_bucket_label": "best_bucket_label",
            "mean_future_return": "best_bucket_mean_future_return",
        }
    )

    scorecard_df = q10_low_summary_df.merge(
        strongest_bucket_df,
        on=["sample_split", "state_key", "horizon_key"],
        how="left",
        validate="one_to_one",
    )
    for hypothesis_label, column_name in (
        ("Q10 Low vs Q10 High", "q10_low_vs_q10_high"),
        ("Q10 Low vs Middle Low", "q10_low_vs_middle_low"),
        ("Q10 Low vs Middle High", "q10_low_vs_middle_high"),
    ):
        scoped_df = state_hypothesis_df[
            state_hypothesis_df["hypothesis_label"] == hypothesis_label
        ][
            [
                "sample_split",
                "state_key",
                "horizon_key",
                "mean_difference",
            ]
        ].rename(columns={"mean_difference": column_name})
        scorecard_df = scorecard_df.merge(
            scoped_df,
            on=["sample_split", "state_key", "horizon_key"],
            how="left",
            validate="one_to_one",
        )
    spread_columns = [
        "q10_low_vs_q10_high",
        "q10_low_vs_middle_low",
        "q10_low_vs_middle_high",
    ]
    scorecard_df["q10_low_spread_available_count"] = scorecard_df[spread_columns].notna().sum(
        axis=1
    )
    scorecard_df["q10_low_spread_positive_count"] = scorecard_df[spread_columns].gt(0).sum(
        axis=1
    )
    scorecard_df["q10_low_pairwise_edge_mean"] = scorecard_df[spread_columns].mean(
        axis=1, skipna=True
    )
    scorecard_df["q10_low_is_best_bucket"] = scorecard_df["q10_low_rank"].eq(1.0)
    return _sort_interaction_frame(scorecard_df)


def _build_state_execution_summary_df(scorecard_df: pd.DataFrame) -> pd.DataFrame:
    if scorecard_df.empty:
        return pd.DataFrame()

    summary_df = (
        scorecard_df.groupby(
            ["sample_split", "state_key", "state_label", "long_mode", "short_mode"],
            observed=True,
            as_index=False,
        )
        .agg(
            horizons_covered=("horizon_days", "nunique"),
            top_rank_horizon_count=("q10_low_is_best_bucket", "sum"),
            mean_q10_low_rank=("q10_low_rank", "mean"),
            mean_q10_low_future_return=("q10_low_mean_future_return", "mean"),
            mean_q10_low_pairwise_edge=("q10_low_pairwise_edge_mean", "mean"),
            mean_q10_low_date_count=("q10_low_date_count", "mean"),
            mean_q10_low_hit_rate_positive=("q10_low_hit_rate_positive", "mean"),
            mean_q10_low_positive_share=("q10_low_mean_positive_share", "mean"),
        )
    )
    return _sort_interaction_frame(summary_df)


def _build_validation_q10_state_matrix_df(
    state_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    scoped_df = state_bucket_summary_df[
        (state_bucket_summary_df["sample_split"] == "validation")
        & state_bucket_summary_df["combined_bucket"].isin(("q10_volume_low", "q10_volume_high"))
    ].copy()
    if scoped_df.empty:
        return pd.DataFrame()

    group_columns = [
        "combined_bucket",
        "combined_bucket_label",
        "state_key",
        "state_label",
        "long_mode",
        "short_mode",
    ]
    aggregated_df = (
        scoped_df.groupby(group_columns, observed=True, as_index=False)
        .agg(
            avg_future_return=("mean_future_return", "mean"),
            avg_hit_rate_positive=("hit_rate_positive", "mean"),
            avg_date_count=("date_count", "mean"),
            horizons_covered=("horizon_days", "nunique"),
        )
    )

    for horizon_days in (1, 5, 10):
        horizon_df = scoped_df[scoped_df["horizon_days"] == horizon_days].copy()
        if horizon_df.empty:
            continue
        renamed_df = horizon_df[group_columns + ["mean_future_return"]].rename(
            columns={"mean_future_return": f"future_return_{horizon_days}d"}
        )
        aggregated_df = aggregated_df.merge(
            renamed_df,
            on=group_columns,
            how="left",
            validate="one_to_one",
        )

    aggregated_df["bucket_rank"] = (
        aggregated_df.groupby("combined_bucket", observed=True)["avg_future_return"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    return _sort_interaction_frame(aggregated_df)


def _sort_interaction_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    if "sample_split" in sorted_df.columns:
        sorted_df["sample_split"] = pd.Categorical(
            sorted_df["sample_split"],
            categories=list(_SPLIT_ORDER),
            ordered=True,
        )
    if "state_key" in sorted_df.columns:
        sorted_df["state_key"] = pd.Categorical(
            sorted_df["state_key"].astype(str),
            categories=list(MULTI_TIMEFRAME_STATE_ORDER),
            ordered=True,
        )
    if "combined_bucket" in sorted_df.columns:
        sorted_df["combined_bucket"] = pd.Categorical(
            sorted_df["combined_bucket"].astype(str),
            categories=list(Q10_MIDDLE_COMBINED_BUCKET_ORDER),
            ordered=True,
        )
    sort_columns = [
        column
        for column in [
            "sample_split",
            "date",
            "horizon_days",
            "horizon_key",
            "state_key",
            "combined_bucket",
            "hypothesis_label",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if not sort_columns:
        return sorted_df.reset_index(drop=True)
    return sorted_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)


def _iter_split_frames(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", df.copy())]
    if "sample_split" not in df.columns:
        return split_frames
    for split_name in ("discovery", "validation"):
        split_df = df[df["sample_split"] == split_name].copy()
        if not split_df.empty:
            split_frames.append((split_name, split_df))
    return split_frames


def _build_research_bundle_summary_markdown(
    result: Topix100Q10BounceStreak353ConditioningResearchResult,
) -> str:
    validation_scorecard_df = result.state_scorecard_df[
        result.state_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_hypothesis_df = result.state_hypothesis_df[
        result.state_hypothesis_df["sample_split"] == "validation"
    ].copy()
    validation_state_summary_df = _build_state_execution_summary_df(
        validation_scorecard_df
    )
    validation_q10_state_matrix_df = result.validation_q10_state_matrix_df.copy()
    best_state_row = _select_best_execution_state_row(validation_state_summary_df)
    secondary_state_row = _select_secondary_execution_state_row(
        validation_state_summary_df,
        best_state_key=None if best_state_row is None else str(best_state_row["state_key"]),
    )
    avoid_state_row = _select_avoid_execution_state_row(validation_state_summary_df)

    lines = [
        "# TOPIX100 Q10 Bounce x Streak 3/53 Conditioning",
        "",
        "This study keeps both ingredients fixed and asks a narrower question: when does the existing `Q10 vs middle` bounce bucket become stronger or weaker once the stock is already in a particular streak 3/53 state?",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Price / volume lens: `{result.price_feature}` x `{result.volume_feature}`",
        f"- Fixed short / long pair: `{result.short_window_streaks} / {result.long_window_streaks}` streak candles",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Min names per date-state-bucket: `{result.min_constituents_per_bucket_state_date}`",
        f"- Universe constituents: `{result.universe_constituent_count}`",
        f"- Joined state-bucket events: `{result.joined_event_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        "",
        "## Validation Execution Read",
        "",
    ]

    if best_state_row is None:
        lines.append("- No validation execution-state rows were available.")
    else:
        lines.append(
            "- "
            f"Best execution state: `{best_state_row['state_label']}`. `Q10 Low` ranked `#1` in "
            f"`{int(best_state_row['top_rank_horizon_count'])}/{int(best_state_row['horizons_covered'])}` horizons, "
            f"averaged `{_format_return(float(best_state_row['mean_q10_low_future_return']))}`, and carried an "
            f"average pairwise edge of `{_format_return(float(best_state_row['mean_q10_low_pairwise_edge']))}`."
        )
    if secondary_state_row is not None:
        lines.append(
            "- "
            f"Secondary state: `{secondary_state_row['state_label']}`. `Q10 Low` still ranked `#1` in "
            f"`{int(secondary_state_row['top_rank_horizon_count'])}/{int(secondary_state_row['horizons_covered'])}` horizons, "
            f"but the average pairwise edge compressed to `{_format_return(float(secondary_state_row['mean_q10_low_pairwise_edge']))}`."
        )
    if avoid_state_row is not None:
        lines.append(
            "- "
            f"Avoid state: `{avoid_state_row['state_label']}`. `Q10 Low` averaged "
            f"`{_format_return(float(avoid_state_row['mean_q10_low_future_return']))}` with an average rank of "
            f"`{float(avoid_state_row['mean_q10_low_rank']):.1f}`."
        )

    lines.extend(
        [
            "",
        "## Validation Q10 Low Scorecard",
        "",
        ]
    )

    if validation_scorecard_df.empty:
        lines.append("- No validation scorecard rows were available.")
    else:
        for horizon_days in sorted(validation_scorecard_df["horizon_days"].unique()):
            horizon_df = validation_scorecard_df[
                validation_scorecard_df["horizon_days"] == horizon_days
            ].copy()
            horizon_df = horizon_df[horizon_df["q10_low_mean_future_return"].notna()].copy()
            if horizon_df.empty:
                continue
            best_row = horizon_df.sort_values(
                [
                    "q10_low_rank",
                    "q10_low_pairwise_edge_mean",
                    "q10_low_mean_future_return",
                    "state_key",
                ],
                ascending=[True, False, False, True],
                kind="stable",
            ).iloc[0]
            lines.append(
                "- "
                f"{int(horizon_days)}d best conditioned read: `{best_row['state_label']}` with "
                f"`Q10 Low rank #{int(best_row['q10_low_rank'])}` and "
                f"`Q10 Low {_format_return(float(best_row['q10_low_mean_future_return']))}` "
                f"(average pairwise edge `{_format_return(float(best_row['q10_low_pairwise_edge_mean']))}`)."
            )

    lines.extend(["", "## Validation Q10 State Matrix", ""])
    q10_low_order_line = _format_q10_bucket_order_line(
        validation_q10_state_matrix_df,
        combined_bucket="q10_volume_low",
    )
    q10_high_order_line = _format_q10_bucket_order_line(
        validation_q10_state_matrix_df,
        combined_bucket="q10_volume_high",
    )
    short_bearish_volume_line = _format_short_bearish_volume_split_line(
        validation_q10_state_matrix_df
    )
    short_bullish_caution_line = _format_short_bullish_caution_line(
        validation_q10_state_matrix_df
    )
    if all(
        line is None
        for line in (
            q10_low_order_line,
            q10_high_order_line,
            short_bearish_volume_line,
            short_bullish_caution_line,
        )
    ):
        lines.append("- No validation Q10 matrix rows were available.")
    else:
        for line in (
            q10_low_order_line,
            q10_high_order_line,
            short_bearish_volume_line,
            short_bullish_caution_line,
        ):
            if line is not None:
                lines.append(f"- {line}")

    lines.extend(["", "## Validation Directed Hypotheses", ""])
    if validation_hypothesis_df.empty:
        lines.append("- No validation hypothesis rows were available.")
    else:
        for hypothesis_label in (
            "Q10 Low vs Q10 High",
            "Q10 Low vs Middle Low",
            "Q10 Low vs Middle High",
        ):
            scoped_df = validation_hypothesis_df[
                validation_hypothesis_df["hypothesis_label"] == hypothesis_label
            ].copy()
            scoped_df = scoped_df[scoped_df["mean_difference"].notna()].copy()
            if scoped_df.empty:
                continue
            best_row = scoped_df.sort_values(
                ["mean_difference", "horizon_days", "state_key"],
                ascending=[False, True, True],
                kind="stable",
            ).iloc[0]
            lines.append(
                "- "
                f"{hypothesis_label}: strongest in `{best_row['state_label']}` at "
                f"`{int(best_row['horizon_days'])}d` with "
                f"`{_format_return(float(best_row['mean_difference']))}`."
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `state_bucket_horizon_panel_df`",
            "- `state_bucket_daily_means_df`",
            "- `state_bucket_summary_df`",
            "- `state_bucket_pairwise_significance_df`",
            "- `state_hypothesis_df`",
            "- `state_scorecard_df`",
            "- `validation_q10_state_matrix_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Q10BounceStreak353ConditioningResearchResult,
) -> dict[str, Any]:
    validation_scorecard_df = result.state_scorecard_df[
        result.state_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_hypothesis_df = result.state_hypothesis_df[
        result.state_hypothesis_df["sample_split"] == "validation"
    ].copy()
    validation_state_summary_df = _build_state_execution_summary_df(
        validation_scorecard_df
    )
    validation_q10_state_matrix_df = result.validation_q10_state_matrix_df.copy()

    strongest_scorecard_row = _select_best_scorecard_row(validation_scorecard_df)
    strongest_hypothesis_row = _select_best_hypothesis_row(validation_hypothesis_df)
    best_state_row = _select_best_execution_state_row(validation_state_summary_df)
    secondary_state_row = _select_secondary_execution_state_row(
        validation_state_summary_df,
        best_state_key=None if best_state_row is None else str(best_state_row["state_key"]),
    )
    avoid_state_row = _select_avoid_execution_state_row(validation_state_summary_df)

    result_bullets = [
        "This is a true fusion study, but not a parameter search. The bucket side is fixed to the existing `price_vs_sma_50_gap x volume_sma_5_20` Q10 bounce lens, and the state side is fixed to streak `3 / 53`.",
        f"The `state x bucket` intersection is much sparser than the original bucket study, so the practical floor is `min names = {result.min_constituents_per_bucket_state_date}`. The old `5-name` cutoff erased `q10_volume_low` from validation altogether.",
    ]
    highlights = [
        {
            "label": "Bucket lens",
            "value": "Q10 / middle",
            "tone": "accent",
            "detail": f"{result.price_feature} x {result.volume_feature}",
        },
        {
            "label": "Fixed state pair",
            "value": f"{result.short_window_streaks} / {result.long_window_streaks}",
            "tone": "neutral",
            "detail": "streak candles",
        },
        {
            "label": "Min names",
            "value": str(result.min_constituents_per_bucket_state_date),
            "tone": "neutral",
            "detail": "per date-state-bucket",
        },
    ]

    if best_state_row is not None:
        result_bullets.append(
            f"The clearest execution state was `{best_state_row['state_label']}`: `Q10 Low` ranked `#1` in "
            f"{int(best_state_row['top_rank_horizon_count'])}/{int(best_state_row['horizons_covered'])} horizons, "
            f"averaged {_format_return(float(best_state_row['mean_q10_low_future_return']))}, and carried a mean pairwise edge of "
            f"{_format_return(float(best_state_row['mean_q10_low_pairwise_edge']))}."
        )
        highlights.append(
            {
                "label": "Best execution state",
                "value": str(best_state_row["state_label"]),
                "tone": "success",
                "detail": f"avg edge {_format_return(float(best_state_row['mean_q10_low_pairwise_edge']))}",
            }
        )
    if secondary_state_row is not None:
        result_bullets.append(
            f"`{secondary_state_row['state_label']}` was the usable secondary state. `Q10 Low` still stayed on top, but the average edge shrank to "
            f"{_format_return(float(secondary_state_row['mean_q10_low_pairwise_edge']))}, so this reads as a weaker follow-up setup rather than the main entry state."
        )
    if avoid_state_row is not None:
        result_bullets.append(
            f"`{avoid_state_row['state_label']}` was the clear avoid state. `Q10 Low` averaged "
            f"{_format_return(float(avoid_state_row['mean_q10_low_future_return']))} with a mean rank of "
            f"{float(avoid_state_row['mean_q10_low_rank']):.1f}, so the bounce bucket was structurally weak there."
        )
        highlights.append(
            {
                "label": "Avoid state",
                "value": str(avoid_state_row["state_label"]),
                "tone": "warning",
                "detail": f"avg rank {float(avoid_state_row['mean_q10_low_rank']):.1f}",
            }
        )

    if strongest_scorecard_row is not None:
        result_bullets.append(
            f"The single best state-horizon cell was {strongest_scorecard_row['state_label']} at {int(strongest_scorecard_row['horizon_days'])}d, where `Q10 Low` averaged "
            f"{_format_return(float(strongest_scorecard_row['q10_low_mean_future_return']))} and its mean pairwise edge reached "
            f"{_format_return(float(strongest_scorecard_row['q10_low_pairwise_edge_mean']))}."
        )
        result_bullets.append(
            f"In that same state, the winning bucket was `{strongest_scorecard_row['best_bucket_label']}` rather than treating every Q10 bounce entry as equally attractive."
        )
    if strongest_hypothesis_row is not None:
        result_bullets.append(
            f"The directed edge stayed most visible in `{strongest_hypothesis_row['hypothesis_label']}`, strongest under {strongest_hypothesis_row['state_label']} at {int(strongest_hypothesis_row['horizon_days'])}d {_format_return(float(strongest_hypothesis_row['mean_difference']))}."
        )
        highlights.append(
            {
                "label": "Strongest directed spread",
                "value": str(strongest_hypothesis_row["hypothesis_label"]),
                "tone": "success",
                "detail": _format_return(float(strongest_hypothesis_row["mean_difference"])),
            }
        )

    q10_low_order_line = _format_q10_bucket_order_line(
        validation_q10_state_matrix_df,
        combined_bucket="q10_volume_low",
    )
    q10_high_order_line = _format_q10_bucket_order_line(
        validation_q10_state_matrix_df,
        combined_bucket="q10_volume_high",
    )
    if q10_low_order_line is not None:
        result_bullets.append(q10_low_order_line)
    if q10_high_order_line is not None:
        result_bullets.append(q10_high_order_line)

    short_bearish_volume_line = _format_short_bearish_volume_split_line(
        validation_q10_state_matrix_df
    )
    if short_bearish_volume_line is not None:
        result_bullets.append(short_bearish_volume_line)

    short_bullish_caution_line = _format_short_bullish_caution_line(
        validation_q10_state_matrix_df
    )
    if short_bullish_caution_line is not None:
        result_bullets.append(short_bullish_caution_line)

    headline = (
        "The fusion works as an execution filter, not as a broad overlay: `Q10 Low` is strongest in `Long Bearish / Short Bearish`, still usable in `Long Bullish / Short Bearish`, and clearly weak in `Long Bullish / Short Bullish`."
    )

    return {
        "title": "TOPIX100 Q10 Bounce x Streak 3/53 Conditioning",
        "tags": ["TOPIX100", "bucket", "streaks", "mean-reversion"],
        "purpose": (
            "Fuse the existing TOPIX100 Q10 bounce bucket research with the fixed streak 3/53 state model, then test whether the bucket edge gets meaningfully stronger or weaker inside particular states."
        ),
        "method": [
            "Reuse the existing TOPIX100 `Q10 vs middle` bucket lens on `price_vs_sma_50_gap x volume_sma_5_20` without changing the bucket definition.",
            "Reuse the fixed TOPIX-learned streak `3 / 53` state transfer without re-optimizing windows on TOPIX100.",
            "Join both on the same stock-date-horizon rows, then compare `q10_volume_low` against the other three buckets within each streak state using date-balanced means and paired tests.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This fusion is useful exactly because the roles are separate: `state` decides when the bounce setup is worth trusting, and `bucket` decides which names to prefer inside that state.",
            "The key discriminator is the short streak mode. Both `short bearish` states keep the bounce setup alive, while `short bullish` either weakens it materially or inverts it.",
            "Inside the Q10 bucket itself, `volume low` is the better expression of the bounce in the two `short bearish` states. `Q10 x Volume High` stays positive there, but it is consistently weaker than `Q10 x Volume Low`.",
            "Be careful with apparently strong cells that come from tiny support. In validation, `Long Bullish / Short Bullish` inside `Q10 x Volume High` only had about two usable dates, so it should not be treated as a real edge.",
            "Even with the lower `min names = 3` threshold, this is still a sparse corner of the universe. Read it as a directional execution filter first, then verify it with a constrained backtest on the strongest state-bucket combinations.",
        ],
        "selectedParameters": [
            {"label": "Price feature", "value": result.price_feature},
            {"label": "Volume feature", "value": result.volume_feature},
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {
                "label": "Min names / date-state-bucket",
                "value": str(result.min_constituents_per_bucket_state_date),
            },
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "state_scorecard_df",
                "label": "State-conditioned Q10 scorecard",
                "description": "Best state for `q10_volume_low` and its spread against the other buckets.",
            },
            {
                "name": "validation_q10_state_matrix_df",
                "label": "Validation Q10 state matrix",
                "description": "Q10 Volume Low / High broken down by long-short streak state with 1/5/10d reads.",
            },
            {
                "name": "state_hypothesis_df",
                "label": "Directed hypothesis table",
                "description": "Q10 Low vs Q10 High / Middle Low / Middle High inside each state.",
            },
            {
                "name": "state_bucket_summary_df",
                "label": "State x bucket summary",
                "description": "Date-balanced bucket means for every streak state.",
            },
        ],
}


def _select_q10_matrix_row(
    matrix_df: pd.DataFrame,
    *,
    combined_bucket: str,
    state_key: str,
) -> pd.Series | None:
    if matrix_df.empty:
        return None
    scoped_df = matrix_df[
        (matrix_df["combined_bucket"].astype(str) == combined_bucket)
        & (matrix_df["state_key"].astype(str) == state_key)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _format_q10_bucket_order_line(
    matrix_df: pd.DataFrame,
    *,
    combined_bucket: str,
) -> str | None:
    scoped_df = matrix_df[matrix_df["combined_bucket"].astype(str) == combined_bucket].copy()
    if scoped_df.empty:
        return None
    ordered_df = scoped_df.sort_values(
        ["bucket_rank", "avg_future_return", "state_key"],
        ascending=[True, False, True],
        kind="stable",
    )
    bucket_label = str(ordered_df["combined_bucket_label"].iloc[0])
    fragments = [
        f"`{row['state_label']}` {_format_return(float(row['avg_future_return']))}"
        for _, row in ordered_df.iterrows()
    ]
    return f"Within `{bucket_label}`, the state ordering was {' > '.join(fragments)}."


def _format_short_bearish_volume_split_line(matrix_df: pd.DataFrame) -> str | None:
    long_bearish_low = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_low",
        state_key="long_bearish__short_bearish",
    )
    long_bearish_high = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_high",
        state_key="long_bearish__short_bearish",
    )
    long_bullish_low = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_low",
        state_key="long_bullish__short_bearish",
    )
    long_bullish_high = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_high",
        state_key="long_bullish__short_bearish",
    )
    if any(
        row is None
        for row in (
            long_bearish_low,
            long_bearish_high,
            long_bullish_low,
            long_bullish_high,
        )
    ):
        return None
    return (
        "`short bearish` was also where the volume split mattered most: under "
        f"`Long Bearish / Short Bearish`, `Q10 x Volume Low` averaged "
        f"{_format_return(float(long_bearish_low['avg_future_return']))} versus "
        f"`Q10 x Volume High` {_format_return(float(long_bearish_high['avg_future_return']))}; "
        f"under `Long Bullish / Short Bearish`, the same comparison was "
        f"{_format_return(float(long_bullish_low['avg_future_return']))} versus "
        f"{_format_return(float(long_bullish_high['avg_future_return']))}."
    )


def _format_short_bullish_caution_line(matrix_df: pd.DataFrame) -> str | None:
    long_bearish_low = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_low",
        state_key="long_bearish__short_bullish",
    )
    long_bearish_high = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_high",
        state_key="long_bearish__short_bullish",
    )
    long_bullish_low = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_low",
        state_key="long_bullish__short_bullish",
    )
    long_bullish_high = _select_q10_matrix_row(
        matrix_df,
        combined_bucket="q10_volume_high",
        state_key="long_bullish__short_bullish",
    )
    if any(
        row is None
        for row in (
            long_bearish_low,
            long_bearish_high,
            long_bullish_low,
            long_bullish_high,
        )
    ):
        return None
    return (
        "`short bullish` weakened the whole setup. `Long Bearish / Short Bullish` stayed negative at 1d/5d in both Q10 volume buckets and only recovered by 10d in `Q10 x Volume Low`, while `Long Bullish / Short Bullish` was clearly poor in `Q10 x Volume Low` "
        f"({_format_return(float(long_bullish_low['avg_future_return']))} average). `Q10 x Volume High` looked better there, but it only carried about "
        f"{float(long_bullish_high['avg_date_count']):.0f} validation dates, so it is not reliable."
    )


def _select_best_scorecard_row(scorecard_df: pd.DataFrame) -> pd.Series | None:
    if scorecard_df.empty:
        return None
    scorecard_df = scorecard_df[scorecard_df["q10_low_mean_future_return"].notna()].copy()
    if scorecard_df.empty:
        return None
    sorted_df = scorecard_df.sort_values(
        [
            "q10_low_rank",
            "q10_low_pairwise_edge_mean",
            "q10_low_mean_future_return",
            "horizon_days",
            "state_key",
        ],
        ascending=[True, False, False, True, True],
        kind="stable",
    )
    return sorted_df.iloc[0]


def _select_best_hypothesis_row(hypothesis_df: pd.DataFrame) -> pd.Series | None:
    if hypothesis_df.empty:
        return None
    hypothesis_df = hypothesis_df[hypothesis_df["mean_difference"].notna()].copy()
    if hypothesis_df.empty:
        return None
    sorted_df = hypothesis_df.sort_values(
        ["mean_difference", "horizon_days", "state_key", "hypothesis_label"],
        ascending=[False, True, True, True],
        kind="stable",
    )
    return sorted_df.iloc[0]


def _select_best_execution_state_row(state_summary_df: pd.DataFrame) -> pd.Series | None:
    if state_summary_df.empty:
        return None
    sorted_df = state_summary_df.sort_values(
        [
            "top_rank_horizon_count",
            "mean_q10_low_rank",
            "mean_q10_low_pairwise_edge",
            "mean_q10_low_future_return",
            "state_key",
        ],
        ascending=[False, True, False, False, True],
        kind="stable",
    )
    return sorted_df.iloc[0]


def _select_secondary_execution_state_row(
    state_summary_df: pd.DataFrame,
    *,
    best_state_key: str | None,
) -> pd.Series | None:
    if state_summary_df.empty:
        return None
    scoped_df = state_summary_df.copy()
    if best_state_key is not None:
        scoped_df = scoped_df[scoped_df["state_key"].astype(str) != best_state_key].copy()
    if scoped_df.empty:
        return None
    sorted_df = scoped_df.sort_values(
        [
            "top_rank_horizon_count",
            "mean_q10_low_rank",
            "mean_q10_low_pairwise_edge",
            "mean_q10_low_future_return",
            "state_key",
        ],
        ascending=[False, True, False, False, True],
        kind="stable",
    )
    return sorted_df.iloc[0]


def _select_avoid_execution_state_row(state_summary_df: pd.DataFrame) -> pd.Series | None:
    if state_summary_df.empty:
        return None
    sorted_df = state_summary_df.sort_values(
        [
            "mean_q10_low_rank",
            "top_rank_horizon_count",
            "mean_q10_low_future_return",
            "mean_q10_low_pairwise_edge",
            "state_key",
        ],
        ascending=[False, True, True, True, True],
        kind="stable",
    )
    return sorted_df.iloc[0]
