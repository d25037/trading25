"""
Walk-forward validation for TOPIX100 streak 3/53 stage-2 LightGBM scores.

This extends the fixed-split stage-2 research by repeating the same baseline
vs LightGBM comparison over rolling train/test windows.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_strongest_setup_q10_threshold import (
    _build_state_decile_horizon_panel,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    DEFAULT_LONG_TARGET_HORIZON_DAYS,
    DEFAULT_SHORT_TARGET_HORIZON_DAYS,
    DEFAULT_TOP_K_VALUES,
    _build_baseline_lookup_df,
    _build_baseline_scorecard,
    _build_baseline_validation_prediction_df,
    _build_feature_panel_df,
    _build_lightgbm_validation_prediction_df,
    _build_validation_model_comparison_df,
    _build_validation_model_summary_df,
    _build_validation_score_decile_df,
    _build_validation_topk_tables,
    _format_int_sequence,
    _format_return,
    _load_lightgbm_regressor_cls,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.backtest.core.walkforward import generate_walkforward_splits

DEFAULT_WALKFORWARD_TRAIN_WINDOW = 756
DEFAULT_WALKFORWARD_TEST_WINDOW = 126
DEFAULT_WALKFORWARD_STEP = 126
TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-signal-score-lightgbm-walkforward"
)
_SIDE_ORDER: tuple[str, ...] = ("long", "short")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "split_config_df",
    "walkforward_topk_pick_df",
    "walkforward_topk_daily_df",
    "walkforward_split_summary_df",
    "walkforward_split_comparison_df",
    "walkforward_model_summary_df",
    "walkforward_model_comparison_df",
    "walkforward_score_decile_df",
    "walkforward_feature_importance_split_df",
    "walkforward_feature_importance_df",
)


@dataclass(frozen=True)
class Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult:
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
    long_target_horizon_days: int
    short_target_horizon_days: int
    top_k_values: tuple[int, ...]
    train_window: int
    test_window: int
    step: int
    split_count: int
    categorical_feature_columns: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    split_config_df: pd.DataFrame
    walkforward_topk_pick_df: pd.DataFrame
    walkforward_topk_daily_df: pd.DataFrame
    walkforward_split_summary_df: pd.DataFrame
    walkforward_split_comparison_df: pd.DataFrame
    walkforward_model_summary_df: pd.DataFrame
    walkforward_model_comparison_df: pd.DataFrame
    walkforward_score_decile_df: pd.DataFrame
    walkforward_feature_importance_split_df: pd.DataFrame
    walkforward_feature_importance_df: pd.DataFrame


def run_topix100_streak_353_signal_score_lightgbm_walkforward_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    long_target_horizon_days: int = DEFAULT_LONG_TARGET_HORIZON_DAYS,
    short_target_horizon_days: int = DEFAULT_SHORT_TARGET_HORIZON_DAYS,
    top_k_values: tuple[int, ...] | list[int] | None = None,
    train_window: int = DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    test_window: int = DEFAULT_WALKFORWARD_TEST_WINDOW,
    step: int = DEFAULT_WALKFORWARD_STEP,
) -> Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")

    resolved_top_k_values = _normalize_positive_int_sequence(
        top_k_values,
        default=DEFAULT_TOP_K_VALUES,
        name="top_k_values",
    )
    requested_horizons = tuple(
        sorted({int(long_target_horizon_days), int(short_target_horizon_days)})
    )
    price_feature_to_window = {
        feature: window
        for feature, window in zip(PRICE_FEATURE_ORDER, PRICE_SMA_WINDOW_ORDER, strict=True)
    }
    volume_feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }

    price_result = run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        price_sma_windows=(price_feature_to_window[price_feature],),
        volume_sma_windows=(volume_feature_to_window[volume_feature],),
    )
    state_result = run_topix100_streak_353_transfer_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        future_horizons=requested_horizons,
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )
    state_decile_horizon_panel_df = _build_state_decile_horizon_panel(
        event_panel_df=price_result.event_panel_df,
        state_horizon_event_df=state_result.state_horizon_event_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
        future_horizons=requested_horizons,
    )
    feature_panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature=price_feature,
        volume_feature=volume_feature,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )

    return _run_walkforward_from_panels(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=price_result.available_start_date,
        available_end_date=price_result.available_end_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
        top_k_values=resolved_top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
    )


def _run_walkforward_from_panels(
    *,
    db_path: str,
    source_mode: SourceMode,
    source_detail: str,
    available_start_date: str | None,
    available_end_date: str | None,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    validation_ratio: float,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
    top_k_values: tuple[int, ...],
    train_window: int,
    test_window: int,
    step: int,
    state_decile_horizon_panel_df: pd.DataFrame,
    feature_panel_df: pd.DataFrame,
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
) -> Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult:
    unique_dates = pd.to_datetime(sorted(feature_panel_df["date"].astype(str).unique()))
    splits = generate_walkforward_splits(
        unique_dates,
        train_window=train_window,
        test_window=test_window,
        step=step,
    )
    if not splits:
        raise ValueError("No walk-forward splits were generated for the selected panel")

    regressor_cls = _load_lightgbm_regressor_cls()
    split_config_records: list[dict[str, Any]] = []
    topk_pick_frames: list[pd.DataFrame] = []
    topk_daily_frames: list[pd.DataFrame] = []
    split_summary_frames: list[pd.DataFrame] = []
    split_comparison_frames: list[pd.DataFrame] = []
    feature_importance_frames: list[pd.DataFrame] = []
    prediction_frames: list[pd.DataFrame] = []
    requested_horizons = tuple(sorted({short_target_horizon_days, long_target_horizon_days}))

    for split_index, split in enumerate(splits, start=1):
        train_feature_df = _slice_by_date_range(
            feature_panel_df,
            start_date=split.train_start,
            end_date=split.train_end,
        )
        test_feature_df = _slice_by_date_range(
            feature_panel_df,
            start_date=split.test_start,
            end_date=split.test_end,
        )
        train_state_df = _slice_by_date_range(
            state_decile_horizon_panel_df,
            start_date=split.train_start,
            end_date=split.train_end,
        )
        if train_feature_df.empty or test_feature_df.empty or train_state_df.empty:
            continue

        split_config_records.append(
            {
                "split_index": split_index,
                "train_start": split.train_start,
                "train_end": split.train_end,
                "test_start": split.test_start,
                "test_end": split.test_end,
                "train_row_count": int(len(train_feature_df)),
                "test_row_count": int(len(test_feature_df)),
                "train_date_count": int(train_feature_df["date"].nunique()),
                "test_date_count": int(test_feature_df["date"].nunique()),
            }
        )

        split_train_state_df = train_state_df.copy()
        split_train_state_df["sample_split"] = "discovery"
        baseline_lookup_df = _build_baseline_lookup_df(
            split_train_state_df,
            future_horizons=requested_horizons,
        )
        baseline_scorecard = _build_baseline_scorecard(baseline_lookup_df)

        split_feature_df = pd.concat(
            [
                train_feature_df.assign(sample_split="discovery"),
                test_feature_df.assign(sample_split="validation"),
            ],
            ignore_index=True,
        )
        split_prediction_frames = [
            _build_baseline_validation_prediction_df(
                split_feature_df,
                baseline_scorecard=baseline_scorecard,
                long_target_horizon_days=long_target_horizon_days,
                short_target_horizon_days=short_target_horizon_days,
            )
        ]
        split_importance_frames: list[pd.DataFrame] = []
        for side in _SIDE_ORDER:
            prediction_df, _config_record, importance_df = (
                _build_lightgbm_validation_prediction_df(
                    split_feature_df,
                    side=side,
                    regressor_cls=regressor_cls,
                    categorical_feature_columns=categorical_feature_columns,
                    continuous_feature_columns=continuous_feature_columns,
                    long_target_horizon_days=long_target_horizon_days,
                    short_target_horizon_days=short_target_horizon_days,
                )
            )
            split_prediction_frames.append(prediction_df)
            split_importance_frames.append(
                importance_df.assign(
                    split_index=split_index,
                    train_start=split.train_start,
                    train_end=split.train_end,
                    test_start=split.test_start,
                    test_end=split.test_end,
                )
            )

        split_prediction_df = pd.concat(split_prediction_frames, ignore_index=True).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        prediction_frames.append(split_prediction_df)

        split_topk_pick_df, split_topk_daily_df = _build_validation_topk_tables(
            split_prediction_df,
            top_k_values=top_k_values,
        )
        split_topk_pick_df = split_topk_pick_df.assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        split_topk_daily_df = split_topk_daily_df.assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        topk_pick_frames.append(split_topk_pick_df)
        topk_daily_frames.append(split_topk_daily_df)
        split_summary_df = _build_validation_model_summary_df(split_topk_daily_df).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        split_summary_frames.append(split_summary_df)
        split_comparison_df = _build_validation_model_comparison_df(split_summary_df).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        split_comparison_frames.append(split_comparison_df)
        feature_importance_frames.extend(split_importance_frames)

    if not topk_daily_frames:
        raise ValueError("Walk-forward evaluation produced no valid splits")

    split_config_df = pd.DataFrame.from_records(split_config_records)
    walkforward_topk_pick_df = pd.concat(topk_pick_frames, ignore_index=True)
    walkforward_topk_daily_df = pd.concat(topk_daily_frames, ignore_index=True)
    walkforward_split_summary_df = pd.concat(split_summary_frames, ignore_index=True)
    walkforward_split_comparison_df = pd.concat(split_comparison_frames, ignore_index=True)
    walkforward_model_summary_df = _build_validation_model_summary_df(walkforward_topk_daily_df)
    walkforward_model_comparison_df = _build_validation_model_comparison_df(
        walkforward_model_summary_df
    )
    walkforward_score_decile_df = _build_validation_score_decile_df(
        pd.concat(prediction_frames, ignore_index=True)
    )
    walkforward_feature_importance_split_df = pd.concat(
        feature_importance_frames,
        ignore_index=True,
    )
    walkforward_feature_importance_df = (
        walkforward_feature_importance_split_df.groupby(
            ["side", "model_name", "feature_name"],
            observed=True,
            sort=False,
        )
        .agg(
            mean_importance_gain=("importance_gain", "mean"),
            mean_importance_share=("importance_share", "mean"),
            split_count=("split_index", "nunique"),
        )
        .reset_index()
        .sort_values(
            ["side", "mean_importance_gain", "feature_name"],
            ascending=[True, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    walkforward_feature_importance_df["importance_rank"] = (
        walkforward_feature_importance_df.groupby("side", observed=True).cumcount() + 1
    )

    return Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=str(feature_panel_df["date"].min()),
        analysis_end_date=str(feature_panel_df["date"].max()),
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
        top_k_values=top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        split_count=int(split_config_df["split_index"].nunique()),
        categorical_feature_columns=categorical_feature_columns,
        continuous_feature_columns=continuous_feature_columns,
        split_config_df=split_config_df,
        walkforward_topk_pick_df=walkforward_topk_pick_df,
        walkforward_topk_daily_df=walkforward_topk_daily_df,
        walkforward_split_summary_df=walkforward_split_summary_df,
        walkforward_split_comparison_df=walkforward_split_comparison_df,
        walkforward_model_summary_df=walkforward_model_summary_df,
        walkforward_model_comparison_df=walkforward_model_comparison_df,
        walkforward_score_decile_df=walkforward_score_decile_df,
        walkforward_feature_importance_split_df=walkforward_feature_importance_split_df,
        walkforward_feature_importance_df=walkforward_feature_importance_df,
    )


def write_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle(
    result: Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_signal_score_lightgbm_walkforward_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "long_target_horizon_days": result.long_target_horizon_days,
            "short_target_horizon_days": result.short_target_horizon_days,
            "top_k_values": list(result.top_k_values),
            "train_window": result.train_window,
            "test_window": result.test_window,
            "step": result.step,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_signal_score_lightgbm_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_signal_score_lightgbm_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _slice_by_date_range(
    df: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    long_overall = _select_comparison_row(
        result.walkforward_model_comparison_df, side="long", top_k=primary_top_k
    )
    short_overall = _select_comparison_row(
        result.walkforward_model_comparison_df, side="short", top_k=primary_top_k
    )
    long_win = _count_split_wins(
        result.walkforward_split_comparison_df, side="long", top_k=primary_top_k
    )
    short_win = _count_split_wins(
        result.walkforward_split_comparison_df, side="short", top_k=primary_top_k
    )

    lines = [
        "# TOPIX100 Streak 3/53 Signal Score LightGBM Walk-Forward",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Walk-forward windows: `train {result.train_window} / test {result.test_window} / step {result.step}`",
        f"- Split count: `{result.split_count}`",
        f"- Targets: `long {result.long_target_horizon_days}d`, `short {result.short_target_horizon_days}d`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        "",
        "## Current Read",
        "",
        "This is the overfitting check for the stage-2 score. Every split rebuilds the baseline from the train window only, retrains LightGBM on that same window, and then evaluates both only on the next out-of-sample block.",
    ]
    if long_overall is not None:
        lines.append(
            f"- Long Top {primary_top_k}: baseline `{_format_return(float(long_overall['baseline_avg_selected_edge']))}`, LightGBM `{_format_return(float(long_overall['lightgbm_avg_selected_edge']))}`, lift `{_format_return(float(long_overall['edge_lift_vs_baseline']))}`, split wins `{long_win}`."
        )
    if short_overall is not None:
        lines.append(
            f"- Short Top {primary_top_k}: baseline `{_format_return(float(short_overall['baseline_avg_selected_edge']))}`, LightGBM `{_format_return(float(short_overall['lightgbm_avg_selected_edge']))}`, lift `{_format_return(float(short_overall['edge_lift_vs_baseline']))}`, split wins `{short_win}`."
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353SignalScoreLightgbmWalkforwardResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    long_overall = _select_comparison_row(
        result.walkforward_model_comparison_df, side="long", top_k=primary_top_k
    )
    short_overall = _select_comparison_row(
        result.walkforward_model_comparison_df, side="short", top_k=primary_top_k
    )
    long_win = _count_split_wins(
        result.walkforward_split_comparison_df, side="long", top_k=primary_top_k
    )
    short_win = _count_split_wins(
        result.walkforward_split_comparison_df, side="short", top_k=primary_top_k
    )
    best_long_feature = _select_top_feature(result.walkforward_feature_importance_df, "long")
    best_short_feature = _select_top_feature(result.walkforward_feature_importance_df, "short")

    headline = (
        "Walk-forward validation tests whether the stage-2 LightGBM score keeps its edge once the model is repeatedly retrained and pushed into the next out-of-sample block."
    )
    bullets = [
        "Each split rebuilds the stage-1 baseline lookup from the train window only, so the baseline stays leakage-free inside the walk-forward loop.",
        "LightGBM is retrained separately for each split on the same train window, then both models are judged only on the next test block.",
    ]
    if long_overall is not None:
        bullets.append(
            f"Across all out-of-sample blocks, long Top {primary_top_k} was `{_format_return(float(long_overall['baseline_avg_selected_edge']))}` for baseline versus `{_format_return(float(long_overall['lightgbm_avg_selected_edge']))}` for LightGBM. LightGBM won `{long_win}`."
        )
    if short_overall is not None:
        bullets.append(
            f"Across all out-of-sample blocks, short Top {primary_top_k} was `{_format_return(float(short_overall['baseline_avg_selected_edge']))}` for baseline versus `{_format_return(float(short_overall['lightgbm_avg_selected_edge']))}` for LightGBM. LightGBM won `{short_win}`."
        )
    if best_long_feature is not None:
        bullets.append(
            f"Average long-side feature importance is still led by `{best_long_feature['feature_name']}` at `{float(best_long_feature['mean_importance_share']):.2%}`."
        )
    if best_short_feature is not None:
        bullets.append(
            f"Average short-side feature importance is still led by `{best_short_feature['feature_name']}` at `{float(best_short_feature['mean_importance_share']):.2%}`."
        )

    return {
        "title": "TOPIX100 Streak 3/53 Signal Score LightGBM Walk-Forward",
        "tags": ["TOPIX100", "streaks", "lightgbm", "walk-forward"],
        "purpose": (
            "Check whether the stage-2 LightGBM score still beats the stage-1 lookup when both are re-estimated in a rolling walk-forward loop."
        ),
        "method": [
            "Build the same TOPIX100 streak 3 / 53 feature panel used in the fixed-split stage-2 study.",
            "Generate rolling train/test windows, rebuild the baseline and retrain LightGBM inside each train window, then score only the following test block.",
            "Aggregate out-of-sample top-k results across all splits and count how often LightGBM actually beats the baseline.",
        ],
        "resultHeadline": headline,
        "resultBullets": bullets,
        "considerations": [
            "This is much closer to a production check than the fixed split, but it still ignores fees, borrow cost, and turnover control.",
            "Short 1d remains the noisiest target. Even if walk-forward stays positive, it should still be treated as the more fragile side.",
            "The split windows are fixed hyperparameters. Different train/test lengths can change the result, so this should be read as one disciplined walk-forward setting, not the final word.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {"label": "Train/Test", "value": f"{result.train_window}/{result.test_window}"},
            {"label": "Step", "value": str(result.step)},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
            {"label": "Split count", "value": str(result.split_count)},
        ],
        "highlights": [
            {
                "label": "Split count",
                "value": str(result.split_count),
                "tone": "accent",
                "detail": "walk-forward blocks",
            },
            {
                "label": "Primary Top-K",
                "value": str(primary_top_k),
                "tone": "neutral",
                "detail": "out-of-sample",
            },
        ],
        "tableHighlights": [
            {
                "name": "walkforward_model_comparison_df",
                "label": "Overall walk-forward lift",
                "description": "Aggregated out-of-sample lift of LightGBM versus the baseline over all walk-forward test blocks.",
            },
            {
                "name": "walkforward_split_comparison_df",
                "label": "Per-split comparison",
                "description": "One row per split/side/top-k showing whether LightGBM beat the baseline in that out-of-sample block.",
            },
            {
                "name": "walkforward_feature_importance_df",
                "label": "Average feature importance",
                "description": "Mean LightGBM gain importance across all walk-forward splits.",
            },
        ],
    }


def _resolve_primary_top_k(top_k_values: tuple[int, ...]) -> int:
    if 10 in top_k_values:
        return 10
    return int(top_k_values[0])


def _select_comparison_row(
    comparison_df: pd.DataFrame,
    *,
    side: str,
    top_k: int,
) -> pd.Series | None:
    scoped_df = comparison_df[
        (comparison_df["side"] == side) & (comparison_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _count_split_wins(
    split_comparison_df: pd.DataFrame,
    *,
    side: str,
    top_k: int,
) -> str:
    scoped_df = split_comparison_df[
        (split_comparison_df["side"] == side) & (split_comparison_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return "0/0"
    wins = int((scoped_df["edge_lift_vs_baseline"] > 0).sum())
    return f"{wins}/{len(scoped_df)}"


def _select_top_feature(
    feature_df: pd.DataFrame,
    side: str,
) -> pd.Series | None:
    scoped_df = feature_df[feature_df["side"] == side].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["mean_importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    return scoped_df.iloc[0]
