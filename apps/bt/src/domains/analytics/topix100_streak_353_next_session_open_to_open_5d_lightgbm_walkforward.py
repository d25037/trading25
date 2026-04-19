"""
Walk-forward validation for TOPIX100 streak 3/53 next-session open-to-open 5D LightGBM.

This defines a leak-free swing study:

- features are built using information available up to signal date X
- entry is at X+1 open
- exit is at X+6 open
- evaluation is long-only, with primary KPI vs TOPIX and secondary KPI vs the
  equal-weight TOPIX100 universe
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
from src.domains.analytics.topix100_streak_lightgbm_validation_support import (
    DEFAULT_TOPIX100_STREAK_LIGHTGBM_TOP_K_VALUES as DEFAULT_TOP_K_VALUES,
    build_topix100_streak_baseline_selector_value_key as _build_baseline_selector_value_key,
    build_topix100_streak_validation_score_decile_df as _build_validation_score_decile_df,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_open_5d_lightgbm import (
    TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
    _build_feature_panel_from_state_event_df,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    _build_category_lookup,
    _build_lightgbm_params,
    _build_model_matrix,
    _load_lightgbm_regressor_cls,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    build_topix100_streak_daily_state_panel_df,
)
from src.domains.analytics.topix_streak_extreme_mode import (
    _format_int_sequence,
    _format_return,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_stock_history,
)
from src.domains.backtest.core.walkforward import generate_walkforward_splits

DEFAULT_WALKFORWARD_TRAIN_WINDOW = 756
DEFAULT_WALKFORWARD_TEST_WINDOW = 126
DEFAULT_WALKFORWARD_STEP = 126
PRIMARY_BENCHMARK_KEY = "topix"
SECONDARY_BENCHMARK_KEY = "topix100_universe"
_LONG_BASELINE_BLEND_PRIOR = 260.0
_BASELINE_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("bucket", "bucket"),
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "split_config_df",
    "benchmark_daily_df",
    "walkforward_topk_pick_df",
    "walkforward_topk_daily_df",
    "portfolio_stats_df",
    "daily_return_distribution_df",
    "walkforward_split_summary_df",
    "walkforward_split_comparison_df",
    "walkforward_model_summary_df",
    "walkforward_model_comparison_df",
    "walkforward_score_decile_df",
    "walkforward_feature_importance_split_df",
    "walkforward_feature_importance_df",
)


@dataclass(frozen=True)
class _BaselineLookupRow:
    subset_key: str
    selector_value_key: str
    avg_target_return: float
    date_count: int
    avg_stock_count: float


@dataclass(frozen=True)
class _BaselineScorecard:
    universe_return: float
    rows_by_subset: dict[str, dict[str, _BaselineLookupRow]]


@dataclass(frozen=True)
class Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult:
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
    top_k_values: tuple[int, ...]
    train_window: int
    test_window: int
    step: int
    purge_signal_dates: int
    split_count: int
    primary_benchmark: str
    secondary_benchmark: str
    categorical_feature_columns: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    split_config_df: pd.DataFrame
    benchmark_daily_df: pd.DataFrame
    walkforward_topk_pick_df: pd.DataFrame
    walkforward_topk_daily_df: pd.DataFrame
    portfolio_stats_df: pd.DataFrame
    daily_return_distribution_df: pd.DataFrame
    walkforward_split_summary_df: pd.DataFrame
    walkforward_split_comparison_df: pd.DataFrame
    walkforward_model_summary_df: pd.DataFrame
    walkforward_model_comparison_df: pd.DataFrame
    walkforward_score_decile_df: pd.DataFrame
    walkforward_feature_importance_split_df: pd.DataFrame
    walkforward_feature_importance_df: pd.DataFrame


@dataclass(frozen=True)
class _WalkforwardPredictionArtifacts:
    split_count: int
    split_config_df: pd.DataFrame
    benchmark_daily_df: pd.DataFrame
    walkforward_prediction_df: pd.DataFrame
    walkforward_topk_pick_df: pd.DataFrame
    walkforward_topk_daily_df: pd.DataFrame
    walkforward_split_summary_df: pd.DataFrame
    walkforward_split_comparison_df: pd.DataFrame
    walkforward_feature_importance_split_df: pd.DataFrame


def run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    top_k_values: tuple[int, ...] | list[int] | None = None,
    train_window: int = DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    test_window: int = DEFAULT_WALKFORWARD_TEST_WINDOW,
    step: int = DEFAULT_WALKFORWARD_STEP,
    purge_signal_dates: int = 0,
) -> Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult:
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
    price_result, feature_panel_df = _build_research_feature_panel_df(
        db_path=db_path,
        start_date=start_date,
        end_date=end_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
    )
    return _run_walkforward_from_panel(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=str(feature_panel_df["date"].min()) if not feature_panel_df.empty else None,
        available_end_date=str(feature_panel_df["date"].max()) if not feature_panel_df.empty else None,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        top_k_values=resolved_top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        purge_signal_dates=purge_signal_dates,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
    )


def _build_research_feature_panel_df(
    *,
    db_path: str,
    start_date: str | None,
    end_date: str | None,
    price_feature: str,
    volume_feature: str,
    validation_ratio: float,
    short_window_streaks: int,
    long_window_streaks: int,
) -> tuple[Any, pd.DataFrame]:
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
    with _open_analysis_connection(db_path) as ctx:
        history_df = _query_topix100_stock_history(
            ctx.connection,
            end_date=end_date,
        )
    if history_df.empty:
        raise ValueError("No TOPIX100 constituent stock history was available")

    state_panel_df = build_topix100_streak_daily_state_panel_df(
        history_df,
        analysis_start_date=start_date,
        analysis_end_date=end_date,
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
    )
    feature_panel_df = _build_feature_panel_from_state_event_df(
        event_panel_df=price_result.event_panel_df,
        state_event_df=state_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    if feature_panel_df.empty:
        raise ValueError("Feature panel was empty after building the swing target")
    return price_result, feature_panel_df


def _run_walkforward_from_panel(
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
    top_k_values: tuple[int, ...],
    train_window: int,
    test_window: int,
    step: int,
    purge_signal_dates: int,
    feature_panel_df: pd.DataFrame,
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
) -> Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult:
    artifacts = _build_walkforward_prediction_artifacts(
        db_path=db_path,
        top_k_values=top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        purge_signal_dates=purge_signal_dates,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=categorical_feature_columns,
        continuous_feature_columns=continuous_feature_columns,
    )
    portfolio_stats_df = _build_portfolio_stats_df(artifacts.walkforward_topk_daily_df)
    daily_return_distribution_df = _build_daily_return_distribution_df(
        artifacts.walkforward_topk_daily_df
    )
    walkforward_model_summary_df = _build_walkforward_model_summary_df(
        artifacts.walkforward_topk_daily_df
    )
    walkforward_model_comparison_df = _build_walkforward_model_comparison_df(
        walkforward_model_summary_df
    )
    walkforward_score_decile_df = _build_validation_score_decile_df(
        artifacts.walkforward_prediction_df
    )
    walkforward_feature_importance_df = (
        artifacts.walkforward_feature_importance_split_df.groupby(
            ["model_name", "feature_name"],
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
            ["mean_importance_gain", "feature_name"],
            ascending=[False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    walkforward_feature_importance_df["importance_rank"] = range(
        1, len(walkforward_feature_importance_df) + 1
    )

    return Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult(
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
        top_k_values=top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        purge_signal_dates=purge_signal_dates,
        split_count=artifacts.split_count,
        primary_benchmark=PRIMARY_BENCHMARK_KEY,
        secondary_benchmark=SECONDARY_BENCHMARK_KEY,
        categorical_feature_columns=categorical_feature_columns,
        continuous_feature_columns=continuous_feature_columns,
        split_config_df=artifacts.split_config_df,
        benchmark_daily_df=artifacts.benchmark_daily_df,
        walkforward_topk_pick_df=artifacts.walkforward_topk_pick_df,
        walkforward_topk_daily_df=artifacts.walkforward_topk_daily_df,
        portfolio_stats_df=portfolio_stats_df,
        daily_return_distribution_df=daily_return_distribution_df,
        walkforward_split_summary_df=artifacts.walkforward_split_summary_df,
        walkforward_split_comparison_df=artifacts.walkforward_split_comparison_df,
        walkforward_model_summary_df=walkforward_model_summary_df,
        walkforward_model_comparison_df=walkforward_model_comparison_df,
        walkforward_score_decile_df=walkforward_score_decile_df,
        walkforward_feature_importance_split_df=artifacts.walkforward_feature_importance_split_df,
        walkforward_feature_importance_df=walkforward_feature_importance_df,
    )


def _build_walkforward_prediction_artifacts(
    *,
    db_path: str,
    top_k_values: tuple[int, ...],
    train_window: int,
    test_window: int,
    step: int,
    purge_signal_dates: int,
    feature_panel_df: pd.DataFrame,
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
) -> _WalkforwardPredictionArtifacts:
    unique_dates = pd.to_datetime(sorted(feature_panel_df["date"].astype(str).unique()))
    splits = generate_walkforward_splits(
        unique_dates,
        train_window=train_window,
        test_window=test_window,
        step=step,
        purge_window=purge_signal_dates,
    )
    if not splits:
        raise ValueError("No walk-forward splits were generated for the selected panel")

    benchmark_daily_df = _load_topix_open_to_open_5d_benchmark_df(
        db_path,
        start_date=str(feature_panel_df["date"].min()),
        end_date=str(feature_panel_df["date"].max()),
    )
    regressor_cls = _load_lightgbm_regressor_cls()

    split_config_records: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    topk_pick_frames: list[pd.DataFrame] = []
    topk_daily_frames: list[pd.DataFrame] = []
    split_summary_frames: list[pd.DataFrame] = []
    split_comparison_frames: list[pd.DataFrame] = []
    feature_importance_frames: list[pd.DataFrame] = []

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
        if train_feature_df.empty or test_feature_df.empty:
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

        baseline_lookup_df = _build_baseline_lookup_df(
            train_feature_df.assign(sample_split="discovery"),
            target_column="next_session_open_to_open_5d_return",
        )
        baseline_scorecard = _build_baseline_scorecard(baseline_lookup_df)
        split_feature_df = pd.concat(
            [
                train_feature_df.assign(sample_split="discovery"),
                test_feature_df.assign(sample_split="validation"),
            ],
            ignore_index=True,
        )
        baseline_prediction_df = _build_baseline_validation_prediction_df(
            split_feature_df,
            baseline_scorecard=baseline_scorecard,
            target_column="next_session_open_to_open_5d_return",
        )
        lightgbm_prediction_df, importance_df = _build_lightgbm_validation_prediction_df(
            split_feature_df,
            regressor_cls=regressor_cls,
            categorical_feature_columns=categorical_feature_columns,
            continuous_feature_columns=continuous_feature_columns,
            target_column="next_session_open_to_open_5d_return",
        )
        split_prediction_df = pd.concat(
            [baseline_prediction_df, lightgbm_prediction_df],
            ignore_index=True,
        ).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        prediction_frames.append(split_prediction_df)

        split_benchmark_df = _slice_by_date_range(
            benchmark_daily_df,
            start_date=split.test_start,
            end_date=split.test_end,
        )
        split_topk_pick_df, split_topk_daily_df = _build_validation_topk_tables(
            split_prediction_df,
            benchmark_daily_df=split_benchmark_df,
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

        split_summary_df = _build_walkforward_model_summary_df(split_topk_daily_df).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        split_comparison_df = _build_walkforward_model_comparison_df(split_summary_df).assign(
            split_index=split_index,
            train_start=split.train_start,
            train_end=split.train_end,
            test_start=split.test_start,
            test_end=split.test_end,
        )
        split_summary_frames.append(split_summary_df)
        split_comparison_frames.append(split_comparison_df)
        feature_importance_frames.append(
            importance_df.assign(
                split_index=split_index,
                train_start=split.train_start,
                train_end=split.train_end,
                test_start=split.test_start,
                test_end=split.test_end,
            )
        )

    if not topk_daily_frames:
        raise ValueError("Walk-forward evaluation produced no valid splits")

    split_config_df = pd.DataFrame.from_records(split_config_records)
    walkforward_prediction_df = pd.concat(prediction_frames, ignore_index=True)
    walkforward_topk_pick_df = pd.concat(topk_pick_frames, ignore_index=True)
    walkforward_topk_daily_df = pd.concat(topk_daily_frames, ignore_index=True)
    walkforward_split_summary_df = pd.concat(split_summary_frames, ignore_index=True)
    walkforward_split_comparison_df = pd.concat(split_comparison_frames, ignore_index=True)
    walkforward_feature_importance_split_df = pd.concat(
        feature_importance_frames,
        ignore_index=True,
    )
    return _WalkforwardPredictionArtifacts(
        split_count=int(split_config_df["split_index"].nunique()),
        split_config_df=split_config_df,
        benchmark_daily_df=benchmark_daily_df,
        walkforward_prediction_df=walkforward_prediction_df,
        walkforward_topk_pick_df=walkforward_topk_pick_df,
        walkforward_topk_daily_df=walkforward_topk_daily_df,
        walkforward_split_summary_df=walkforward_split_summary_df,
        walkforward_split_comparison_df=walkforward_split_comparison_df,
        walkforward_feature_importance_split_df=walkforward_feature_importance_split_df,
    )


def _build_baseline_lookup_df(
    feature_panel_df: pd.DataFrame,
    *,
    target_column: str,
) -> pd.DataFrame:
    discovery_df = feature_panel_df[feature_panel_df["sample_split"] == "discovery"].copy()
    if discovery_df.empty:
        raise ValueError("Feature panel has no discovery rows for baseline lookup")

    records: list[dict[str, Any]] = []
    universe_daily_df = (
        discovery_df.groupby("date", observed=True)
        .agg(
            target_return_mean=(target_column, "mean"),
            stock_count=("code", "nunique"),
        )
        .reset_index()
    )
    records.append(
        {
            "subset_key": "universe",
            "selector_value_key": "universe",
            "avg_target_return": float(universe_daily_df["target_return_mean"].mean()),
            "date_count": int(universe_daily_df["date"].nunique()),
            "avg_stock_count": float(universe_daily_df["stock_count"].mean()),
        }
    )

    for subset_key, selector_kind in _BASELINE_CHAIN[1:]:
        working_df = discovery_df.copy()
        working_df["selector_value_key"] = working_df.apply(
            lambda row: _build_baseline_selector_value_key(
                selector_kind,
                {
                    "bucket": f"Q{int(row['decile_num'])}",
                },
            ),
            axis=1,
        )
        daily_df = (
            working_df.groupby(["selector_value_key", "date"], observed=True)
            .agg(
                target_return_mean=(target_column, "mean"),
                stock_count=("code", "nunique"),
            )
            .reset_index()
        )
        summary_df = (
            daily_df.groupby("selector_value_key", observed=True)
            .agg(
                avg_target_return=("target_return_mean", "mean"),
                date_count=("date", "nunique"),
                avg_stock_count=("stock_count", "mean"),
            )
            .reset_index()
        )
        for row in summary_df.to_dict(orient="records"):
            records.append(
                {
                    "subset_key": subset_key,
                    "selector_value_key": str(row["selector_value_key"]),
                    "avg_target_return": float(row["avg_target_return"]),
                    "date_count": int(row["date_count"]),
                    "avg_stock_count": float(row["avg_stock_count"]),
                }
            )

    return pd.DataFrame.from_records(records).sort_values(
        ["subset_key", "selector_value_key"],
        kind="stable",
    ).reset_index(drop=True)


def _build_baseline_scorecard(baseline_lookup_df: pd.DataFrame) -> _BaselineScorecard:
    if baseline_lookup_df.empty:
        raise ValueError("Baseline lookup table is empty")

    rows_by_subset: dict[str, dict[str, _BaselineLookupRow]] = {}
    for row in baseline_lookup_df.to_dict(orient="records"):
        subset_key = str(row["subset_key"])
        selector_value_key = str(row["selector_value_key"])
        rows_by_subset.setdefault(subset_key, {})[selector_value_key] = _BaselineLookupRow(
            subset_key=subset_key,
            selector_value_key=selector_value_key,
            avg_target_return=float(row["avg_target_return"]),
            date_count=int(row["date_count"]),
            avg_stock_count=float(row["avg_stock_count"]),
        )
    universe_row = rows_by_subset.get("universe", {}).get("universe")
    if universe_row is None:
        raise ValueError("Baseline lookup is missing the universe row")
    return _BaselineScorecard(
        universe_return=float(universe_row.avg_target_return),
        rows_by_subset=rows_by_subset,
    )


def _score_baseline_target(
    scorecard: _BaselineScorecard,
    *,
    price_decile: int,
) -> float:
    values = {
        "bucket": f"Q{price_decile}",
    }
    current_value = scorecard.universe_return
    for subset_key, selector_kind in _BASELINE_CHAIN[1:]:
        row = scorecard.rows_by_subset.get(subset_key, {}).get(
            _build_baseline_selector_value_key(selector_kind, values)
        )
        if row is None:
            continue
        weight = row.date_count / (row.date_count + _LONG_BASELINE_BLEND_PRIOR)
        current_value = current_value * (1.0 - weight) + row.avg_target_return * weight
    return float(current_value)


def _build_baseline_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    baseline_scorecard: _BaselineScorecard,
    target_column: str,
) -> pd.DataFrame:
    validation_df = feature_panel_df[feature_panel_df["sample_split"] == "validation"].copy()
    if validation_df.empty:
        raise ValueError("Feature panel has no validation rows")

    validation_df["score"] = validation_df.apply(
        lambda row: _score_baseline_target(
            baseline_scorecard,
            price_decile=int(row["decile_num"]),
        ),
        axis=1,
    )
    validation_df["model_name"] = "baseline"
    validation_df["realized_return"] = validation_df[target_column].astype(float)
    base_columns = [
        "model_name",
        "date",
        "code",
        "company_name",
        "decile_num",
        "decile",
        "score",
        "realized_return",
    ]
    optional_columns = [
        column
        for column in ("swing_entry_date", "swing_exit_date")
        if column in validation_df.columns
    ]
    return validation_df[base_columns + optional_columns].copy()


def _build_lightgbm_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    regressor_cls: type[Any],
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
    target_column: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    training_df = feature_panel_df[
        (feature_panel_df["sample_split"] == "discovery")
        & feature_panel_df[target_column].notna()
    ].copy()
    validation_df = feature_panel_df[
        (feature_panel_df["sample_split"] == "validation")
        & feature_panel_df[target_column].notna()
    ].copy()
    if training_df.empty:
        raise ValueError("No discovery rows were available for the swing target.")
    if validation_df.empty:
        raise ValueError("No validation rows were available for the swing target.")

    categories = _build_category_lookup(feature_panel_df)
    feature_columns = [*categorical_feature_columns, *continuous_feature_columns]
    train_matrix = _build_model_matrix(
        training_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
        categories=categories,
    )
    validation_matrix = _build_model_matrix(
        validation_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
        categories=categories,
    )
    regressor = regressor_cls(**_build_lightgbm_params())
    regressor.fit(
        train_matrix,
        training_df[target_column].astype(float),
        categorical_feature=list(categorical_feature_columns),
    )
    predictions = pd.Series(
        regressor.predict(validation_matrix),
        index=validation_df.index,
        name="score",
        dtype=float,
    )
    base_columns = [
        "date",
        "code",
        "company_name",
        "decile_num",
        "decile",
    ]
    optional_columns = [
        column
        for column in ("swing_entry_date", "swing_exit_date")
        if column in validation_df.columns
    ]
    prediction_df = validation_df[base_columns + optional_columns].copy()
    prediction_df["score"] = predictions
    prediction_df["model_name"] = "lightgbm"
    prediction_df["realized_return"] = validation_df[target_column].astype(float)
    prediction_df = prediction_df[
        ["model_name", *base_columns, "score", "realized_return", *optional_columns]
    ].copy()

    importance_values = pd.Series(
        getattr(regressor, "feature_importances_", []),
        dtype=float,
    )
    if len(importance_values) != len(feature_columns):
        raise ValueError("Unexpected LightGBM feature importance output.")
    feature_importance_df = pd.DataFrame(
        {
            "model_name": "lightgbm",
            "feature_name": feature_columns,
            "importance_gain": importance_values.to_numpy(),
        }
    )
    total_importance = float(feature_importance_df["importance_gain"].sum())
    if total_importance > 0.0:
        feature_importance_df["importance_share"] = (
            feature_importance_df["importance_gain"] / total_importance
        )
    else:
        feature_importance_df["importance_share"] = 0.0
    feature_importance_df = feature_importance_df.sort_values(
        ["importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)
    feature_importance_df["importance_rank"] = range(1, len(feature_importance_df) + 1)
    return prediction_df, feature_importance_df


def _build_validation_topk_tables(
    validation_prediction_df: pd.DataFrame,
    *,
    benchmark_daily_df: pd.DataFrame,
    top_k_values: tuple[int, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ranked_df = validation_prediction_df.copy()
    ranked_df["selection_rank"] = (
        ranked_df.groupby(["model_name", "date"], observed=True)["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    universe_daily_df = (
        ranked_df.groupby("date", observed=True)
        .agg(
            universe_return_mean=("realized_return", "mean"),
            universe_stock_count=("code", "nunique"),
        )
        .reset_index()
    )

    pick_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for top_k in top_k_values:
        selected_df = ranked_df[ranked_df["selection_rank"] <= top_k].copy()
        if selected_df.empty:
            continue
        selected_df["top_k"] = int(top_k)
        selected_df = selected_df.merge(
            universe_daily_df,
            on="date",
            how="left",
            validate="many_to_one",
        ).merge(
            benchmark_daily_df,
            on="date",
            how="left",
            validate="many_to_one",
        )
        pick_frames.append(selected_df.copy())

        daily_df = (
            selected_df.groupby(["model_name", "top_k", "date"], observed=True)
            .agg(
                selected_return_mean=("realized_return", "mean"),
                selected_stock_count=("code", "nunique"),
                selected_score_mean=("score", "mean"),
                universe_return_mean=("universe_return_mean", "mean"),
                universe_stock_count=("universe_stock_count", "mean"),
                topix_benchmark_return=("topix_benchmark_return", "mean"),
            )
            .reset_index()
        )
        daily_df["excess_vs_topix"] = (
            daily_df["selected_return_mean"] - daily_df["topix_benchmark_return"]
        )
        daily_df["excess_vs_universe"] = (
            daily_df["selected_return_mean"] - daily_df["universe_return_mean"]
        )
        daily_df["beat_topix"] = daily_df["excess_vs_topix"] > 0
        daily_df["beat_universe"] = daily_df["excess_vs_universe"] > 0
        daily_frames.append(daily_df)

    if not pick_frames or not daily_frames:
        raise ValueError("Validation top-k evaluation produced no rows")

    pick_df = pd.concat(pick_frames, ignore_index=True).sort_values(
        ["model_name", "top_k", "date", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df = pd.concat(daily_frames, ignore_index=True).sort_values(
        ["model_name", "top_k", "date"],
        kind="stable",
    ).reset_index(drop=True)
    return pick_df, daily_df


def _build_walkforward_model_summary_df(
    walkforward_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = (
        walkforward_topk_daily_df.groupby(
            ["model_name", "top_k"],
            observed=True,
            sort=False,
        )
        .agg(
            date_count=("date", "nunique"),
            avg_long_return=("selected_return_mean", "mean"),
            long_hit_rate_positive_return=(
                "selected_return_mean",
                lambda values: float((values > 0).mean()),
            ),
            avg_topix_benchmark_return=("topix_benchmark_return", "mean"),
            avg_universe_return=("universe_return_mean", "mean"),
            avg_excess_vs_topix=("excess_vs_topix", "mean"),
            avg_excess_vs_universe=("excess_vs_universe", "mean"),
            hit_rate_vs_topix=(
                "excess_vs_topix",
                lambda values: float((values.dropna() > 0).mean()) if not values.dropna().empty else float("nan"),
            ),
            hit_rate_vs_universe=(
                "excess_vs_universe",
                lambda values: float((values.dropna() > 0).mean()) if not values.dropna().empty else float("nan"),
            ),
            avg_selected_stock_count=("selected_stock_count", "mean"),
            avg_selected_score=("selected_score_mean", "mean"),
        )
        .reset_index()
    )
    return summary_df.sort_values(["top_k", "model_name"], kind="stable").reset_index(drop=True)


def _build_walkforward_model_comparison_df(
    walkforward_model_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for _, group in walkforward_model_summary_df.groupby("top_k", observed=True, sort=False):
        resolved_top_k = int(group["top_k"].iloc[0])
        baseline_row = group[group["model_name"] == "baseline"]
        lightgbm_row = group[group["model_name"] == "lightgbm"]
        if baseline_row.empty or lightgbm_row.empty:
            continue
        baseline = baseline_row.iloc[0]
        lightgbm = lightgbm_row.iloc[0]
        records.append(
            {
                "top_k": resolved_top_k,
                "baseline_avg_long_return": float(baseline["avg_long_return"]),
                "lightgbm_avg_long_return": float(lightgbm["avg_long_return"]),
                "long_return_lift_vs_baseline": float(lightgbm["avg_long_return"])
                - float(baseline["avg_long_return"]),
                "baseline_avg_excess_vs_topix": float(baseline["avg_excess_vs_topix"]),
                "lightgbm_avg_excess_vs_topix": float(lightgbm["avg_excess_vs_topix"]),
                "excess_vs_topix_lift_vs_baseline": float(lightgbm["avg_excess_vs_topix"])
                - float(baseline["avg_excess_vs_topix"]),
                "baseline_avg_excess_vs_universe": float(baseline["avg_excess_vs_universe"]),
                "lightgbm_avg_excess_vs_universe": float(lightgbm["avg_excess_vs_universe"]),
                "excess_vs_universe_lift_vs_baseline": float(lightgbm["avg_excess_vs_universe"])
                - float(baseline["avg_excess_vs_universe"]),
                "baseline_hit_rate_vs_topix": float(baseline["hit_rate_vs_topix"]),
                "lightgbm_hit_rate_vs_topix": float(lightgbm["hit_rate_vs_topix"]),
                "hit_rate_vs_topix_lift_vs_baseline": float(lightgbm["hit_rate_vs_topix"])
                - float(baseline["hit_rate_vs_topix"]),
                "baseline_hit_rate_vs_universe": float(baseline["hit_rate_vs_universe"]),
                "lightgbm_hit_rate_vs_universe": float(lightgbm["hit_rate_vs_universe"]),
                "hit_rate_vs_universe_lift_vs_baseline": float(lightgbm["hit_rate_vs_universe"])
                - float(baseline["hit_rate_vs_universe"]),
            }
        )
    comparison_df = pd.DataFrame.from_records(records)
    if comparison_df.empty:
        return comparison_df
    return comparison_df.sort_values(["top_k"], kind="stable").reset_index(drop=True)


def _build_portfolio_stats_df(
    walkforward_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (model_name, top_k), scoped_df in walkforward_topk_daily_df.groupby(
        ["model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values("date", kind="stable").reset_index(drop=True)
        for series_name, series_label, series in _iter_daily_return_series(ordered_df):
            records.append(
                {
                    "model_name": str(model_name),
                    "top_k": int(top_k),
                    "series_name": series_name,
                    "series_label": series_label,
                    **_compute_portfolio_performance_stats(series),
                }
            )
    return pd.DataFrame.from_records(records).sort_values(
        ["top_k", "model_name", "series_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_daily_return_distribution_df(
    walkforward_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (model_name, top_k), scoped_df in walkforward_topk_daily_df.groupby(
        ["model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values("date", kind="stable").reset_index(drop=True)
        for series_name, series_label, series in _iter_daily_return_series(ordered_df):
            records.append(
                {
                    "model_name": str(model_name),
                    "top_k": int(top_k),
                    "series_name": series_name,
                    "series_label": series_label,
                    **_compute_daily_return_distribution_stats(series),
                }
            )
    return pd.DataFrame.from_records(records).sort_values(
        ["top_k", "model_name", "series_name"],
        kind="stable",
    ).reset_index(drop=True)


def _iter_daily_return_series(
    ordered_df: pd.DataFrame,
) -> tuple[tuple[str, str, pd.Series], ...]:
    return (
        ("long", "Long raw return", ordered_df["selected_return_mean"].astype(float)),
        (
            "excess_vs_topix",
            "Long excess vs TOPIX",
            ordered_df["excess_vs_topix"].astype(float),
        ),
        (
            "excess_vs_universe",
            "Long excess vs TOPIX100 universe",
            ordered_df["excess_vs_universe"].astype(float),
        ),
    )


def _compute_portfolio_performance_stats(series: pd.Series) -> dict[str, Any]:
    values = series.astype(float).dropna().reset_index(drop=True)
    day_count = int(len(values))
    if day_count == 0:
        return {
            "day_count": 0,
            "avg_daily_return": float("nan"),
            "median_daily_return": float("nan"),
            "daily_volatility": float("nan"),
            "annualized_volatility": float("nan"),
            "sharpe_ratio": float("nan"),
            "max_drawdown": float("nan"),
            "total_return": float("nan"),
            "cagr": float("nan"),
            "positive_rate": float("nan"),
            "non_negative_rate": float("nan"),
            "best_day_return": float("nan"),
            "worst_day_return": float("nan"),
        }

    daily_volatility = _safe_std(values)
    equity_curve = (1.0 + values).cumprod()
    running_max = equity_curve.cummax()
    drawdown = equity_curve.div(running_max).sub(1.0)
    total_return = float(equity_curve.iloc[-1] - 1.0)
    cagr = float(equity_curve.iloc[-1] ** (252.0 / day_count) - 1.0)
    annualized_volatility = float(daily_volatility * (252.0**0.5))
    sharpe_ratio = (
        float(values.mean() / daily_volatility * (252.0**0.5))
        if daily_volatility > 0
        else float("nan")
    )
    return {
        "day_count": day_count,
        "avg_daily_return": float(values.mean()),
        "median_daily_return": float(values.median()),
        "daily_volatility": float(daily_volatility),
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": float(drawdown.min()),
        "total_return": total_return,
        "cagr": cagr,
        "positive_rate": float((values > 0).mean()),
        "non_negative_rate": float((values >= 0).mean()),
        "best_day_return": float(values.max()),
        "worst_day_return": float(values.min()),
    }


def _compute_daily_return_distribution_stats(series: pd.Series) -> dict[str, Any]:
    values = series.astype(float).dropna().reset_index(drop=True)
    if values.empty:
        return {
            "day_count": 0,
            "mean_return": float("nan"),
            "median_return": float("nan"),
            "std_return": float("nan"),
            "min_return": float("nan"),
            "p05_return": float("nan"),
            "p25_return": float("nan"),
            "p75_return": float("nan"),
            "p95_return": float("nan"),
            "max_return": float("nan"),
            "positive_rate": float("nan"),
            "non_negative_rate": float("nan"),
        }
    return {
        "day_count": int(len(values)),
        "mean_return": float(values.mean()),
        "median_return": float(values.median()),
        "std_return": float(_safe_std(values)),
        "min_return": float(values.min()),
        "p05_return": float(values.quantile(0.05)),
        "p25_return": float(values.quantile(0.25)),
        "p75_return": float(values.quantile(0.75)),
        "p95_return": float(values.quantile(0.95)),
        "max_return": float(values.max()),
        "positive_rate": float((values > 0).mean()),
        "non_negative_rate": float((values >= 0).mean()),
    }


def _safe_std(series: pd.Series) -> float:
    if len(series) <= 1:
        return 0.0
    return float(series.std(ddof=1))


def write_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle(
    result: Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "top_k_values": list(result.top_k_values),
            "train_window": result.train_window,
            "test_window": result.test_window,
            "step": result.step,
            "purge_signal_dates": result.purge_signal_dates,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
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
    result: Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.walkforward_model_comparison_df,
        top_k=primary_top_k,
    )
    top_feature = _select_top_feature(result.walkforward_feature_importance_df)
    lightgbm_long_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="long",
    )
    lightgbm_topix_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="excess_vs_topix",
    )

    lines = [
        "# TOPIX100 Streak 3/53 Next-Session Open-to-Open 5D LightGBM Walk-Forward",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Walk-forward windows: `train {result.train_window} / test {result.test_window} / step {result.step}`",
        f"- Purge signal dates: `{result.purge_signal_dates}`",
        f"- Split count: `{result.split_count}`",
        "- Target: `X+1 open -> X+6 open return`",
        f"- Primary benchmark: `{result.primary_benchmark}`",
        f"- Secondary benchmark: `{result.secondary_benchmark}`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        "",
        "## Current Read",
        "",
        "This study rebuilds a long-only lookup baseline and a LightGBM regressor inside each train block, then buys the top-ranked names at X+1 open and evaluates the realized X+6 open outcome only in the following out-of-sample block.",
    ]
    if comparison_row is not None:
        lines.extend(
            [
                f"- Top {primary_top_k} long raw return: baseline `{_format_return(float(comparison_row['baseline_avg_long_return']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_long_return']))}`, lift `{_format_return(float(comparison_row['long_return_lift_vs_baseline']))}`.",
                f"- Top {primary_top_k} excess vs TOPIX: baseline `{_format_return(float(comparison_row['baseline_avg_excess_vs_topix']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_excess_vs_topix']))}`, lift `{_format_return(float(comparison_row['excess_vs_topix_lift_vs_baseline']))}`.",
                f"- Top {primary_top_k} excess vs TOPIX100 universe: baseline `{_format_return(float(comparison_row['baseline_avg_excess_vs_universe']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_excess_vs_universe']))}`, lift `{_format_return(float(comparison_row['excess_vs_universe_lift_vs_baseline']))}`.",
            ]
        )
    if lightgbm_long_stats is not None:
        lines.extend(
            [
                "",
                "## Execution Read",
                "",
                f"- LightGBM Top {primary_top_k} long average daily return: `{_format_return(float(lightgbm_long_stats['avg_daily_return']))}`",
                f"- LightGBM Top {primary_top_k} long Sharpe: `{float(lightgbm_long_stats['sharpe_ratio']):.2f}`",
                f"- LightGBM Top {primary_top_k} long max drawdown: `{_format_return(float(lightgbm_long_stats['max_drawdown']))}`",
                f"- LightGBM Top {primary_top_k} long positive-day rate: `{float(lightgbm_long_stats['positive_rate']):.2%}`",
            ]
        )
    if lightgbm_topix_stats is not None:
        lines.append(
            f"- LightGBM excess vs TOPIX Sharpe: `{float(lightgbm_topix_stats['sharpe_ratio']):.2f}` with max drawdown `{_format_return(float(lightgbm_topix_stats['max_drawdown']))}`."
        )
    if top_feature is not None:
        lines.append(
            f"- Average feature importance leader: `{top_feature['feature_name']}` at share `{float(top_feature['mean_importance_share']):.2%}`."
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `walkforward_model_summary_df`: raw return, vs TOPIX, and vs TOPIX100 universe scorecard for each model and top-k",
            "- `walkforward_model_comparison_df`: LightGBM lift vs the rebuilt lookup baseline on all three lenses",
            "- `walkforward_topk_daily_df`: date-level top-k long outcome with benchmark joins",
            "- `benchmark_daily_df`: signal-date-aligned TOPIX X+1 open -> X+6 open benchmark series",
            "- `walkforward_feature_importance_df`: average LightGBM feature importance across splits",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionOpenToOpen5dLightgbmWalkforwardResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.walkforward_model_comparison_df,
        top_k=primary_top_k,
    )
    lightgbm_summary_row = _select_model_summary_row(
        result.walkforward_model_summary_df,
        model_name="lightgbm",
        top_k=primary_top_k,
    )
    lightgbm_long_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="long",
    )
    lightgbm_topix_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="excess_vs_topix",
    )
    lightgbm_dist_row = _select_distribution_row(
        result.daily_return_distribution_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="long",
    )
    top_feature = _select_top_feature(result.walkforward_feature_importance_df)

    result_bullets = [
        "Each split rebuilds the lookup baseline from the train window only, then retrains LightGBM on the same leak-free train block.",
        "The portfolio is long-only: buy the top-ranked names at X+1 open, hold until X+6 open, then compare raw return, excess vs TOPIX, and excess vs the equal-weight TOPIX100 universe.",
    ]
    if comparison_row is not None:
        result_bullets.extend(
            [
                f"Across all out-of-sample blocks, Top {primary_top_k} long raw return was {_format_return(float(comparison_row['baseline_avg_long_return']))} for baseline versus {_format_return(float(comparison_row['lightgbm_avg_long_return']))} for LightGBM.",
                f"Against TOPIX, Top {primary_top_k} excess return was {_format_return(float(comparison_row['baseline_avg_excess_vs_topix']))} for baseline versus {_format_return(float(comparison_row['lightgbm_avg_excess_vs_topix']))} for LightGBM.",
                f"Against the TOPIX100 equal-weight universe, Top {primary_top_k} excess return was {_format_return(float(comparison_row['baseline_avg_excess_vs_universe']))} for baseline versus {_format_return(float(comparison_row['lightgbm_avg_excess_vs_universe']))} for LightGBM.",
            ]
        )
    if lightgbm_long_stats is not None:
        result_bullets.append(
            f"The LightGBM Top {primary_top_k} long book averaged {_format_return(float(lightgbm_long_stats['avg_daily_return']))} per signal day with Sharpe {float(lightgbm_long_stats['sharpe_ratio']):.2f} and max drawdown {_format_return(float(lightgbm_long_stats['max_drawdown']))}."
        )
    if lightgbm_topix_stats is not None:
        result_bullets.append(
            f"Measured as excess over TOPIX, the same book posted Sharpe {float(lightgbm_topix_stats['sharpe_ratio']):.2f}."
        )
    if lightgbm_dist_row is not None:
        result_bullets.append(
            f"Its daily raw-return distribution ran from {_format_return(float(lightgbm_dist_row['min_return']))} to {_format_return(float(lightgbm_dist_row['max_return']))}, with 5/95 percentiles at {_format_return(float(lightgbm_dist_row['p05_return']))} and {_format_return(float(lightgbm_dist_row['p95_return']))}."
        )
    if top_feature is not None:
        result_bullets.append(
            f"Average feature importance is led by {top_feature['feature_name']} at {float(top_feature['mean_importance_share']):.2%}."
        )

    highlights = [
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
            "detail": "long-only book",
        },
    ]
    if lightgbm_summary_row is not None:
        highlights.extend(
            [
                {
                    "label": "Long return",
                    "value": _format_return(float(lightgbm_summary_row["avg_long_return"])),
                    "tone": "accent",
                    "detail": f"Top {primary_top_k}",
                },
                {
                    "label": "Vs TOPIX",
                    "value": _format_return(float(lightgbm_summary_row["avg_excess_vs_topix"])),
                    "tone": "success",
                    "detail": f"Top {primary_top_k} excess",
                },
                {
                    "label": "Vs Universe",
                    "value": _format_return(float(lightgbm_summary_row["avg_excess_vs_universe"])),
                    "tone": "neutral",
                    "detail": f"Top {primary_top_k} excess",
                },
            ]
        )
    if lightgbm_long_stats is not None:
        highlights.extend(
            [
                {
                    "label": "Long Sharpe",
                    "value": f"{float(lightgbm_long_stats['sharpe_ratio']):.2f}",
                    "tone": "accent",
                    "detail": "raw return",
                },
                {
                    "label": "Long Max DD",
                    "value": _format_return(float(lightgbm_long_stats["max_drawdown"])),
                    "tone": "danger",
                    "detail": "raw return",
                },
            ]
        )

    return {
        "title": "TOPIX100 Streak 3/53 Next-Session Open-to-Open 5D LightGBM Walk-Forward",
        "tags": ["TOPIX100", "streaks", "lightgbm", "swing", "walk-forward"],
        "purpose": (
            "Check whether a leak-free TOPIX100 streak 3 / 53 swing score can beat both TOPIX and the equal-weight TOPIX100 universe once the baseline and LightGBM model are repeatedly re-estimated in a rolling walk-forward loop."
        ),
        "method": [
            "Build the leak-free TOPIX100 streak 3 / 53 daily state panel and target each signal date with X+1 open -> X+6 open return.",
            "Inside each train/test block, rebuild a lookup baseline and retrain LightGBM on the same train rows, then rank only the following out-of-sample block.",
            "Evaluate top-k long books on raw return, excess vs TOPIX, and excess vs the equal-weight TOPIX100 universe.",
        ],
        "resultHeadline": (
            "This walk-forward checks whether the swing score adds value once beta is allowed back in, while still forcing the result to clear TOPIX first and the TOPIX100 universe second."
        ),
        "resultBullets": result_bullets,
        "considerations": [
            "This still ignores fees, open auction slippage, and capacity constraints.",
            "The primary KPI is excess vs TOPIX; the secondary benchmark is there to detect cases where the book only rides a TOPIX100 mega-cap rebound.",
            "Train/test window lengths are still hyperparameters, so this should be read as one disciplined walk-forward configuration rather than the final answer.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {
                "label": "Discrete features",
                "value": ", ".join(result.categorical_feature_columns) or "none",
            },
            {"label": "Target", "value": "X+6 open / X+1 open - 1"},
            {"label": "Primary KPI", "value": "excess vs TOPIX"},
            {"label": "Secondary KPI", "value": "excess vs TOPIX100 universe"},
            {"label": "Train/Test", "value": f"{result.train_window}/{result.test_window}"},
            {"label": "Step", "value": str(result.step)},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
            {"label": "Split count", "value": str(result.split_count)},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "walkforward_model_comparison_df",
                "label": "Overall model lift",
                "description": "Aggregated out-of-sample lift of LightGBM versus the lookup baseline on raw return, excess vs TOPIX, and excess vs TOPIX100 universe.",
            },
            {
                "name": "walkforward_split_comparison_df",
                "label": "Per-split comparison",
                "description": "One row per split and top-k showing where LightGBM beat or lagged the baseline.",
            },
            {
                "name": "walkforward_topk_daily_df",
                "label": "Date-level top-k outcomes",
                "description": "Signal-date-level top-k long return with benchmark joins for TOPIX and the equal-weight universe.",
            },
            {
                "name": "benchmark_daily_df",
                "label": "TOPIX benchmark series",
                "description": "Signal-date-aligned X+1 open -> X+6 open benchmark return for TOPIX.",
            },
            {
                "name": "walkforward_feature_importance_df",
                "label": "Average feature importance",
                "description": "Mean LightGBM gain importance across all walk-forward splits.",
            },
        ],
    }


def _resolve_primary_top_k(top_k_values: tuple[int, ...]) -> int:
    if 3 in top_k_values:
        return 3
    return int(top_k_values[0])


def _select_comparison_row(
    comparison_df: pd.DataFrame,
    *,
    top_k: int,
) -> pd.Series | None:
    scoped_df = comparison_df[comparison_df["top_k"] == top_k].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_model_summary_row(
    summary_df: pd.DataFrame,
    *,
    model_name: str,
    top_k: int,
) -> pd.Series | None:
    scoped_df = summary_df[
        (summary_df["model_name"] == model_name) & (summary_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_portfolio_stats_row(
    stats_df: pd.DataFrame,
    *,
    model_name: str,
    top_k: int,
    series_name: str,
) -> pd.Series | None:
    scoped_df = stats_df[
        (stats_df["model_name"] == model_name)
        & (stats_df["top_k"] == top_k)
        & (stats_df["series_name"] == series_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_distribution_row(
    distribution_df: pd.DataFrame,
    *,
    model_name: str,
    top_k: int,
    series_name: str,
) -> pd.Series | None:
    scoped_df = distribution_df[
        (distribution_df["model_name"] == model_name)
        & (distribution_df["top_k"] == top_k)
        & (distribution_df["series_name"] == series_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_top_feature(
    feature_importance_df: pd.DataFrame,
) -> pd.Series | None:
    if feature_importance_df.empty:
        return None
    return feature_importance_df.sort_values(
        ["mean_importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    ).iloc[0]


def _load_topix_open_to_open_5d_benchmark_df(
    db_path: str,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        if _table_exists(conn, "topix_data"):
            benchmark_df = conn.execute(
                """
                WITH ranked_sessions AS (
                    SELECT
                        date,
                        LEAD(date, 1) OVER (ORDER BY date ASC) AS benchmark_entry_date,
                        LEAD(open, 1) OVER (ORDER BY date ASC) AS benchmark_entry_open,
                        LEAD(date, 6) OVER (ORDER BY date ASC) AS benchmark_exit_date,
                        LEAD(open, 6) OVER (ORDER BY date ASC) AS benchmark_exit_open
                    FROM topix_data
                )
                SELECT
                    date,
                    benchmark_entry_date,
                    benchmark_exit_date,
                    benchmark_exit_open / NULLIF(benchmark_entry_open, 0) - 1 AS topix_benchmark_return
                FROM ranked_sessions
                WHERE date >= ? AND date <= ?
                ORDER BY date
                """,
                [start_date, end_date],
            ).df()
        elif _table_exists(conn, "index_master") and _table_exists(conn, "indices_data"):
            benchmark_df = conn.execute(
                """
                WITH topix_code AS (
                    SELECT code
                    FROM index_master
                    WHERE lower(coalesce(category, '')) = 'topix'
                    ORDER BY CASE WHEN upper(code) = 'TOPIX' THEN 0 ELSE 1 END, code ASC
                    LIMIT 1
                ),
                ranked_sessions AS (
                    SELECT
                        id.date,
                        LEAD(id.date, 1) OVER (ORDER BY id.date ASC) AS benchmark_entry_date,
                        LEAD(id.open, 1) OVER (ORDER BY id.date ASC) AS benchmark_entry_open,
                        LEAD(id.date, 6) OVER (ORDER BY id.date ASC) AS benchmark_exit_date,
                        LEAD(id.open, 6) OVER (ORDER BY id.date ASC) AS benchmark_exit_open
                    FROM indices_data id
                    JOIN topix_code tc ON tc.code = id.code
                )
                SELECT
                    date,
                    benchmark_entry_date,
                    benchmark_exit_date,
                    benchmark_exit_open / NULLIF(benchmark_entry_open, 0) - 1 AS topix_benchmark_return
                FROM ranked_sessions
                WHERE date >= ? AND date <= ?
                ORDER BY date
                """,
                [start_date, end_date],
            ).df()
        else:
            raise ValueError("No TOPIX benchmark table was available for the swing study")

    if benchmark_df.empty:
        raise ValueError("TOPIX benchmark series was empty for the selected swing range")
    benchmark_df["date"] = benchmark_df["date"].astype(str)
    return benchmark_df.sort_values("date", kind="stable").reset_index(drop=True)


def _table_exists(connection: Any, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None
