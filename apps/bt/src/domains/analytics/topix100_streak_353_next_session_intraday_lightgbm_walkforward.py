"""
Walk-forward validation for TOPIX100 streak 3/53 next-session intraday LightGBM.

This extends the fixed-split next-session intraday study by retraining both the
baseline lookup and LightGBM on rolling train windows, then evaluating only on
the following out-of-sample block.
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
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
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
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
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
TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-lightgbm-walkforward"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "split_config_df",
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
class Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult:
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
    categorical_feature_columns: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    split_config_df: pd.DataFrame
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
    walkforward_prediction_df: pd.DataFrame
    walkforward_topk_pick_df: pd.DataFrame
    walkforward_topk_daily_df: pd.DataFrame
    walkforward_split_summary_df: pd.DataFrame
    walkforward_split_comparison_df: pd.DataFrame
    walkforward_feature_importance_split_df: pd.DataFrame


def run_topix100_streak_353_next_session_intraday_lightgbm_walkforward_research(
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
) -> Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult:
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
        future_horizons=(1,),
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )
    feature_panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature=price_feature,
        volume_feature=volume_feature,
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
        categorical_feature_columns=DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
    )


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
) -> Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult:
    artifacts = _build_walkforward_prediction_artifacts(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
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
    walkforward_model_summary_df = _build_validation_model_summary_df(
        artifacts.walkforward_topk_daily_df
    )
    walkforward_model_comparison_df = _build_validation_model_comparison_df(
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

    return Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult(
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
        categorical_feature_columns=categorical_feature_columns,
        continuous_feature_columns=continuous_feature_columns,
        split_config_df=artifacts.split_config_df,
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

    regressor_cls = _load_lightgbm_regressor_cls()
    split_config_records: list[dict[str, Any]] = []
    topk_pick_frames: list[pd.DataFrame] = []
    topk_daily_frames: list[pd.DataFrame] = []
    split_summary_frames: list[pd.DataFrame] = []
    split_comparison_frames: list[pd.DataFrame] = []
    feature_importance_frames: list[pd.DataFrame] = []
    prediction_frames: list[pd.DataFrame] = []

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
        )
        lightgbm_prediction_df, _config_record, importance_df = (
            _build_lightgbm_validation_prediction_df(
                split_feature_df,
                regressor_cls=regressor_cls,
                categorical_feature_columns=categorical_feature_columns,
                continuous_feature_columns=continuous_feature_columns,
            )
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
        split_comparison_df = _build_validation_model_comparison_df(split_summary_df).assign(
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
        walkforward_prediction_df=walkforward_prediction_df,
        walkforward_topk_pick_df=walkforward_topk_pick_df,
        walkforward_topk_daily_df=walkforward_topk_daily_df,
        walkforward_split_summary_df=walkforward_split_summary_df,
        walkforward_split_comparison_df=walkforward_split_comparison_df,
        walkforward_feature_importance_split_df=walkforward_feature_importance_split_df,
    )


def write_topix100_streak_353_next_session_intraday_lightgbm_walkforward_research_bundle(
    result: Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_intraday_lightgbm_walkforward_research",
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


def load_topix100_streak_353_next_session_intraday_lightgbm_walkforward_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_intraday_lightgbm_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_intraday_lightgbm_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID,
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


def _build_portfolio_stats_df(walkforward_topk_daily_df: pd.DataFrame) -> pd.DataFrame:
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
        ("long", "Long leg", ordered_df["long_return_mean"].astype(float)),
        ("short_edge", "Short edge", ordered_df["short_edge_mean"].astype(float)),
        ("gross_spread", "Gross spread", ordered_df["gross_edge"].astype(float)),
        (
            "pair_50_50",
            "Pair 50/50",
            ordered_df["gross_edge"].astype(float) / 2.0,
        ),
    )


def _compute_portfolio_performance_stats(series: pd.Series) -> dict[str, Any]:
    values = series.astype(float).reset_index(drop=True)
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
    sharpe_ratio = float(values.mean() / daily_volatility * (252.0**0.5)) if daily_volatility > 0 else float("nan")
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
    values = series.astype(float).reset_index(drop=True)
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


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.walkforward_model_comparison_df,
        top_k=primary_top_k,
    )
    pair_stats_row = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    win_text = _count_split_wins(
        result.walkforward_split_comparison_df,
        top_k=primary_top_k,
    )
    top_feature = _select_top_feature(result.walkforward_feature_importance_df)

    lines = [
        "# TOPIX100 Streak 3/53 Next-Session Intraday LightGBM Walk-Forward",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Walk-forward windows: `train {result.train_window} / test {result.test_window} / step {result.step}`",
        f"- Purge signal dates: `{result.purge_signal_dates}`",
        f"- Split count: `{result.split_count}`",
        "- Target: `next-session open -> close return`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        "",
        "## Current Read",
        "",
        "This is the overfitting check for the intraday score. Every split rebuilds the baseline from the train window only, retrains LightGBM on the same window, and evaluates both only on the following out-of-sample block.",
    ]
    if comparison_row is not None:
        lines.extend(
            [
                f"- Top {primary_top_k} long: baseline `{_format_return(float(comparison_row['baseline_avg_long_return']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_long_return']))}`, lift `{_format_return(float(comparison_row['long_return_lift_vs_baseline']))}`.",
                f"- Bottom {primary_top_k} short edge: baseline `{_format_return(float(comparison_row['baseline_avg_short_edge']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_short_edge']))}`, lift `{_format_return(float(comparison_row['short_edge_lift_vs_baseline']))}`.",
                f"- Top/Bottom {primary_top_k} spread: baseline `{_format_return(float(comparison_row['baseline_avg_long_short_spread']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_long_short_spread']))}`, lift `{_format_return(float(comparison_row['spread_lift_vs_baseline']))}`, split wins `{win_text}`.",
            ]
        )
    if pair_stats_row is not None:
        lines.extend(
            [
                "",
                "## Execution Read",
                "",
                f"- Pair 50/50 average daily return: `{_format_return(float(pair_stats_row['avg_daily_return']))}`",
                f"- Pair 50/50 Sharpe: `{float(pair_stats_row['sharpe_ratio']):.2f}`",
                f"- Pair 50/50 max drawdown: `{_format_return(float(pair_stats_row['max_drawdown']))}`",
                f"- Pair 50/50 positive-day rate: `{float(pair_stats_row['positive_rate']):.2%}`",
            ]
        )
    if top_feature is not None:
        lines.append(
            f"- Average feature importance leader: `{top_feature['feature_name']}` at share `{float(top_feature['mean_importance_share']):.2%}`."
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionIntradayLightgbmWalkforwardResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.walkforward_model_comparison_df,
        top_k=primary_top_k,
    )
    best_summary_row = _select_model_summary_row(
        result.walkforward_model_summary_df,
        model_name="lightgbm",
        top_k=primary_top_k,
    )
    pair_stats_row = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    pair_distribution_row = _select_distribution_row(
        result.daily_return_distribution_df,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    top_feature = _select_top_feature(result.walkforward_feature_importance_df)
    win_text = _count_split_wins(
        result.walkforward_split_comparison_df,
        top_k=primary_top_k,
    )

    headline = (
        "Walk-forward validation checks whether the next-session intraday LightGBM score still works after the model is repeatedly retrained and pushed into the following out-of-sample block."
    )
    result_bullets = [
        "Each split rebuilds the lookup baseline from the train window only, so the baseline remains leakage-free inside the rolling evaluation.",
        "LightGBM is retrained on each train block and judged only on the next test block, using the same top-k long / bottom-k short / spread lens as the fixed study.",
    ]
    if comparison_row is not None:
        result_bullets.extend(
            [
                f"Across all out-of-sample blocks, Top {primary_top_k} long was {_format_return(float(comparison_row['baseline_avg_long_return']))} for baseline versus {_format_return(float(comparison_row['lightgbm_avg_long_return']))} for LightGBM.",
                f"Across all out-of-sample blocks, Bottom {primary_top_k} short edge was {_format_return(float(comparison_row['baseline_avg_short_edge']))} for baseline versus {_format_return(float(comparison_row['lightgbm_avg_short_edge']))} for LightGBM.",
                f"The combined Top/Bottom {primary_top_k} spread was {_format_return(float(comparison_row['lightgbm_avg_long_short_spread']))} for LightGBM versus {_format_return(float(comparison_row['baseline_avg_long_short_spread']))} for baseline, with split wins {win_text}.",
            ]
        )
    if pair_stats_row is not None:
        result_bullets.extend(
            [
                f"Interpreted as a 50/50 dollar-neutral pair, the LightGBM Top/Bottom {primary_top_k} book averaged {_format_return(float(pair_stats_row['avg_daily_return']))} per day with Sharpe {float(pair_stats_row['sharpe_ratio']):.2f} and max drawdown {_format_return(float(pair_stats_row['max_drawdown']))}.",
                f"The same pair book had positive days {float(pair_stats_row['positive_rate']):.2%} of the time over {int(pair_stats_row['day_count'])} out-of-sample sessions.",
            ]
        )
    if pair_distribution_row is not None:
        result_bullets.append(
            f"Its daily distribution ran from { _format_return(float(pair_distribution_row['min_return'])) } to { _format_return(float(pair_distribution_row['max_return'])) }, with 5/95 percentiles at { _format_return(float(pair_distribution_row['p05_return'])) } and { _format_return(float(pair_distribution_row['p95_return'])) }."
        )
    if top_feature is not None:
        result_bullets.append(
            f"Average feature importance is still led by {top_feature['feature_name']} at {float(top_feature['mean_importance_share']):.2%}."
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
            "detail": "long + short legs",
        },
    ]
    if best_summary_row is not None:
        highlights.extend(
            [
                {
                    "label": "Long return",
                    "value": _format_return(float(best_summary_row["avg_long_return"])),
                    "tone": "success",
                    "detail": f"Top {primary_top_k}",
                },
                {
                    "label": "Short edge",
                    "value": _format_return(float(best_summary_row["avg_short_edge"])),
                    "tone": "danger",
                    "detail": f"Bottom {primary_top_k}",
                },
                {
                    "label": "Long-short spread",
                    "value": _format_return(float(best_summary_row["avg_long_short_spread"])),
                    "tone": "accent",
                    "detail": f"Top/Bottom {primary_top_k}",
                },
            ]
        )
    if pair_stats_row is not None:
        highlights.extend(
            [
                {
                    "label": "Pair 50/50",
                    "value": _format_return(float(pair_stats_row["avg_daily_return"])),
                    "tone": "accent",
                    "detail": f"avg daily, Top/Bottom {primary_top_k}",
                },
                {
                    "label": "Pair Sharpe",
                    "value": f"{float(pair_stats_row['sharpe_ratio']):.2f}",
                    "tone": "success",
                    "detail": "50/50 dollar-neutral",
                },
                {
                    "label": "Pair Max DD",
                    "value": _format_return(float(pair_stats_row["max_drawdown"])),
                    "tone": "danger",
                    "detail": "50/50 dollar-neutral",
                },
            ]
        )

    return {
        "title": "TOPIX100 Streak 3/53 Next-Session Intraday LightGBM Walk-Forward",
        "tags": ["TOPIX100", "streaks", "lightgbm", "intraday", "walk-forward"],
        "purpose": (
            "Check whether the next-session intraday LightGBM score still beats the lookup baseline once both are repeatedly re-estimated in a rolling walk-forward loop."
        ),
        "method": [
            "Build the same TOPIX100 streak 3 / 53 feature panel used in the fixed intraday study.",
            "Generate rolling train/test windows, rebuild the baseline and retrain LightGBM inside each train window, then score only the following test block.",
            "Aggregate out-of-sample top-k long, bottom-k short, and combined spread across all splits.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This is much closer to a deployable check than the fixed split, but it still ignores fees, open auction slippage, borrow cost, and turnover control.",
            "The target is next-session open to close, so live execution quality around the open remains a major practical risk.",
            "Train/test window lengths are still hyperparameters. This should be read as one disciplined walk-forward setting, not the final word.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {
                "label": "Discrete features",
                "value": ", ".join(result.categorical_feature_columns) or "none",
            },
            {"label": "Target", "value": "next-session close / open - 1"},
            {"label": "Train/Test", "value": f"{result.train_window}/{result.test_window}"},
            {"label": "Step", "value": str(result.step)},
            {"label": "Purge", "value": str(result.purge_signal_dates)},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
            {"label": "Split count", "value": str(result.split_count)},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "walkforward_model_comparison_df",
                "label": "Overall walk-forward lift",
                "description": "Aggregated out-of-sample lift of LightGBM versus the baseline over all walk-forward test blocks.",
            },
            {
                "name": "walkforward_split_comparison_df",
                "label": "Per-split comparison",
                "description": "One row per split and top-k showing whether LightGBM beat the baseline in that out-of-sample block.",
            },
            {
                "name": "walkforward_feature_importance_df",
                "label": "Average feature importance",
                "description": "Mean LightGBM gain importance across all walk-forward splits.",
            },
            {
                "name": "portfolio_stats_df",
                "label": "Execution portfolio stats",
                "description": "Return, Sharpe, volatility, and drawdown for long leg, short edge, gross spread, and 50/50 pair interpretations.",
            },
            {
                "name": "daily_return_distribution_df",
                "label": "Daily return distribution",
                "description": "Percentile view of day-level returns for each execution interpretation.",
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


def _count_split_wins(
    split_comparison_df: pd.DataFrame,
    *,
    top_k: int,
) -> str:
    scoped_df = split_comparison_df[split_comparison_df["top_k"] == top_k].copy()
    if scoped_df.empty:
        return "0/0"
    wins = int((scoped_df["spread_lift_vs_baseline"] > 0).sum())
    return f"{wins}/{len(scoped_df)}"


def _select_top_feature(feature_df: pd.DataFrame) -> pd.Series | None:
    if feature_df.empty:
        return None
    scoped_df = feature_df.sort_values(
        ["mean_importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    return scoped_df.iloc[0]
