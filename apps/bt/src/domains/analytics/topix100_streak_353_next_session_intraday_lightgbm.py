"""
LightGBM research for TOPIX100 streak 3/53 next-session intraday ranking.

This study keeps the same feature family as the existing stage-2 TOPIX100
score, but changes the target to the next trading session's intraday return:

- entry: next trading day's open
- exit: same trading day's close
- target: next_close / next_open - 1

The intended use is a daily ranking strategy:
- buy the top predicted names at the next open
- short the bottom predicted names at the next open
- close both at the same day's close
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
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
    _enrich_event_panel,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    LIGHTGBM_RESEARCH_INSTALL_HINT,
    LIGHTGBM_LIBOMP_INSTALL_HINT,
    Topix100Streak353SignalScoreLightgbmResearchError,
    _build_category_lookup,
    _build_lightgbm_params,
    _build_model_matrix,
    _build_price_feature_frame,
    _build_scoring_snapshot_df,
    _format_int_sequence,
    _format_return,
    _load_lightgbm_regressor_cls,
    _predict_lightgbm_snapshot_scores,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    build_topix100_streak_daily_state_panel_df,
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
    DECILE_ORDER,
    _query_topix100_stock_history,
)
from src.domains.backtest.core.walkforward import generate_walkforward_splits

DEFAULT_TOP_K_VALUES: tuple[int, ...] = (1, 3, 5, 10, 20)
TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-lightgbm"
)
TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-lightgbm-walkforward"
)
DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS: tuple[str, ...] = ("decile",)
DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS = 756
DEFAULT_RUNTIME_TEST_WINDOW_DAYS = 126
DEFAULT_RUNTIME_STEP_DAYS = 126
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "feature_panel_df",
    "baseline_lookup_df",
    "model_config_df",
    "validation_topk_pick_df",
    "validation_topk_daily_df",
    "validation_model_summary_df",
    "validation_model_comparison_df",
    "validation_score_decile_df",
    "feature_importance_df",
)
_BASELINE_BLEND_PRIOR = 320.0
_BASELINE_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("bucket", "bucket"),
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
class Topix100Streak353NextSessionIntradayLightgbmResearchResult:
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
    categorical_feature_columns: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    discovery_row_count: int
    validation_row_count: int
    discovery_date_count: int
    validation_date_count: int
    feature_panel_df: pd.DataFrame
    baseline_lookup_df: pd.DataFrame
    model_config_df: pd.DataFrame
    validation_topk_pick_df: pd.DataFrame
    validation_topk_daily_df: pd.DataFrame
    validation_model_summary_df: pd.DataFrame
    validation_model_comparison_df: pd.DataFrame
    validation_score_decile_df: pd.DataFrame
    feature_importance_df: pd.DataFrame


@dataclass(frozen=True)
class Topix100Streak353NextSessionIntradayLightgbmSnapshotRow:
    code: str
    company_name: str
    date: str
    intraday_score: float | None


@dataclass(frozen=True)
class Topix100Streak353NextSessionIntradayLightgbmSnapshot:
    score_source_run_id: str | None
    price_feature: str
    volume_feature: str
    short_window_streaks: int
    long_window_streaks: int
    score_model_type: Literal["walkforward_frozen_split", "daily_refit"]
    train_window_days: int
    test_window_days: int | None
    step_days: int | None
    split_train_start: str | None
    split_train_end: str | None
    split_test_start: str | None
    split_test_end: str | None
    split_is_partial_tail: bool
    rows_by_code: dict[str, Topix100Streak353NextSessionIntradayLightgbmSnapshotRow]


@dataclass(frozen=True)
class _RuntimeWalkforwardSplit:
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    is_partial_tail: bool


def _missing_lightgbm_message() -> str:
    return (
        "LightGBM research is unavailable because lightgbm is not installed. "
        f"Install it with: {LIGHTGBM_RESEARCH_INSTALL_HINT}"
    )


def _lightgbm_runtime_message(error_message: str | None = None) -> str:
    base_message = (
        "LightGBM research is unavailable because the lightgbm runtime could not "
        "be loaded. On macOS, install libomp with: "
        f"{LIGHTGBM_LIBOMP_INSTALL_HINT}"
    )
    if not error_message:
        return base_message
    return f"{base_message}. Original error: {error_message}"


def format_topix100_streak_353_next_session_intraday_lightgbm_notebook_error(
    exc: Exception,
) -> str:
    if isinstance(exc, Topix100Streak353SignalScoreLightgbmResearchError):
        return str(exc)
    if isinstance(exc, ModuleNotFoundError):
        return _missing_lightgbm_message()
    if isinstance(exc, OSError):
        return _lightgbm_runtime_message(str(exc))
    return str(exc)


def run_topix100_streak_353_next_session_intraday_lightgbm_research(
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
) -> Topix100Streak353NextSessionIntradayLightgbmResearchResult:
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

    baseline_lookup_df = _build_baseline_lookup_df(feature_panel_df)
    baseline_scorecard = _build_baseline_scorecard(baseline_lookup_df)
    baseline_prediction_df = _build_baseline_validation_prediction_df(
        feature_panel_df,
        baseline_scorecard=baseline_scorecard,
    )

    regressor_cls = _load_lightgbm_regressor_cls()
    lightgbm_prediction_df, model_config_record, feature_importance_df = (
        _build_lightgbm_validation_prediction_df(
            feature_panel_df,
            regressor_cls=regressor_cls,
            categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
            continuous_feature_columns=(
                price_feature,
                volume_feature,
                *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
            ),
        )
    )
    validation_prediction_df = pd.concat(
        [baseline_prediction_df, lightgbm_prediction_df],
        ignore_index=True,
    )
    validation_topk_pick_df, validation_topk_daily_df = _build_validation_topk_tables(
        validation_prediction_df,
        top_k_values=resolved_top_k_values,
    )
    validation_model_summary_df = _build_validation_model_summary_df(validation_topk_daily_df)
    validation_model_comparison_df = _build_validation_model_comparison_df(
        validation_model_summary_df
    )
    validation_score_decile_df = _build_validation_score_decile_df(validation_prediction_df)

    discovery_df = feature_panel_df[feature_panel_df["sample_split"] == "discovery"].copy()
    validation_df = feature_panel_df[feature_panel_df["sample_split"] == "validation"].copy()
    model_config_df = pd.DataFrame.from_records([model_config_record])

    return Topix100Streak353NextSessionIntradayLightgbmResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=(
            str(feature_panel_df["date"].min()) if not feature_panel_df.empty else None
        ),
        available_end_date=(
            str(feature_panel_df["date"].max()) if not feature_panel_df.empty else None
        ),
        analysis_start_date=(
            str(feature_panel_df["date"].min()) if not feature_panel_df.empty else None
        ),
        analysis_end_date=(
            str(feature_panel_df["date"].max()) if not feature_panel_df.empty else None
        ),
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        top_k_values=resolved_top_k_values,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
        discovery_row_count=int(len(discovery_df)),
        validation_row_count=int(len(validation_df)),
        discovery_date_count=int(discovery_df["date"].nunique()),
        validation_date_count=int(validation_df["date"].nunique()),
        feature_panel_df=feature_panel_df,
        baseline_lookup_df=baseline_lookup_df,
        model_config_df=model_config_df,
        validation_topk_pick_df=validation_topk_pick_df,
        validation_topk_daily_df=validation_topk_daily_df,
        validation_model_summary_df=validation_model_summary_df,
        validation_model_comparison_df=validation_model_comparison_df,
        validation_score_decile_df=validation_score_decile_df,
        feature_importance_df=feature_importance_df,
    )


def _build_feature_panel_df(
    *,
    event_panel_df: pd.DataFrame,
    state_result: Any,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    return _build_feature_panel_from_state_event_df(
        event_panel_df=event_panel_df,
        state_event_df=_coerce_intraday_state_panel_df(state_result),
        price_feature=price_feature,
        volume_feature=volume_feature,
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
    feature_panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    if feature_panel_df.empty:
        raise ValueError("Feature panel was empty after building the next-session target")
    return price_result, feature_panel_df


def _coerce_intraday_state_panel_df(state_source: Any) -> pd.DataFrame:
    if isinstance(state_source, pd.DataFrame):
        state_df = state_source.copy()
    elif hasattr(state_source, "daily_state_panel_df") and isinstance(
        getattr(state_source, "daily_state_panel_df"),
        pd.DataFrame,
    ):
        state_df = cast(pd.DataFrame, getattr(state_source, "daily_state_panel_df")).copy()
    elif hasattr(state_source, "state_event_df") and isinstance(
        getattr(state_source, "state_event_df"),
        pd.DataFrame,
    ):
        state_df = cast(pd.DataFrame, getattr(state_source, "state_event_df")).copy()
    else:
        raise ValueError("Unable to resolve a state panel dataframe")

    if "segment_end_date" in state_df.columns and "date" not in state_df.columns:
        state_df = state_df.rename(columns={"segment_end_date": "date"})
    return state_df


def _build_feature_panel_from_state_event_df(
    *,
    event_panel_df: pd.DataFrame,
    state_event_df: pd.DataFrame,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    if event_panel_df.empty or state_event_df.empty:
        raise ValueError("Base price/state inputs were empty")

    price_df = _build_price_feature_frame(
        event_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    price_df = price_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    grouped = price_df.groupby("code", observed=True, sort=False)
    price_df["next_session_open"] = grouped["open"].shift(-1).astype(float)
    price_df["next_session_close"] = grouped["close"].shift(-1).astype(float)
    price_df["next_session_intraday_return"] = price_df["next_session_close"].div(
        price_df["next_session_open"].replace(0, pd.NA)
    ).sub(1.0)

    state_df = _coerce_intraday_state_panel_df(state_event_df)
    state_columns = [
        "state_event_id",
        "code",
        "company_name",
        "sample_split",
        "segment_id",
        "date",
        "segment_return",
        "segment_day_count",
    ]
    missing_state_columns = [column for column in state_columns if column not in state_df.columns]
    if missing_state_columns:
        raise ValueError(f"Missing state event columns: {missing_state_columns}")

    state_df = state_df[state_columns].copy()
    state_df["date"] = state_df["date"].astype(str)
    state_df["code"] = state_df["code"].astype(str).str.zfill(4)

    merged_df = price_df.merge(
        state_df,
        on=["date", "code", "company_name"],
        how="inner",
        validate="one_to_one",
    )
    merged_df = merged_df.dropna(subset=["next_session_intraday_return"]).copy()
    if merged_df.empty:
        raise ValueError("No next-session intraday targets remained after joining price and state rows")

    merged_df["segment_abs_return"] = merged_df["segment_return"].astype(float).abs()
    ordered_columns = [
        "date",
        "code",
        "company_name",
        "sample_split",
        "state_event_id",
        "segment_id",
        "decile_num",
        "decile",
        price_feature,
        volume_feature,
        "recent_return_1d",
        "recent_return_3d",
        "recent_return_5d",
        "intraday_return",
        "range_pct",
        "segment_return",
        "segment_abs_return",
        "segment_day_count",
        "next_session_intraday_return",
    ]
    return merged_df[ordered_columns].sort_values(
        ["sample_split", "date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
    db_path: str,
    *,
    target_date: str,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    categorical_feature_columns: tuple[str, ...] = DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
    train_lookback_days: int | None = None,
    test_window_days: int = DEFAULT_RUNTIME_TEST_WINDOW_DAYS,
    step_days: int = DEFAULT_RUNTIME_STEP_DAYS,
    purge_signal_dates: int = 0,
    allow_partial_test_window: bool = True,
    connection: Any | None = None,
) -> Topix100Streak353NextSessionIntradayLightgbmSnapshot:
    if connection is not None:
        return _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
            db_path,
            target_date,
            price_feature,
            volume_feature,
            short_window_streaks,
            long_window_streaks,
            categorical_feature_columns,
            train_lookback_days,
            test_window_days,
            step_days,
            purge_signal_dates,
            allow_partial_test_window,
            connection=connection,
        )
    return _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot_cached(
        db_path,
        target_date,
        price_feature,
        volume_feature,
        short_window_streaks,
        long_window_streaks,
        categorical_feature_columns,
        train_lookback_days,
        test_window_days,
        step_days,
        purge_signal_dates,
        allow_partial_test_window,
    )


@lru_cache(maxsize=8)
def _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot_cached(
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    categorical_feature_columns: tuple[str, ...],
    train_lookback_days: int | None,
    test_window_days: int,
    step_days: int,
    purge_signal_dates: int,
    allow_partial_test_window: bool,
) -> Topix100Streak353NextSessionIntradayLightgbmSnapshot:
    return _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
        db_path,
        target_date,
        price_feature,
        volume_feature,
        short_window_streaks,
        long_window_streaks,
        categorical_feature_columns,
        train_lookback_days,
        test_window_days,
        step_days,
        purge_signal_dates,
        allow_partial_test_window,
    )


def _resolve_snapshot_score_source_run_id(
    categorical_feature_columns: tuple[str, ...],
) -> str | None:
    if categorical_feature_columns != DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS:
        return None
    experiment_id = TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID
    bundle_path = find_latest_research_bundle_path(experiment_id)
    if bundle_path is None:
        return None
    return load_research_bundle_info(bundle_path).run_id


def _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    categorical_feature_columns: tuple[str, ...],
    train_lookback_days: int | None,
    test_window_days: int,
    step_days: int,
    purge_signal_dates: int,
    allow_partial_test_window: bool,
    *,
    connection: Any | None = None,
) -> Topix100Streak353NextSessionIntradayLightgbmSnapshot:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if train_lookback_days is not None and train_lookback_days < 1:
        raise ValueError("train_lookback_days must be >= 1 when provided")
    if test_window_days < 1:
        raise ValueError("test_window_days must be >= 1")
    if step_days < 1:
        raise ValueError("step_days must be >= 1")
    if purge_signal_dates < 0:
        raise ValueError("purge_signal_dates must be >= 0")

    score_source_run_id = _resolve_snapshot_score_source_run_id(
        categorical_feature_columns
    )
    train_window_days = train_lookback_days or DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS
    price_feature_to_window = {
        feature: window
        for feature, window in zip(PRICE_FEATURE_ORDER, PRICE_SMA_WINDOW_ORDER, strict=True)
    }
    volume_feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }

    if connection is None:
        with _open_analysis_connection(db_path) as ctx:
            history_df = _query_topix100_stock_history(
                ctx.connection,
                end_date=target_date,
            )
    else:
        history_df = _query_topix100_stock_history(
            connection,
            end_date=target_date,
        )
    if history_df.empty:
        return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            score_model_type="daily_refit",
            train_window_days=train_window_days,
            test_window_days=1,
            step_days=1,
            split_train_start=None,
            split_train_end=None,
            split_test_start=None,
            split_test_end=None,
            split_is_partial_tail=False,
            rows_by_code={},
        )

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=None,
        analysis_end_date=target_date,
        min_constituents_per_day=1,
        price_sma_windows=(price_feature_to_window[price_feature],),
        volume_sma_windows=(volume_feature_to_window[volume_feature],),
    )
    if event_panel_df.empty:
        return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            score_model_type="daily_refit",
            train_window_days=train_window_days,
            test_window_days=1,
            step_days=1,
            split_train_start=None,
            split_train_end=None,
            split_test_start=None,
            split_test_end=None,
            split_is_partial_tail=False,
            rows_by_code={},
        )

    try:
        state_panel_df = build_topix100_streak_daily_state_panel_df(
            history_df,
            analysis_end_date=target_date,
            validation_ratio=None,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
        full_feature_panel_df = _build_feature_panel_from_state_event_df(
            event_panel_df=event_panel_df,
            state_event_df=state_panel_df,
            price_feature=price_feature,
            volume_feature=volume_feature,
        )
    except ValueError:
        return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            score_model_type="daily_refit",
            train_window_days=train_window_days,
            test_window_days=1,
            step_days=1,
            split_train_start=None,
            split_train_end=None,
            split_test_start=None,
            split_test_end=None,
            split_is_partial_tail=False,
            rows_by_code={},
        )
    training_source_df = (
        full_feature_panel_df[full_feature_panel_df["date"].astype(str) < target_date]
        .copy()
        .reset_index(drop=True)
    )
    training_feature_panel_df, train_start, train_end = _slice_feature_panel_to_recent_dates(
        training_source_df,
        max_date_count=train_window_days,
    )
    if training_feature_panel_df.empty or train_start is None or train_end is None:
        return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            score_model_type="daily_refit",
            train_window_days=train_window_days,
            test_window_days=1,
            step_days=1,
            split_train_start=None,
            split_train_end=None,
            split_test_start=None,
            split_test_end=None,
            split_is_partial_tail=False,
            rows_by_code={},
        )
    snapshot_feature_df = (
        full_feature_panel_df[full_feature_panel_df["date"].astype(str) == target_date]
        .copy()
        .reset_index(drop=True)
    )
    if snapshot_feature_df.empty:
        snapshot_feature_df = _build_scoring_snapshot_df(
            event_panel_df=event_panel_df,
            history_df=history_df,
            target_date=target_date,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
    if snapshot_feature_df.empty:
        return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            score_model_type="daily_refit",
            train_window_days=train_window_days,
            test_window_days=1,
            step_days=1,
            split_train_start=train_start,
            split_train_end=train_end,
            split_test_start=None,
            split_test_end=None,
            split_is_partial_tail=False,
            rows_by_code={},
        )
    category_source_df = pd.concat(
        [training_feature_panel_df, snapshot_feature_df],
        ignore_index=True,
    )
    regressor_cls = _load_lightgbm_regressor_cls()
    categories = _build_category_lookup(category_source_df)
    feature_columns = [
        *categorical_feature_columns,
        price_feature,
        volume_feature,
        *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    ]
    intraday_scores = _predict_lightgbm_snapshot_scores(
        training_feature_panel_df=training_feature_panel_df,
        snapshot_df=snapshot_feature_df,
        target_column="next_session_intraday_return",
        regressor_cls=regressor_cls,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
        categories=categories,
    )

    rows_by_code: dict[str, Topix100Streak353NextSessionIntradayLightgbmSnapshotRow] = {}
    for row in snapshot_feature_df.to_dict(orient="records"):
        normalized_row = {str(key): value for key, value in row.items()}
        code = str(normalized_row["code"])
        score_value = intraday_scores.get(code)
        rows_by_code[code] = Topix100Streak353NextSessionIntradayLightgbmSnapshotRow(
            code=code,
            company_name=str(normalized_row["company_name"]),
            date=str(normalized_row["date"]),
            intraday_score=(
                float(score_value) if score_value is not None and pd.notna(score_value) else None
            ),
        )

    return Topix100Streak353NextSessionIntradayLightgbmSnapshot(
        score_source_run_id=score_source_run_id,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        score_model_type="daily_refit",
        train_window_days=train_window_days,
        test_window_days=1,
        step_days=1,
        split_train_start=train_start,
        split_train_end=train_end,
        split_test_start=None,
        split_test_end=None,
        split_is_partial_tail=False,
        rows_by_code=rows_by_code,
    )


def _slice_feature_panel_by_date_range(
    feature_panel_df: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    date_values = feature_panel_df["date"].astype(str)
    return (
        feature_panel_df[(date_values >= start_date) & (date_values <= end_date)]
        .copy()
        .reset_index(drop=True)
    )


def _slice_feature_panel_to_recent_dates(
    feature_panel_df: pd.DataFrame,
    *,
    max_date_count: int,
) -> tuple[pd.DataFrame, str | None, str | None]:
    ordered_dates = (
        feature_panel_df["date"].astype(str).drop_duplicates().sort_values(kind="stable").tolist()
    )
    if not ordered_dates:
        return feature_panel_df.iloc[0:0].copy(), None, None
    selected_dates = ordered_dates[-max_date_count:]
    start_date = selected_dates[0]
    end_date = selected_dates[-1]
    return (
        _slice_feature_panel_by_date_range(
            feature_panel_df,
            start_date=start_date,
            end_date=end_date,
        ),
        start_date,
        end_date,
    )


def _resolve_runtime_walkforward_split(  # pyright: ignore[reportUnusedFunction]
    *,
    feature_panel_df: pd.DataFrame,
    target_date: str,
    snapshot_df: pd.DataFrame,
    train_window_days: int,
    test_window_days: int,
    step_days: int,
    purge_signal_dates: int,
    allow_partial_test_window: bool,
) -> _RuntimeWalkforwardSplit | None:
    signal_dates = (
        feature_panel_df["date"].astype(str).drop_duplicates().sort_values(kind="stable").tolist()
    )
    if not snapshot_df.empty and target_date not in set(signal_dates):
        signal_dates.append(target_date)
    if not signal_dates:
        return None

    ordered_dates = pd.DatetimeIndex(pd.to_datetime(signal_dates)).sort_values().unique()
    complete_splits = generate_walkforward_splits(
        ordered_dates,
        train_window=train_window_days,
        test_window=test_window_days,
        step=step_days,
        purge_window=purge_signal_dates,
    )
    for split in complete_splits:
        if split.test_start <= target_date <= split.test_end:
            return _RuntimeWalkforwardSplit(
                train_start=split.train_start,
                train_end=split.train_end,
                test_start=split.test_start,
                test_end=split.test_end,
                is_partial_tail=False,
            )

    if not allow_partial_test_window:
        return None

    total = len(ordered_dates)
    start_index = 0
    while start_index + train_window_days + purge_signal_dates < total:
        test_start_index = start_index + train_window_days + purge_signal_dates
        full_test_end_index = test_start_index + test_window_days - 1
        if full_test_end_index < total:
            start_index += step_days
            continue

        partial_split = _RuntimeWalkforwardSplit(
            train_start=ordered_dates[start_index].date().isoformat(),
            train_end=ordered_dates[start_index + train_window_days - 1].date().isoformat(),
            test_start=ordered_dates[test_start_index].date().isoformat(),
            test_end=ordered_dates[-1].date().isoformat(),
            is_partial_tail=True,
        )
        if partial_split.test_start <= target_date <= partial_split.test_end:
            return partial_split
        break

    return None


def _build_baseline_selector_value_key(
    selector_kind: str,
    values: dict[str, str],
) -> str:
    if selector_kind == "universe":
        return "universe"
    if selector_kind == "bucket":
        return values["bucket"]
    raise ValueError(f"Unsupported selector kind: {selector_kind}")


def _build_baseline_lookup_df(feature_panel_df: pd.DataFrame) -> pd.DataFrame:
    discovery_df = feature_panel_df[feature_panel_df["sample_split"] == "discovery"].copy()
    if discovery_df.empty:
        raise ValueError("Feature panel has no discovery rows for baseline lookup")

    records: list[dict[str, Any]] = []

    universe_daily_df = (
        discovery_df.groupby("date", observed=True)
        .agg(
            target_return_mean=("next_session_intraday_return", "mean"),
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
                target_return_mean=("next_session_intraday_return", "mean"),
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

    baseline_lookup_df = pd.DataFrame.from_records(records)
    return baseline_lookup_df.sort_values(
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
        weight = row.date_count / (row.date_count + _BASELINE_BLEND_PRIOR)
        current_value = current_value * (1.0 - weight) + row.avg_target_return * weight
    return float(current_value)


def _build_baseline_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    baseline_scorecard: _BaselineScorecard,
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
    validation_df["realized_return"] = validation_df["next_session_intraday_return"].astype(float)
    return validation_df[
        [
            "model_name",
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "score",
            "realized_return",
        ]
    ].copy()


def _build_lightgbm_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    regressor_cls: type[Any],
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    target_column = "next_session_intraday_return"
    training_df = feature_panel_df[
        (feature_panel_df["sample_split"] == "discovery")
        & feature_panel_df[target_column].notna()
    ].copy()
    validation_df = feature_panel_df[
        (feature_panel_df["sample_split"] == "validation")
        & feature_panel_df[target_column].notna()
    ].copy()
    if training_df.empty:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            "No discovery rows were available for the next-session intraday target."
        )
    if validation_df.empty:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            "No validation rows were available for the next-session intraday target."
        )

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
    prediction_df = validation_df[
        [
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            target_column,
        ]
    ].copy()
    prediction_df["score"] = predictions
    prediction_df["model_name"] = "lightgbm"
    prediction_df["realized_return"] = prediction_df[target_column].astype(float)
    prediction_df = prediction_df[
        [
            "model_name",
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "score",
            "realized_return",
        ]
    ].copy()

    importance_values = pd.Series(
        getattr(regressor, "feature_importances_", []),
        dtype=float,
    )
    if len(importance_values) != len(feature_columns):
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            "Unexpected LightGBM feature importance output."
        )
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

    config_record = {
        "model_name": "lightgbm",
        "target_column": target_column,
        "training_row_count": int(len(training_df)),
        "training_date_count": int(training_df["date"].nunique()),
        "validation_row_count": int(len(validation_df)),
        "validation_date_count": int(validation_df["date"].nunique()),
        "categorical_feature_columns": json.dumps(list(categorical_feature_columns)),
        "continuous_feature_columns": json.dumps(list(continuous_feature_columns)),
        "params_json": json.dumps(_build_lightgbm_params(), sort_keys=True),
    }
    return prediction_df, config_record, feature_importance_df


def _build_validation_topk_tables(
    validation_prediction_df: pd.DataFrame,
    *,
    top_k_values: tuple[int, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ranked_df = validation_prediction_df.copy()
    ranked_df["long_rank"] = (
        ranked_df.groupby(["model_name", "date"], observed=True)["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    ranked_df["short_rank"] = (
        ranked_df.groupby(["model_name", "date"], observed=True)["score"]
        .rank(method="first", ascending=True)
        .astype(int)
    )
    universe_daily_df = (
        ranked_df.groupby(["model_name", "date"], observed=True)
        .agg(
            universe_return_mean=("realized_return", "mean"),
            universe_stock_count=("code", "nunique"),
        )
        .reset_index()
    )

    pick_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for top_k in top_k_values:
        long_selected_df = ranked_df[ranked_df["long_rank"] <= top_k].copy()
        short_selected_df = ranked_df[ranked_df["short_rank"] <= top_k].copy()
        if long_selected_df.empty or short_selected_df.empty:
            continue

        long_selected_df["selection_side"] = "long"
        long_selected_df["selection_rank"] = long_selected_df["long_rank"]
        long_selected_df["target_edge"] = long_selected_df["realized_return"].astype(float)
        long_selected_df["top_k"] = int(top_k)

        short_selected_df["selection_side"] = "short"
        short_selected_df["selection_rank"] = short_selected_df["short_rank"]
        short_selected_df["target_edge"] = -short_selected_df["realized_return"].astype(float)
        short_selected_df["top_k"] = int(top_k)

        pick_frames.extend([long_selected_df.copy(), short_selected_df.copy()])

        long_daily_df = (
            long_selected_df.groupby(["model_name", "top_k", "date"], observed=True)
            .agg(
                long_return_mean=("realized_return", "mean"),
                long_edge_mean=("target_edge", "mean"),
                long_stock_count=("code", "nunique"),
                long_score_mean=("score", "mean"),
            )
            .reset_index()
        )
        short_daily_df = (
            short_selected_df.groupby(["model_name", "top_k", "date"], observed=True)
            .agg(
                short_return_mean=("realized_return", "mean"),
                short_edge_mean=("target_edge", "mean"),
                short_stock_count=("code", "nunique"),
                short_score_mean=("score", "mean"),
            )
            .reset_index()
        )
        daily_df = long_daily_df.merge(
            short_daily_df,
            on=["model_name", "top_k", "date"],
            how="inner",
            validate="one_to_one",
        )
        daily_df = daily_df.merge(
            universe_daily_df,
            on=["model_name", "date"],
            how="left",
            validate="many_to_one",
        )
        daily_df["long_short_spread"] = (
            daily_df["long_return_mean"] - daily_df["short_return_mean"]
        )
        daily_df["gross_edge"] = (
            daily_df["long_edge_mean"] + daily_df["short_edge_mean"]
        )
        daily_df["spread_vs_universe"] = (
            daily_df["long_short_spread"] - daily_df["universe_return_mean"]
        )
        daily_frames.append(daily_df)

    if not pick_frames or not daily_frames:
        raise ValueError("Validation top-k evaluation produced no rows")

    pick_df = pd.concat(pick_frames, ignore_index=True)
    pick_df = pick_df[
        [
            "model_name",
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "selection_side",
            "selection_rank",
            "top_k",
            "score",
            "target_edge",
            "realized_return",
        ]
    ].sort_values(
        ["model_name", "top_k", "selection_side", "date", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df = pd.concat(daily_frames, ignore_index=True)
    daily_df = daily_df.sort_values(
        ["model_name", "top_k", "date"],
        kind="stable",
    ).reset_index(drop=True)
    return pick_df, daily_df


def _build_validation_model_summary_df(
    validation_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = (
        validation_topk_daily_df.groupby(
            ["model_name", "top_k"],
            observed=True,
            sort=False,
        )
        .agg(
            date_count=("date", "nunique"),
            avg_long_return=("long_return_mean", "mean"),
            long_hit_rate_positive_return=(
                "long_return_mean",
                lambda values: float((values > 0).mean()),
            ),
            avg_short_edge=("short_edge_mean", "mean"),
            short_hit_rate_positive_edge=(
                "short_edge_mean",
                lambda values: float((values > 0).mean()),
            ),
            avg_short_return=("short_return_mean", "mean"),
            avg_long_short_spread=("long_short_spread", "mean"),
            spread_hit_rate_positive=(
                "long_short_spread",
                lambda values: float((values > 0).mean()),
            ),
            avg_gross_edge=("gross_edge", "mean"),
            avg_spread_vs_universe=("spread_vs_universe", "mean"),
            avg_universe_return=("universe_return_mean", "mean"),
            avg_long_stock_count=("long_stock_count", "mean"),
            avg_short_stock_count=("short_stock_count", "mean"),
            avg_long_score=("long_score_mean", "mean"),
            avg_short_score=("short_score_mean", "mean"),
        )
        .reset_index()
    )
    return summary_df.sort_values(
        ["top_k", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_validation_model_comparison_df(
    validation_model_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for _, group in validation_model_summary_df.groupby("top_k", observed=True, sort=False):
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
                "baseline_avg_short_edge": float(baseline["avg_short_edge"]),
                "lightgbm_avg_short_edge": float(lightgbm["avg_short_edge"]),
                "short_edge_lift_vs_baseline": float(lightgbm["avg_short_edge"])
                - float(baseline["avg_short_edge"]),
                "baseline_avg_long_short_spread": float(
                    baseline["avg_long_short_spread"]
                ),
                "lightgbm_avg_long_short_spread": float(
                    lightgbm["avg_long_short_spread"]
                ),
                "spread_lift_vs_baseline": float(lightgbm["avg_long_short_spread"])
                - float(baseline["avg_long_short_spread"]),
                "baseline_spread_hit_rate_positive": float(
                    baseline["spread_hit_rate_positive"]
                ),
                "lightgbm_spread_hit_rate_positive": float(
                    lightgbm["spread_hit_rate_positive"]
                ),
                "spread_hit_rate_lift_vs_baseline": float(
                    lightgbm["spread_hit_rate_positive"]
                )
                - float(baseline["spread_hit_rate_positive"]),
            }
        )
    comparison_df = pd.DataFrame.from_records(records)
    if comparison_df.empty:
        return comparison_df
    return comparison_df.sort_values(["top_k"], kind="stable").reset_index(drop=True)


def _build_validation_score_decile_df(
    validation_prediction_df: pd.DataFrame,
) -> pd.DataFrame:
    ranked_df = validation_prediction_df.copy()
    ranked_df["date_constituent_count"] = ranked_df.groupby(
        ["model_name", "date"],
        observed=True,
    )["code"].transform("size")
    ranked_df["score_rank_desc"] = ranked_df.groupby(
        ["model_name", "date"],
        observed=True,
    )["score"].rank(method="first", ascending=False)
    ranked_df["score_decile_index"] = (
        ((ranked_df["score_rank_desc"] - 1) * len(DECILE_ORDER))
        // ranked_df["date_constituent_count"]
    ) + 1
    ranked_df["score_decile_index"] = ranked_df["score_decile_index"].clip(
        1, len(DECILE_ORDER)
    )
    ranked_df["score_decile"] = ranked_df["score_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    return (
        ranked_df.groupby(
            ["model_name", "score_decile_index", "score_decile"],
            observed=True,
            sort=False,
        )
        .agg(
            mean_realized_return=("realized_return", "mean"),
            stock_count=("code", "count"),
            date_count=("date", "nunique"),
        )
        .reset_index()
        .sort_values(["model_name", "score_decile_index"], kind="stable")
        .reset_index(drop=True)
    )


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353NextSessionIntradayLightgbmResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.validation_model_comparison_df,
        top_k=primary_top_k,
    )
    top_feature = _select_top_feature(result.feature_importance_df)

    lines = [
        "# TOPIX100 Streak 3/53 Next-Session Intraday LightGBM",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Fixed state pair: `{result.short_window_streaks} / {result.long_window_streaks}` streaks",
        "- Target: `next-session open -> close return`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        f"- Discovery / validation rows: `{result.discovery_row_count} / {result.validation_row_count}`",
        "",
        "## Current Read",
        "",
        "This study predicts the next trading session's open-to-close return. The intended action is to buy the highest predicted names at the next open and short the lowest predicted names at the same open, then close both legs at the same-day close.",
    ]
    if comparison_row is not None:
        lines.extend(
            [
                f"- Top {primary_top_k} long: baseline `{_format_return(float(comparison_row['baseline_avg_long_return']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_long_return']))}`, lift `{_format_return(float(comparison_row['long_return_lift_vs_baseline']))}`.",
                f"- Bottom {primary_top_k} short edge: baseline `{_format_return(float(comparison_row['baseline_avg_short_edge']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_short_edge']))}`, lift `{_format_return(float(comparison_row['short_edge_lift_vs_baseline']))}`.",
                f"- Top/Bottom {primary_top_k} long-short spread: baseline `{_format_return(float(comparison_row['baseline_avg_long_short_spread']))}`, LightGBM `{_format_return(float(comparison_row['lightgbm_avg_long_short_spread']))}`, lift `{_format_return(float(comparison_row['spread_lift_vs_baseline']))}`.",
            ]
        )
    if top_feature is not None:
        lines.append(
            f"- Feature importance leader: `{top_feature['feature_name']}` at share `{float(top_feature['importance_share']):.2%}`."
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `validation_model_summary_df`: validation scorecard for top-k long, bottom-k short, and combined spread",
            "- `validation_model_comparison_df`: LightGBM lift vs the rebuilt lookup baseline",
            "- `validation_topk_daily_df`: date-level long/short/spread outcomes for each model and top-k",
            "- `feature_importance_df`: LightGBM feature importance for the signed next-session intraday target",
            "- `validation_score_decile_df`: monotonicity check for predicted return deciles",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionIntradayLightgbmResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    comparison_row = _select_comparison_row(
        result.validation_model_comparison_df,
        top_k=primary_top_k,
    )
    best_summary_row = _select_model_summary_row(
        result.validation_model_summary_df,
        model_name="lightgbm",
        top_k=primary_top_k,
    )
    top_feature = _select_top_feature(result.feature_importance_df)

    if comparison_row is not None and float(comparison_row["spread_lift_vs_baseline"]) >= 0.0:
        headline = (
            f"On the fixed split, LightGBM improves the next-session intraday ranking at Top/Bottom {primary_top_k}, "
            "so the same 3/53 state family still helps after switching from multi-day closes to next-day open/close."
        )
    else:
        headline = (
            "This study tests whether the existing 3/53 feature family can rank the next trading session's open-to-close return."
        )

    result_bullets = [
        "The target is one signed return: next-session close divided by next-session open minus one. Long and short legs are read from the same predicted-return ranking.",
        "The baseline is rebuilt from discovery only using the same bucket / volume / short-mode / long-mode family, then compared with a LightGBM model that adds continuous features.",
    ]
    if comparison_row is not None:
        result_bullets.extend(
            [
                f"For Top {primary_top_k} longs, LightGBM delivered {_format_return(float(comparison_row['lightgbm_avg_long_return']))} versus baseline {_format_return(float(comparison_row['baseline_avg_long_return']))}.",
                f"For Bottom {primary_top_k} shorts, LightGBM delivered {_format_return(float(comparison_row['lightgbm_avg_short_edge']))} of short edge versus baseline {_format_return(float(comparison_row['baseline_avg_short_edge']))}.",
                f"The combined Top/Bottom {primary_top_k} spread was {_format_return(float(comparison_row['lightgbm_avg_long_short_spread']))} for LightGBM versus {_format_return(float(comparison_row['baseline_avg_long_short_spread']))} for baseline.",
            ]
        )
    if top_feature is not None:
        result_bullets.append(
            f"The strongest feature was {top_feature['feature_name']} with gain share {float(top_feature['importance_share']):.2%}."
        )

    highlights = [
        {
            "label": "State pair",
            "value": f"{result.short_window_streaks} / {result.long_window_streaks}",
            "tone": "accent",
            "detail": "streaks",
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

    return {
        "title": "TOPIX100 Streak 3/53 Next-Session Intraday LightGBM",
        "tags": ["TOPIX100", "streaks", "lightgbm", "intraday"],
        "purpose": (
            "Test whether the existing TOPIX100 streak 3/53 feature family can rank the next trading session's intraday return, "
            "so the strategy can buy the top names at the next open and short the bottom names at the same open."
        ),
        "method": [
            "Build the same TOPIX100 3/53 stock-date panel as the existing stage-2 study, keeping exact decile, volume split, short mode, long mode, and continuous price/volume/streak features.",
            "Define the target as next-session close divided by next-session open minus one, and train a single LightGBM regressor on discovery rows.",
            "On validation, rank each day by predicted return, then evaluate the top-k long basket, the bottom-k short basket, and the equal-weight long-short spread against a rebuilt lookup baseline.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This is still a fixed discovery/validation split. It is a useful first strategy screen, but not the final overfitting check.",
            "The target uses next-session open and close, so any real deployment must account for open auction slippage, availability, and short borrow constraints.",
            "The bottom basket is evaluated as short edge, not raw return. A positive short edge means the basket went down intraday.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {"label": "Target", "value": "next-session close / open - 1"},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "validation_model_summary_df",
                "label": "Validation strategy scorecard",
                "description": "Top-k long return, bottom-k short edge, and combined long-short spread for each model.",
            },
            {
                "name": "validation_model_comparison_df",
                "label": "LightGBM lift vs baseline",
                "description": "Direct comparison table showing how much the model improves the long leg, short leg, and combined spread.",
            },
            {
                "name": "validation_topk_daily_df",
                "label": "Daily strategy outcomes",
                "description": "Date-level long/short/spread results for the top and bottom predicted baskets.",
            },
            {
                "name": "feature_importance_df",
                "label": "Feature importance",
                "description": "Gain-based LightGBM feature importance for the signed next-session intraday return target.",
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
    if comparison_df.empty:
        return None
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


def _select_top_feature(
    feature_importance_df: pd.DataFrame,
) -> pd.Series | None:
    if feature_importance_df.empty:
        return None
    scoped_df = feature_importance_df.sort_values(
        ["importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    return scoped_df.iloc[0]


def write_topix100_streak_353_next_session_intraday_lightgbm_research_bundle(
    result: Topix100Streak353NextSessionIntradayLightgbmResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_intraday_lightgbm_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "top_k_values": list(result.top_k_values),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_next_session_intraday_lightgbm_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionIntradayLightgbmResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionIntradayLightgbmResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_intraday_lightgbm_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_intraday_lightgbm_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_LIGHTGBM_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
