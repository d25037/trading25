"""
Stage-2 score research for TOPIX100 streak 3/53 ranking.

This study asks whether the stage-1 lookup score can be improved by adding
continuous features and fitting a LightGBM model on the same discovery split.

Targets:
- long score: future 5-day return
- short score: future 1-day downside, expressed as positive short edge

Design:
- Rebuild the stage-1 baseline from discovery only, using the same bucket /
  volume / short-mode / long-mode lookup chain.
- Train separate LightGBM regressors for the long and short targets on the same
  discovery rows.
- Evaluate both on validation with a daily top-k ranking lens.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from itertools import combinations
import json
from pathlib import Path
from typing import Any, cast

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
    VolumeBucketKey,
    VOLUME_BUCKET_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    _enrich_event_panel,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    build_topix100_streak_state_snapshot_df,
    build_topix100_streak_daily_state_panel_df,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    DECILE_ORDER,
    _query_topix100_stock_history,
)
from src.domains.analytics.topix_streak_extreme_mode import (
    _format_int_sequence,
    _format_return,
)

LIGHTGBM_RESEARCH_INSTALL_HINT = "uv sync --project apps/bt --group research"
LIGHTGBM_LIBOMP_INSTALL_HINT = "brew install libomp"
DEFAULT_LONG_TARGET_HORIZON_DAYS = 5
DEFAULT_SHORT_TARGET_HORIZON_DAYS = 1
TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_LONG_SCORE_HORIZON_DAYS = (
    DEFAULT_LONG_TARGET_HORIZON_DAYS
)
TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_SHORT_SCORE_HORIZON_DAYS = (
    DEFAULT_SHORT_TARGET_HORIZON_DAYS
)
DEFAULT_TOP_K_VALUES: tuple[int, ...] = (5, 10, 20)
DEFAULT_CATEGORICAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "decile",
    "volume_bucket",
    "short_mode",
    "long_mode",
)
DEFAULT_CONTINUOUS_FEATURE_SUFFIXES: tuple[str, ...] = (
    "recent_return_1d",
    "recent_return_3d",
    "recent_return_5d",
    "intraday_return",
    "range_pct",
    "segment_return",
    "segment_abs_return",
    "segment_day_count",
)
TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-signal-score-lightgbm"
)
_LONG_BLEND_PRIOR = 260.0
_SHORT_BLEND_PRIOR = 320.0
_FEATURE_ORDER: tuple[str, ...] = ("bucket", "volume", "short_mode", "long_mode")
_FEATURE_LABEL_MAP: dict[str, str] = {
    "bucket": "Bucket",
    "volume": "Volume",
    "short_mode": "Short mode",
    "long_mode": "Long mode",
}
_MODE_VALUE_LABEL_MAP: dict[str, str] = {
    "bullish": "Bullish",
    "bearish": "Bearish",
}
_ALL_SENTINEL = "all"
_LONG_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("short_mode", "short_mode"),
    ("bucket+short_mode", "bucket+short_mode"),
    ("bucket+short_mode+long_mode", "bucket+short_mode+long_mode"),
    ("bucket+volume+short_mode+long_mode", "full"),
)
_SHORT_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("short_mode", "short_mode"),
    ("volume+short_mode", "volume+short_mode"),
    ("volume+short_mode+long_mode", "volume+short_mode+long_mode"),
    ("bucket+volume+short_mode+long_mode", "full"),
)
_SIDE_ORDER: tuple[str, ...] = ("long", "short")
_MODEL_ORDER: tuple[str, ...] = ("baseline", "lightgbm")
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
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


class Topix100Streak353SignalScoreLightgbmResearchError(RuntimeError):
    """Raised when the LightGBM stage-2 path cannot run."""


@dataclass(frozen=True)
class _BaselineLookupRow:
    subset_key: str
    selector_value_key: str
    avg_return_1d: float
    avg_return_5d: float
    date_count_1d: int
    date_count_5d: int


@dataclass(frozen=True)
class _BaselineScorecard:
    universe_long_score_5d: float
    universe_short_score_1d: float
    rows_by_subset: dict[str, dict[str, _BaselineLookupRow]]


@dataclass(frozen=True)
class Topix100Streak353SignalScoreLightgbmResearchResult:
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
class Topix100Streak353SignalScoreLightgbmSnapshotRow:
    code: str
    company_name: str
    date: str
    short_mode: str | None
    long_mode: str | None
    state_key: str | None
    state_label: str | None
    long_score_5d: float | None
    short_score_1d: float | None


@dataclass(frozen=True)
class Topix100Streak353SignalScoreLightgbmSnapshot:
    score_source_run_id: str | None
    price_feature: str
    volume_feature: str
    short_window_streaks: int
    long_window_streaks: int
    long_target_horizon_days: int
    short_target_horizon_days: int
    rows_by_code: dict[str, Topix100Streak353SignalScoreLightgbmSnapshotRow]


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


def format_topix100_streak_353_signal_score_lightgbm_notebook_error(
    exc: Exception,
) -> str:
    if isinstance(exc, Topix100Streak353SignalScoreLightgbmResearchError):
        return str(exc)
    if isinstance(exc, ModuleNotFoundError):
        return _missing_lightgbm_message()
    if isinstance(exc, OSError):
        return _lightgbm_runtime_message(str(exc))
    return str(exc)


def _load_lightgbm_regressor_cls() -> type[Any]:
    try:
        lightgbm_module = import_module("lightgbm")
    except ModuleNotFoundError as exc:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            _missing_lightgbm_message()
        ) from exc
    except OSError as exc:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            _lightgbm_runtime_message(str(exc))
        ) from exc

    regressor_cls = getattr(lightgbm_module, "LGBMRegressor", None)
    if regressor_cls is None:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            "LightGBM research is unavailable because lightgbm.LGBMRegressor "
            "could not be imported."
        )
    return cast(type[Any], regressor_cls)


def run_topix100_streak_353_signal_score_lightgbm_research(
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
) -> Topix100Streak353SignalScoreLightgbmResearchResult:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if long_target_horizon_days <= 0 or short_target_horizon_days <= 0:
        raise ValueError("target horizons must be positive")

    resolved_top_k_values = _normalize_positive_int_sequence(
        top_k_values,
        default=DEFAULT_TOP_K_VALUES,
        name="top_k_values",
    )
    requested_horizons = tuple(
        sorted({int(long_target_horizon_days), int(short_target_horizon_days)})
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
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )
    if feature_panel_df.empty:
        raise ValueError("Feature panel was empty after joining price and state inputs")
    state_decile_horizon_panel_df = _build_state_decile_horizon_panel_from_feature_panel_df(
        feature_panel_df,
        future_horizons=requested_horizons,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )
    baseline_lookup_df = _build_baseline_lookup_df(
        state_decile_horizon_panel_df,
        future_horizons=requested_horizons,
    )
    baseline_scorecard = _build_baseline_scorecard(baseline_lookup_df)

    categorical_feature_columns = DEFAULT_CATEGORICAL_FEATURE_COLUMNS
    continuous_feature_columns = (
        price_feature,
        volume_feature,
        *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    )

    validation_prediction_frames: list[pd.DataFrame] = []
    feature_importance_frames: list[pd.DataFrame] = []
    model_config_records: list[dict[str, Any]] = []

    baseline_prediction_df = _build_baseline_validation_prediction_df(
        feature_panel_df,
        baseline_scorecard=baseline_scorecard,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )
    validation_prediction_frames.append(baseline_prediction_df)

    regressor_cls = _load_lightgbm_regressor_cls()
    for side in _SIDE_ORDER:
        prediction_df, config_record, importance_df = _build_lightgbm_validation_prediction_df(
            feature_panel_df,
            side=side,
            regressor_cls=regressor_cls,
            categorical_feature_columns=categorical_feature_columns,
            continuous_feature_columns=continuous_feature_columns,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
        )
        validation_prediction_frames.append(prediction_df)
        model_config_records.append(config_record)
        feature_importance_frames.append(importance_df)

    validation_prediction_df = pd.concat(validation_prediction_frames, ignore_index=True)
    validation_prediction_df = validation_prediction_df.sort_values(
        ["side", "model_name", "date", "score", "code"],
        ascending=[True, True, True, False, True],
        kind="stable",
    ).reset_index(drop=True)

    validation_topk_pick_df, validation_topk_daily_df = _build_validation_topk_tables(
        validation_prediction_df,
        top_k_values=resolved_top_k_values,
    )
    validation_model_summary_df = _build_validation_model_summary_df(
        validation_topk_daily_df
    )
    validation_model_comparison_df = _build_validation_model_comparison_df(
        validation_model_summary_df
    )
    validation_score_decile_df = _build_validation_score_decile_df(
        validation_prediction_df
    )
    feature_importance_df = pd.concat(feature_importance_frames, ignore_index=True)
    feature_importance_df = feature_importance_df.sort_values(
        ["side", "importance_rank", "feature_name"],
        kind="stable",
    ).reset_index(drop=True)
    model_config_df = pd.DataFrame.from_records(model_config_records)
    model_config_df = model_config_df.sort_values(
        ["side", "model_name"],
        kind="stable",
    ).reset_index(drop=True)

    discovery_df = feature_panel_df[feature_panel_df["sample_split"] == "discovery"].copy()
    validation_df = feature_panel_df[feature_panel_df["sample_split"] == "validation"].copy()

    return Topix100Streak353SignalScoreLightgbmResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=price_result.available_start_date,
        available_end_date=price_result.available_end_date,
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
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
        top_k_values=resolved_top_k_values,
        categorical_feature_columns=categorical_feature_columns,
        continuous_feature_columns=continuous_feature_columns,
        discovery_row_count=int(len(discovery_df)),
        validation_row_count=int(len(validation_df)),
        discovery_date_count=int(discovery_df["date"].nunique()) if not discovery_df.empty else 0,
        validation_date_count=int(validation_df["date"].nunique()) if not validation_df.empty else 0,
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
    long_target_horizon_days: int,
    short_target_horizon_days: int,
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
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )
    return price_result, feature_panel_df


def write_topix100_streak_353_signal_score_lightgbm_research_bundle(
    result: Topix100Streak353SignalScoreLightgbmResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_signal_score_lightgbm_research",
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
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_signal_score_lightgbm_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353SignalScoreLightgbmResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353SignalScoreLightgbmResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_signal_score_lightgbm_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_SIGNAL_SCORE_LIGHTGBM_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def score_topix100_streak_353_signal_lightgbm_snapshot(
    db_path: str,
    *,
    target_date: str,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    long_target_horizon_days: int = DEFAULT_LONG_TARGET_HORIZON_DAYS,
    short_target_horizon_days: int = DEFAULT_SHORT_TARGET_HORIZON_DAYS,
    connection: Any | None = None,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    if connection is not None:
        return _score_topix100_streak_353_signal_lightgbm_snapshot(
            db_path,
            target_date,
            price_feature,
            volume_feature,
            short_window_streaks,
            long_window_streaks,
            long_target_horizon_days,
            short_target_horizon_days,
            connection=connection,
        )
    return _score_topix100_streak_353_signal_lightgbm_snapshot_cached(
        db_path,
        target_date,
        price_feature,
        volume_feature,
        short_window_streaks,
        long_window_streaks,
        long_target_horizon_days,
        short_target_horizon_days,
    )


@lru_cache(maxsize=8)
def _score_topix100_streak_353_signal_lightgbm_snapshot_cached(
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    return _score_topix100_streak_353_signal_lightgbm_snapshot(
        db_path,
        target_date,
        price_feature,
        volume_feature,
        short_window_streaks,
        long_window_streaks,
        long_target_horizon_days,
        short_target_horizon_days,
    )


def _score_topix100_streak_353_signal_lightgbm_snapshot(
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
    *,
    connection: Any | None = None,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")

    bundle_path = get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path()
    score_source_run_id = (
        load_research_bundle_info(bundle_path).run_id if bundle_path is not None else None
    )
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
        return Topix100Streak353SignalScoreLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
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
        return Topix100Streak353SignalScoreLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
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
        training_feature_panel_df = _build_feature_panel_from_state_df(
            event_panel_df=event_panel_df,
            state_event_df=state_panel_df,
            price_feature=price_feature,
            volume_feature=volume_feature,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
        )
        snapshot_df = _build_scoring_snapshot_df(
            event_panel_df=event_panel_df,
            history_df=history_df,
            target_date=target_date,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
    except ValueError:
        return Topix100Streak353SignalScoreLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
            rows_by_code={},
        )
    if snapshot_df.empty:
        return Topix100Streak353SignalScoreLightgbmSnapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            long_target_horizon_days=long_target_horizon_days,
            short_target_horizon_days=short_target_horizon_days,
            rows_by_code={},
        )

    regressor_cls = _load_lightgbm_regressor_cls()
    categories = _build_category_lookup(training_feature_panel_df)
    feature_columns = [
        *DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        price_feature,
        volume_feature,
        *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    ]
    long_scores = _predict_lightgbm_snapshot_scores(
        training_feature_panel_df=training_feature_panel_df,
        snapshot_df=snapshot_df,
        target_column="future_return_5d",
        regressor_cls=regressor_cls,
        feature_columns=feature_columns,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        categories=categories,
    )
    short_scores = _predict_lightgbm_snapshot_scores(
        training_feature_panel_df=training_feature_panel_df,
        snapshot_df=snapshot_df,
        target_column="short_edge_1d",
        regressor_cls=regressor_cls,
        feature_columns=feature_columns,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        categories=categories,
    )

    rows_by_code: dict[str, Topix100Streak353SignalScoreLightgbmSnapshotRow] = {}
    for row in snapshot_df.to_dict(orient="records"):
        normalized_row = {str(key): value for key, value in row.items()}
        code = str(normalized_row["code"])
        long_score_value = long_scores.get(code)
        short_score_value = short_scores.get(code)
        rows_by_code[code] = Topix100Streak353SignalScoreLightgbmSnapshotRow(
            code=code,
            company_name=str(normalized_row["company_name"]),
            date=str(normalized_row["date"]),
            short_mode=cast(str | None, normalized_row.get("short_mode")),
            long_mode=cast(str | None, normalized_row.get("long_mode")),
            state_key=cast(str | None, normalized_row.get("state_key")),
            state_label=cast(str | None, normalized_row.get("state_label")),
            long_score_5d=(
                float(long_score_value) if long_score_value is not None and pd.notna(long_score_value) else None
            ),
            short_score_1d=(
                float(short_score_value)
                if short_score_value is not None and pd.notna(short_score_value)
                else None
            ),
        )

    return Topix100Streak353SignalScoreLightgbmSnapshot(
        score_source_run_id=score_source_run_id,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
        rows_by_code=rows_by_code,
    )


def _build_feature_panel_df(
    *,
    event_panel_df: pd.DataFrame,
    state_result: Any,
    price_feature: str,
    volume_feature: str,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> pd.DataFrame:
    return _build_feature_panel_from_state_df(
        event_panel_df=event_panel_df,
        state_event_df=_coerce_signal_state_panel_df(state_result),
        price_feature=price_feature,
        volume_feature=volume_feature,
        long_target_horizon_days=long_target_horizon_days,
        short_target_horizon_days=short_target_horizon_days,
    )


def _coerce_signal_state_panel_df(state_source: Any) -> pd.DataFrame:
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


def _build_feature_panel_from_state_df(
    *,
    event_panel_df: pd.DataFrame,
    state_event_df: pd.DataFrame,
    price_feature: str,
    volume_feature: str,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> pd.DataFrame:
    if event_panel_df.empty or state_event_df.empty:
        raise ValueError("Base price/state inputs were empty")

    price_df = _build_price_feature_frame(
        event_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    grouped_close = price_df.groupby("code", observed=True, sort=False)["close"]
    close_base = price_df["close"].replace(0, pd.NA).astype(float)
    short_event_column = f"t_plus_{short_target_horizon_days}_return"
    long_event_column = f"t_plus_{long_target_horizon_days}_return"
    if short_event_column in price_df.columns:
        price_df["future_return_1d"] = price_df[short_event_column].astype(float)
    else:
        short_future_close = grouped_close.shift(-short_target_horizon_days).astype(float)
        price_df["future_return_1d"] = short_future_close.div(close_base).sub(1.0)
    if long_event_column in price_df.columns:
        price_df["future_return_5d"] = price_df[long_event_column].astype(float)
    else:
        long_future_close = grouped_close.shift(-long_target_horizon_days).astype(float)
        price_df["future_return_5d"] = long_future_close.div(close_base).sub(1.0)

    state_df = _coerce_signal_state_panel_df(state_event_df)
    state_columns = [
        "state_event_id",
        "code",
        "company_name",
        "sample_split",
        "segment_id",
        "date",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
    ]
    missing_state_columns = [
        column for column in state_columns if column not in state_df.columns
    ]
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
    if merged_df.empty:
        raise ValueError("Joining price rows with state rows produced no overlap")

    merged_df["segment_abs_return"] = merged_df["segment_return"].astype(float).abs()
    merged_df["short_edge_1d"] = -merged_df["future_return_1d"].astype(float)
    merged_df["price_feature_label"] = PRICE_FEATURE_LABEL_MAP[price_feature]
    merged_df["volume_feature_label"] = VOLUME_FEATURE_LABEL_MAP[volume_feature]

    ordered_columns = [
        "date",
        "code",
        "company_name",
        "sample_split",
        "state_event_id",
        "segment_id",
        "decile_num",
        "decile",
        "volume_bucket",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
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
        "future_return_1d",
        "future_return_5d",
        "short_edge_1d",
    ]
    return merged_df[ordered_columns].sort_values(
        ["sample_split", "date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _build_state_decile_horizon_panel_from_feature_panel_df(
    feature_panel_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> pd.DataFrame:
    if feature_panel_df.empty:
        raise ValueError("Feature panel is empty")

    horizon_to_column: dict[int, str] = {
        int(short_target_horizon_days): "future_return_1d",
        int(long_target_horizon_days): "future_return_5d",
    }
    frames: list[pd.DataFrame] = []
    for horizon in future_horizons:
        target_column = horizon_to_column.get(int(horizon))
        if target_column is None:
            raise ValueError(f"Unsupported future horizon for feature panel: {horizon}")
        frame = feature_panel_df[
            [
                "date",
                "code",
                "company_name",
                "sample_split",
                "state_key",
                "state_label",
                "short_mode",
                "long_mode",
                "decile_num",
                "decile",
                "volume_bucket",
                target_column,
            ]
        ].copy()
        frame = frame.rename(columns={target_column: "future_return"})
        frame["horizon_days"] = int(horizon)
        frame = frame.dropna(subset=["future_return"]).copy()
        frames.append(frame)
    if not frames:
        raise ValueError("No horizon rows remained after shaping the feature panel")
    return pd.concat(frames, ignore_index=True)


def _build_price_feature_frame(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    required_event_columns = [
        "date",
        "code",
        "company_name",
        "open",
        "high",
        "low",
        "close",
        price_feature,
        volume_feature,
        "date_constituent_count",
    ]
    missing_event_columns = [
        column for column in required_event_columns if column not in event_panel_df.columns
    ]
    if missing_event_columns:
        raise ValueError(f"Missing event panel columns: {missing_event_columns}")

    optional_future_columns = [
        column
        for column in event_panel_df.columns
        if column.startswith("t_plus_") and column.endswith("_return")
    ]
    price_df = event_panel_df[[*required_event_columns, *optional_future_columns]].copy()
    price_df["date"] = price_df["date"].astype(str)
    price_df["code"] = price_df["code"].astype(str).str.zfill(4)
    price_df = price_df.dropna(subset=[price_feature, volume_feature]).copy()
    if price_df.empty:
        raise ValueError("No event rows remained after dropping missing feature values")

    price_df = price_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    close_group = price_df.groupby("code", observed=True)["close"]
    price_df["recent_return_1d"] = close_group.pct_change(1)
    price_df["recent_return_3d"] = close_group.pct_change(3)
    price_df["recent_return_5d"] = close_group.pct_change(5)
    price_df["intraday_return"] = price_df["close"].astype(float).div(
        price_df["open"].replace(0, pd.NA).astype(float)
    ).sub(1.0)
    price_df["range_pct"] = (
        price_df["high"].astype(float).sub(price_df["low"].astype(float))
    ).div(price_df["close"].replace(0, pd.NA).astype(float))
    price_df["price_rank_desc"] = (
        price_df.groupby("date", observed=True)[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    price_df["decile_num"] = (
        ((price_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // price_df["date_constituent_count"]
    ) + 1
    price_df["decile_num"] = price_df["decile_num"].clip(1, len(DECILE_ORDER))
    price_df["decile"] = price_df["decile_num"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    price_df["decile_size"] = price_df.groupby(["date", "decile"], observed=True)[
        "code"
    ].transform("size")
    price_df["volume_rank_desc_within_decile"] = (
        price_df.groupby(["date", "decile"], observed=True)[volume_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    price_df["volume_bucket_index"] = (
        ((price_df["volume_rank_desc_within_decile"] - 1) * 2)
        // price_df["decile_size"]
    ) + 1
    price_df["volume_bucket_index"] = price_df["volume_bucket_index"].clip(1, 2)
    price_df["volume_bucket"] = price_df["volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    return price_df


def _build_scoring_snapshot_df(
    *,
    event_panel_df: pd.DataFrame,
    history_df: pd.DataFrame,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
) -> pd.DataFrame:
    price_df = _build_price_feature_frame(
        event_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    snapshot_price_df = price_df[price_df["date"] == target_date].copy()
    if snapshot_price_df.empty:
        return pd.DataFrame()

    state_snapshot_df = build_topix100_streak_state_snapshot_df(
        history_df,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
    )
    if state_snapshot_df.empty:
        return pd.DataFrame()

    state_snapshot_df = state_snapshot_df.copy()
    state_snapshot_df["date"] = state_snapshot_df["date"].astype(str)
    state_snapshot_df["code"] = state_snapshot_df["code"].astype(str).str.zfill(4)
    merged_df = snapshot_price_df.merge(
        state_snapshot_df[
            [
                "date",
                "code",
                "company_name",
                "current_streak_day_count",
                "current_streak_segment_return",
                "current_streak_segment_abs_return",
                "short_mode",
                "long_mode",
                "state_key",
                "state_label",
            ]
        ],
        on=["date", "code", "company_name"],
        how="inner",
        validate="one_to_one",
    )
    if merged_df.empty:
        return pd.DataFrame()
    ordered_columns = [
        "date",
        "code",
        "company_name",
        "decile_num",
        "decile",
        "volume_bucket",
        "current_streak_day_count",
        "current_streak_segment_return",
        "current_streak_segment_abs_return",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
        price_feature,
        volume_feature,
        "recent_return_1d",
        "recent_return_3d",
        "recent_return_5d",
        "intraday_return",
        "range_pct",
    ]
    snapshot_df = merged_df[ordered_columns].copy()
    snapshot_df = snapshot_df.rename(
        columns={
            "current_streak_day_count": "segment_day_count",
            "current_streak_segment_return": "segment_return",
            "current_streak_segment_abs_return": "segment_abs_return",
        }
    )
    return snapshot_df.sort_values(["date", "code"], kind="stable").reset_index(drop=True)


def _predict_lightgbm_snapshot_scores(
    *,
    training_feature_panel_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    target_column: str,
    regressor_cls: type[Any],
    feature_columns: list[str],
    categorical_feature_columns: tuple[str, ...],
    categories: dict[str, list[str]],
) -> pd.Series:
    training_df = training_feature_panel_df[
        training_feature_panel_df[target_column].notna()
    ].copy()
    if training_df.empty or snapshot_df.empty:
        return pd.Series(dtype=float)

    train_matrix = _build_model_matrix(
        training_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
        categories=categories,
    )
    snapshot_matrix = _build_model_matrix(
        snapshot_df,
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
    return pd.Series(
        regressor.predict(snapshot_matrix),
        index=snapshot_df["code"].astype(str),
        dtype=float,
    )


def _iter_feature_subsets() -> list[tuple[str, ...]]:
    subsets: list[tuple[str, ...]] = [tuple()]
    for size in range(1, len(_FEATURE_ORDER) + 1):
        for subset in combinations(_FEATURE_ORDER, size):
            subsets.append(_normalize_subset(subset))
    return subsets


def _normalize_subset(subset: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    subset_set = set(subset)
    return tuple(feature for feature in _FEATURE_ORDER if feature in subset_set)


def _build_subset_key(subset: tuple[str, ...] | list[str]) -> str:
    normalized = _normalize_subset(subset)
    if not normalized:
        return "universe"
    return "+".join(normalized)


def _build_subset_label(subset: tuple[str, ...] | list[str]) -> str:
    normalized = _normalize_subset(subset)
    if not normalized:
        return "Universe"
    return " + ".join(_FEATURE_LABEL_MAP[feature] for feature in normalized)


def _build_selector_value_key(row: pd.Series, subset: tuple[str, ...]) -> str:
    return "|".join(str(row[feature]) for feature in subset)


def _build_selector_value_label(row: pd.Series, subset: tuple[str, ...]) -> str:
    return " + ".join(_format_feature_value(feature, row[feature]) for feature in subset)


def _format_feature_value(feature: str, value: Any) -> str:
    if value is None or pd.isna(value) or value == _ALL_SENTINEL:
        return "All"
    if feature == "bucket":
        return str(value)
    if feature == "volume":
        volume_key = str(value)
        if volume_key in VOLUME_BUCKET_LABEL_MAP:
            return VOLUME_BUCKET_LABEL_MAP[cast(VolumeBucketKey, volume_key)]
        return volume_key
    if feature in {"short_mode", "long_mode"}:
        prefix = "Short" if feature == "short_mode" else "Long"
        mode_label = _MODE_VALUE_LABEL_MAP.get(str(value), str(value).title())
        return f"{prefix} {mode_label}"
    return str(value)


def _sort_subset_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    split_order = {name: index for index, name in enumerate(_SPLIT_ORDER)}
    sortable = df.copy()
    if "sample_split" in sortable.columns:
        sortable["_split_order"] = sortable["sample_split"].map(split_order).fillna(999)
    else:
        sortable["_split_order"] = 999
    if "feature_count" not in sortable.columns:
        sortable["feature_count"] = 0
    if "subset_key" not in sortable.columns:
        sortable["subset_key"] = ""
    if "selector_value_key" not in sortable.columns:
        sortable["selector_value_key"] = ""
    sortable = sortable.sort_values(
        ["_split_order", "feature_count", "subset_key", "selector_value_key"],
        ascending=[True, True, True, True],
        kind="stable",
    )
    return sortable.drop(columns=["_split_order"]).reset_index(drop=True)


def _build_subset_daily_panel_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    if panel_df.empty:
        return pd.DataFrame()

    working_df = panel_df.copy()
    working_df["bucket"] = working_df["decile"].astype(str)
    working_df["volume"] = working_df["volume_bucket"].astype(str)
    working_df["short_mode"] = working_df["short_mode"].astype(str)
    working_df["long_mode"] = working_df["long_mode"].astype(str)

    subset_frames: list[pd.DataFrame] = []
    for subset in _iter_feature_subsets():
        group_columns = ["sample_split", "date", "horizon_days", *subset]
        daily_df = (
            working_df.groupby(group_columns, observed=True, sort=False)
            .agg(
                equal_weight_return=("future_return", "mean"),
                stock_count=("code", "nunique"),
            )
            .reset_index()
        )
        if daily_df.empty:
            continue
        daily_df["subset_key"] = _build_subset_key(subset)
        daily_df["subset_label"] = _build_subset_label(subset)
        daily_df["feature_count"] = len(subset)
        if subset:
            daily_df["selector_value_key"] = daily_df.apply(
                lambda row: _build_selector_value_key(row, subset), axis=1
            )
            daily_df["selector_value_label"] = daily_df.apply(
                lambda row: _build_selector_value_label(row, subset), axis=1
            )
        else:
            daily_df["selector_value_key"] = "universe"
            daily_df["selector_value_label"] = "Universe"
        for feature in _FEATURE_ORDER:
            if feature not in daily_df.columns:
                daily_df[feature] = _ALL_SENTINEL
        subset_frames.append(
            daily_df[
                [
                    "sample_split",
                    "date",
                    "horizon_days",
                    "subset_key",
                    "subset_label",
                    "feature_count",
                    "selector_value_key",
                    "selector_value_label",
                    *_FEATURE_ORDER,
                    "equal_weight_return",
                    "stock_count",
                ]
            ].copy()
        )
    if not subset_frames:
        return pd.DataFrame()
    return _sort_subset_frame(pd.concat(subset_frames, ignore_index=True))


def _build_subset_candidate_scorecard_df(
    subset_daily_panel_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if subset_daily_panel_df.empty:
        return pd.DataFrame()

    group_columns = [
        "sample_split",
        "subset_key",
        "subset_label",
        "feature_count",
        "selector_value_key",
        "selector_value_label",
        *_FEATURE_ORDER,
    ]
    summary_rows: list[dict[str, Any]] = []
    grouped = subset_daily_panel_df.groupby(
        group_columns + ["horizon_days"], observed=True, sort=False
    )
    for keys, scoped_df in grouped:
        keys = cast(tuple[Any, ...], keys if isinstance(keys, tuple) else (keys,))
        (
            sample_split,
            subset_key,
            subset_label,
            feature_count,
            selector_value_key,
            selector_value_label,
            bucket,
            volume,
            short_mode,
            long_mode,
            horizon_days,
        ) = keys
        summary_rows.append(
            {
                "sample_split": sample_split,
                "subset_key": subset_key,
                "subset_label": subset_label,
                "feature_count": int(feature_count),
                "selector_value_key": selector_value_key,
                "selector_value_label": selector_value_label,
                "bucket": bucket,
                "volume": volume,
                "short_mode": short_mode,
                "long_mode": long_mode,
                "horizon_days": int(horizon_days),
                "mean_equal_weight_return": float(scoped_df["equal_weight_return"].mean()),
                "positive_hit_rate": float((scoped_df["equal_weight_return"] > 0).mean()),
                "negative_hit_rate": float((scoped_df["equal_weight_return"] < 0).mean()),
                "date_count": int(scoped_df["date"].nunique()),
                "avg_stock_count": float(scoped_df["stock_count"].mean()),
            }
        )

    long_df = pd.DataFrame(summary_rows)
    if long_df.empty:
        return long_df

    records: list[dict[str, Any]] = []
    wide_group_columns = [
        "sample_split",
        "subset_key",
        "subset_label",
        "feature_count",
        "selector_value_key",
        "selector_value_label",
        "bucket",
        "volume",
        "short_mode",
        "long_mode",
    ]
    grouped_wide = long_df.groupby(wide_group_columns, observed=True, sort=False)
    for keys, scoped_df in grouped_wide:
        if not isinstance(keys, tuple):
            keys = (keys,)
        record: dict[str, Any] = {
            column: value for column, value in zip(wide_group_columns, keys, strict=True)
        }
        long_scores: list[float] = []
        short_scores: list[float] = []
        long_hit_rates: list[float] = []
        short_hit_rates: list[float] = []
        for horizon in future_horizons:
            horizon_df = scoped_df[scoped_df["horizon_days"] == horizon]
            if horizon_df.empty:
                record[f"avg_return_{horizon}d"] = None
                record[f"positive_hit_rate_{horizon}d"] = None
                record[f"negative_hit_rate_{horizon}d"] = None
                record[f"date_count_{horizon}d"] = 0
                record[f"avg_stock_count_{horizon}d"] = None
                continue
            row = horizon_df.iloc[0]
            avg_return = float(row["mean_equal_weight_return"])
            record[f"avg_return_{horizon}d"] = avg_return
            record[f"positive_hit_rate_{horizon}d"] = float(row["positive_hit_rate"])
            record[f"negative_hit_rate_{horizon}d"] = float(row["negative_hit_rate"])
            record[f"date_count_{horizon}d"] = int(row["date_count"])
            record[f"avg_stock_count_{horizon}d"] = float(row["avg_stock_count"])
            long_scores.append(avg_return)
            short_scores.append(-avg_return)
            long_hit_rates.append(float(row["positive_hit_rate"]))
            short_hit_rates.append(float(row["negative_hit_rate"]))
        record["primary_long_score"] = (
            float(sum(long_scores) / len(long_scores)) if long_scores else None
        )
        record["primary_short_score"] = (
            float(sum(short_scores) / len(short_scores)) if short_scores else None
        )
        record["primary_long_hit_rate"] = (
            float(sum(long_hit_rates) / len(long_hit_rates)) if long_hit_rates else None
        )
        record["primary_short_hit_rate"] = (
            float(sum(short_hit_rates) / len(short_hit_rates)) if short_hit_rates else None
        )
        records.append(record)

    return _sort_subset_frame(pd.DataFrame(records))


def _build_baseline_lookup_df(
    state_decile_horizon_panel_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    subset_daily_panel_df = _build_subset_daily_panel_df(state_decile_horizon_panel_df)
    subset_candidate_scorecard_df = _build_subset_candidate_scorecard_df(
        subset_daily_panel_df,
        future_horizons=future_horizons,
    )
    return subset_candidate_scorecard_df[
        subset_candidate_scorecard_df["sample_split"] == "discovery"
    ].reset_index(drop=True)


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
            avg_return_1d=float(row["avg_return_1d"]),
            avg_return_5d=float(row["avg_return_5d"]),
            date_count_1d=int(row["date_count_1d"]),
            date_count_5d=int(row["date_count_5d"]),
        )

    universe_row = rows_by_subset.get("universe", {}).get("universe")
    if universe_row is None:
        raise ValueError("Baseline lookup is missing the universe row")

    return _BaselineScorecard(
        universe_long_score_5d=float(universe_row.avg_return_5d),
        universe_short_score_1d=float(-universe_row.avg_return_1d),
        rows_by_subset=rows_by_subset,
    )


def _build_baseline_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    baseline_scorecard: _BaselineScorecard,
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> pd.DataFrame:
    del long_target_horizon_days, short_target_horizon_days
    validation_df = feature_panel_df[feature_panel_df["sample_split"] == "validation"].copy()
    if validation_df.empty:
        raise ValueError("Feature panel has no validation rows")

    prediction_frames: list[pd.DataFrame] = []
    for side in _SIDE_ORDER:
        side_df = validation_df.copy()
        if side == "long":
            side_df["score"] = side_df.apply(
                lambda row: _score_baseline_target(
                    baseline_scorecard,
                    price_decile=int(row["decile_num"]),
                    volume_bucket=str(row["volume_bucket"]),
                    short_mode=str(row["short_mode"]),
                    long_mode=str(row["long_mode"]),
                    target="long_5d",
                ),
                axis=1,
            )
            side_df["target_edge"] = side_df["future_return_5d"].astype(float)
            side_df["realized_return"] = side_df["target_edge"]
        else:
            side_df["score"] = side_df.apply(
                lambda row: _score_baseline_target(
                    baseline_scorecard,
                    price_decile=int(row["decile_num"]),
                    volume_bucket=str(row["volume_bucket"]),
                    short_mode=str(row["short_mode"]),
                    long_mode=str(row["long_mode"]),
                    target="short_1d",
                ),
                axis=1,
            )
            side_df["target_edge"] = -side_df["future_return_1d"].astype(float)
            side_df["realized_return"] = side_df["future_return_1d"].astype(float)
        side_df["side"] = side
        side_df["model_name"] = "baseline"
        prediction_frames.append(
            side_df[
                [
                    "side",
                    "model_name",
                    "date",
                    "code",
                    "company_name",
                    "decile_num",
                    "decile",
                    "volume_bucket",
                    "short_mode",
                    "long_mode",
                    "score",
                    "target_edge",
                    "realized_return",
                ]
            ].copy()
        )
    return pd.concat(prediction_frames, ignore_index=True)


def _build_lightgbm_validation_prediction_df(
    feature_panel_df: pd.DataFrame,
    *,
    side: str,
    regressor_cls: type[Any],
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
    long_target_horizon_days: int,
    short_target_horizon_days: int,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    if side not in _SIDE_ORDER:
        raise ValueError(f"Unsupported side: {side}")

    target_column = "future_return_5d" if side == "long" else "short_edge_1d"
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
            f"No discovery rows were available for the {side} target."
        )
    if validation_df.empty:
        raise Topix100Streak353SignalScoreLightgbmResearchError(
            f"No validation rows were available for the {side} target."
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
            "volume_bucket",
            "short_mode",
            "long_mode",
            "future_return_1d",
            "future_return_5d",
            "short_edge_1d",
        ]
    ].copy()
    prediction_df["score"] = predictions
    prediction_df["side"] = side
    prediction_df["model_name"] = "lightgbm"
    if side == "long":
        prediction_df["target_edge"] = prediction_df["future_return_5d"].astype(float)
        prediction_df["realized_return"] = prediction_df["future_return_5d"].astype(float)
    else:
        prediction_df["target_edge"] = prediction_df["short_edge_1d"].astype(float)
        prediction_df["realized_return"] = prediction_df["future_return_1d"].astype(float)
    prediction_df = prediction_df[
        [
            "side",
            "model_name",
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "volume_bucket",
            "short_mode",
            "long_mode",
            "score",
            "target_edge",
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
            "side": side,
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
        "side": side,
        "model_name": "lightgbm",
        "target_column": target_column,
        "long_target_horizon_days": long_target_horizon_days,
        "short_target_horizon_days": short_target_horizon_days,
        "training_row_count": int(len(training_df)),
        "training_date_count": int(training_df["date"].nunique()),
        "validation_row_count": int(len(validation_df)),
        "validation_date_count": int(validation_df["date"].nunique()),
        "categorical_feature_columns": json.dumps(list(categorical_feature_columns)),
        "continuous_feature_columns": json.dumps(list(continuous_feature_columns)),
        "params_json": json.dumps(_build_lightgbm_params(), sort_keys=True),
    }
    return prediction_df, config_record, feature_importance_df


def _build_category_lookup(feature_panel_df: pd.DataFrame) -> dict[str, list[str]]:
    return {
        "decile": list(DECILE_ORDER),
        "volume_bucket": ["volume_high", "volume_low"],
        "short_mode": ["bullish", "bearish"],
        "long_mode": ["bullish", "bearish"],
    }


def _build_model_matrix(
    df: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_feature_columns: tuple[str, ...],
    categories: dict[str, list[str]],
) -> pd.DataFrame:
    matrix_df = df[feature_columns].copy()
    for column in categorical_feature_columns:
        matrix_df[column] = pd.Categorical(
            matrix_df[column].astype(str),
            categories=categories[column],
        )
    return matrix_df


def _build_lightgbm_params() -> dict[str, Any]:
    return {
        "objective": "regression",
        "learning_rate": 0.05,
        "n_estimators": 220,
        "num_leaves": 31,
        "min_data_in_leaf": 40,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "random_state": 42,
        "n_jobs": -1,
        "verbosity": -1,
    }


def _score_baseline_target(
    scorecard: _BaselineScorecard,
    *,
    price_decile: int,
    volume_bucket: str,
    short_mode: str,
    long_mode: str,
    target: str,
) -> float:
    values = {
        "bucket": f"Q{price_decile}",
        "volume": volume_bucket,
        "short_mode": short_mode,
        "long_mode": long_mode,
    }
    chain = _LONG_CHAIN if target == "long_5d" else _SHORT_CHAIN
    prior_strength = _LONG_BLEND_PRIOR if target == "long_5d" else _SHORT_BLEND_PRIOR
    current_value = (
        scorecard.universe_long_score_5d
        if target == "long_5d"
        else scorecard.universe_short_score_1d
    )
    for subset_key, selector_kind in chain[1:]:
        row = scorecard.rows_by_subset.get(subset_key, {}).get(
            _build_baseline_selector_value_key(selector_kind, values)
        )
        if row is None:
            continue
        if target == "long_5d":
            row_value = row.avg_return_5d
            row_count = row.date_count_5d
        else:
            row_value = -row.avg_return_1d
            row_count = row.date_count_1d
        weight = row_count / (row_count + prior_strength)
        current_value = current_value * (1.0 - weight) + row_value * weight
    return float(current_value)


def _build_baseline_selector_value_key(
    selector_kind: str,
    values: dict[str, str],
) -> str:
    if selector_kind == "universe":
        return "universe"
    if selector_kind == "short_mode":
        return values["short_mode"]
    if selector_kind == "bucket+short_mode":
        return f'{values["bucket"]}|{values["short_mode"]}'
    if selector_kind == "bucket+short_mode+long_mode":
        return f'{values["bucket"]}|{values["short_mode"]}|{values["long_mode"]}'
    if selector_kind == "volume+short_mode":
        return f'{values["volume"]}|{values["short_mode"]}'
    if selector_kind == "volume+short_mode+long_mode":
        return f'{values["volume"]}|{values["short_mode"]}|{values["long_mode"]}'
    if selector_kind == "full":
        return (
            f'{values["bucket"]}|{values["volume"]}|'
            f'{values["short_mode"]}|{values["long_mode"]}'
        )
    raise ValueError(f"Unsupported selector kind: {selector_kind}")


def _build_validation_topk_tables(
    validation_prediction_df: pd.DataFrame,
    *,
    top_k_values: tuple[int, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ranked_df = validation_prediction_df.copy()
    ranked_df["selection_rank"] = (
        ranked_df.groupby(["side", "model_name", "date"], observed=True)["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    universe_daily_df = (
        ranked_df.groupby(["side", "date"], observed=True)
        .agg(
            universe_edge_mean=("target_edge", "mean"),
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
        pick_frames.append(selected_df.copy())

        daily_df = (
            selected_df.groupby(["side", "model_name", "top_k", "date"], observed=True)
            .agg(
                selected_edge_mean=("target_edge", "mean"),
                selected_return_mean=("realized_return", "mean"),
                selected_stock_count=("code", "nunique"),
                selected_score_mean=("score", "mean"),
            )
            .reset_index()
        )
        daily_df = daily_df.merge(
            universe_daily_df,
            on=["side", "date"],
            how="left",
            validate="many_to_one",
        )
        daily_df["edge_spread_vs_universe"] = (
            daily_df["selected_edge_mean"] - daily_df["universe_edge_mean"]
        )
        daily_df["return_spread_vs_universe"] = (
            daily_df["selected_return_mean"] - daily_df["universe_return_mean"]
        )
        daily_frames.append(daily_df)

    if not pick_frames or not daily_frames:
        raise ValueError("Validation top-k evaluation produced no rows")

    pick_df = pd.concat(pick_frames, ignore_index=True)
    pick_df = pick_df.sort_values(
        ["side", "model_name", "top_k", "date", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df = pd.concat(daily_frames, ignore_index=True)
    daily_df = daily_df.sort_values(
        ["side", "model_name", "top_k", "date"],
        kind="stable",
    ).reset_index(drop=True)
    return pick_df, daily_df


def _build_validation_model_summary_df(
    validation_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = (
        validation_topk_daily_df.groupby(
            ["side", "model_name", "top_k"],
            observed=True,
            sort=False,
        )
        .agg(
            date_count=("date", "nunique"),
            avg_selected_edge=("selected_edge_mean", "mean"),
            avg_selected_return=("selected_return_mean", "mean"),
            avg_universe_edge=("universe_edge_mean", "mean"),
            avg_universe_return=("universe_return_mean", "mean"),
            avg_edge_spread_vs_universe=("edge_spread_vs_universe", "mean"),
            avg_return_spread_vs_universe=("return_spread_vs_universe", "mean"),
            hit_rate_positive_edge=(
                "selected_edge_mean",
                lambda values: float((values > 0).mean()),
            ),
            avg_selected_stock_count=("selected_stock_count", "mean"),
            avg_selected_score=("selected_score_mean", "mean"),
        )
        .reset_index()
    )
    summary_df = summary_df.sort_values(
        ["side", "top_k", "model_name"],
        kind="stable",
    ).reset_index(drop=True)
    return summary_df


def _build_validation_model_comparison_df(
    validation_model_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    grouped = validation_model_summary_df.groupby(["side", "top_k"], observed=True, sort=False)
    for (side, top_k), group in grouped:
        baseline_row = group[group["model_name"] == "baseline"]
        lightgbm_row = group[group["model_name"] == "lightgbm"]
        if baseline_row.empty or lightgbm_row.empty:
            continue
        baseline = baseline_row.iloc[0]
        lightgbm = lightgbm_row.iloc[0]
        records.append(
            {
                "side": side,
                "top_k": int(top_k),
                "baseline_avg_selected_edge": float(baseline["avg_selected_edge"]),
                "lightgbm_avg_selected_edge": float(lightgbm["avg_selected_edge"]),
                "edge_lift_vs_baseline": float(lightgbm["avg_selected_edge"])
                - float(baseline["avg_selected_edge"]),
                "baseline_avg_edge_spread_vs_universe": float(
                    baseline["avg_edge_spread_vs_universe"]
                ),
                "lightgbm_avg_edge_spread_vs_universe": float(
                    lightgbm["avg_edge_spread_vs_universe"]
                ),
                "spread_lift_vs_baseline": float(
                    lightgbm["avg_edge_spread_vs_universe"]
                )
                - float(baseline["avg_edge_spread_vs_universe"]),
                "baseline_hit_rate_positive_edge": float(
                    baseline["hit_rate_positive_edge"]
                ),
                "lightgbm_hit_rate_positive_edge": float(
                    lightgbm["hit_rate_positive_edge"]
                ),
                "hit_rate_lift_vs_baseline": float(lightgbm["hit_rate_positive_edge"])
                - float(baseline["hit_rate_positive_edge"]),
            }
        )
    comparison_df = pd.DataFrame.from_records(records)
    if comparison_df.empty:
        return comparison_df
    return comparison_df.sort_values(
        ["side", "top_k"],
        kind="stable",
    ).reset_index(drop=True)


def _build_validation_score_decile_df(
    validation_prediction_df: pd.DataFrame,
) -> pd.DataFrame:
    ranked_df = validation_prediction_df.copy()
    ranked_df["date_constituent_count"] = ranked_df.groupby(
        ["side", "model_name", "date"],
        observed=True,
    )["code"].transform("size")
    ranked_df["score_rank_desc"] = ranked_df.groupby(
        ["side", "model_name", "date"],
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
    summary_df = (
        ranked_df.groupby(
            ["side", "model_name", "score_decile_index", "score_decile"],
            observed=True,
            sort=False,
        )
        .agg(
            mean_target_edge=("target_edge", "mean"),
            mean_realized_return=("realized_return", "mean"),
            stock_count=("code", "count"),
            date_count=("date", "nunique"),
        )
        .reset_index()
    )
    return summary_df.sort_values(
        ["side", "model_name", "score_decile_index"],
        kind="stable",
    ).reset_index(drop=True)


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353SignalScoreLightgbmResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    long_comparison = _select_comparison_row(
        result.validation_model_comparison_df,
        side="long",
        top_k=primary_top_k,
    )
    short_comparison = _select_comparison_row(
        result.validation_model_comparison_df,
        side="short",
        top_k=primary_top_k,
    )
    best_long_feature = _select_top_feature(result.feature_importance_df, side="long")
    best_short_feature = _select_top_feature(result.feature_importance_df, side="short")

    lines = [
        "# TOPIX100 Streak 3/53 Signal Score LightGBM",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Fixed state pair: `{result.short_window_streaks} / {result.long_window_streaks}` streaks",
        f"- Targets: `long {result.long_target_horizon_days}d`, `short {result.short_target_horizon_days}d`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        f"- Discovery / validation rows: `{result.discovery_row_count} / {result.validation_row_count}`",
        "",
        "## Current Read",
        "",
        "This is the first direct stage-1 vs stage-2 score test. The baseline rebuilds the existing lookup score from discovery only; the LightGBM path adds continuous values on top of the same categorical state/bucket inputs and is judged only on validation top-k selection.",
    ]
    if long_comparison is not None:
        lines.append(
            f"- Long Top {primary_top_k}: baseline edge `{_format_return(float(long_comparison['baseline_avg_selected_edge']))}`, LightGBM `{_format_return(float(long_comparison['lightgbm_avg_selected_edge']))}`, lift `{_format_return(float(long_comparison['edge_lift_vs_baseline']))}`."
        )
    if short_comparison is not None:
        lines.append(
            f"- Short Top {primary_top_k}: baseline short-edge `{_format_return(float(short_comparison['baseline_avg_selected_edge']))}`, LightGBM `{_format_return(float(short_comparison['lightgbm_avg_selected_edge']))}`, lift `{_format_return(float(short_comparison['edge_lift_vs_baseline']))}`."
        )
    if best_long_feature is not None:
        lines.append(
            f"- Long feature importance leader: `{best_long_feature['feature_name']}` at share `{float(best_long_feature['importance_share']):.2%}`."
        )
    if best_short_feature is not None:
        lines.append(
            f"- Short feature importance leader: `{best_short_feature['feature_name']}` at share `{float(best_short_feature['importance_share']):.2%}`."
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `baseline_lookup_df`: discovery-only lookup table used by the stage-1 baseline rebuild",
            "- `validation_model_summary_df`: main validation scorecard across model/side/top-k",
            "- `validation_model_comparison_df`: LightGBM lift vs baseline at each top-k",
            "- `feature_importance_df`: LightGBM gain importance for long and short targets",
            "- `validation_score_decile_df`: monotonicity check for predicted score deciles",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353SignalScoreLightgbmResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    long_comparison = _select_comparison_row(
        result.validation_model_comparison_df,
        side="long",
        top_k=primary_top_k,
    )
    short_comparison = _select_comparison_row(
        result.validation_model_comparison_df,
        side="short",
        top_k=primary_top_k,
    )
    best_long_feature = _select_top_feature(result.feature_importance_df, side="long")
    best_short_feature = _select_top_feature(result.feature_importance_df, side="short")
    best_long_model = _select_model_summary_row(
        result.validation_model_summary_df,
        side="long",
        model_name="lightgbm",
        top_k=primary_top_k,
    )
    best_short_model = _select_model_summary_row(
        result.validation_model_summary_df,
        side="short",
        model_name="lightgbm",
        top_k=primary_top_k,
    )

    if long_comparison is not None and float(long_comparison["edge_lift_vs_baseline"]) >= 0.0:
        headline = (
            f"LightGBM improves the stage-1 lookup on the long side at Top {primary_top_k}, "
            "but the short-side question remains much harder."
        )
    elif long_comparison is not None:
        headline = (
            f"On this fixed split, the stage-1 lookup still beats LightGBM at Top {primary_top_k} "
            "on the long side, so the added continuous features are not yet enough."
        )
    else:
        headline = (
            "This study compares the rebuilt stage-1 lookup with a continuous-feature "
            "LightGBM model on the same TOPIX100 streak 3/53 panel."
        )

    result_bullets = [
        "The baseline is rebuilt from discovery only. It does not reuse the published validation lookup, so the comparison is not contaminated by the runtime score overlay.",
        "LightGBM sees the same categorical state inputs plus continuous values such as raw SMA gap, raw volume ratio, recent returns, and current streak magnitude/length.",
    ]
    if long_comparison is not None:
        result_bullets.append(
            f"For the 5-day long target at Top {primary_top_k}, baseline delivered {_format_return(float(long_comparison['baseline_avg_selected_edge']))} and LightGBM delivered {_format_return(float(long_comparison['lightgbm_avg_selected_edge']))}, a lift of {_format_return(float(long_comparison['edge_lift_vs_baseline']))}."
        )
    if short_comparison is not None:
        result_bullets.append(
            f"For the 1-day short target at Top {primary_top_k}, baseline short-edge was {_format_return(float(short_comparison['baseline_avg_selected_edge']))} and LightGBM was {_format_return(float(short_comparison['lightgbm_avg_selected_edge']))}, a lift of {_format_return(float(short_comparison['edge_lift_vs_baseline']))}."
        )
    if best_long_feature is not None:
        result_bullets.append(
            f"The long model's top feature is {best_long_feature['feature_name']} with gain share {float(best_long_feature['importance_share']):.2%}."
        )
    if best_short_feature is not None:
        result_bullets.append(
            f"The short model's top feature is {best_short_feature['feature_name']} with gain share {float(best_short_feature['importance_share']):.2%}."
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
            "detail": "daily selections",
        },
    ]
    if best_long_model is not None:
        highlights.append(
            {
                "label": "Long Top-K edge",
                "value": _format_return(float(best_long_model["avg_selected_edge"])),
                "tone": "success",
                "detail": f"LightGBM Top {primary_top_k}",
            }
        )
    if best_short_model is not None:
        highlights.append(
            {
                "label": "Short Top-K edge",
                "value": _format_return(float(best_short_model["avg_selected_edge"])),
                "tone": "danger",
                "detail": f"LightGBM Top {primary_top_k}",
            }
        )

    return {
        "title": "TOPIX100 Streak 3/53 Signal Score LightGBM",
        "tags": ["TOPIX100", "streaks", "lightgbm", "ranking-score"],
        "purpose": (
            "Test whether the stage-1 lookup score for TOPIX100 can be improved by "
            "adding continuous features and fitting a LightGBM model, with long "
            "target 5d and short target 1d."
        ),
        "method": [
            "Build the same TOPIX100 streak 3 / 53 stock-date panel used in the bucket/state research, keeping exact decile, volume split, short mode, and long mode.",
            "Rebuild the stage-1 lookup from discovery only using date-balanced subset returns, then score validation rows with the same shrinkage chain used by the runtime ranking overlay.",
            "Train separate LightGBM regressors for long 5d return and short 1d downside, then compare both models on validation with daily top-k selection metrics.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This is a fixed discovery/validation split, not a walk-forward test. It is the right next step for architecture choice, but not the final production gate.",
            "The short target is only 1 day, so it is much noisier than the long 5-day target. A model can look decent on raw feature importance and still fail to create durable short edge.",
            "No fees, borrow cost, or turnover penalty are included here. The right read is ranking usefulness, not deployable net PnL yet.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {"label": "Long target", "value": f"{result.long_target_horizon_days}d"},
            {"label": "Short target", "value": f"{result.short_target_horizon_days}d"},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "validation_model_summary_df",
                "label": "Validation model scorecard",
                "description": "Daily top-k validation results for baseline and LightGBM on both long and short targets.",
            },
            {
                "name": "validation_model_comparison_df",
                "label": "LightGBM lift vs baseline",
                "description": "Direct lift table showing whether LightGBM actually improves selected-edge and spread over the rebuilt lookup baseline.",
            },
            {
                "name": "feature_importance_df",
                "label": "Feature importance",
                "description": "Gain-based LightGBM feature importance for the long 5d model and short 1d model.",
            },
            {
                "name": "validation_score_decile_df",
                "label": "Predicted score deciles",
                "description": "Monotonicity table for each model's predicted score deciles on the validation rows.",
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
    if comparison_df.empty:
        return None
    scoped_df = comparison_df[
        (comparison_df["side"] == side) & (comparison_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_model_summary_row(
    summary_df: pd.DataFrame,
    *,
    side: str,
    model_name: str,
    top_k: int,
) -> pd.Series | None:
    scoped_df = summary_df[
        (summary_df["side"] == side)
        & (summary_df["model_name"] == model_name)
        & (summary_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_top_feature(
    feature_importance_df: pd.DataFrame,
    *,
    side: str,
) -> pd.Series | None:
    scoped_df = feature_importance_df[feature_importance_df["side"] == side].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["importance_gain", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    return scoped_df.iloc[0]
