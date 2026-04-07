"""
Walk-forward ablation for redundant discrete features in the TOPIX100 intraday model.

This study keeps the next-session intraday target and the same rolling
walk-forward protocol, but removes low-contribution discrete features from the
LightGBM input one group at a time to see whether the out-of-sample top-k read
actually changes.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
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
    DEFAULT_TOP_K_VALUES,
    _build_feature_panel_df,
    _format_int_sequence,
    _format_return,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm_walkforward import (
    DEFAULT_WALKFORWARD_STEP,
    DEFAULT_WALKFORWARD_TEST_WINDOW,
    DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    _run_walkforward_from_panel,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
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

TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_DISCRETE_ABLATION_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-discrete-ablation-walkforward"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "variant_config_df",
    "baseline_reference_df",
    "variant_model_summary_df",
    "variant_vs_full_df",
    "variant_split_comparison_df",
    "variant_score_decile_df",
    "variant_feature_importance_df",
)


@dataclass(frozen=True)
class _AblationVariantSpec:
    key: str
    label: str
    description: str
    categorical_feature_columns: tuple[str, ...]


_ABLATION_VARIANTS: tuple[_AblationVariantSpec, ...] = (
    _AblationVariantSpec(
        key="full",
        label="Full",
        description="Current intraday model with all discrete features.",
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
    ),
    _AblationVariantSpec(
        key="no_modes",
        label="Drop Modes",
        description="Remove short_mode and long_mode while keeping decile and volume bucket.",
        categorical_feature_columns=("decile", "volume_bucket"),
    ),
    _AblationVariantSpec(
        key="no_volume_bucket",
        label="Drop Vol Bucket",
        description="Remove volume_bucket while keeping decile and short/long modes.",
        categorical_feature_columns=("decile", "short_mode", "long_mode"),
    ),
    _AblationVariantSpec(
        key="decile_only",
        label="Decile Only",
        description="Keep only decile on top of the continuous feature family.",
        categorical_feature_columns=("decile",),
    ),
    _AblationVariantSpec(
        key="continuous_only",
        label="Continuous Only",
        description="Drop every discrete feature and rely only on continuous inputs.",
        categorical_feature_columns=(),
    ),
)


@dataclass(frozen=True)
class Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult:
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
    split_count: int
    variant_keys: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    variant_config_df: pd.DataFrame
    baseline_reference_df: pd.DataFrame
    variant_model_summary_df: pd.DataFrame
    variant_vs_full_df: pd.DataFrame
    variant_split_comparison_df: pd.DataFrame
    variant_score_decile_df: pd.DataFrame
    variant_feature_importance_df: pd.DataFrame


def run_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research(
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
) -> Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult:
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
    if feature_panel_df.empty:
        raise ValueError("Feature panel is empty")

    continuous_feature_columns = (
        price_feature,
        volume_feature,
        *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    )

    variant_config_records: list[dict[str, Any]] = []
    baseline_reference_df: pd.DataFrame | None = None
    variant_model_summary_frames: list[pd.DataFrame] = []
    variant_split_comparison_frames: list[pd.DataFrame] = []
    variant_score_decile_frames: list[pd.DataFrame] = []
    variant_feature_importance_frames: list[pd.DataFrame] = []

    split_count: int | None = None

    for spec in _ABLATION_VARIANTS:
        variant_result = _run_walkforward_from_panel(
            db_path=db_path,
            source_mode=cast(SourceMode, price_result.source_mode),
            source_detail=str(price_result.source_detail),
            available_start_date=str(feature_panel_df["date"].min()),
            available_end_date=str(feature_panel_df["date"].max()),
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            validation_ratio=validation_ratio,
            top_k_values=resolved_top_k_values,
            train_window=train_window,
            test_window=test_window,
            step=step,
            feature_panel_df=feature_panel_df,
            categorical_feature_columns=spec.categorical_feature_columns,
            continuous_feature_columns=continuous_feature_columns,
        )
        split_count = variant_result.split_count if split_count is None else split_count

        variant_config_records.append(
            {
                "variant_key": spec.key,
                "variant_label": spec.label,
                "description": spec.description,
                "categorical_feature_count": len(spec.categorical_feature_columns),
                "categorical_feature_columns": json.dumps(list(spec.categorical_feature_columns)),
                "removed_categorical_features": json.dumps(
                    [
                        column
                        for column in DEFAULT_CATEGORICAL_FEATURE_COLUMNS
                        if column not in spec.categorical_feature_columns
                    ]
                ),
                "uses_decile": "decile" in spec.categorical_feature_columns,
                "uses_volume_bucket": "volume_bucket" in spec.categorical_feature_columns,
                "uses_short_mode": "short_mode" in spec.categorical_feature_columns,
                "uses_long_mode": "long_mode" in spec.categorical_feature_columns,
            }
        )

        if baseline_reference_df is None:
            baseline_reference_df = (
                variant_result.walkforward_model_summary_df[
                    variant_result.walkforward_model_summary_df["model_name"] == "baseline"
                ]
                .copy()
                .reset_index(drop=True)
            )

        lightgbm_summary_df = (
            variant_result.walkforward_model_summary_df[
                variant_result.walkforward_model_summary_df["model_name"] == "lightgbm"
            ]
            .copy()
            .merge(
                variant_result.walkforward_model_comparison_df,
                on="top_k",
                how="left",
                validate="one_to_one",
            )
            .assign(
                variant_key=spec.key,
                variant_label=spec.label,
                categorical_feature_count=len(spec.categorical_feature_columns),
                categorical_feature_columns=json.dumps(list(spec.categorical_feature_columns)),
            )
        )
        variant_model_summary_frames.append(lightgbm_summary_df)

        variant_split_comparison_frames.append(
            variant_result.walkforward_split_comparison_df.assign(
                variant_key=spec.key,
                variant_label=spec.label,
            )
        )
        variant_score_decile_frames.append(
            variant_result.walkforward_score_decile_df[
                variant_result.walkforward_score_decile_df["model_name"] == "lightgbm"
            ].assign(
                variant_key=spec.key,
                variant_label=spec.label,
            )
        )
        variant_feature_importance_frames.append(
            variant_result.walkforward_feature_importance_df.assign(
                variant_key=spec.key,
                variant_label=spec.label,
            )
        )

    variant_config_df = pd.DataFrame.from_records(variant_config_records)
    variant_model_summary_df = pd.concat(
        variant_model_summary_frames,
        ignore_index=True,
    ).sort_values(
        ["top_k", "avg_long_short_spread", "variant_key"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    variant_vs_full_df = _build_variant_vs_full_df(variant_model_summary_df)
    variant_split_comparison_df = pd.concat(
        variant_split_comparison_frames,
        ignore_index=True,
    )
    variant_score_decile_df = pd.concat(
        variant_score_decile_frames,
        ignore_index=True,
    )
    variant_feature_importance_df = pd.concat(
        variant_feature_importance_frames,
        ignore_index=True,
    )

    return Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=str(feature_panel_df["date"].min()),
        available_end_date=str(feature_panel_df["date"].max()),
        analysis_start_date=str(feature_panel_df["date"].min()),
        analysis_end_date=str(feature_panel_df["date"].max()),
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        top_k_values=resolved_top_k_values,
        train_window=train_window,
        test_window=test_window,
        step=step,
        split_count=split_count or 0,
        variant_keys=tuple(spec.key for spec in _ABLATION_VARIANTS),
        continuous_feature_columns=continuous_feature_columns,
        variant_config_df=variant_config_df,
        baseline_reference_df=baseline_reference_df if baseline_reference_df is not None else pd.DataFrame(),
        variant_model_summary_df=variant_model_summary_df,
        variant_vs_full_df=variant_vs_full_df,
        variant_split_comparison_df=variant_split_comparison_df,
        variant_score_decile_df=variant_score_decile_df,
        variant_feature_importance_df=variant_feature_importance_df,
    )


def write_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research_bundle(
    result: Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_DISCRETE_ABLATION_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research",
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
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_DISCRETE_ABLATION_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_DISCRETE_ABLATION_WALKFORWARD_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_variant_vs_full_df(variant_model_summary_df: pd.DataFrame) -> pd.DataFrame:
    full_df = (
        variant_model_summary_df[variant_model_summary_df["variant_key"] == "full"][
            [
                "top_k",
                "avg_long_return",
                "avg_short_edge",
                "avg_long_short_spread",
                "spread_hit_rate_positive",
            ]
        ]
        .rename(
            columns={
                "avg_long_return": "full_avg_long_return",
                "avg_short_edge": "full_avg_short_edge",
                "avg_long_short_spread": "full_avg_long_short_spread",
                "spread_hit_rate_positive": "full_spread_hit_rate_positive",
            }
        )
        .copy()
    )
    if full_df.empty:
        return pd.DataFrame()
    merged_df = (
        variant_model_summary_df.merge(
            full_df,
            on="top_k",
            how="left",
            validate="many_to_one",
        )
        .copy()
    )
    merged_df = merged_df[merged_df["variant_key"] != "full"].copy()
    merged_df["long_return_delta_vs_full"] = (
        merged_df["avg_long_return"] - merged_df["full_avg_long_return"]
    )
    merged_df["short_edge_delta_vs_full"] = (
        merged_df["avg_short_edge"] - merged_df["full_avg_short_edge"]
    )
    merged_df["spread_delta_vs_full"] = (
        merged_df["avg_long_short_spread"] - merged_df["full_avg_long_short_spread"]
    )
    merged_df["spread_hit_rate_delta_vs_full"] = (
        merged_df["spread_hit_rate_positive"] - merged_df["full_spread_hit_rate_positive"]
    )
    full_spread_series = merged_df["full_avg_long_short_spread"].astype(float)
    merged_df["spread_retention_vs_full"] = (
        merged_df["avg_long_short_spread"].astype(float) / full_spread_series
    )
    merged_df.loc[full_spread_series == 0.0, "spread_retention_vs_full"] = pd.NA
    return merged_df.sort_values(
        ["top_k", "spread_delta_vs_full", "variant_key"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    full_row = _select_variant_row(result.variant_model_summary_df, "full", primary_top_k)
    no_modes_row = _select_variant_row(result.variant_model_summary_df, "no_modes", primary_top_k)
    no_volume_row = _select_variant_row(result.variant_model_summary_df, "no_volume_bucket", primary_top_k)
    decile_only_row = _select_variant_row(result.variant_model_summary_df, "decile_only", primary_top_k)
    continuous_only_row = _select_variant_row(result.variant_model_summary_df, "continuous_only", primary_top_k)
    best_simplified_row = _select_best_simplified_variant(
        result.variant_model_summary_df,
        top_k=primary_top_k,
    )

    lines = [
        "# TOPIX100 Next-Session Intraday LightGBM Discrete Ablation Walk-Forward",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Walk-forward windows: `train {result.train_window} / test {result.test_window} / step {result.step}`",
        f"- Split count: `{result.split_count}`",
        "- Target: `next-session open -> close return`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        f"- Variants: `{', '.join(result.variant_keys)}`",
        "",
        "## Current Read",
        "",
        "This study asks whether the low-importance discrete features are actually needed. Every variant keeps the same continuous family and the same rolling refit schedule, then removes subsets of the discrete inputs from the LightGBM only.",
    ]
    if full_row is not None:
        lines.append(
            f"- Full Top {primary_top_k} spread: `{_format_return(float(full_row['avg_long_short_spread']))}`."
        )
    for label, row in (
        ("Drop modes", no_modes_row),
        ("Drop volume bucket", no_volume_row),
        ("Decile only", decile_only_row),
        ("Continuous only", continuous_only_row),
    ):
        if row is None or full_row is None:
            continue
        spread_delta = float(row["avg_long_short_spread"]) - float(full_row["avg_long_short_spread"])
        retention = (
            float(row["avg_long_short_spread"]) / float(full_row["avg_long_short_spread"])
            if float(full_row["avg_long_short_spread"]) != 0.0
            else float("nan")
        )
        lines.append(
            f"- {label} Top {primary_top_k} spread: `{_format_return(float(row['avg_long_short_spread']))}` (delta vs full `{_format_return(spread_delta)}`, retention `{retention:.1%}`)."
        )
    if best_simplified_row is not None:
        lines.append(
            f"- Best simplified variant at Top {primary_top_k}: `{best_simplified_row['variant_label']}` with spread `{_format_return(float(best_simplified_row['avg_long_short_spread']))}`."
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionIntradayDiscreteAblationWalkforwardResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    full_row = _select_variant_row(result.variant_model_summary_df, "full", primary_top_k)
    no_modes_row = _select_variant_row(result.variant_model_summary_df, "no_modes", primary_top_k)
    no_volume_row = _select_variant_row(result.variant_model_summary_df, "no_volume_bucket", primary_top_k)
    decile_only_row = _select_variant_row(result.variant_model_summary_df, "decile_only", primary_top_k)
    continuous_only_row = _select_variant_row(result.variant_model_summary_df, "continuous_only", primary_top_k)
    best_simplified_row = _select_best_simplified_variant(
        result.variant_model_summary_df,
        top_k=primary_top_k,
    )

    def _comparison_bullet(label: str, row: pd.Series | None) -> str | None:
        if row is None or full_row is None:
            return None
        spread_delta = float(row["avg_long_short_spread"]) - float(full_row["avg_long_short_spread"])
        retention = (
            float(row["avg_long_short_spread"]) / float(full_row["avg_long_short_spread"])
            if float(full_row["avg_long_short_spread"]) != 0.0
            else float("nan")
        )
        return (
            f"{label} kept a Top {primary_top_k} spread of "
            f"{_format_return(float(row['avg_long_short_spread']))}, "
            f"which is {retention:.1%} of full and "
            f"{_format_return(spread_delta)} versus full."
        )

    result_bullets = [
        "Every variant keeps the same continuous feature family and walk-forward schedule, so the only moving part is which discrete inputs remain in the LightGBM.",
    ]
    if full_row is not None:
        result_bullets.append(
            f"Full Top {primary_top_k} spread was {_format_return(float(full_row['avg_long_short_spread']))}, with long {_format_return(float(full_row['avg_long_return']))} and short edge {_format_return(float(full_row['avg_short_edge']))}."
        )
    for bullet in (
        _comparison_bullet("Drop modes", no_modes_row),
        _comparison_bullet("Drop volume bucket", no_volume_row),
        _comparison_bullet("Decile only", decile_only_row),
        _comparison_bullet("Continuous only", continuous_only_row),
    ):
        if bullet is not None:
            result_bullets.append(bullet)

    considerations = [
        "If drop-modes and drop-volume-bucket stay close to full, those discrete fields are better treated as optional UI filters than core model inputs.",
        "If decile-only remains close to full while continuous-only weakens, decile is the only discrete feature with durable incremental value.",
        "Runtime speed will not move much from dropping these columns alone because the heavy work is still panel construction and daily LightGBM training, not the extra categorical columns.",
    ]
    if best_simplified_row is not None:
        considerations.insert(
            0,
            f"The best simplified variant here is {best_simplified_row['variant_label']}, so that is the first candidate for a lighter runtime model.",
        )

    highlights = [
        {
            "label": "Primary Top-K",
            "value": str(primary_top_k),
            "tone": "accent",
            "detail": "long + short legs",
        },
        {
            "label": "Split count",
            "value": str(result.split_count),
            "tone": "neutral",
            "detail": "walk-forward blocks",
        },
    ]
    if full_row is not None:
        highlights.append(
            {
                "label": "Full spread",
                "value": _format_return(float(full_row["avg_long_short_spread"])),
                "tone": "success",
                "detail": f"Top {primary_top_k}",
            }
        )
    if best_simplified_row is not None:
        highlights.append(
            {
                "label": "Best simplified",
                "value": _format_return(float(best_simplified_row["avg_long_short_spread"])),
                "tone": "accent",
                "detail": str(best_simplified_row["variant_label"]),
            }
        )

    return {
        "title": "TOPIX100 Intraday Discrete Ablation Walk-Forward",
        "headline": (
            "This study checks whether the low-contribution discrete features in the next-session intraday model are actually needed once the score is judged in rolling out-of-sample blocks."
        ),
        "purpose": (
            "Quantify how much performance is lost when short_mode, long_mode, and volume_bucket are removed from the intraday LightGBM while keeping the same continuous inputs and the same walk-forward evaluation."
        ),
        "method": (
            "Build one TOPIX100 stock-date intraday panel, then rerun the same rolling LightGBM validation across five feature sets: full, drop modes, drop volume bucket, decile only, and continuous only."
        ),
        "results": result_bullets,
        "considerations": considerations,
        "highlights": highlights,
        "tables": [
            {
                "key": "variant_model_summary_df",
                "title": "Variant Summary",
                "description": "Top-k long / short / spread metrics for each LightGBM feature-set variant.",
            },
            {
                "key": "variant_vs_full_df",
                "title": "Variant vs Full",
                "description": "How much each simplified variant gains or loses versus the full discrete set.",
            },
            {
                "key": "variant_feature_importance_df",
                "title": "Variant Feature Importance",
                "description": "Mean LightGBM importance inside each walk-forward variant.",
            },
        ],
    }


def _resolve_primary_top_k(top_k_values: tuple[int, ...]) -> int:
    if 3 in top_k_values:
        return 3
    return int(top_k_values[0])


def _select_variant_row(
    variant_model_summary_df: pd.DataFrame,
    variant_key: str,
    top_k: int,
) -> pd.Series | None:
    if variant_model_summary_df.empty:
        return None
    scoped_df = variant_model_summary_df[
        (variant_model_summary_df["variant_key"] == variant_key)
        & (variant_model_summary_df["top_k"] == top_k)
    ]
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_best_simplified_variant(
    variant_model_summary_df: pd.DataFrame,
    *,
    top_k: int,
) -> pd.Series | None:
    scoped_df = variant_model_summary_df[
        (variant_model_summary_df["variant_key"] != "full")
        & (variant_model_summary_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["avg_long_short_spread", "variant_key"],
        ascending=[False, True],
        kind="stable",
    )
    return scoped_df.iloc[0]
