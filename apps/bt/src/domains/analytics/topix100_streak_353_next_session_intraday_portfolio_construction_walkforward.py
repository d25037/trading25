"""
Walk-forward portfolio-construction comparison for TOPIX100 intraday LightGBM.

This study keeps the same next-session intraday prediction panel and rolling
refit schedule as the core walk-forward research, but changes only how the
daily book is assembled from the model scores.
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
    _build_feature_panel_df,
    _format_return,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm_walkforward import (
    DEFAULT_WALKFORWARD_STEP,
    DEFAULT_WALKFORWARD_TEST_WINDOW,
    DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    _build_walkforward_prediction_artifacts,
    _compute_daily_return_distribution_stats,
    _compute_portfolio_performance_stats,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix_close_return_streaks import DEFAULT_VALIDATION_RATIO
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode

DEFAULT_REFERENCE_TOP_K = 3
DEFAULT_ABSOLUTE_SELECTION_COUNT = 5
TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_PORTFOLIO_CONSTRUCTION_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-portfolio-construction-walkforward"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "split_config_df",
    "variant_config_df",
    "variant_pick_df",
    "variant_daily_df",
    "variant_split_summary_df",
    "variant_model_summary_df",
    "variant_vs_reference_df",
    "variant_model_comparison_df",
    "portfolio_stats_df",
    "daily_return_distribution_df",
)
_SPLIT_METADATA_COLUMNS: tuple[str, ...] = (
    "split_index",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
)


@dataclass(frozen=True)
class _PortfolioConstructionVariantSpec:
    key: str
    label: str
    description: str


_REFERENCE_VARIANT = _PortfolioConstructionVariantSpec(
    key="top_bottom_reference",
    label="Top 3 / Bottom 3",
    description=(
        "Current symmetric book: buy the top 3 scores and sell the bottom 3 scores."
    ),
)
_ABSOLUTE_VARIANT = _PortfolioConstructionVariantSpec(
    key="abs_score_signed",
    label="Abs Score Top 5 Signed",
    description=(
        "Select the 5 largest absolute scores each day, go long when the score is "
        "positive, and short when the score is negative."
    ),
)


@dataclass(frozen=True)
class Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult:
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
    reference_top_k: int
    absolute_selection_count: int
    train_window: int
    test_window: int
    step: int
    split_count: int
    variant_keys: tuple[str, ...]
    split_config_df: pd.DataFrame
    variant_config_df: pd.DataFrame
    variant_pick_df: pd.DataFrame
    variant_daily_df: pd.DataFrame
    variant_split_summary_df: pd.DataFrame
    variant_model_summary_df: pd.DataFrame
    variant_vs_reference_df: pd.DataFrame
    variant_model_comparison_df: pd.DataFrame
    portfolio_stats_df: pd.DataFrame
    daily_return_distribution_df: pd.DataFrame


def run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    reference_top_k: int = DEFAULT_REFERENCE_TOP_K,
    absolute_selection_count: int = DEFAULT_ABSOLUTE_SELECTION_COUNT,
    train_window: int = DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    test_window: int = DEFAULT_WALKFORWARD_TEST_WINDOW,
    step: int = DEFAULT_WALKFORWARD_STEP,
) -> Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if reference_top_k <= 0:
        raise ValueError("reference_top_k must be positive")
    if absolute_selection_count <= 0:
        raise ValueError("absolute_selection_count must be positive")

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

    prediction_artifacts = _build_walkforward_prediction_artifacts(
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
        top_k_values=(reference_top_k,),
        train_window=train_window,
        test_window=test_window,
        step=step,
        feature_panel_df=feature_panel_df,
        categorical_feature_columns=("decile",),
        continuous_feature_columns=(
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
        ),
    )

    reference_pick_df = _build_reference_pick_df(
        prediction_artifacts.walkforward_topk_pick_df,
        top_k=reference_top_k,
    )
    absolute_pick_df = _build_absolute_score_pick_df(
        prediction_artifacts.walkforward_prediction_df,
        selection_count=absolute_selection_count,
    )
    variant_pick_df = pd.concat(
        [reference_pick_df, absolute_pick_df],
        ignore_index=True,
    ).sort_values(
        ["variant_key", "model_name", "date", "selection_rank", "code"],
        kind="stable",
    ).reset_index(drop=True)
    variant_daily_df = _build_variant_daily_df(
        variant_pick_df,
        prediction_artifacts.walkforward_prediction_df,
    )
    variant_split_summary_df = _build_variant_summary_df(
        variant_daily_df,
        group_cols=(
            "variant_key",
            "variant_label",
            "variant_description",
            "model_name",
            *_SPLIT_METADATA_COLUMNS,
        ),
    )
    variant_model_summary_df = _build_variant_summary_df(
        variant_daily_df,
        group_cols=("variant_key", "variant_label", "variant_description", "model_name"),
    )
    variant_vs_reference_df = _build_variant_vs_reference_df(variant_model_summary_df)
    variant_model_comparison_df = _build_variant_model_comparison_df(
        variant_model_summary_df
    )
    portfolio_stats_df = _build_portfolio_stats_df(variant_daily_df)
    daily_return_distribution_df = _build_daily_return_distribution_df(variant_daily_df)
    variant_config_df = pd.DataFrame.from_records(
        [
            {
                "variant_key": _REFERENCE_VARIANT.key,
                "variant_label": _REFERENCE_VARIANT.label,
                "variant_description": _REFERENCE_VARIANT.description,
                "reference_top_k": reference_top_k,
                "absolute_selection_count": pd.NA,
                "selection_count_target": reference_top_k * 2,
                "is_reference": True,
            },
            {
                "variant_key": _ABSOLUTE_VARIANT.key,
                "variant_label": _ABSOLUTE_VARIANT.label,
                "variant_description": _ABSOLUTE_VARIANT.description,
                "reference_top_k": pd.NA,
                "absolute_selection_count": absolute_selection_count,
                "selection_count_target": absolute_selection_count,
                "is_reference": False,
            },
        ]
    )

    return Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult(
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
        reference_top_k=reference_top_k,
        absolute_selection_count=absolute_selection_count,
        train_window=train_window,
        test_window=test_window,
        step=step,
        split_count=prediction_artifacts.split_count,
        variant_keys=(_REFERENCE_VARIANT.key, _ABSOLUTE_VARIANT.key),
        split_config_df=prediction_artifacts.split_config_df,
        variant_config_df=variant_config_df,
        variant_pick_df=variant_pick_df,
        variant_daily_df=variant_daily_df,
        variant_split_summary_df=variant_split_summary_df,
        variant_model_summary_df=variant_model_summary_df,
        variant_vs_reference_df=variant_vs_reference_df,
        variant_model_comparison_df=variant_model_comparison_df,
        portfolio_stats_df=portfolio_stats_df,
        daily_return_distribution_df=daily_return_distribution_df,
    )


def write_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research_bundle(
    result: Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_PORTFOLIO_CONSTRUCTION_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "reference_top_k": result.reference_top_k,
            "absolute_selection_count": result.absolute_selection_count,
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


def load_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_PORTFOLIO_CONSTRUCTION_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_PORTFOLIO_CONSTRUCTION_WALKFORWARD_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_reference_pick_df(
    walkforward_topk_pick_df: pd.DataFrame,
    *,
    top_k: int,
) -> pd.DataFrame:
    pick_df = walkforward_topk_pick_df[walkforward_topk_pick_df["top_k"] == top_k].copy()
    if pick_df.empty:
        raise ValueError(f"Reference Top/Bottom {top_k} picks were empty")
    pick_df["variant_key"] = _REFERENCE_VARIANT.key
    pick_df["variant_label"] = _REFERENCE_VARIANT.label
    pick_df["variant_description"] = _REFERENCE_VARIANT.description
    pick_df["selection_count_target"] = int(top_k) * 2
    pick_df["abs_score"] = pick_df["score"].astype(float).abs()
    return pick_df[
        [
            "variant_key",
            "variant_label",
            "variant_description",
            "model_name",
            *_SPLIT_METADATA_COLUMNS,
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "volume_bucket",
            "short_mode",
            "long_mode",
            "state_key",
            "state_label",
            "selection_side",
            "selection_rank",
            "selection_count_target",
            "score",
            "abs_score",
            "target_edge",
            "realized_return",
        ]
    ].copy()


def _build_absolute_score_pick_df(
    walkforward_prediction_df: pd.DataFrame,
    *,
    selection_count: int,
) -> pd.DataFrame:
    ranked_df = walkforward_prediction_df.copy()
    ranked_df["abs_score"] = ranked_df["score"].astype(float).abs()
    ranked_df["selection_rank"] = (
        ranked_df.groupby(["model_name", "date"], observed=True)["abs_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    pick_df = ranked_df[ranked_df["selection_rank"] <= selection_count].copy()
    if pick_df.empty:
        raise ValueError("Absolute score selection produced no rows")

    pick_df["variant_key"] = _ABSOLUTE_VARIANT.key
    pick_df["variant_label"] = _ABSOLUTE_VARIANT.label
    pick_df["variant_description"] = _ABSOLUTE_VARIANT.description
    pick_df["selection_count_target"] = int(selection_count)
    pick_df["selection_side"] = "flat"
    pick_df.loc[pick_df["score"].astype(float) > 0.0, "selection_side"] = "long"
    pick_df.loc[pick_df["score"].astype(float) < 0.0, "selection_side"] = "short"
    pick_df["target_edge"] = 0.0
    long_mask = pick_df["selection_side"] == "long"
    short_mask = pick_df["selection_side"] == "short"
    pick_df.loc[long_mask, "target_edge"] = pick_df.loc[long_mask, "realized_return"].astype(
        float
    )
    pick_df.loc[short_mask, "target_edge"] = -pick_df.loc[short_mask, "realized_return"].astype(
        float
    )
    return pick_df[
        [
            "variant_key",
            "variant_label",
            "variant_description",
            "model_name",
            *_SPLIT_METADATA_COLUMNS,
            "date",
            "code",
            "company_name",
            "decile_num",
            "decile",
            "volume_bucket",
            "short_mode",
            "long_mode",
            "state_key",
            "state_label",
            "selection_side",
            "selection_rank",
            "selection_count_target",
            "score",
            "abs_score",
            "target_edge",
            "realized_return",
        ]
    ].copy()


def _build_variant_daily_df(
    variant_pick_df: pd.DataFrame,
    walkforward_prediction_df: pd.DataFrame,
) -> pd.DataFrame:
    universe_daily_df = (
        walkforward_prediction_df.groupby(
            ["model_name", *_SPLIT_METADATA_COLUMNS, "date"],
            observed=True,
            sort=False,
        )
        .agg(
            universe_return_mean=("realized_return", "mean"),
            universe_stock_count=("code", "size"),
        )
        .reset_index()
    )

    group_columns = (
        "variant_key",
        "variant_label",
        "variant_description",
        "model_name",
        *_SPLIT_METADATA_COLUMNS,
        "date",
    )
    records: list[dict[str, Any]] = []
    for group_key, scoped_df in variant_pick_df.groupby(
        list(group_columns),
        observed=True,
        sort=False,
    ):
        key_values = group_key if isinstance(group_key, tuple) else (group_key,)
        record: dict[str, Any] = dict(zip(group_columns, key_values, strict=True))
        long_df = scoped_df[scoped_df["selection_side"] == "long"]
        short_df = scoped_df[scoped_df["selection_side"] == "short"]
        flat_df = scoped_df[scoped_df["selection_side"] == "flat"]
        selected_count = int(len(scoped_df))
        record.update(
            {
                "selected_stock_count": selected_count,
                "long_count": int(len(long_df)),
                "short_count": int(len(short_df)),
                "flat_count": int(len(flat_df)),
                "long_return_mean": float(long_df["realized_return"].mean())
                if not long_df.empty
                else float("nan"),
                "short_edge_mean": float(short_df["target_edge"].mean())
                if not short_df.empty
                else float("nan"),
                "selected_edge_mean": float(scoped_df["target_edge"].astype(float).mean()),
                "selected_score_mean": float(scoped_df["score"].astype(float).mean()),
                "selected_abs_score_mean": float(
                    scoped_df["abs_score"].astype(float).mean()
                ),
                "avg_selection_rank": float(scoped_df["selection_rank"].astype(float).mean()),
                "selection_count_target": int(scoped_df["selection_count_target"].iloc[0]),
                "net_exposure": (
                    (len(long_df) - len(short_df)) / selected_count
                    if selected_count > 0
                    else float("nan")
                ),
            }
        )
        records.append(record)

    daily_df = pd.DataFrame.from_records(records)
    daily_df = daily_df.merge(
        universe_daily_df,
        on=["model_name", *_SPLIT_METADATA_COLUMNS, "date"],
        how="left",
        validate="many_to_one",
    )
    daily_df["edge_vs_universe"] = (
        daily_df["selected_edge_mean"] - daily_df["universe_return_mean"]
    )
    return daily_df.sort_values(
        ["variant_key", "model_name", "split_index", "date"],
        kind="stable",
    ).reset_index(drop=True)


def _build_variant_summary_df(
    variant_daily_df: pd.DataFrame,
    *,
    group_cols: tuple[str, ...],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for group_key, scoped_df in variant_daily_df.groupby(
        list(group_cols),
        observed=True,
        sort=False,
    ):
        key_values = group_key if isinstance(group_key, tuple) else (group_key,)
        record: dict[str, Any] = dict(zip(group_cols, key_values, strict=True))
        selected_edge = scoped_df["selected_edge_mean"].astype(float)
        record.update(
            {
                "date_count": int(scoped_df["date"].nunique()),
                "avg_selected_edge": float(selected_edge.mean()),
                "hit_rate_positive_edge": float((selected_edge > 0.0).mean()),
                "avg_selected_stock_count": float(
                    scoped_df["selected_stock_count"].astype(float).mean()
                ),
                "avg_long_count": float(scoped_df["long_count"].astype(float).mean()),
                "avg_short_count": float(scoped_df["short_count"].astype(float).mean()),
                "avg_flat_count": float(scoped_df["flat_count"].astype(float).mean()),
                "avg_long_return": float(scoped_df["long_return_mean"].astype(float).mean()),
                "avg_short_edge": float(scoped_df["short_edge_mean"].astype(float).mean()),
                "avg_selected_score": float(
                    scoped_df["selected_score_mean"].astype(float).mean()
                ),
                "avg_abs_score": float(
                    scoped_df["selected_abs_score_mean"].astype(float).mean()
                ),
                "avg_net_exposure": float(scoped_df["net_exposure"].astype(float).mean()),
                "avg_selection_rank": float(
                    scoped_df["avg_selection_rank"].astype(float).mean()
                ),
                "avg_edge_vs_universe": float(
                    scoped_df["edge_vs_universe"].astype(float).mean()
                ),
                "avg_universe_return": float(
                    scoped_df["universe_return_mean"].astype(float).mean()
                ),
            }
        )
        records.append(record)
    summary_df = pd.DataFrame.from_records(records)
    if summary_df.empty:
        return summary_df
    return summary_df.sort_values(
        [*group_cols],
        kind="stable",
    ).reset_index(drop=True)


def _build_variant_vs_reference_df(
    variant_model_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    reference_df = (
        variant_model_summary_df[
            variant_model_summary_df["variant_key"] == _REFERENCE_VARIANT.key
        ][
            [
                "model_name",
                "avg_selected_edge",
                "hit_rate_positive_edge",
                "avg_long_count",
                "avg_short_count",
                "avg_net_exposure",
            ]
        ]
        .rename(
            columns={
                "avg_selected_edge": "reference_avg_selected_edge",
                "hit_rate_positive_edge": "reference_hit_rate_positive_edge",
                "avg_long_count": "reference_avg_long_count",
                "avg_short_count": "reference_avg_short_count",
                "avg_net_exposure": "reference_avg_net_exposure",
            }
        )
        .copy()
    )
    if reference_df.empty:
        return pd.DataFrame()

    comparison_df = variant_model_summary_df.merge(
        reference_df,
        on="model_name",
        how="left",
        validate="many_to_one",
    ).copy()
    comparison_df["selected_edge_delta_vs_reference"] = (
        comparison_df["avg_selected_edge"] - comparison_df["reference_avg_selected_edge"]
    )
    comparison_df["hit_rate_delta_vs_reference"] = (
        comparison_df["hit_rate_positive_edge"]
        - comparison_df["reference_hit_rate_positive_edge"]
    )
    comparison_df["avg_long_count_delta_vs_reference"] = (
        comparison_df["avg_long_count"] - comparison_df["reference_avg_long_count"]
    )
    comparison_df["avg_short_count_delta_vs_reference"] = (
        comparison_df["avg_short_count"] - comparison_df["reference_avg_short_count"]
    )
    comparison_df["net_exposure_delta_vs_reference"] = (
        comparison_df["avg_net_exposure"] - comparison_df["reference_avg_net_exposure"]
    )
    comparison_df["selected_edge_retention_vs_reference"] = (
        comparison_df["avg_selected_edge"] / comparison_df["reference_avg_selected_edge"]
    )
    comparison_df.loc[
        comparison_df["reference_avg_selected_edge"].astype(float) == 0.0,
        "selected_edge_retention_vs_reference",
    ] = pd.NA
    return comparison_df.sort_values(
        ["model_name", "variant_key"],
        kind="stable",
    ).reset_index(drop=True)


def _build_variant_model_comparison_df(
    variant_model_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    baseline_df = (
        variant_model_summary_df[variant_model_summary_df["model_name"] == "baseline"][
            [
                "variant_key",
                "variant_label",
                "avg_selected_edge",
                "hit_rate_positive_edge",
            ]
        ]
        .rename(
            columns={
                "avg_selected_edge": "baseline_avg_selected_edge",
                "hit_rate_positive_edge": "baseline_hit_rate_positive_edge",
            }
        )
        .copy()
    )
    lightgbm_df = (
        variant_model_summary_df[variant_model_summary_df["model_name"] == "lightgbm"][
            [
                "variant_key",
                "variant_label",
                "avg_selected_edge",
                "hit_rate_positive_edge",
            ]
        ]
        .rename(
            columns={
                "avg_selected_edge": "lightgbm_avg_selected_edge",
                "hit_rate_positive_edge": "lightgbm_hit_rate_positive_edge",
            }
        )
        .copy()
    )
    if baseline_df.empty or lightgbm_df.empty:
        return pd.DataFrame()
    comparison_df = baseline_df.merge(
        lightgbm_df,
        on=["variant_key", "variant_label"],
        how="inner",
        validate="one_to_one",
    )
    comparison_df["selected_edge_lift_vs_baseline"] = (
        comparison_df["lightgbm_avg_selected_edge"]
        - comparison_df["baseline_avg_selected_edge"]
    )
    comparison_df["hit_rate_lift_vs_baseline"] = (
        comparison_df["lightgbm_hit_rate_positive_edge"]
        - comparison_df["baseline_hit_rate_positive_edge"]
    )
    return comparison_df.sort_values(["variant_key"], kind="stable").reset_index(drop=True)


def _build_portfolio_stats_df(variant_daily_df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (variant_key, variant_label, model_name), scoped_df in variant_daily_df.groupby(
        ["variant_key", "variant_label", "model_name"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values(["split_index", "date"], kind="stable").reset_index(
            drop=True
        )
        records.append(
            {
                "variant_key": str(variant_key),
                "variant_label": str(variant_label),
                "model_name": str(model_name),
                **_compute_portfolio_performance_stats(
                    ordered_df["selected_edge_mean"].astype(float)
                ),
            }
        )
    return pd.DataFrame.from_records(records).sort_values(
        ["variant_key", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_daily_return_distribution_df(variant_daily_df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (variant_key, variant_label, model_name), scoped_df in variant_daily_df.groupby(
        ["variant_key", "variant_label", "model_name"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values(["split_index", "date"], kind="stable").reset_index(
            drop=True
        )
        records.append(
            {
                "variant_key": str(variant_key),
                "variant_label": str(variant_label),
                "model_name": str(model_name),
                **_compute_daily_return_distribution_stats(
                    ordered_df["selected_edge_mean"].astype(float)
                ),
            }
        )
    return pd.DataFrame.from_records(records).sort_values(
        ["variant_key", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult,
) -> str:
    reference_row = _select_variant_summary_row(
        result.variant_model_summary_df,
        variant_key=_REFERENCE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_row = _select_variant_summary_row(
        result.variant_model_summary_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_vs_reference_row = _select_variant_vs_reference_row(
        result.variant_vs_reference_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )
    reference_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        variant_key=_REFERENCE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )

    lines = [
        "# TOPIX100 Streak 3/53 Next-Session Intraday Portfolio Construction Walk-Forward",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Walk-forward windows: `train {result.train_window} / test {result.test_window} / step {result.step}`",
        f"- Split count: `{result.split_count}`",
        "- Target: `next-session open -> close return`",
        f"- Reference book: `Top {result.reference_top_k} / Bottom {result.reference_top_k}`",
        f"- Alternative book: `abs(score) Top {result.absolute_selection_count} signed`",
        "",
        "## Current Read",
        "",
        "This study keeps the same rolling prediction engine and changes only the book-construction rule after scoring.",
    ]
    if reference_row is not None and reference_stats is not None:
        lines.append(
            f"- LightGBM Top {result.reference_top_k} / Bottom {result.reference_top_k}: average selected-edge `{_format_return(float(reference_row['avg_selected_edge']))}`, Sharpe `{float(reference_stats['sharpe_ratio']):.2f}`, max drawdown `{_format_return(float(reference_stats['max_drawdown']))}`."
        )
    if absolute_row is not None and absolute_stats is not None:
        lines.append(
            f"- LightGBM abs-score Top {result.absolute_selection_count} signed: average selected-edge `{_format_return(float(absolute_row['avg_selected_edge']))}`, Sharpe `{float(absolute_stats['sharpe_ratio']):.2f}`, max drawdown `{_format_return(float(absolute_stats['max_drawdown']))}`."
        )
        lines.append(
            f"- Average book shape under abs-score Top {result.absolute_selection_count}: long `{float(absolute_row['avg_long_count']):.2f}`, short `{float(absolute_row['avg_short_count']):.2f}`, net exposure `{float(absolute_row['avg_net_exposure']):+.2f}`."
        )
    if absolute_vs_reference_row is not None:
        lines.append(
            f"- Delta vs current LightGBM book: selected-edge `{_format_return(float(absolute_vs_reference_row['selected_edge_delta_vs_reference']))}`, hit-rate `{float(absolute_vs_reference_row['hit_rate_delta_vs_reference']):+.2%}`, retention `{float(absolute_vs_reference_row['selected_edge_retention_vs_reference']):.1%}`."
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionIntradayPortfolioConstructionWalkforwardResearchResult,
) -> dict[str, Any]:
    reference_row = _select_variant_summary_row(
        result.variant_model_summary_df,
        variant_key=_REFERENCE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_row = _select_variant_summary_row(
        result.variant_model_summary_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_vs_reference_row = _select_variant_vs_reference_row(
        result.variant_vs_reference_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )
    reference_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        variant_key=_REFERENCE_VARIANT.key,
        model_name="lightgbm",
    )
    absolute_stats = _select_portfolio_stats_row(
        result.portfolio_stats_df,
        variant_key=_ABSOLUTE_VARIANT.key,
        model_name="lightgbm",
    )

    result_bullets = [
        "Every split uses the same train/test windows as the core intraday walk-forward study, so the only moving part is how the scored names are turned into a book.",
    ]
    if reference_row is not None:
        result_bullets.append(
            f"Under the current Top {result.reference_top_k} / Bottom {result.reference_top_k} book, LightGBM averaged {_format_return(float(reference_row['avg_selected_edge']))} of selected-edge per day."
        )
    if absolute_row is not None:
        result_bullets.append(
            f"Under abs-score Top {result.absolute_selection_count} signed selection, LightGBM averaged {_format_return(float(absolute_row['avg_selected_edge']))} per day with average long {float(absolute_row['avg_long_count']):.2f}, short {float(absolute_row['avg_short_count']):.2f}, and net exposure {float(absolute_row['avg_net_exposure']):+.2f}."
        )
    if absolute_vs_reference_row is not None:
        result_bullets.append(
            f"Versus the current book, the abs-score Top {result.absolute_selection_count} variant changed selected-edge by {_format_return(float(absolute_vs_reference_row['selected_edge_delta_vs_reference']))} and hit-rate by {float(absolute_vs_reference_row['hit_rate_delta_vs_reference']):+.2%}."
        )
    if reference_stats is not None and absolute_stats is not None:
        result_bullets.append(
            f"Sharpe moved from {float(reference_stats['sharpe_ratio']):.2f} on the current book to {float(absolute_stats['sharpe_ratio']):.2f} on the abs-score book, while max drawdown moved from {_format_return(float(reference_stats['max_drawdown']))} to {_format_return(float(absolute_stats['max_drawdown']))}."
        )

    highlights = [
        {
            "label": "Split count",
            "value": str(result.split_count),
            "tone": "accent",
            "detail": "walk-forward blocks",
        },
        {
            "label": "Reference book",
            "value": f"{result.reference_top_k}/{result.reference_top_k}",
            "tone": "neutral",
            "detail": "top / bottom",
        },
        {
            "label": "Alt book",
            "value": str(result.absolute_selection_count),
            "tone": "neutral",
            "detail": "abs-score signed picks",
        },
    ]
    if reference_row is not None:
        highlights.append(
            {
                "label": "Current edge",
                "value": _format_return(float(reference_row["avg_selected_edge"])),
                "tone": "success",
                "detail": "LightGBM selected-edge",
            }
        )
    if absolute_row is not None:
        highlights.append(
            {
                "label": "Abs-score edge",
                "value": _format_return(float(absolute_row["avg_selected_edge"])),
                "tone": "accent",
                "detail": "LightGBM selected-edge",
            }
        )
    if absolute_stats is not None:
        highlights.append(
            {
                "label": "Abs-score Sharpe",
                "value": f"{float(absolute_stats['sharpe_ratio']):.2f}",
                "tone": "accent",
                "detail": "LightGBM",
            }
        )

    return {
        "title": "TOPIX100 Intraday Portfolio Construction Walk-Forward",
        "headline": (
            "This study reuses the same next-session intraday walk-forward score and asks whether a signed abs-score book beats the current symmetric Top/Bottom construction."
        ),
        "purpose": (
            "Compare the current Top 3 / Bottom 3 book against a new abs(score) Top 5 signed book while keeping the same walk-forward training schedule, score model, and universe."
        ),
        "method": (
            "Build one rolling prediction panel for baseline and LightGBM, then evaluate two book-construction rules from the same scores: symmetric Top/Bottom and abs-score signed selection."
        ),
        "results": result_bullets,
        "considerations": [
            "The abs-score book is not forced to stay dollar-neutral; its long and short counts vary with the score sign mix, so net exposure matters as much as raw edge.",
            "Selected-edge is the equal-weight average signed edge across the chosen names, which makes the current 3/3 book directly comparable to the new 5-name signed book.",
        ],
        "highlights": highlights,
        "tables": [
            {
                "key": "variant_model_summary_df",
                "title": "Variant Summary",
                "description": "Average selected-edge, hit-rate, and book shape for each portfolio-construction variant and model.",
            },
            {
                "key": "variant_vs_reference_df",
                "title": "Variant vs Reference",
                "description": "How much the abs-score signed book gains or loses versus the current Top/Bottom book.",
            },
            {
                "key": "portfolio_stats_df",
                "title": "Portfolio Stats",
                "description": "Sharpe, drawdown, CAGR, and total return for each model and book-construction variant.",
            },
        ],
    }


def _select_variant_summary_row(
    variant_model_summary_df: pd.DataFrame,
    *,
    variant_key: str,
    model_name: str,
) -> pd.Series | None:
    scoped_df = variant_model_summary_df[
        (variant_model_summary_df["variant_key"] == variant_key)
        & (variant_model_summary_df["model_name"] == model_name)
    ]
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_variant_vs_reference_row(
    variant_vs_reference_df: pd.DataFrame,
    *,
    variant_key: str,
    model_name: str,
) -> pd.Series | None:
    scoped_df = variant_vs_reference_df[
        (variant_vs_reference_df["variant_key"] == variant_key)
        & (variant_vs_reference_df["model_name"] == model_name)
    ]
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_portfolio_stats_row(
    portfolio_stats_df: pd.DataFrame,
    *,
    variant_key: str,
    model_name: str,
) -> pd.Series | None:
    scoped_df = portfolio_stats_df[
        (portfolio_stats_df["variant_key"] == variant_key)
        & (portfolio_stats_df["model_name"] == model_name)
    ]
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]
