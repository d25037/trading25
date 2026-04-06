"""
TOPIX100 multivariate priority study under the fixed streak 3/53 transfer model.

This study treats the available information itself as the variable. On the same
joined TOPIX100 panel used by the existing streak/bucket studies, it asks:

1. If a long-only selector can observe only some subset of
   {bucket, volume, short mode, long mode}, how much out-of-sample edge can it
   retain?
2. The same question for the short side.
3. Which feature contributes the most marginal value once the others are
   already known?

The study uses a discovery/validation split. For every subset of the four
features, discovery chooses the best rule and validation scores it. Feature
priority is then quantified with both exact Shapley values and leave-one-out
gaps.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import factorial
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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    VOLUME_BUCKET_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_strongest_setup_q10_threshold import (
    _build_state_decile_horizon_panel,
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
from src.domains.analytics.topix_streak_extreme_mode import (
    _format_int_sequence,
    _format_return,
)

DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (1, 5, 10)
DEFAULT_MIN_DISCOVERY_DATE_COUNT = 60
DEFAULT_MIN_VALIDATION_DATE_COUNT = 40
TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-multivariate-priority"
)
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
_SIDE_ORDER: tuple[str, ...] = ("long", "short")
_FEATURE_ORDER: tuple[str, ...] = ("bucket", "volume", "short_mode", "long_mode")
_FEATURE_COLUMN_MAP: dict[str, str] = {
    "bucket": "decile",
    "volume": "volume_bucket",
    "short_mode": "short_mode",
    "long_mode": "long_mode",
}
_FEATURE_LABEL_MAP: dict[str, str] = {
    "bucket": "Bucket",
    "volume": "Volume",
    "short_mode": "Short mode",
    "long_mode": "Long mode",
}
_ALL_SENTINEL = "all"
_MODE_VALUE_LABEL_MAP: dict[str, str] = {
    "bullish": "Bullish",
    "bearish": "Bearish",
}
_PRIMARY_HORIZON_WEIGHT_ORDER: tuple[int, ...] = DEFAULT_FUTURE_HORIZONS


@dataclass(frozen=True)
class Topix100Streak353MultivariatePriorityResearchResult:
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
    future_horizons: tuple[int, ...]
    validation_ratio: float
    min_discovery_date_count: int
    min_validation_date_count: int
    universe_constituent_count: int
    covered_constituent_count: int
    joined_event_count: int
    valid_date_count: int
    state_decile_horizon_panel_df: pd.DataFrame
    subset_daily_panel_df: pd.DataFrame
    subset_candidate_scorecard_df: pd.DataFrame
    subset_rule_scorecard_df: pd.DataFrame
    feature_priority_df: pd.DataFrame
    feature_leave_one_out_df: pd.DataFrame
    full_feature_setup_df: pd.DataFrame
    validation_extreme_bucket_comparison_df: pd.DataFrame


def run_topix100_streak_353_multivariate_priority_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    future_horizons: tuple[int, ...] | list[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    min_discovery_date_count: int = DEFAULT_MIN_DISCOVERY_DATE_COUNT,
    min_validation_date_count: int = DEFAULT_MIN_VALIDATION_DATE_COUNT,
) -> Topix100Streak353MultivariatePriorityResearchResult:
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if min_discovery_date_count <= 0:
        raise ValueError("min_discovery_date_count must be positive")
    if min_validation_date_count <= 0:
        raise ValueError("min_validation_date_count must be positive")

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
        future_horizons=resolved_horizons,
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
        future_horizons=resolved_horizons,
    )
    subset_daily_panel_df = _build_subset_daily_panel_df(state_decile_horizon_panel_df)
    subset_candidate_scorecard_df = _build_subset_candidate_scorecard_df(
        subset_daily_panel_df,
        future_horizons=resolved_horizons,
    )
    subset_rule_scorecard_df = _build_subset_rule_scorecard_df(
        subset_candidate_scorecard_df,
        future_horizons=resolved_horizons,
        min_discovery_date_count=min_discovery_date_count,
        min_validation_date_count=min_validation_date_count,
    )
    feature_priority_df = _build_feature_priority_df(
        subset_rule_scorecard_df,
        future_horizons=resolved_horizons,
    )
    feature_leave_one_out_df = _build_feature_leave_one_out_df(
        subset_rule_scorecard_df,
        future_horizons=resolved_horizons,
    )
    full_feature_setup_df = _build_full_feature_setup_df(subset_rule_scorecard_df)
    validation_extreme_bucket_comparison_df = _build_validation_extreme_bucket_comparison_df(
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        subset_candidate_scorecard_df=subset_candidate_scorecard_df,
        future_horizons=resolved_horizons,
    )

    analysis_start_date = (
        str(state_decile_horizon_panel_df["date"].min())
        if not state_decile_horizon_panel_df.empty
        else None
    )
    analysis_end_date = (
        str(state_decile_horizon_panel_df["date"].max())
        if not state_decile_horizon_panel_df.empty
        else None
    )

    return Topix100Streak353MultivariatePriorityResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=price_result.available_start_date,
        available_end_date=price_result.available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        min_discovery_date_count=min_discovery_date_count,
        min_validation_date_count=min_validation_date_count,
        universe_constituent_count=int(price_result.topix100_constituent_count),
        covered_constituent_count=int(state_decile_horizon_panel_df["code"].nunique()),
        joined_event_count=int(len(state_decile_horizon_panel_df)),
        valid_date_count=int(state_decile_horizon_panel_df["date"].nunique()),
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        subset_daily_panel_df=subset_daily_panel_df,
        subset_candidate_scorecard_df=subset_candidate_scorecard_df,
        subset_rule_scorecard_df=subset_rule_scorecard_df,
        feature_priority_df=feature_priority_df,
        feature_leave_one_out_df=feature_leave_one_out_df,
        full_feature_setup_df=full_feature_setup_df,
        validation_extreme_bucket_comparison_df=validation_extreme_bucket_comparison_df,
    )


def write_topix100_streak_353_multivariate_priority_research_bundle(
    result: Topix100Streak353MultivariatePriorityResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_multivariate_priority_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "min_discovery_date_count": result.min_discovery_date_count,
            "min_validation_date_count": result.min_validation_date_count,
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
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "min_discovery_date_count": result.min_discovery_date_count,
            "min_validation_date_count": result.min_validation_date_count,
            "universe_constituent_count": result.universe_constituent_count,
            "covered_constituent_count": result.covered_constituent_count,
            "joined_event_count": result.joined_event_count,
            "valid_date_count": result.valid_date_count,
        },
        result_tables={
            "state_decile_horizon_panel_df": result.state_decile_horizon_panel_df,
            "subset_daily_panel_df": result.subset_daily_panel_df,
            "subset_candidate_scorecard_df": result.subset_candidate_scorecard_df,
            "subset_rule_scorecard_df": result.subset_rule_scorecard_df,
            "feature_priority_df": result.feature_priority_df,
            "feature_leave_one_out_df": result.feature_leave_one_out_df,
            "full_feature_setup_df": result.full_feature_setup_df,
            "validation_extreme_bucket_comparison_df": result.validation_extreme_bucket_comparison_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_multivariate_priority_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353MultivariatePriorityResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return Topix100Streak353MultivariatePriorityResearchResult(
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
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        validation_ratio=float(metadata["validation_ratio"]),
        min_discovery_date_count=int(metadata["min_discovery_date_count"]),
        min_validation_date_count=int(metadata["min_validation_date_count"]),
        universe_constituent_count=int(metadata["universe_constituent_count"]),
        covered_constituent_count=int(metadata["covered_constituent_count"]),
        joined_event_count=int(metadata["joined_event_count"]),
        valid_date_count=int(metadata["valid_date_count"]),
        state_decile_horizon_panel_df=tables["state_decile_horizon_panel_df"],
        subset_daily_panel_df=tables["subset_daily_panel_df"],
        subset_candidate_scorecard_df=tables["subset_candidate_scorecard_df"],
        subset_rule_scorecard_df=tables["subset_rule_scorecard_df"],
        feature_priority_df=tables["feature_priority_df"],
        feature_leave_one_out_df=tables["feature_leave_one_out_df"],
        full_feature_setup_df=tables["full_feature_setup_df"],
        validation_extreme_bucket_comparison_df=tables[
            "validation_extreme_bucket_comparison_df"
        ],
    )


def get_topix100_streak_353_multivariate_priority_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


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
    grouped = subset_daily_panel_df.groupby(group_columns + ["horizon_days"], observed=True, sort=False)
    for keys, scoped_df in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
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
        record = {
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


def _build_subset_rule_scorecard_df(
    subset_candidate_scorecard_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
    min_discovery_date_count: int,
    min_validation_date_count: int,
) -> pd.DataFrame:
    if subset_candidate_scorecard_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for side in _SIDE_ORDER:
        discovery_score_column = f"primary_{side}_score"
        validation_score_column = f"primary_{side}_score"
        discovery_hit_rate_column = f"primary_{side}_hit_rate"
        validation_hit_rate_column = f"primary_{side}_hit_rate"

        for subset in _iter_feature_subsets():
            subset_key = _build_subset_key(subset)
            discovery_df = subset_candidate_scorecard_df[
                (subset_candidate_scorecard_df["sample_split"] == "discovery")
                & (subset_candidate_scorecard_df["subset_key"] == subset_key)
            ].copy()
            validation_df = subset_candidate_scorecard_df[
                (subset_candidate_scorecard_df["sample_split"] == "validation")
                & (subset_candidate_scorecard_df["subset_key"] == subset_key)
            ].copy()
            if discovery_df.empty or validation_df.empty:
                continue

            merged_df = discovery_df.merge(
                validation_df,
                on=["subset_key", "selector_value_key"],
                suffixes=("_discovery", "_validation"),
                how="inner",
            )
            if merged_df.empty:
                continue

            strict_df = merged_df[
                merged_df.apply(
                    lambda row: _row_meets_date_thresholds(
                        row,
                        future_horizons=future_horizons,
                        suffix="discovery",
                        min_date_count=min_discovery_date_count,
                    )
                    and _row_meets_date_thresholds(
                        row,
                        future_horizons=future_horizons,
                        suffix="validation",
                        min_date_count=min_validation_date_count,
                    ),
                    axis=1,
                )
            ].copy()

            if strict_df.empty:
                selected_df = merged_df[
                    merged_df.apply(
                        lambda row: _row_has_any_validation_dates(
                            row,
                            future_horizons=future_horizons,
                        ),
                        axis=1,
                    )
                ].copy()
                gate_status = "fallback_any_validation"
            else:
                selected_df = strict_df
                gate_status = "thresholded"

            if selected_df.empty:
                continue

            ordered_df = _order_rule_candidates(
                selected_df,
                side=side,
                discovery_score_column=f"{discovery_score_column}_discovery",
            )
            selected_row = ordered_df.iloc[0]

            record: dict[str, Any] = {
                "side": side,
                "subset_key": subset_key,
                "subset_label": _build_subset_label(subset),
                "feature_count": len(subset),
                "selection_gate_status": gate_status,
                "selector_value_key": str(selected_row["selector_value_key"]),
                "selector_value_label": str(selected_row["selector_value_label_discovery"]),
                "bucket": selected_row.get("bucket_discovery"),
                "volume": selected_row.get("volume_discovery"),
                "short_mode": selected_row.get("short_mode_discovery"),
                "long_mode": selected_row.get("long_mode_discovery"),
                "discovery_primary_score": float(selected_row[f"{discovery_score_column}_discovery"]),
                "validation_primary_score": float(selected_row[f"{validation_score_column}_validation"]),
                "discovery_primary_hit_rate": float(selected_row[f"{discovery_hit_rate_column}_discovery"]),
                "validation_primary_hit_rate": float(selected_row[f"{validation_hit_rate_column}_validation"]),
            }
            for horizon in future_horizons:
                record[f"discovery_avg_return_{horizon}d"] = float(
                    selected_row[f"avg_return_{horizon}d_discovery"]
                )
                record[f"validation_avg_return_{horizon}d"] = float(
                    selected_row[f"avg_return_{horizon}d_validation"]
                )
                record[f"discovery_date_count_{horizon}d"] = int(
                    selected_row[f"date_count_{horizon}d_discovery"]
                )
                record[f"validation_date_count_{horizon}d"] = int(
                    selected_row[f"date_count_{horizon}d_validation"]
                )
                record[f"validation_positive_hit_rate_{horizon}d"] = float(
                    selected_row[f"positive_hit_rate_{horizon}d_validation"]
                )
                record[f"validation_negative_hit_rate_{horizon}d"] = float(
                    selected_row[f"negative_hit_rate_{horizon}d_validation"]
                )
                record[f"validation_avg_stock_count_{horizon}d"] = float(
                    selected_row[f"avg_stock_count_{horizon}d_validation"]
                )
            records.append(record)

    return _sort_rule_scorecard_df(pd.DataFrame(records))


def _build_feature_priority_df(
    subset_rule_scorecard_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if subset_rule_scorecard_df.empty:
        return pd.DataFrame()

    score_columns = {
        "primary": "validation_primary_score",
        **{
            f"{horizon}d": f"validation_avg_return_{horizon}d"
            for horizon in future_horizons
        },
    }
    records: list[dict[str, Any]] = []
    all_feature_count = len(_FEATURE_ORDER)
    denominator = factorial(all_feature_count)
    for side in _SIDE_ORDER:
        side_df = subset_rule_scorecard_df[subset_rule_scorecard_df["side"] == side].copy()
        if side_df.empty:
            continue
        score_map: dict[tuple[str, ...], dict[str, float]] = {}
        for _, row in side_df.iterrows():
            subset = _parse_subset_key(str(row["subset_key"]))
            metric_values: dict[str, float] = {}
            for metric_name, column_name in score_columns.items():
                raw_value = row.get(column_name)
                if raw_value is None or pd.isna(raw_value):
                    continue
                numeric_value = float(raw_value)
                if side == "short" and metric_name != "primary":
                    numeric_value = -numeric_value
                metric_values[metric_name] = numeric_value
            score_map[subset] = metric_values

        for feature in _FEATURE_ORDER:
            shapley_values = {metric_name: 0.0 for metric_name in score_columns}
            for subset in _iter_feature_subsets(exclude_feature=feature):
                with_feature = _normalize_subset((*subset, feature))
                without_metrics = score_map.get(subset)
                with_metrics = score_map.get(with_feature)
                if without_metrics is None or with_metrics is None:
                    continue
                subset_size = len(subset)
                weight = (
                    factorial(subset_size)
                    * factorial(all_feature_count - subset_size - 1)
                    / denominator
                )
                for metric_name in score_columns:
                    if metric_name not in without_metrics or metric_name not in with_metrics:
                        continue
                    shapley_values[metric_name] += weight * (
                        with_metrics[metric_name] - without_metrics[metric_name]
                    )
            records.append(
                {
                    "side": side,
                    "feature_name": feature,
                    "feature_label": _FEATURE_LABEL_MAP[feature],
                    **{
                        f"shapley_{metric_name}_score": value
                        for metric_name, value in shapley_values.items()
                    },
                }
            )

    feature_priority_df = pd.DataFrame(records)
    if feature_priority_df.empty:
        return feature_priority_df

    for side in _SIDE_ORDER:
        mask = feature_priority_df["side"] == side
        scoped = feature_priority_df[mask].copy()
        if scoped.empty:
            continue
        total_abs = scoped["shapley_primary_score"].abs().sum()
        feature_priority_df.loc[mask, "abs_share_primary"] = (
            scoped["shapley_primary_score"].abs() / total_abs if total_abs else 0.0
        )
        ranking = scoped["shapley_primary_score"].rank(
            method="first",
            ascending=False,
        ).astype(int)
        feature_priority_df.loc[mask, "priority_rank_primary"] = ranking.values

    return feature_priority_df.sort_values(
        ["side", "priority_rank_primary", "feature_name"],
        ascending=[True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_feature_leave_one_out_df(
    subset_rule_scorecard_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if subset_rule_scorecard_df.empty:
        return pd.DataFrame()

    full_subset_key = _build_subset_key(_FEATURE_ORDER)
    records: list[dict[str, Any]] = []
    for side in _SIDE_ORDER:
        side_df = subset_rule_scorecard_df[subset_rule_scorecard_df["side"] == side].copy()
        full_df = side_df[side_df["subset_key"] == full_subset_key].copy()
        if full_df.empty:
            continue
        full_row = full_df.iloc[0]
        for feature in _FEATURE_ORDER:
            reduced_subset = tuple(item for item in _FEATURE_ORDER if item != feature)
            reduced_key = _build_subset_key(reduced_subset)
            reduced_df = side_df[side_df["subset_key"] == reduced_key].copy()
            if reduced_df.empty:
                continue
            reduced_row = reduced_df.iloc[0]
            record: dict[str, Any] = {
                "side": side,
                "feature_name": feature,
                "feature_label": _FEATURE_LABEL_MAP[feature],
                "full_selector_value_label": str(full_row["selector_value_label"]),
                "without_feature_selector_value_label": str(
                    reduced_row["selector_value_label"]
                ),
                "full_validation_primary_score": float(full_row["validation_primary_score"]),
                "without_feature_validation_primary_score": float(
                    reduced_row["validation_primary_score"]
                ),
                "primary_score_delta": float(full_row["validation_primary_score"])
                - float(reduced_row["validation_primary_score"]),
            }
            for horizon in future_horizons:
                full_value = float(full_row[f"validation_avg_return_{horizon}d"])
                reduced_value = float(reduced_row[f"validation_avg_return_{horizon}d"])
                if side == "short":
                    full_score = -full_value
                    reduced_score = -reduced_value
                else:
                    full_score = full_value
                    reduced_score = reduced_value
                record[f"full_score_{horizon}d"] = full_score
                record[f"without_feature_score_{horizon}d"] = reduced_score
                record[f"score_delta_{horizon}d"] = full_score - reduced_score
            records.append(record)

    leave_one_out_df = pd.DataFrame(records)
    if leave_one_out_df.empty:
        return leave_one_out_df
    return leave_one_out_df.sort_values(
        ["side", "primary_score_delta", "feature_name"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_full_feature_setup_df(
    subset_rule_scorecard_df: pd.DataFrame,
) -> pd.DataFrame:
    if subset_rule_scorecard_df.empty:
        return pd.DataFrame()
    full_subset_key = _build_subset_key(_FEATURE_ORDER)
    full_df = subset_rule_scorecard_df[
        subset_rule_scorecard_df["subset_key"] == full_subset_key
    ].copy()
    if full_df.empty:
        return full_df
    return full_df.sort_values(
        ["side", "validation_primary_score"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)


def _build_validation_extreme_bucket_comparison_df(
    *,
    state_decile_horizon_panel_df: pd.DataFrame,
    subset_candidate_scorecard_df: pd.DataFrame,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if state_decile_horizon_panel_df.empty or subset_candidate_scorecard_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []

    bucket_only_rows = subset_candidate_scorecard_df[
        (subset_candidate_scorecard_df["sample_split"] == "validation")
        & (subset_candidate_scorecard_df["subset_key"] == "bucket")
        & (subset_candidate_scorecard_df["bucket"].isin(["Q1", "Q10"]))
    ].copy()
    for _, row in bucket_only_rows.iterrows():
        records.extend(
            _build_candidate_rows(
                row=row,
                side="long",
                context_key="bucket_only_long",
                context_label="Bucket only (long read)",
                score_column="primary_long_score",
                future_horizons=future_horizons,
            )
        )
        records.extend(
            _build_candidate_rows(
                row=row,
                side="short",
                context_key="bucket_only_short",
                context_label="Bucket only (short read)",
                score_column="primary_short_score",
                future_horizons=future_horizons,
            )
        )

    records.extend(
        _build_validation_band_rows(
            panel_df=state_decile_horizon_panel_df,
            side="long",
            context_key="full_feature_long",
            context_label="Full feature long (Volume Low + Short Bearish + Long Bearish)",
            future_horizons=future_horizons,
            bucket_groups=(
                ("Q1", ("Q1",)),
                ("Q1-Q2", ("Q1", "Q2")),
                ("Q8-Q10", ("Q8", "Q9", "Q10")),
                ("Q10", ("Q10",)),
            ),
            volume_bucket="volume_low",
            short_mode="bearish",
            long_mode="bearish",
        )
    )
    records.extend(
        _build_validation_band_rows(
            panel_df=state_decile_horizon_panel_df,
            side="short",
            context_key="full_feature_short",
            context_label="Full feature short (Volume High + Short Bullish + Long Bullish)",
            future_horizons=future_horizons,
            bucket_groups=(
                ("Q1", ("Q1",)),
                ("Q10", ("Q10",)),
            ),
            volume_bucket="volume_high",
            short_mode="bullish",
            long_mode="bullish",
        )
    )

    comparison_df = pd.DataFrame(records)
    if comparison_df.empty:
        return comparison_df
    return comparison_df.sort_values(
        ["side", "context_key", "band_order", "horizon_days"],
        ascending=[True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_candidate_rows(
    *,
    row: pd.Series,
    side: str,
    context_key: str,
    context_label: str,
    score_column: str,
    future_horizons: tuple[int, ...],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for horizon in future_horizons:
        records.append(
            {
                "side": side,
                "context_key": context_key,
                "context_label": context_label,
                "band_label": str(row["selector_value_label"]),
                "band_order": _band_order_for_label(str(row["selector_value_label"])),
                "horizon_days": int(horizon),
                "mean_equal_weight_return": float(row[f"avg_return_{horizon}d"]),
                "selector_score": float(row[score_column]),
                "date_count": int(row[f"date_count_{horizon}d"]),
                "avg_stock_count": float(row[f"avg_stock_count_{horizon}d"]),
            }
        )
    return records


def _build_validation_band_rows(
    *,
    panel_df: pd.DataFrame,
    side: str,
    context_key: str,
    context_label: str,
    future_horizons: tuple[int, ...],
    bucket_groups: tuple[tuple[str, tuple[str, ...]], ...],
    volume_bucket: str,
    short_mode: str,
    long_mode: str,
) -> list[dict[str, Any]]:
    validation_df = panel_df[
        (panel_df["sample_split"] == "validation")
        & (panel_df["volume_bucket"] == volume_bucket)
        & (panel_df["short_mode"] == short_mode)
        & (panel_df["long_mode"] == long_mode)
        & (panel_df["horizon_days"].isin(future_horizons))
    ].copy()
    if validation_df.empty:
        return []

    records: list[dict[str, Any]] = []
    for band_label, deciles in bucket_groups:
        band_df = validation_df[validation_df["decile"].isin(deciles)].copy()
        if band_df.empty:
            continue
        daily_df = (
            band_df.groupby(["date", "horizon_days"], observed=True, sort=False)
            .agg(
                daily_equal_weight_return=("future_return", "mean"),
                stock_count=("code", "nunique"),
            )
            .reset_index()
        )
        for horizon in future_horizons:
            horizon_df = daily_df[daily_df["horizon_days"] == horizon].copy()
            if horizon_df.empty:
                continue
            records.append(
                {
                    "side": side,
                    "context_key": context_key,
                    "context_label": context_label,
                    "band_label": band_label,
                    "band_order": _band_order_for_label(band_label),
                    "horizon_days": int(horizon),
                    "mean_equal_weight_return": float(
                        horizon_df["daily_equal_weight_return"].mean()
                    ),
                    "selector_score": float(
                        horizon_df["daily_equal_weight_return"].mean()
                    ),
                    "date_count": int(horizon_df["date"].nunique()),
                    "avg_stock_count": float(horizon_df["stock_count"].mean()),
                }
            )
    return records


def _band_order_for_label(band_label: str) -> int:
    order_map = {
        "Q1": 1,
        "Q1-Q2": 2,
        "Q8-Q10": 3,
        "Q10": 4,
    }
    return order_map.get(band_label, 999)


def _row_meets_date_thresholds(
    row: pd.Series,
    *,
    future_horizons: tuple[int, ...],
    suffix: str,
    min_date_count: int,
) -> bool:
    for horizon in future_horizons:
        value = row.get(f"date_count_{horizon}d_{suffix}")
        if value is None or pd.isna(value) or int(value) < min_date_count:
            return False
    return True


def _row_has_any_validation_dates(
    row: pd.Series,
    *,
    future_horizons: tuple[int, ...],
) -> bool:
    return any(
        int(row.get(f"date_count_{horizon}d_validation", 0) or 0) > 0
        for horizon in future_horizons
    )


def _order_rule_candidates(
    merged_df: pd.DataFrame,
    *,
    side: str,
    discovery_score_column: str,
) -> pd.DataFrame:
    if side == "long":
        return merged_df.sort_values(
            [
                discovery_score_column,
                "avg_return_5d_discovery",
                "avg_return_10d_discovery",
                "avg_return_1d_discovery",
                "avg_stock_count_5d_discovery",
                "selector_value_key",
            ],
            ascending=[False, False, False, False, False, True],
            kind="stable",
        )
    return merged_df.sort_values(
        [
            discovery_score_column,
            "avg_return_5d_discovery",
            "avg_return_10d_discovery",
            "avg_return_1d_discovery",
            "avg_stock_count_5d_discovery",
            "selector_value_key",
        ],
        ascending=[False, True, True, True, False, True],
        kind="stable",
    )


def _iter_feature_subsets(
    *,
    exclude_feature: str | None = None,
) -> list[tuple[str, ...]]:
    features = tuple(
        feature for feature in _FEATURE_ORDER if feature != exclude_feature
    )
    subsets: list[tuple[str, ...]] = [tuple()]
    for size in range(1, len(features) + 1):
        for subset in combinations(features, size):
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


def _parse_subset_key(subset_key: str) -> tuple[str, ...]:
    if subset_key == "universe":
        return tuple()
    return _normalize_subset(tuple(part for part in subset_key.split("+") if part))


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
        return str(VOLUME_BUCKET_LABEL_MAP.get(str(value), value))
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


def _sort_rule_scorecard_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    side_order = {name: index for index, name in enumerate(_SIDE_ORDER)}
    sortable = df.copy()
    sortable["_side_order"] = sortable["side"].map(side_order).fillna(999)
    sortable = sortable.sort_values(
        ["_side_order", "feature_count", "subset_key"],
        ascending=[True, True, True],
        kind="stable",
    )
    return sortable.drop(columns=["_side_order"]).reset_index(drop=True)


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353MultivariatePriorityResearchResult,
) -> str:
    full_setup_df = result.full_feature_setup_df.copy()
    feature_priority_df = result.feature_priority_df.copy()
    leave_one_out_df = result.feature_leave_one_out_df.copy()
    extreme_comparison_df = result.validation_extreme_bucket_comparison_df.copy()
    lines = [
        "# TOPIX100 Streak 3/53 Multivariate Priority Study",
        "",
        "This study keeps the fixed streak transfer lens (short=3 / long=53) and the existing price/volume bucket lens, but changes the question. Instead of asking for a single best setup, it asks which pieces of information are most worth knowing before taking a long or short.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Price feature: `{result.price_feature}`",
        f"- Volume feature: `{result.volume_feature}`",
        f"- Fixed short / long pair: `{result.short_window_streaks} / {result.long_window_streaks}`",
        f"- Future horizons: `{_format_int_sequence(result.future_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Min discovery dates / rule: `{result.min_discovery_date_count}`",
        f"- Min validation dates / rule: `{result.min_validation_date_count}`",
        f"- Joined horizon rows: `{result.joined_event_count}`",
        "",
        "## Best Full-Feature Rules",
        "",
    ]
    if full_setup_df.empty:
        lines.append("- No full-feature rule passed the selection gates.")
    else:
        for row in full_setup_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"{str(row['side']).title()}: `{row['selector_value_label']}` "
                f"with validation primary score `{float(row['validation_primary_score']):.4f}` "
                f"and returns "
                + ", ".join(
                    f"{int(h)}d `{_format_return(float(row[f'validation_avg_return_{int(h)}d']))}`"
                    for h in result.future_horizons
                )
            )

    lines.extend(["", "## Q1 vs Q10 Comparison", ""])
    if extreme_comparison_df.empty:
        lines.append("- No validation comparison rows were available.")
    else:
        for context_key in (
            "bucket_only_long",
            "full_feature_long",
            "bucket_only_short",
            "full_feature_short",
        ):
            context_df = extreme_comparison_df[
                extreme_comparison_df["context_key"] == context_key
            ].copy()
            if context_df.empty:
                continue
            lines.append(f"- {str(context_df['context_label'].iloc[0])}:")
            for band_label in context_df["band_label"].drop_duplicates().tolist():
                band_rows = context_df[context_df["band_label"] == band_label].copy()
                band_rows = band_rows.sort_values("horizon_days", kind="stable")
                metrics = ", ".join(
                    f"{int(row['horizon_days'])}d `{_format_return(float(row['mean_equal_weight_return']))}`"
                    for row in band_rows.to_dict(orient="records")
                )
                lines.append(f"  - `{band_label}` -> {metrics}")

    lines.extend(["", "## Feature Priority", ""])
    if feature_priority_df.empty:
        lines.append("- No feature-priority rows were available.")
    else:
        for side in _SIDE_ORDER:
            side_df = feature_priority_df[feature_priority_df["side"] == side].copy()
            if side_df.empty:
                continue
            ordered = side_df.sort_values(
                ["priority_rank_primary", "feature_name"],
                ascending=[True, True],
                kind="stable",
            )
            ordering = " > ".join(
                f"{str(row['feature_label'])} ({float(row['shapley_primary_score']):.4f})"
                for row in ordered.to_dict(orient="records")
            )
            lines.append(f"- {side.title()}: {ordering}")

    lines.extend(["", "## Leave-One-Out Gaps", ""])
    if leave_one_out_df.empty:
        lines.append("- No leave-one-out rows were available.")
    else:
        for side in _SIDE_ORDER:
            side_df = leave_one_out_df[leave_one_out_df["side"] == side].copy()
            if side_df.empty:
                continue
            top_row = side_df.iloc[0]
            lines.append(
                "- "
                f"{side.title()}: removing `{top_row['feature_label']}` cuts the validation primary score by "
                f"`{float(top_row['primary_score_delta']):.4f}` "
                f"(full `{top_row['full_selector_value_label']}` vs reduced `{top_row['without_feature_selector_value_label']}`)."
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `state_decile_horizon_panel_df`",
            "- `subset_daily_panel_df`",
            "- `subset_candidate_scorecard_df`",
            "- `subset_rule_scorecard_df`",
            "- `feature_priority_df`",
            "- `feature_leave_one_out_df`",
            "- `full_feature_setup_df`",
            "- `validation_extreme_bucket_comparison_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353MultivariatePriorityResearchResult,
) -> dict[str, Any]:
    feature_priority_df = result.feature_priority_df.copy()
    leave_one_out_df = result.feature_leave_one_out_df.copy()
    full_feature_setup_df = result.full_feature_setup_df.copy()
    extreme_comparison_df = result.validation_extreme_bucket_comparison_df.copy()

    long_priority = _select_top_priority_row(feature_priority_df, "long")
    short_priority = _select_top_priority_row(feature_priority_df, "short")
    long_leave_one_out = _select_top_leave_one_out_row(leave_one_out_df, "long")
    short_leave_one_out = _select_top_leave_one_out_row(leave_one_out_df, "short")
    long_setup = _select_setup_row(full_feature_setup_df, "long")
    short_setup = _select_setup_row(full_feature_setup_df, "short")
    bucket_only_long_q1 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="bucket_only_long",
        band_label="Q1",
        horizon_days=5,
    )
    bucket_only_long_q10 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="bucket_only_long",
        band_label="Q10",
        horizon_days=5,
    )
    full_long_q1 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="full_feature_long",
        band_label="Q1",
        horizon_days=5,
    )
    full_long_q10 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="full_feature_long",
        band_label="Q10",
        horizon_days=5,
    )
    full_long_q1q2 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="full_feature_long",
        band_label="Q1-Q2",
        horizon_days=5,
    )
    full_long_q8q10 = _select_comparison_row(
        extreme_comparison_df,
        side="long",
        context_key="full_feature_long",
        band_label="Q8-Q10",
        horizon_days=5,
    )
    full_short_q1 = _select_comparison_row(
        extreme_comparison_df,
        side="short",
        context_key="full_feature_short",
        band_label="Q1",
        horizon_days=5,
    )
    full_short_q10 = _select_comparison_row(
        extreme_comparison_df,
        side="short",
        context_key="full_feature_short",
        band_label="Q10",
        horizon_days=5,
    )

    result_bullets = [
        "This is an information-priority study, not another parameter search. For each subset of {bucket, volume, short mode, long mode}, discovery chooses the best rule and validation measures how much edge that information can actually keep.",
        "The main read is `subset_rule_scorecard_df`: it tells you what the best discovery-selected rule becomes once each information subset is all you are allowed to know at decision time.",
    ]
    highlights = [
        {
            "label": "Fixed pair",
            "value": f"{result.short_window_streaks} / {result.long_window_streaks}",
            "tone": "accent",
            "detail": "streak candles",
        },
        {
            "label": "Candidate subsets",
            "value": str(len(_iter_feature_subsets())),
            "tone": "neutral",
            "detail": "including Universe baseline",
        },
    ]

    if long_setup is not None:
        result_bullets.append(
            f"On the long side, the full-feature selector ends up at {long_setup['selector_value_label']} with validation returns "
            + ", ".join(
                f"{int(h)}d {_format_return(float(long_setup[f'validation_avg_return_{int(h)}d']))}"
                for h in result.future_horizons
            )
            + "."
        )
        highlights.append(
            {
                "label": "Best long setup",
                "value": str(long_setup["selector_value_label"]),
                "tone": "success",
                "detail": f"score {float(long_setup['validation_primary_score']):.4f}",
            }
        )
    if short_setup is not None:
        result_bullets.append(
            f"On the short side, the full-feature selector ends up at {short_setup['selector_value_label']} with validation returns "
            + ", ".join(
                f"{int(h)}d {_format_return(float(short_setup[f'validation_avg_return_{int(h)}d']))}"
                for h in result.future_horizons
            )
            + "."
        )
        highlights.append(
            {
                "label": "Best short setup",
                "value": str(short_setup["selector_value_label"]),
                "tone": "danger",
                "detail": f"score {float(short_setup['validation_primary_score']):.4f}",
            }
        )
    if bucket_only_long_q1 is not None and bucket_only_long_q10 is not None:
        bucket_winner_label = (
            "Q10"
            if float(bucket_only_long_q10["mean_equal_weight_return"])
            >= float(bucket_only_long_q1["mean_equal_weight_return"])
            else "Q1"
        )
        result_bullets.append(
            f"Bucket alone points first to `{bucket_winner_label}` on the long side: at 5d the validation equal-weight return was {_format_return(float(bucket_only_long_q10['mean_equal_weight_return']))} for `Q10` versus {_format_return(float(bucket_only_long_q1['mean_equal_weight_return']))} for `Q1`."
        )
    if (
        full_long_q1 is not None
        and full_long_q10 is not None
        and full_long_q1q2 is not None
        and full_long_q8q10 is not None
    ):
        full_long_winner_label = (
            "Q1"
            if float(full_long_q1["mean_equal_weight_return"])
            >= float(full_long_q10["mean_equal_weight_return"])
            else "Q10"
        )
        result_bullets.append(
            f"Once the full long setup is fixed, the best extreme bucket becomes `{full_long_winner_label}`: at 5d `Q1` was {_format_return(float(full_long_q1['mean_equal_weight_return']))}, `Q10` was {_format_return(float(full_long_q10['mean_equal_weight_return']))}, `Q1-Q2` was {_format_return(float(full_long_q1q2['mean_equal_weight_return']))}, and `Q8-Q10` was {_format_return(float(full_long_q8q10['mean_equal_weight_return']))}."
        )
    if full_short_q1 is not None and full_short_q10 is not None:
        short_winner_label = (
            "Q1"
            if float(full_short_q1["mean_equal_weight_return"])
            <= float(full_short_q10["mean_equal_weight_return"])
            else "Q10"
        )
        result_bullets.append(
            f"On the short side, the cleaner 5d downside pocket is `{short_winner_label}` under the full short setup: `Q1` was {_format_return(float(full_short_q1['mean_equal_weight_return']))} and `Q10` was {_format_return(float(full_short_q10['mean_equal_weight_return']))}."
        )
    if long_priority is not None:
        result_bullets.append(
            f"Long-side Shapley priority is led by {long_priority['feature_label']} at {float(long_priority['shapley_primary_score']):.4f}, meaning this feature contributed the largest average out-of-sample uplift once the other selectors were already known."
        )
    if short_priority is not None:
        result_bullets.append(
            f"Short-side Shapley priority is led by {short_priority['feature_label']} at {float(short_priority['shapley_primary_score']):.4f}, so this is the first item to preserve if the short filter has to be simplified."
        )
    if long_leave_one_out is not None:
        result_bullets.append(
            f"Leave-one-out says the long selector is most fragile to losing {long_leave_one_out['feature_label']}: removing it cuts the validation primary score by {float(long_leave_one_out['primary_score_delta']):.4f}."
        )
    if short_leave_one_out is not None:
        result_bullets.append(
            f"Leave-one-out says the short selector is most fragile to losing {short_leave_one_out['feature_label']}: removing it cuts the validation primary score by {float(short_leave_one_out['primary_score_delta']):.4f}."
        )

    headline = (
        "The multivariate read turns the current TOPIX100 streak research into a priority stack: it quantifies which of bucket, volume, short mode, and long mode matter most on the long side and on the short side."
    )

    return {
        "title": "TOPIX100 Streak 3/53 Multivariate Priority",
        "tags": ["TOPIX100", "streaks", "multivariate", "feature-priority"],
        "purpose": (
            "Quantify how much decision value each of the four selectors contributes on TOPIX100: bucket, volume high/low, short streak mode, and long streak mode."
        ),
        "method": [
            "Join the existing TOPIX100 price-vs-SMA decile lens with each stock's own fixed streak 3 / 53 state, then keep the resulting event panel at the stock-date-horizon level.",
            "For every subset of the four selectors, collapse the panel to date-balanced equal-weight rule returns, choose the best rule on discovery, and score it on validation.",
            "Turn those validation scores into feature priority two ways: exact Shapley values across all subsets, and leave-one-out drops versus the full four-feature selector.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This measures the value of information, not pure causality. If two selectors are highly correlated, one can absorb part of the other's edge and make its marginal contribution look smaller.",
            "The study is date-balanced by construction. That matters because raw pooled-event means can be dominated by dates where one setup simply appears more often.",
            "The practical interpretation is operational: when the screen or ranking page cannot expose every selector, keep the ones with the highest Shapley and leave-one-out importance first.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {"label": "Future horizons", "value": _format_int_sequence(result.future_horizons)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
            {
                "label": "Discovery min dates",
                "value": str(result.min_discovery_date_count),
            },
            {
                "label": "Validation min dates",
                "value": str(result.min_validation_date_count),
            },
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "subset_rule_scorecard_df",
                "label": "Best rule by information subset",
                "description": "Discovery-selected long and short rules, re-scored on validation for every subset of the four selectors.",
            },
            {
                "name": "feature_priority_df",
                "label": "Shapley feature priority",
                "description": "Exact marginal contribution of bucket, volume, short mode, and long mode on both the long and short side.",
            },
            {
                "name": "feature_leave_one_out_df",
                "label": "Leave-one-out gaps",
                "description": "How much validation score the full selector loses when each feature is removed once.",
            },
            {
                "name": "validation_extreme_bucket_comparison_df",
                "label": "Q1/Q10 comparison table",
                "description": "Validation comparison of Q1, Q1-Q2, Q8-Q10, and Q10 across bucket-only and full-feature contexts.",
            },
        ],
    }


def _select_top_priority_row(
    feature_priority_df: pd.DataFrame,
    side: str,
) -> pd.Series | None:
    side_df = feature_priority_df[feature_priority_df["side"] == side].copy()
    if side_df.empty:
        return None
    sorted_df = side_df.sort_values(
        ["shapley_primary_score", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    if sorted_df.empty:
        return None
    return sorted_df.iloc[0]


def _select_top_leave_one_out_row(
    leave_one_out_df: pd.DataFrame,
    side: str,
) -> pd.Series | None:
    if leave_one_out_df.empty or "side" not in leave_one_out_df.columns:
        return None
    side_df = leave_one_out_df[leave_one_out_df["side"] == side].copy()
    if side_df.empty:
        return None
    sorted_df = side_df.sort_values(
        ["primary_score_delta", "feature_name"],
        ascending=[False, True],
        kind="stable",
    )
    if sorted_df.empty:
        return None
    return sorted_df.iloc[0]


def _select_setup_row(
    full_feature_setup_df: pd.DataFrame,
    side: str,
) -> pd.Series | None:
    side_df = full_feature_setup_df[full_feature_setup_df["side"] == side].copy()
    if side_df.empty:
        return None
    return side_df.iloc[0]


def _select_comparison_row(
    comparison_df: pd.DataFrame,
    *,
    side: str,
    context_key: str,
    band_label: str,
    horizon_days: int,
) -> pd.Series | None:
    if comparison_df.empty:
        return None
    scoped = comparison_df[
        (comparison_df["side"] == side)
        & (comparison_df["context_key"] == context_key)
        & (comparison_df["band_label"] == band_label)
        & (comparison_df["horizon_days"] == horizon_days)
    ].copy()
    if scoped.empty:
        return None
    return scoped.iloc[0]
