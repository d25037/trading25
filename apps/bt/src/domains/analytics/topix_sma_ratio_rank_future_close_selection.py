"""
Selection and composite helper tables for TOPIX SMA-ratio rank research.

This module isolates discovery/validation feature selection and composite score
construction from the public research entrypoint.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    _assign_feature_deciles as _core_assign_feature_deciles,
    _build_daily_group_means as _core_build_daily_group_means,
    _build_global_significance as _core_build_global_significance,
    _build_horizon_panel as _core_build_horizon_panel,
    _build_pairwise_significance as _core_build_pairwise_significance,
    _summarize_future_targets as _core_summarize_future_targets,
    _summarize_ranking_features as _core_summarize_ranking_features,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_buckets import (
    _build_extreme_vs_middle_daily_means,
    _build_extreme_vs_middle_significance,
    _summarize_extreme_vs_middle,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    COMPOSITE_METHOD_ORDER,
    DISCOVERY_END_DATE,
    HORIZON_ORDER,
    HorizonKey,
    MetricKey,
    RANKING_FEATURE_LABEL_MAP,
    RANKING_FEATURE_ORDER,
    VALIDATION_START_DATE,
    DecileKey,
    _sort_frame,
)


def _oriented_rank_score(
    event_panel_df: pd.DataFrame,
    *,
    feature_name: str,
    direction: str,
) -> pd.Series:
    rank_pct = (
        event_panel_df.groupby("date")[feature_name]
        .rank(method="first", pct=True, ascending=True)
        .astype(float)
    )
    min_score = 1.0 / event_panel_df["date_constituent_count"].astype(float)
    if direction == "high":
        return rank_pct
    return (1.0 - rank_pct + min_score).clip(lower=min_score, upper=1.0)


def _build_composite_feature_name(
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> str:
    return (
        f"composite::{price_feature}:{price_direction}"
        f"__{volume_feature}:{volume_direction}"
        f"__{score_method}"
    )


def _build_composite_feature_label(
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> str:
    method_label = "Rank Mean" if score_method == "rank_mean" else "Rank Product"
    return (
        f"{method_label} | "
        f"{RANKING_FEATURE_LABEL_MAP.get(price_feature, price_feature)} ({price_direction}) + "
        f"{RANKING_FEATURE_LABEL_MAP.get(volume_feature, volume_feature)} ({volume_direction})"
    )


def _build_composite_ranked_panel(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    price_score = _oriented_rank_score(
        event_panel_df,
        feature_name=price_feature,
        direction=price_direction,
    )
    volume_score = _oriented_rank_score(
        event_panel_df,
        feature_name=volume_feature,
        direction=volume_direction,
    )
    if score_method == "rank_mean":
        ranking_value = (price_score + volume_score) / 2.0
    else:
        ranking_value = np.sqrt(price_score * volume_score)

    composite_name = _build_composite_feature_name(
        price_feature=price_feature,
        price_direction=price_direction,
        volume_feature=volume_feature,
        volume_direction=volume_direction,
        score_method=score_method,
    )
    composite_label = _build_composite_feature_label(
        price_feature=price_feature,
        price_direction=price_direction,
        volume_feature=volume_feature,
        volume_direction=volume_direction,
        score_method=score_method,
    )
    ranked_panel_df = event_panel_df[
        [
            "date",
            "code",
            "company_name",
            "close",
            "volume",
            "date_constituent_count",
            *[f"{horizon_key}_close" for horizon_key in HORIZON_ORDER],
            *[f"{horizon_key}_return" for horizon_key in HORIZON_ORDER],
        ]
    ].copy()
    ranked_panel_df["ranking_feature"] = composite_name
    ranked_panel_df["ranking_feature_label"] = composite_label
    ranked_panel_df["ranking_value"] = ranking_value.astype(float)
    ranked_panel_df["price_feature"] = price_feature
    ranked_panel_df["price_direction"] = price_direction
    ranked_panel_df["volume_feature"] = volume_feature
    ranked_panel_df["volume_direction"] = volume_direction
    ranked_panel_df["score_method"] = score_method
    return _core_assign_feature_deciles(ranked_panel_df)


def _filter_df_by_date_split(df: pd.DataFrame, *, split_name: str) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df.copy()
    if split_name == "discovery":
        return df.loc[df["date"] <= DISCOVERY_END_DATE].copy()
    if split_name == "validation":
        return df.loc[df["date"] >= VALIDATION_START_DATE].copy()
    raise ValueError(f"Unsupported split_name: {split_name}")


def _analyze_ranked_panel(ranked_panel_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if ranked_panel_df.empty:
        empty_df = pd.DataFrame()
        return {
            "ranking_feature_summary_df": empty_df,
            "decile_future_summary_df": empty_df,
            "daily_group_means_df": empty_df,
            "global_significance_df": empty_df,
            "pairwise_significance_df": empty_df,
            "extreme_vs_middle_summary_df": empty_df,
            "extreme_vs_middle_daily_means_df": empty_df,
            "extreme_vs_middle_significance_df": empty_df,
        }

    horizon_panel_df = _core_build_horizon_panel(
        ranked_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )
    daily_group_means_df = _core_build_daily_group_means(
        horizon_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )
    extreme_vs_middle_daily_means_df = _build_extreme_vs_middle_daily_means(
        horizon_panel_df
    )
    return {
        "ranking_feature_summary_df": _core_summarize_ranking_features(
            ranked_panel_df,
            known_feature_order=RANKING_FEATURE_ORDER,
        ),
        "decile_future_summary_df": _core_summarize_future_targets(
            horizon_panel_df,
            known_feature_order=RANKING_FEATURE_ORDER,
        ),
        "daily_group_means_df": daily_group_means_df,
        "global_significance_df": _core_build_global_significance(
            daily_group_means_df,
            known_feature_order=RANKING_FEATURE_ORDER,
        ),
        "pairwise_significance_df": _core_build_pairwise_significance(
            daily_group_means_df,
            known_feature_order=RANKING_FEATURE_ORDER,
        ),
        "extreme_vs_middle_summary_df": _summarize_extreme_vs_middle(
            extreme_vs_middle_daily_means_df
        ),
        "extreme_vs_middle_daily_means_df": extreme_vs_middle_daily_means_df,
        "extreme_vs_middle_significance_df": _build_extreme_vs_middle_significance(
            extreme_vs_middle_daily_means_df
        ),
    }


def _extract_global_row(
    global_significance_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    metric_key: MetricKey = "future_return",
) -> pd.Series | None:
    if global_significance_df.empty:
        return None
    row = global_significance_df[
        (global_significance_df["ranking_feature"] == ranking_feature)
        & (global_significance_df["horizon_key"] == horizon_key)
        & (global_significance_df["metric_key"] == metric_key)
    ]
    if row.empty:
        return None
    return row.iloc[0]


def _extract_pairwise_row(
    pairwise_significance_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    left_decile: DecileKey = "Q1",
    right_decile: DecileKey = "Q10",
    metric_key: MetricKey = "future_return",
) -> pd.Series | None:
    if pairwise_significance_df.empty:
        return None
    row = pairwise_significance_df[
        (pairwise_significance_df["ranking_feature"] == ranking_feature)
        & (pairwise_significance_df["horizon_key"] == horizon_key)
        & (pairwise_significance_df["metric_key"] == metric_key)
        & (pairwise_significance_df["left_decile"] == left_decile)
        & (pairwise_significance_df["right_decile"] == right_decile)
    ]
    if row.empty:
        return None
    return row.iloc[0]


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _direction_from_difference(raw_difference: float | None) -> str:
    if raw_difference is None:
        return "high"
    return "high" if raw_difference >= 0 else "low"


def _aligned_difference(raw_difference: float | None, *, direction: str) -> float | None:
    if raw_difference is None:
        return None
    return raw_difference if direction == "high" else -raw_difference


def _robustness_score(
    aligned_discovery_diff: float | None,
    aligned_validation_diff: float | None,
) -> float | None:
    if aligned_discovery_diff is None or aligned_validation_diff is None:
        return None
    if aligned_discovery_diff <= 0 or aligned_validation_diff <= 0:
        return float(min(aligned_discovery_diff, aligned_validation_diff))
    return float(min(aligned_discovery_diff, aligned_validation_diff))


def _build_feature_selection(
    *,
    discovery_analysis: dict[str, pd.DataFrame],
    validation_analysis: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    records: list[dict[str, Any]] = []
    for ranking_feature in RANKING_FEATURE_ORDER:
        feature_family = "price" if ranking_feature.startswith("price_") else "volume"
        for horizon_key in HORIZON_ORDER:
            discovery_global = _extract_global_row(
                discovery_analysis["global_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            validation_global = _extract_global_row(
                validation_analysis["global_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            discovery_pairwise = _extract_pairwise_row(
                discovery_analysis["pairwise_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            validation_pairwise = _extract_pairwise_row(
                validation_analysis["pairwise_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )

            discovery_diff = _as_float(
                discovery_global["q1_minus_q10_mean"] if discovery_global is not None else None
            )
            validation_diff = _as_float(
                validation_global["q1_minus_q10_mean"] if validation_global is not None else None
            )
            direction = _direction_from_difference(discovery_diff)
            aligned_discovery_diff = abs(discovery_diff) if discovery_diff is not None else None
            aligned_validation_diff = _aligned_difference(
                validation_diff,
                direction=direction,
            )
            records.append(
                {
                    "ranking_feature": ranking_feature,
                    "ranking_feature_label": RANKING_FEATURE_LABEL_MAP[ranking_feature],
                    "feature_family": feature_family,
                    "horizon_key": horizon_key,
                    "discovery_direction": direction,
                    "discovery_q1_mean": _as_float(discovery_global["q1_mean"] if discovery_global is not None else None),
                    "discovery_q10_mean": _as_float(discovery_global["q10_mean"] if discovery_global is not None else None),
                    "discovery_q1_minus_q10_mean": discovery_diff,
                    "discovery_friedman_p_value": _as_float(discovery_global["friedman_p_value"] if discovery_global is not None else None),
                    "discovery_kendalls_w": _as_float(discovery_global["kendalls_w"] if discovery_global is not None else None),
                    "discovery_q1_q10_paired_t_p_value": _as_float(discovery_pairwise["paired_t_p_value_holm"] if discovery_pairwise is not None else None),
                    "discovery_q1_q10_wilcoxon_p_value": _as_float(discovery_pairwise["wilcoxon_p_value_holm"] if discovery_pairwise is not None else None),
                    "validation_q1_mean": _as_float(validation_global["q1_mean"] if validation_global is not None else None),
                    "validation_q10_mean": _as_float(validation_global["q10_mean"] if validation_global is not None else None),
                    "validation_q1_minus_q10_mean": validation_diff,
                    "validation_aligned_q1_minus_q10_mean": aligned_validation_diff,
                    "validation_friedman_p_value": _as_float(validation_global["friedman_p_value"] if validation_global is not None else None),
                    "validation_kendalls_w": _as_float(validation_global["kendalls_w"] if validation_global is not None else None),
                    "validation_q1_q10_paired_t_p_value": _as_float(validation_pairwise["paired_t_p_value_holm"] if validation_pairwise is not None else None),
                    "validation_q1_q10_wilcoxon_p_value": _as_float(validation_pairwise["wilcoxon_p_value_holm"] if validation_pairwise is not None else None),
                    "direction_consistent": bool(aligned_validation_diff is not None and aligned_validation_diff > 0),
                    "robustness_score": _robustness_score(
                        aligned_discovery_diff,
                        aligned_validation_diff,
                    ),
                }
            )

    feature_selection_df = _sort_frame(pd.DataFrame.from_records(records))
    if feature_selection_df.empty:
        return feature_selection_df, feature_selection_df

    sort_df = feature_selection_df.copy()
    sort_df["_robustness_score"] = pd.to_numeric(
        sort_df["robustness_score"], errors="coerce"
    ).fillna(float("-inf"))
    sort_df["_validation_p"] = pd.to_numeric(
        sort_df["validation_q1_q10_paired_t_p_value"], errors="coerce"
    ).fillna(1.0)
    sort_df["_validation_global_p"] = pd.to_numeric(
        sort_df["validation_friedman_p_value"], errors="coerce"
    ).fillna(1.0)
    sort_df = sort_df.sort_values(
        [
            "feature_family",
            "horizon_key",
            "_robustness_score",
            "_validation_p",
            "_validation_global_p",
        ],
        ascending=[True, True, False, True, True],
    )
    selected_feature_df = (
        sort_df.groupby(["feature_family", "horizon_key"], as_index=False)
        .head(1)
        .drop(columns=["_robustness_score", "_validation_p", "_validation_global_p"])
        .reset_index(drop=True)
    )
    return feature_selection_df, _sort_frame(selected_feature_df)


def _build_composite_candidates(
    event_panel_df: pd.DataFrame,
    *,
    selected_feature_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    selection_lookup = {
        (row["feature_family"], row["horizon_key"]): row
        for row in selected_feature_df.to_dict(orient="records")
    }
    candidate_records: list[dict[str, Any]] = []
    selected_combo_analyses: dict[str, dict[str, pd.DataFrame]] = {}
    candidate_rows_by_horizon: dict[HorizonKey, list[dict[str, Any]]] = {
        horizon_key: [] for horizon_key in HORIZON_ORDER
    }
    for horizon_key in HORIZON_ORDER:
        price_row = selection_lookup.get(("price", horizon_key))
        volume_row = selection_lookup.get(("volume", horizon_key))
        if price_row is None or volume_row is None:
            continue

        price_feature = str(price_row["ranking_feature"])
        price_direction = str(price_row["discovery_direction"])
        volume_feature = str(volume_row["ranking_feature"])
        volume_direction = str(volume_row["discovery_direction"])

        for score_method in COMPOSITE_METHOD_ORDER:
            composite_ranked_panel_df = _build_composite_ranked_panel(
                event_panel_df,
                price_feature=price_feature,
                price_direction=price_direction,
                volume_feature=volume_feature,
                volume_direction=volume_direction,
                score_method=score_method,
            )
            if composite_ranked_panel_df.empty:
                continue

            composite_name = str(composite_ranked_panel_df["ranking_feature"].iloc[0])
            discovery_analysis = _analyze_ranked_panel(
                _filter_df_by_date_split(
                    composite_ranked_panel_df,
                    split_name="discovery",
                )
            )
            validation_analysis = _analyze_ranked_panel(
                _filter_df_by_date_split(
                    composite_ranked_panel_df,
                    split_name="validation",
                )
            )
            discovery_global = _extract_global_row(
                discovery_analysis["global_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            validation_global = _extract_global_row(
                validation_analysis["global_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            discovery_pairwise = _extract_pairwise_row(
                discovery_analysis["pairwise_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            validation_pairwise = _extract_pairwise_row(
                validation_analysis["pairwise_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            discovery_diff = _as_float(
                discovery_global["q1_minus_q10_mean"] if discovery_global is not None else None
            )
            validation_diff = _as_float(
                validation_global["q1_minus_q10_mean"] if validation_global is not None else None
            )
            candidate_record = {
                "selected_horizon_key": horizon_key,
                "ranking_feature": composite_name,
                "ranking_feature_label": composite_ranked_panel_df["ranking_feature_label"].iloc[0],
                "price_feature": price_feature,
                "price_feature_label": RANKING_FEATURE_LABEL_MAP[price_feature],
                "price_direction": price_direction,
                "volume_feature": volume_feature,
                "volume_feature_label": RANKING_FEATURE_LABEL_MAP[volume_feature],
                "volume_direction": volume_direction,
                "score_method": score_method,
                "discovery_q1_mean": _as_float(discovery_global["q1_mean"] if discovery_global is not None else None),
                "discovery_q10_mean": _as_float(discovery_global["q10_mean"] if discovery_global is not None else None),
                "discovery_q1_minus_q10_mean": discovery_diff,
                "discovery_friedman_p_value": _as_float(discovery_global["friedman_p_value"] if discovery_global is not None else None),
                "discovery_q1_q10_paired_t_p_value": _as_float(discovery_pairwise["paired_t_p_value_holm"] if discovery_pairwise is not None else None),
                "validation_q1_mean": _as_float(validation_global["q1_mean"] if validation_global is not None else None),
                "validation_q10_mean": _as_float(validation_global["q10_mean"] if validation_global is not None else None),
                "validation_q1_minus_q10_mean": validation_diff,
                "validation_friedman_p_value": _as_float(validation_global["friedman_p_value"] if validation_global is not None else None),
                "validation_q1_q10_paired_t_p_value": _as_float(validation_pairwise["paired_t_p_value_holm"] if validation_pairwise is not None else None),
                "validation_q1_q10_wilcoxon_p_value": _as_float(validation_pairwise["wilcoxon_p_value_holm"] if validation_pairwise is not None else None),
                "direction_consistent": bool(
                    discovery_diff is not None
                    and validation_diff is not None
                    and discovery_diff > 0
                    and validation_diff > 0
                ),
                "robustness_score": _robustness_score(
                    discovery_diff,
                    validation_diff,
                ),
            }
            candidate_records.append(candidate_record)
            candidate_rows_by_horizon[horizon_key].append(candidate_record)
            selected_combo_analyses[composite_name] = _analyze_ranked_panel(
                composite_ranked_panel_df
            )

    composite_candidate_df = pd.DataFrame.from_records(candidate_records)
    if composite_candidate_df.empty:
        return composite_candidate_df, composite_candidate_df, {}

    selected_records: list[dict[str, Any]] = []
    for horizon_key in HORIZON_ORDER:
        horizon_candidates_df = pd.DataFrame.from_records(
            candidate_rows_by_horizon[horizon_key]
        )
        horizon_candidates_df["_robustness_score"] = pd.to_numeric(
            horizon_candidates_df["robustness_score"], errors="coerce"
        ).fillna(float("-inf"))
        horizon_candidates_df["_validation_p"] = pd.to_numeric(
            horizon_candidates_df["validation_q1_q10_paired_t_p_value"],
            errors="coerce",
        ).fillna(1.0)
        horizon_candidates_df["_validation_global_p"] = pd.to_numeric(
            horizon_candidates_df["validation_friedman_p_value"], errors="coerce"
        ).fillna(1.0)
        horizon_candidates_df = horizon_candidates_df.sort_values(
            ["_robustness_score", "_validation_p", "_validation_global_p"],
            ascending=[False, True, True],
        )
        selected_records.append(
            horizon_candidates_df.iloc[0]
            .drop(labels=["_robustness_score", "_validation_p", "_validation_global_p"])
            .to_dict()
        )

    selected_composite_df = _sort_frame(pd.DataFrame.from_records(selected_records))
    return (
        _sort_frame(composite_candidate_df),
        selected_composite_df,
        selected_combo_analyses,
    )


def _collect_selected_composite_tables(
    *,
    selected_composite_df: pd.DataFrame,
    selected_combo_analyses: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    analysis_table_names = (
        "ranking_feature_summary_df",
        "decile_future_summary_df",
        "daily_group_means_df",
        "global_significance_df",
        "pairwise_significance_df",
    )
    frames_by_name: dict[str, list[pd.DataFrame]] = {
        table_name: [] for table_name in analysis_table_names
    }

    if selected_composite_df.empty:
        return {table_name: pd.DataFrame() for table_name in analysis_table_names}

    metadata_columns = [
        "selected_horizon_key",
        "price_feature",
        "price_feature_label",
        "price_direction",
        "volume_feature",
        "volume_feature_label",
        "volume_direction",
        "score_method",
    ]
    for row in selected_composite_df.to_dict(orient="records"):
        ranking_feature = str(row["ranking_feature"])
        analysis = selected_combo_analyses.get(ranking_feature)
        if analysis is None:
            continue
        for table_name in analysis_table_names:
            frame = analysis[table_name].copy()
            if frame.empty:
                continue
            for column in metadata_columns:
                frame[column] = row[column]
            frames_by_name[table_name].append(frame)

    return {
        table_name: _sort_frame(pd.concat(frames, ignore_index=True))
        if frames
        else pd.DataFrame()
        for table_name, frames in frames_by_name.items()
    }
