"""
LightGBM ranking helpers for TOPIX100 SMA-ratio research notebooks.

This module layers a research-only LightGBM ranker on top of the existing
TOPIX100 SMA-ratio event panel. It intentionally does not modify the baseline
research result dataclass or any API-facing interfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, cast

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    DECILE_ORDER,
    _assign_feature_deciles,
    _sort_frame as _core_sort_frame,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    Topix100SmaRatioRankFutureCloseResearchResult,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_selection import (
    _analyze_ranked_panel,
    _build_composite_ranked_panel,
    _extract_global_row,
    _extract_pairwise_row,
    _filter_df_by_date_split,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    COMPOSITE_METHOD_ORDER,
    DISCOVERY_END_DATE,
    HORIZON_ORDER,
    RANKING_FEATURE_LABEL_MAP,
    RankingFeatureKey,
    RANKING_FEATURE_ORDER,
    VALIDATION_START_DATE,
)
from src.domains.backtest.core.walkforward import (
    WalkForwardSplit,
    generate_walkforward_splits,
)

LIGHTGBM_RESEARCH_INSTALL_HINT = (
    "uv sync --project apps/bt --group research"
)
LIGHTGBM_LIBOMP_INSTALL_HINT = "brew install libomp"
DEFAULT_WALKFORWARD_TRAIN_WINDOW = 756
DEFAULT_WALKFORWARD_TEST_WINDOW = 126
DEFAULT_WALKFORWARD_STEP = 126
MIN_SPLIT_COUNT_FOR_GATE = 6
POSITIVE_SPLIT_SHARE_GATE = 0.60


def _records_from_frame(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [cast(dict[str, Any], record) for record in df.to_dict(orient="records")]


class Topix100SmaRatioLightgbmResearchError(RuntimeError):
    """Raised when the notebook-specific LightGBM research path cannot run."""


@dataclass(frozen=True)
class Topix100SmaRatioLightgbmFixedSplitDiagnostic:
    discovery_end_date: str
    validation_start_date: str
    selected_model_df: pd.DataFrame
    comparison_summary_df: pd.DataFrame
    feature_importance_df: pd.DataFrame
    ranked_panel_df: pd.DataFrame
    ranking_feature_summary_df: pd.DataFrame
    decile_future_summary_df: pd.DataFrame
    daily_group_means_df: pd.DataFrame
    global_significance_df: pd.DataFrame
    pairwise_significance_df: pd.DataFrame


@dataclass(frozen=True)
class Topix100SmaRatioLightgbmWalkforwardResearchResult:
    train_window: int
    test_window: int
    step: int
    min_split_count_for_gate: int
    positive_split_share_gate: float
    overall_gate_status: str
    split_config_df: pd.DataFrame
    split_coverage_df: pd.DataFrame
    selected_model_df: pd.DataFrame
    baseline_selected_feature_df: pd.DataFrame
    baseline_selected_composite_df: pd.DataFrame
    comparison_summary_df: pd.DataFrame
    split_spread_df: pd.DataFrame
    feature_importance_df: pd.DataFrame
    feature_importance_split_df: pd.DataFrame
    ranked_panel_df: pd.DataFrame
    ranking_feature_summary_df: pd.DataFrame
    decile_future_summary_df: pd.DataFrame
    daily_group_means_df: pd.DataFrame
    global_significance_df: pd.DataFrame
    pairwise_significance_df: pd.DataFrame
    exploratory_gate_df: pd.DataFrame


@dataclass(frozen=True)
class Topix100SmaRatioLightgbmResearchResult:
    feature_columns: tuple[str, ...]
    walkforward: Topix100SmaRatioLightgbmWalkforwardResearchResult
    diagnostic: Topix100SmaRatioLightgbmFixedSplitDiagnostic | None
    diagnostic_error_message: str | None


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


def format_topix100_sma_ratio_rank_future_close_lightgbm_notebook_error(
    exc: Exception,
) -> str:
    if isinstance(exc, Topix100SmaRatioLightgbmResearchError):
        return str(exc)
    if isinstance(exc, ModuleNotFoundError):
        return _missing_lightgbm_message()
    if isinstance(exc, OSError):
        return _lightgbm_runtime_message(str(exc))
    return str(exc)


def _load_lightgbm_ranker_cls() -> type[Any]:
    try:
        lightgbm_module = import_module("lightgbm")
    except ModuleNotFoundError as exc:
        raise Topix100SmaRatioLightgbmResearchError(
            _missing_lightgbm_message()
        ) from exc
    except OSError as exc:
        raise Topix100SmaRatioLightgbmResearchError(
            _lightgbm_runtime_message(str(exc))
        ) from exc

    ranker_cls = getattr(lightgbm_module, "LGBMRanker", None)
    if ranker_cls is None:
        raise Topix100SmaRatioLightgbmResearchError(
            "LightGBM research is unavailable because lightgbm.LGBMRanker "
            "could not be imported."
        )
    return cast(type[Any], ranker_cls)


def _fixed_split_model_ranking_feature(horizon_key: str) -> str:
    return f"lightgbm::{horizon_key}"


def _fixed_split_model_ranking_feature_label(horizon_key: str) -> str:
    return f"LightGBM Ranker ({horizon_key})"


def _walkforward_lightgbm_ranking_feature(horizon_key: str) -> str:
    return f"walkforward::lightgbm::{horizon_key}"


def _walkforward_lightgbm_ranking_feature_label(horizon_key: str) -> str:
    return f"Walk-Forward LightGBM ({horizon_key})"


def _walkforward_baseline_ranking_feature(horizon_key: str) -> str:
    return f"walkforward::baseline::{horizon_key}"


def _walkforward_baseline_ranking_feature_label(horizon_key: str) -> str:
    return f"Walk-Forward Baseline Composite ({horizon_key})"


def _walkforward_feature_order() -> list[str]:
    return [
        *[_walkforward_baseline_ranking_feature(horizon_key) for horizon_key in HORIZON_ORDER],
        *[_walkforward_lightgbm_ranking_feature(horizon_key) for horizon_key in HORIZON_ORDER],
    ]


def _walkforward_lightgbm_feature_order() -> list[str]:
    return [
        _walkforward_lightgbm_ranking_feature(horizon_key)
        for horizon_key in HORIZON_ORDER
    ]


def _build_training_frame(
    event_panel_df: pd.DataFrame,
    *,
    horizon_key: str,
) -> pd.DataFrame:
    target_column = f"{horizon_key}_return"
    training_df = event_panel_df.dropna(subset=[target_column]).copy()
    if training_df.empty:
        return training_df

    training_df = training_df.sort_values(["date", "code"]).reset_index(drop=True)
    training_df["date_constituent_count"] = (
        training_df.groupby("date")["code"].transform("size").astype(int)
    )
    target_rank_desc = (
        training_df.groupby("date")[target_column]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    group_count = len(DECILE_ORDER)
    target_decile_index = (
        ((target_rank_desc - 1) * group_count) // training_df["date_constituent_count"]
    ).clip(lower=0, upper=group_count - 1)
    training_df["target_relevance"] = (group_count - 1 - target_decile_index).astype(
        int
    )
    return training_df


def _build_query_groups(scored_df: pd.DataFrame) -> list[int]:
    if scored_df.empty:
        return []
    return scored_df.groupby("date", sort=False)["code"].size().astype(int).tolist()


def _build_model_params() -> dict[str, Any]:
    return {
        "objective": "lambdarank",
        "metric": "ndcg",
        "importance_type": "gain",
        "learning_rate": 0.05,
        "n_estimators": 120,
        "num_leaves": 31,
        "min_data_in_leaf": 20,
        "random_state": 42,
        "verbosity": -1,
    }


def _build_scored_ranked_panel(
    scored_df: pd.DataFrame,
    *,
    ranking_feature: str,
    ranking_feature_label: str,
    predictions: pd.Series,
) -> pd.DataFrame:
    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        *[f"{key}_close" for key in HORIZON_ORDER],
        *[f"{key}_return" for key in HORIZON_ORDER],
    ]
    ranked_panel_df = scored_df[base_columns].copy()
    ranked_panel_df["ranking_feature"] = ranking_feature
    ranked_panel_df["ranking_feature_label"] = ranking_feature_label
    ranked_panel_df["ranking_value"] = predictions.astype(float)
    return ranked_panel_df


def _build_baseline_selected_ranked_panel(
    base_result: Topix100SmaRatioRankFutureCloseResearchResult,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for row in _records_from_frame(base_result.selected_composite_df):
        ranked_panel_df = _build_composite_ranked_panel(
            base_result.event_panel_df,
            price_feature=cast(RankingFeatureKey, row["price_feature"]),
            price_direction=str(row["price_direction"]),
            volume_feature=cast(RankingFeatureKey, row["volume_feature"]),
            volume_direction=str(row["volume_direction"]),
            score_method=str(row["score_method"]),
        )
        if ranked_panel_df.empty:
            continue
        ranked_panel_df["selected_horizon_key"] = row["selected_horizon_key"]
        frames.append(ranked_panel_df)
    if not frames:
        return pd.DataFrame()
    return _core_sort_frame(pd.concat(frames, ignore_index=True))


def _filter_df_by_date_range(
    df: pd.DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df.copy()
    mask = pd.Series(True, index=df.index)
    if start_date is not None:
        mask &= df["date"] >= start_date
    if end_date is not None:
        mask &= df["date"] <= end_date
    return df.loc[mask].copy()


def _analyze_ranked_panel_by_split(
    ranked_panel_df: pd.DataFrame,
) -> dict[str, dict[str, pd.DataFrame]]:
    overall = _analyze_ranked_panel(ranked_panel_df)
    discovery = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="discovery")
    )
    validation = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="validation")
    )
    return {
        "overall": overall,
        "discovery": discovery,
        "validation": validation,
    }


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _extract_comparison_record(
    *,
    selected_horizon_key: str,
    model_name: str,
    evaluation_split: str,
    ranking_feature: str,
    ranking_feature_label: str,
    analysis: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    global_row = _extract_global_row(
        analysis["global_significance_df"],
        ranking_feature=ranking_feature,
        horizon_key=cast(Any, selected_horizon_key),
    )
    pairwise_row = _extract_pairwise_row(
        analysis["pairwise_significance_df"],
        ranking_feature=ranking_feature,
        horizon_key=cast(Any, selected_horizon_key),
    )
    n_dates = 0
    if global_row is not None and pd.notna(global_row["n_dates"]):
        n_dates = int(global_row["n_dates"])

    return {
        "selected_horizon_key": selected_horizon_key,
        "model_name": model_name,
        "evaluation_split": evaluation_split,
        "metric_key": "future_return",
        "ranking_feature": ranking_feature,
        "ranking_feature_label": ranking_feature_label,
        "n_dates": n_dates,
        "q1_mean": _as_float(global_row["q1_mean"] if global_row is not None else None),
        "q10_mean": _as_float(
            global_row["q10_mean"] if global_row is not None else None
        ),
        "q1_minus_q10_mean": _as_float(
            global_row["q1_minus_q10_mean"] if global_row is not None else None
        ),
        "friedman_p_value": _as_float(
            global_row["friedman_p_value"] if global_row is not None else None
        ),
        "kruskal_p_value": _as_float(
            global_row["kruskal_p_value"] if global_row is not None else None
        ),
        "paired_t_p_value_holm": _as_float(
            pairwise_row["paired_t_p_value_holm"] if pairwise_row is not None else None
        ),
        "wilcoxon_p_value_holm": _as_float(
            pairwise_row["wilcoxon_p_value_holm"]
            if pairwise_row is not None
            else None
        ),
    }


def _build_fixed_split_comparison_summary_df(
    *,
    baseline_ranked_panel_df: pd.DataFrame,
    base_result: Topix100SmaRatioRankFutureCloseResearchResult,
    lightgbm_analyses: dict[str, dict[str, pd.DataFrame]],
    selected_model_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    baseline_analyses = _analyze_ranked_panel_by_split(baseline_ranked_panel_df)
    baseline_lookup = {
        str(row["selected_horizon_key"]): row
        for row in base_result.selected_composite_df.to_dict(orient="records")
    }
    model_lookup = {
        str(row["selected_horizon_key"]): row
        for row in selected_model_df.to_dict(orient="records")
    }

    for selected_horizon_key in HORIZON_ORDER:
        baseline_row = baseline_lookup.get(selected_horizon_key)
        if baseline_row is not None:
            for evaluation_split in ("overall", "discovery", "validation"):
                records.append(
                    _extract_comparison_record(
                        selected_horizon_key=selected_horizon_key,
                        model_name="baseline",
                        evaluation_split=evaluation_split,
                        ranking_feature=str(baseline_row["ranking_feature"]),
                        ranking_feature_label=str(
                            baseline_row["ranking_feature_label"]
                        ),
                        analysis=baseline_analyses[evaluation_split],
                    )
                )

        model_row = model_lookup.get(selected_horizon_key)
        if model_row is None:
            continue
        for evaluation_split in ("overall", "discovery", "validation"):
            records.append(
                _extract_comparison_record(
                    selected_horizon_key=selected_horizon_key,
                    model_name="lightgbm",
                    evaluation_split=evaluation_split,
                    ranking_feature=str(model_row["ranking_feature"]),
                    ranking_feature_label=str(model_row["ranking_feature_label"]),
                    analysis=lightgbm_analyses[evaluation_split],
                )
            )

    comparison_summary_df = pd.DataFrame.from_records(records)
    if comparison_summary_df.empty:
        return comparison_summary_df

    comparison_summary_df["evaluation_split"] = pd.Categorical(
        comparison_summary_df["evaluation_split"],
        categories=["overall", "discovery", "validation"],
        ordered=True,
    )
    comparison_summary_df["selected_horizon_key"] = pd.Categorical(
        comparison_summary_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    comparison_summary_df = comparison_summary_df.sort_values(
        ["selected_horizon_key", "evaluation_split", "model_name"],
        kind="stable",
    ).reset_index(drop=True)
    comparison_summary_df["selected_horizon_key"] = comparison_summary_df[
        "selected_horizon_key"
    ].astype(str)
    comparison_summary_df["evaluation_split"] = comparison_summary_df[
        "evaluation_split"
    ].astype(str)
    return comparison_summary_df


def _run_fixed_split_diagnostic(
    base_result: Topix100SmaRatioRankFutureCloseResearchResult,
    *,
    ranker_cls: type[Any],
    feature_columns: tuple[str, ...],
) -> Topix100SmaRatioLightgbmFixedSplitDiagnostic:
    ranked_frames: list[pd.DataFrame] = []
    selected_model_records: list[dict[str, Any]] = []
    feature_importance_records: list[dict[str, Any]] = []

    for horizon_key in HORIZON_ORDER:
        scored_df = _build_training_frame(base_result.event_panel_df, horizon_key=horizon_key)
        if scored_df.empty:
            raise Topix100SmaRatioLightgbmResearchError(
                f"LightGBM research found no rows with {horizon_key} targets."
            )

        training_df = scored_df.loc[scored_df["date"] <= DISCOVERY_END_DATE].copy()
        if training_df.empty:
            raise Topix100SmaRatioLightgbmResearchError(
                "Fixed-split LightGBM diagnostic requires discovery rows on or before "
                f"{DISCOVERY_END_DATE}. Adjust the notebook date range or use a "
                "database with earlier history."
            )

        train_groups = _build_query_groups(training_df)
        if not train_groups:
            raise Topix100SmaRatioLightgbmResearchError(
                f"LightGBM research could not build date query groups for {horizon_key}."
            )

        ranker = ranker_cls(**_build_model_params())
        ranker.fit(
            training_df[list(feature_columns)],
            training_df["target_relevance"],
            group=train_groups,
        )

        predictions = pd.Series(
            ranker.predict(scored_df[list(feature_columns)]),
            index=scored_df.index,
            name="ranking_value",
        )
        ranked_panel_df = _build_scored_ranked_panel(
            scored_df,
            ranking_feature=_fixed_split_model_ranking_feature(horizon_key),
            ranking_feature_label=_fixed_split_model_ranking_feature_label(horizon_key),
            predictions=predictions,
        )
        ranked_panel_df = _assign_feature_deciles(
            ranked_panel_df,
            known_feature_order=[
                _fixed_split_model_ranking_feature(key) for key in HORIZON_ORDER
            ],
        )
        ranked_frames.append(ranked_panel_df)

        selected_model_records.append(
            {
                "selected_horizon_key": horizon_key,
                "ranking_feature": _fixed_split_model_ranking_feature(horizon_key),
                "ranking_feature_label": _fixed_split_model_ranking_feature_label(
                    horizon_key
                ),
                "training_row_count": int(len(training_df)),
                "training_date_count": int(training_df["date"].nunique()),
                "training_query_count": int(len(train_groups)),
                "scored_row_count": int(len(scored_df)),
                "scored_date_count": int(scored_df["date"].nunique()),
                "scored_discovery_row_count": int(
                    (scored_df["date"] <= DISCOVERY_END_DATE).sum()
                ),
                "scored_validation_row_count": int(
                    (scored_df["date"] >= VALIDATION_START_DATE).sum()
                ),
            }
        )

        importance_values = pd.Series(
            getattr(ranker, "feature_importances_", []),
            dtype=float,
        )
        if len(importance_values) != len(feature_columns):
            raise Topix100SmaRatioLightgbmResearchError(
                "LightGBM research received unexpected feature importance output."
            )
        importance_df = pd.DataFrame(
            {
                "selected_horizon_key": horizon_key,
                "ranking_feature": _fixed_split_model_ranking_feature(horizon_key),
                "ranking_feature_label": _fixed_split_model_ranking_feature_label(
                    horizon_key
                ),
                "feature_name": feature_columns,
                "feature_label": [
                    RANKING_FEATURE_LABEL_MAP[cast(Any, feature_name)]
                    for feature_name in feature_columns
                ],
                "importance_gain": importance_values.to_numpy(),
            }
        )
        importance_df = importance_df.sort_values(
            ["importance_gain", "feature_name"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)
        importance_df["importance_rank"] = range(1, len(importance_df) + 1)
        feature_importance_records.extend(_records_from_frame(importance_df))

    ranked_panel_df = _core_sort_frame(
        pd.concat(ranked_frames, ignore_index=True),
        known_feature_order=[
            _fixed_split_model_ranking_feature(key) for key in HORIZON_ORDER
        ],
    )
    lightgbm_analyses = _analyze_ranked_panel_by_split(ranked_panel_df)

    selected_model_df = _core_sort_frame(pd.DataFrame.from_records(selected_model_records))
    feature_importance_df = _core_sort_frame(
        pd.DataFrame.from_records(feature_importance_records)
    )
    feature_importance_df["selected_horizon_key"] = pd.Categorical(
        feature_importance_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    feature_importance_df = feature_importance_df.sort_values(
        ["selected_horizon_key", "importance_rank"],
        kind="stable",
    ).reset_index(drop=True)
    feature_importance_df["selected_horizon_key"] = feature_importance_df[
        "selected_horizon_key"
    ].astype(str)
    comparison_summary_df = _build_fixed_split_comparison_summary_df(
        baseline_ranked_panel_df=_build_baseline_selected_ranked_panel(base_result),
        base_result=base_result,
        lightgbm_analyses=lightgbm_analyses,
        selected_model_df=selected_model_df,
    )

    return Topix100SmaRatioLightgbmFixedSplitDiagnostic(
        discovery_end_date=DISCOVERY_END_DATE,
        validation_start_date=VALIDATION_START_DATE,
        selected_model_df=selected_model_df,
        comparison_summary_df=comparison_summary_df,
        feature_importance_df=feature_importance_df,
        ranked_panel_df=ranked_panel_df,
        ranking_feature_summary_df=lightgbm_analyses["overall"][
            "ranking_feature_summary_df"
        ],
        decile_future_summary_df=lightgbm_analyses["overall"][
            "decile_future_summary_df"
        ],
        daily_group_means_df=lightgbm_analyses["overall"]["daily_group_means_df"],
        global_significance_df=lightgbm_analyses["overall"]["global_significance_df"],
        pairwise_significance_df=lightgbm_analyses["overall"][
            "pairwise_significance_df"
        ],
    )


def _build_train_only_feature_selection(
    *,
    split_index: int,
    split: WalkForwardSplit,
    train_analysis: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    records: list[dict[str, Any]] = []
    for ranking_feature in RANKING_FEATURE_ORDER:
        feature_family = "price" if ranking_feature.startswith("price_") else "volume"
        for horizon_key in HORIZON_ORDER:
            train_global = _extract_global_row(
                train_analysis["global_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            train_pairwise = _extract_pairwise_row(
                train_analysis["pairwise_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            train_diff = _as_float(
                train_global["q1_minus_q10_mean"] if train_global is not None else None
            )
            train_direction = "high" if train_diff is None or train_diff >= 0 else "low"
            records.append(
                {
                    "split_index": split_index,
                    "train_start": split.train_start,
                    "train_end": split.train_end,
                    "test_start": split.test_start,
                    "test_end": split.test_end,
                    "feature_family": feature_family,
                    "horizon_key": horizon_key,
                    "ranking_feature": ranking_feature,
                    "ranking_feature_label": RANKING_FEATURE_LABEL_MAP[
                        cast(Any, ranking_feature)
                    ],
                    "train_direction": train_direction,
                    "train_q1_mean": _as_float(
                        train_global["q1_mean"] if train_global is not None else None
                    ),
                    "train_q10_mean": _as_float(
                        train_global["q10_mean"] if train_global is not None else None
                    ),
                    "train_q1_minus_q10_mean": train_diff,
                    "train_aligned_q1_minus_q10_mean": (
                        abs(train_diff) if train_diff is not None else None
                    ),
                    "train_friedman_p_value": _as_float(
                        train_global["friedman_p_value"]
                        if train_global is not None
                        else None
                    ),
                    "train_q1_q10_paired_t_p_value": _as_float(
                        train_pairwise["paired_t_p_value_holm"]
                        if train_pairwise is not None
                        else None
                    ),
                }
            )

    feature_selection_df = pd.DataFrame.from_records(records)
    if feature_selection_df.empty:
        return feature_selection_df, feature_selection_df

    sort_df = feature_selection_df.copy()
    sort_df["_aligned_spread"] = pd.to_numeric(
        sort_df["train_aligned_q1_minus_q10_mean"],
        errors="coerce",
    ).fillna(float("-inf"))
    sort_df["_pairwise_p"] = pd.to_numeric(
        sort_df["train_q1_q10_paired_t_p_value"],
        errors="coerce",
    ).fillna(1.0)
    sort_df["_global_p"] = pd.to_numeric(
        sort_df["train_friedman_p_value"],
        errors="coerce",
    ).fillna(1.0)
    sort_df = sort_df.sort_values(
        [
            "split_index",
            "feature_family",
            "horizon_key",
            "_aligned_spread",
            "_pairwise_p",
            "_global_p",
        ],
        ascending=[True, True, True, False, True, True],
        kind="stable",
    )
    selected_feature_df = (
        sort_df.groupby(["split_index", "feature_family", "horizon_key"], as_index=False)
        .head(1)
        .drop(columns=["_aligned_spread", "_pairwise_p", "_global_p"])
        .reset_index(drop=True)
    )
    feature_selection_df = feature_selection_df.sort_values(
        ["split_index", "feature_family", "horizon_key", "ranking_feature"],
        kind="stable",
    ).reset_index(drop=True)
    selected_feature_df = selected_feature_df.sort_values(
        ["split_index", "feature_family", "horizon_key"],
        kind="stable",
    ).reset_index(drop=True)
    return feature_selection_df, selected_feature_df


def _build_train_only_composite_candidates(
    split_panel_df: pd.DataFrame,
    *,
    split_index: int,
    split: WalkForwardSplit,
    selected_feature_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    selection_lookup = {
        (row["feature_family"], row["horizon_key"]): row
        for row in selected_feature_df.to_dict(orient="records")
    }
    candidate_records: list[dict[str, Any]] = []
    selected_records: list[dict[str, Any]] = []
    composite_panel_lookup: dict[str, pd.DataFrame] = {}

    for horizon_key in HORIZON_ORDER:
        price_row = selection_lookup.get(("price", horizon_key))
        volume_row = selection_lookup.get(("volume", horizon_key))
        if price_row is None or volume_row is None:
            continue

        horizon_records: list[dict[str, Any]] = []
        price_feature = str(price_row["ranking_feature"])
        volume_feature = str(volume_row["ranking_feature"])
        price_direction = str(price_row["train_direction"])
        volume_direction = str(volume_row["train_direction"])

        for score_method in COMPOSITE_METHOD_ORDER:
            composite_ranked_panel_df = _build_composite_ranked_panel(
                split_panel_df,
                price_feature=cast(Any, price_feature),
                price_direction=price_direction,
                volume_feature=cast(Any, volume_feature),
                volume_direction=volume_direction,
                score_method=score_method,
            )
            if composite_ranked_panel_df.empty:
                continue

            composite_name = str(composite_ranked_panel_df["ranking_feature"].iloc[0])
            train_analysis = _analyze_ranked_panel(
                _filter_df_by_date_range(
                    composite_ranked_panel_df,
                    start_date=split.train_start,
                    end_date=split.train_end,
                )
            )
            train_global = _extract_global_row(
                train_analysis["global_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            train_pairwise = _extract_pairwise_row(
                train_analysis["pairwise_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            train_diff = _as_float(
                train_global["q1_minus_q10_mean"] if train_global is not None else None
            )
            record = {
                "split_index": split_index,
                "train_start": split.train_start,
                "train_end": split.train_end,
                "test_start": split.test_start,
                "test_end": split.test_end,
                "selected_horizon_key": horizon_key,
                "ranking_feature": composite_name,
                "ranking_feature_label": composite_ranked_panel_df[
                    "ranking_feature_label"
                ].iloc[0],
                "price_feature": price_feature,
                "price_feature_label": RANKING_FEATURE_LABEL_MAP[
                    cast(Any, price_feature)
                ],
                "price_direction": price_direction,
                "volume_feature": volume_feature,
                "volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                    cast(Any, volume_feature)
                ],
                "volume_direction": volume_direction,
                "score_method": score_method,
                "train_q1_mean": _as_float(
                    train_global["q1_mean"] if train_global is not None else None
                ),
                "train_q10_mean": _as_float(
                    train_global["q10_mean"] if train_global is not None else None
                ),
                "train_q1_minus_q10_mean": train_diff,
                "train_friedman_p_value": _as_float(
                    train_global["friedman_p_value"]
                    if train_global is not None
                    else None
                ),
                "train_q1_q10_paired_t_p_value": _as_float(
                    train_pairwise["paired_t_p_value_holm"]
                    if train_pairwise is not None
                    else None
                ),
            }
            candidate_records.append(record)
            horizon_records.append(record)
            composite_panel_lookup[composite_name] = composite_ranked_panel_df

        if not horizon_records:
            continue

        horizon_candidates_df = pd.DataFrame.from_records(horizon_records)
        horizon_candidates_df["_train_spread"] = pd.to_numeric(
            horizon_candidates_df["train_q1_minus_q10_mean"],
            errors="coerce",
        ).fillna(float("-inf"))
        horizon_candidates_df["_pairwise_p"] = pd.to_numeric(
            horizon_candidates_df["train_q1_q10_paired_t_p_value"],
            errors="coerce",
        ).fillna(1.0)
        horizon_candidates_df["_global_p"] = pd.to_numeric(
            horizon_candidates_df["train_friedman_p_value"],
            errors="coerce",
        ).fillna(1.0)
        horizon_candidates_df = horizon_candidates_df.sort_values(
            ["_train_spread", "_pairwise_p", "_global_p"],
            ascending=[False, True, True],
            kind="stable",
        )
        selected_records.append(
            horizon_candidates_df.iloc[0]
            .drop(labels=["_train_spread", "_pairwise_p", "_global_p"])
            .to_dict()
        )

    composite_candidate_df = pd.DataFrame.from_records(candidate_records)
    selected_composite_df = pd.DataFrame.from_records(selected_records)
    if not composite_candidate_df.empty:
        composite_candidate_df = composite_candidate_df.sort_values(
            ["split_index", "selected_horizon_key", "score_method"],
            kind="stable",
        ).reset_index(drop=True)
    if not selected_composite_df.empty:
        selected_composite_df = selected_composite_df.sort_values(
            ["split_index", "selected_horizon_key"],
            kind="stable",
        ).reset_index(drop=True)
    return composite_candidate_df, selected_composite_df, composite_panel_lookup


def _validate_no_overlapping_oos_rows(
    ranked_panel_df: pd.DataFrame,
    *,
    model_name: str,
) -> None:
    if ranked_panel_df.empty:
        return
    duplicates = ranked_panel_df.duplicated(
        subset=["date", "code", "selected_horizon_key"],
        keep=False,
    )
    if bool(duplicates.any()):
        raise Topix100SmaRatioLightgbmResearchError(
            "Walk-forward OOS evaluation produced overlapping test observations for "
            f"{model_name}. Increase the step size so test windows do not overlap."
        )


def _build_walkforward_split_spread_df(
    *,
    baseline_ranked_panel_df: pd.DataFrame,
    lightgbm_ranked_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for split_index in sorted(
        set(
            baseline_ranked_panel_df.get("split_index", pd.Series(dtype=int)).tolist()
            + lightgbm_ranked_panel_df.get("split_index", pd.Series(dtype=int)).tolist()
        )
    ):
        for selected_horizon_key in HORIZON_ORDER:
            model_inputs = (
                (
                    "baseline",
                    baseline_ranked_panel_df[
                        (baseline_ranked_panel_df["split_index"] == split_index)
                        & (
                            baseline_ranked_panel_df["selected_horizon_key"]
                            == selected_horizon_key
                        )
                    ].copy(),
                    _walkforward_baseline_ranking_feature(selected_horizon_key),
                    _walkforward_baseline_ranking_feature_label(selected_horizon_key),
                ),
                (
                    "lightgbm",
                    lightgbm_ranked_panel_df[
                        (lightgbm_ranked_panel_df["split_index"] == split_index)
                        & (
                            lightgbm_ranked_panel_df["selected_horizon_key"]
                            == selected_horizon_key
                        )
                    ].copy(),
                    _walkforward_lightgbm_ranking_feature(selected_horizon_key),
                    _walkforward_lightgbm_ranking_feature_label(selected_horizon_key),
                ),
            )
            for model_name, model_df, ranking_feature, ranking_feature_label in model_inputs:
                if model_df.empty:
                    continue
                analysis = _analyze_ranked_panel(model_df)
                global_row = _extract_global_row(
                    analysis["global_significance_df"],
                    ranking_feature=ranking_feature,
                    horizon_key=cast(Any, selected_horizon_key),
                )
                pairwise_row = _extract_pairwise_row(
                    analysis["pairwise_significance_df"],
                    ranking_feature=ranking_feature,
                    horizon_key=cast(Any, selected_horizon_key),
                )
                records.append(
                    {
                        "split_index": split_index,
                        "selected_horizon_key": selected_horizon_key,
                        "model_name": model_name,
                        "ranking_feature": ranking_feature,
                        "ranking_feature_label": ranking_feature_label,
                        "train_start": model_df["train_start"].iloc[0],
                        "train_end": model_df["train_end"].iloc[0],
                        "test_start": model_df["test_start"].iloc[0],
                        "test_end": model_df["test_end"].iloc[0],
                        "n_dates": int(global_row["n_dates"])
                        if global_row is not None and pd.notna(global_row["n_dates"])
                        else 0,
                        "q1_mean": _as_float(
                            global_row["q1_mean"] if global_row is not None else None
                        ),
                        "q10_mean": _as_float(
                            global_row["q10_mean"] if global_row is not None else None
                        ),
                        "q1_minus_q10_mean": _as_float(
                            global_row["q1_minus_q10_mean"]
                            if global_row is not None
                            else None
                        ),
                        "friedman_p_value": _as_float(
                            global_row["friedman_p_value"]
                            if global_row is not None
                            else None
                        ),
                        "kruskal_p_value": _as_float(
                            global_row["kruskal_p_value"]
                            if global_row is not None
                            else None
                        ),
                        "paired_t_p_value_holm": _as_float(
                            pairwise_row["paired_t_p_value_holm"]
                            if pairwise_row is not None
                            else None
                        ),
                        "wilcoxon_p_value_holm": _as_float(
                            pairwise_row["wilcoxon_p_value_holm"]
                            if pairwise_row is not None
                            else None
                        ),
                    }
                )

    split_spread_df = pd.DataFrame.from_records(records)
    if split_spread_df.empty:
        return split_spread_df

    split_spread_df["positive_spread"] = (
        pd.to_numeric(split_spread_df["q1_minus_q10_mean"], errors="coerce") > 0
    )
    split_spread_df["selected_horizon_key"] = pd.Categorical(
        split_spread_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    split_spread_df = split_spread_df.sort_values(
        ["selected_horizon_key", "split_index", "model_name"],
        kind="stable",
    ).reset_index(drop=True)
    split_spread_df["selected_horizon_key"] = split_spread_df[
        "selected_horizon_key"
    ].astype(str)
    return split_spread_df


def _build_walkforward_gate_df(
    *,
    lightgbm_analysis: dict[str, pd.DataFrame],
    split_spread_df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    records: list[dict[str, Any]] = []
    for selected_horizon_key in HORIZON_ORDER:
        overall_row = _extract_global_row(
            lightgbm_analysis["global_significance_df"],
            ranking_feature=_walkforward_lightgbm_ranking_feature(selected_horizon_key),
            horizon_key=cast(Any, selected_horizon_key),
        )
        horizon_split_df = split_spread_df[
            (split_spread_df["model_name"] == "lightgbm")
            & (split_spread_df["selected_horizon_key"] == selected_horizon_key)
        ].copy()
        valid_spreads = pd.to_numeric(
            horizon_split_df["q1_minus_q10_mean"],
            errors="coerce",
        ).dropna()
        valid_split_count = int(len(valid_spreads))
        median_split_spread = (
            float(valid_spreads.median()) if not valid_spreads.empty else None
        )
        positive_split_share = (
            float((valid_spreads > 0).mean()) if not valid_spreads.empty else None
        )
        is_gate_horizon = selected_horizon_key in {"t_plus_5", "t_plus_10"}
        gate_eligible = valid_split_count >= MIN_SPLIT_COUNT_FOR_GATE
        gate_passed: bool | None = None
        gate_status = "observed_only"
        overall_spread = _as_float(
            overall_row["q1_minus_q10_mean"] if overall_row is not None else None
        )
        if is_gate_horizon:
            if not gate_eligible:
                gate_status = "insufficient_coverage"
            else:
                gate_passed = bool(
                    overall_spread is not None
                    and overall_spread > 0
                    and median_split_spread is not None
                    and median_split_spread > 0
                    and positive_split_share is not None
                    and positive_split_share >= POSITIVE_SPLIT_SHARE_GATE
                )
                gate_status = "passed" if gate_passed else "failed"

        records.append(
            {
                "selected_horizon_key": selected_horizon_key,
                "is_gate_horizon": is_gate_horizon,
                "overall_q1_minus_q10_mean": overall_spread,
                "median_split_q1_minus_q10_mean": median_split_spread,
                "positive_split_share": positive_split_share,
                "valid_split_count": valid_split_count,
                "min_split_count_for_gate": MIN_SPLIT_COUNT_FOR_GATE,
                "positive_split_share_gate": POSITIVE_SPLIT_SHARE_GATE,
                "gate_eligible": gate_eligible if is_gate_horizon else False,
                "gate_passed": gate_passed,
                "gate_status": gate_status,
            }
        )

    exploratory_gate_df = pd.DataFrame.from_records(records)
    if exploratory_gate_df.empty:
        return exploratory_gate_df, "insufficient_coverage"

    gate_horizon_df = exploratory_gate_df[exploratory_gate_df["is_gate_horizon"]].copy()
    if gate_horizon_df.empty or bool(
        (gate_horizon_df["gate_status"] == "insufficient_coverage").any()
    ):
        overall_gate_status = "insufficient_coverage"
    elif bool(
        gate_horizon_df["gate_passed"]
        .map(lambda value: bool(value) if value is not None else False)
        .all()
    ):
        overall_gate_status = "passed"
    else:
        overall_gate_status = "failed"

    exploratory_gate_df["selected_horizon_key"] = pd.Categorical(
        exploratory_gate_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    exploratory_gate_df = exploratory_gate_df.sort_values(
        ["selected_horizon_key"],
        kind="stable",
    ).reset_index(drop=True)
    exploratory_gate_df["selected_horizon_key"] = exploratory_gate_df[
        "selected_horizon_key"
    ].astype(str)
    return exploratory_gate_df, overall_gate_status


def _build_walkforward_comparison_summary_df(
    *,
    baseline_analysis: dict[str, pd.DataFrame],
    lightgbm_analysis: dict[str, pd.DataFrame],
    split_spread_df: pd.DataFrame,
    exploratory_gate_df: pd.DataFrame,
) -> pd.DataFrame:
    gate_lookup = {
        str(row["selected_horizon_key"]): row
        for row in exploratory_gate_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for selected_horizon_key in HORIZON_ORDER:
        for model_name, analysis, ranking_feature, ranking_feature_label in (
            (
                "baseline",
                baseline_analysis,
                _walkforward_baseline_ranking_feature(selected_horizon_key),
                _walkforward_baseline_ranking_feature_label(selected_horizon_key),
            ),
            (
                "lightgbm",
                lightgbm_analysis,
                _walkforward_lightgbm_ranking_feature(selected_horizon_key),
                _walkforward_lightgbm_ranking_feature_label(selected_horizon_key),
            ),
        ):
            record = _extract_comparison_record(
                selected_horizon_key=selected_horizon_key,
                model_name=model_name,
                evaluation_split="walkforward_oos",
                ranking_feature=ranking_feature,
                ranking_feature_label=ranking_feature_label,
                analysis=analysis,
            )
            horizon_split_df = split_spread_df[
                (split_spread_df["selected_horizon_key"] == selected_horizon_key)
                & (split_spread_df["model_name"] == model_name)
            ].copy()
            valid_spreads = pd.to_numeric(
                horizon_split_df["q1_minus_q10_mean"],
                errors="coerce",
            ).dropna()
            record["valid_split_count"] = int(len(valid_spreads))
            record["median_split_q1_minus_q10_mean"] = (
                float(valid_spreads.median()) if not valid_spreads.empty else None
            )
            record["positive_split_share"] = (
                float((valid_spreads > 0).mean()) if not valid_spreads.empty else None
            )
            gate_row = gate_lookup.get(selected_horizon_key)
            record["gate_status"] = (
                gate_row["gate_status"] if gate_row is not None and model_name == "lightgbm" else None
            )
            record["gate_eligible"] = (
                gate_row["gate_eligible"]
                if gate_row is not None and model_name == "lightgbm"
                else None
            )
            record["gate_passed"] = (
                gate_row["gate_passed"]
                if gate_row is not None and model_name == "lightgbm"
                else None
            )
            records.append(record)

    comparison_summary_df = pd.DataFrame.from_records(records)
    if comparison_summary_df.empty:
        return comparison_summary_df

    comparison_summary_df["selected_horizon_key"] = pd.Categorical(
        comparison_summary_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    comparison_summary_df = comparison_summary_df.sort_values(
        ["selected_horizon_key", "model_name"],
        kind="stable",
    ).reset_index(drop=True)
    comparison_summary_df["selected_horizon_key"] = comparison_summary_df[
        "selected_horizon_key"
    ].astype(str)
    return comparison_summary_df


def _summarize_walkforward_models(
    split_coverage_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for selected_horizon_key in HORIZON_ORDER:
        horizon_df = split_coverage_df[
            split_coverage_df["selected_horizon_key"] == selected_horizon_key
        ].copy()
        valid_df = horizon_df[horizon_df["is_valid_split"]].copy()
        records.append(
            {
                "selected_horizon_key": selected_horizon_key,
                "ranking_feature": _walkforward_lightgbm_ranking_feature(
                    selected_horizon_key
                ),
                "ranking_feature_label": _walkforward_lightgbm_ranking_feature_label(
                    selected_horizon_key
                ),
                "generated_split_count": int(len(horizon_df)),
                "valid_split_count": int(len(valid_df)),
                "total_training_row_count": int(
                    valid_df["lightgbm_train_row_count"].sum()
                )
                if not valid_df.empty
                else 0,
                "median_training_row_count": float(
                    valid_df["lightgbm_train_row_count"].median()
                )
                if not valid_df.empty
                else None,
                "total_test_row_count": int(valid_df["lightgbm_test_row_count"].sum())
                if not valid_df.empty
                else 0,
                "median_test_row_count": float(
                    valid_df["lightgbm_test_row_count"].median()
                )
                if not valid_df.empty
                else None,
                "covered_test_date_count": int(valid_df["lightgbm_test_date_count"].sum())
                if not valid_df.empty
                else 0,
            }
        )

    selected_model_df = pd.DataFrame.from_records(records)
    if selected_model_df.empty:
        return selected_model_df
    selected_model_df["selected_horizon_key"] = pd.Categorical(
        selected_model_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    selected_model_df = selected_model_df.sort_values(
        ["selected_horizon_key"],
        kind="stable",
    ).reset_index(drop=True)
    selected_model_df["selected_horizon_key"] = selected_model_df[
        "selected_horizon_key"
    ].astype(str)
    return selected_model_df


def _summarize_walkforward_feature_importance(
    feature_importance_split_df: pd.DataFrame,
) -> pd.DataFrame:
    if feature_importance_split_df.empty:
        return feature_importance_split_df

    summary_df = (
        feature_importance_split_df.groupby(
            ["selected_horizon_key", "feature_name", "feature_label"],
            as_index=False,
            observed=False,
        )
        .agg(
            split_count=("split_index", "nunique"),
            mean_importance_gain=("importance_gain", "mean"),
            median_importance_gain=("importance_gain", "median"),
        )
        .reset_index(drop=True)
    )
    summary_df["selected_horizon_key"] = pd.Categorical(
        summary_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    summary_df = summary_df.sort_values(
        ["selected_horizon_key", "mean_importance_gain", "feature_name"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    summary_df["importance_rank"] = (
        summary_df.groupby("selected_horizon_key", observed=False).cumcount() + 1
    )
    summary_df["selected_horizon_key"] = summary_df["selected_horizon_key"].astype(str)
    return summary_df


def _run_walkforward_research(
    base_result: Topix100SmaRatioRankFutureCloseResearchResult,
    *,
    ranker_cls: type[Any],
    feature_columns: tuple[str, ...],
    train_window: int,
    test_window: int,
    step: int,
) -> Topix100SmaRatioLightgbmWalkforwardResearchResult:
    unique_dates = pd.DatetimeIndex(base_result.event_panel_df["date"].unique())
    splits = generate_walkforward_splits(
        unique_dates,
        train_window=train_window,
        test_window=test_window,
        step=step,
    )
    if not splits:
        raise Topix100SmaRatioLightgbmResearchError(
            "Walk-forward LightGBM research requires enough valid dates to build at "
            f"least one split with train_window={train_window}, "
            f"test_window={test_window}, step={step}."
        )

    training_frames_by_horizon = {
        horizon_key: _build_training_frame(base_result.event_panel_df, horizon_key=horizon_key)
        for horizon_key in HORIZON_ORDER
    }
    for horizon_key, scored_df in training_frames_by_horizon.items():
        if scored_df.empty:
            raise Topix100SmaRatioLightgbmResearchError(
                f"LightGBM research found no rows with {horizon_key} targets."
            )

    split_config_df = pd.DataFrame.from_records(
        [
            {
                "analysis_start_date": base_result.analysis_start_date,
                "analysis_end_date": base_result.analysis_end_date,
                "event_date_count": int(base_result.event_panel_df["date"].nunique()),
                "train_window": train_window,
                "test_window": test_window,
                "step": step,
                "generated_split_count": int(len(splits)),
                "min_split_count_for_gate": MIN_SPLIT_COUNT_FOR_GATE,
                "positive_split_share_gate": POSITIVE_SPLIT_SHARE_GATE,
            }
        ]
    )

    coverage_records: list[dict[str, Any]] = []
    baseline_selected_feature_frames: list[pd.DataFrame] = []
    baseline_selected_composite_frames: list[pd.DataFrame] = []
    baseline_ranked_frames: list[pd.DataFrame] = []
    lightgbm_ranked_frames: list[pd.DataFrame] = []
    feature_importance_records: list[dict[str, Any]] = []

    for split_index, split in enumerate(splits, start=1):
        split_ranked_panel_df = _filter_df_by_date_range(
            base_result.ranked_panel_df,
            start_date=split.train_start,
            end_date=split.test_end,
        )
        train_ranked_panel_df = _filter_df_by_date_range(
            split_ranked_panel_df,
            start_date=split.train_start,
            end_date=split.train_end,
        )
        baseline_train_analysis = _analyze_ranked_panel(train_ranked_panel_df)
        _, baseline_selected_feature_df = _build_train_only_feature_selection(
            split_index=split_index,
            split=split,
            train_analysis=baseline_train_analysis,
        )
        if not baseline_selected_feature_df.empty:
            baseline_selected_feature_frames.append(baseline_selected_feature_df)

        split_event_panel_df = _filter_df_by_date_range(
            base_result.event_panel_df,
            start_date=split.train_start,
            end_date=split.test_end,
        )
        _, baseline_selected_composite_df, composite_panel_lookup = (
            _build_train_only_composite_candidates(
                split_event_panel_df,
                split_index=split_index,
                split=split,
                selected_feature_df=baseline_selected_feature_df,
            )
        )
        if not baseline_selected_composite_df.empty:
            baseline_selected_composite_frames.append(baseline_selected_composite_df)

        baseline_selected_lookup = {
            str(row["selected_horizon_key"]): row
            for row in baseline_selected_composite_df.to_dict(orient="records")
        }
        split_scheduled_train_dates = int(
            base_result.event_panel_df[
                (base_result.event_panel_df["date"] >= split.train_start)
                & (base_result.event_panel_df["date"] <= split.train_end)
            ]["date"].nunique()
        )
        split_scheduled_test_dates = int(
            base_result.event_panel_df[
                (base_result.event_panel_df["date"] >= split.test_start)
                & (base_result.event_panel_df["date"] <= split.test_end)
            ]["date"].nunique()
        )

        for horizon_key in HORIZON_ORDER:
            scored_df = training_frames_by_horizon[horizon_key]
            training_df = _filter_df_by_date_range(
                scored_df,
                start_date=split.train_start,
                end_date=split.train_end,
            ).sort_values(["date", "code"])
            test_df = _filter_df_by_date_range(
                scored_df,
                start_date=split.test_start,
                end_date=split.test_end,
            ).sort_values(["date", "code"])
            train_groups = _build_query_groups(training_df)
            baseline_selected_row = baseline_selected_lookup.get(horizon_key)
            baseline_test_row_count = 0
            baseline_test_date_count = 0
            if baseline_selected_row is not None:
                composite_name = str(baseline_selected_row["ranking_feature"])
                composite_ranked_panel_df = composite_panel_lookup.get(composite_name)
                if composite_ranked_panel_df is not None:
                    baseline_test_panel_df = _filter_df_by_date_range(
                        composite_ranked_panel_df,
                        start_date=split.test_start,
                        end_date=split.test_end,
                    )
                    baseline_test_panel_df = baseline_test_panel_df.dropna(
                        subset=[f"{horizon_key}_return"]
                    ).copy()
                    baseline_test_row_count = int(len(baseline_test_panel_df))
                    baseline_test_date_count = int(
                        baseline_test_panel_df["date"].nunique()
                    )

            coverage_record = {
                "split_index": split_index,
                "selected_horizon_key": horizon_key,
                "train_start": split.train_start,
                "train_end": split.train_end,
                "test_start": split.test_start,
                "test_end": split.test_end,
                "scheduled_train_date_count": split_scheduled_train_dates,
                "scheduled_test_date_count": split_scheduled_test_dates,
                "lightgbm_train_row_count": int(len(training_df)),
                "lightgbm_train_date_count": int(training_df["date"].nunique()),
                "lightgbm_train_query_count": int(len(train_groups)),
                "lightgbm_test_row_count": int(len(test_df)),
                "lightgbm_test_date_count": int(test_df["date"].nunique()),
                "baseline_selection_available": baseline_selected_row is not None,
                "baseline_test_row_count": baseline_test_row_count,
                "baseline_test_date_count": baseline_test_date_count,
                "is_valid_split": bool(
                    not training_df.empty and not test_df.empty and baseline_selected_row is not None
                ),
            }
            coverage_records.append(coverage_record)

            if training_df.empty or test_df.empty:
                continue

            ranker = ranker_cls(**_build_model_params())
            ranker.fit(
                training_df[list(feature_columns)],
                training_df["target_relevance"],
                group=train_groups,
            )

            predictions = pd.Series(
                ranker.predict(test_df[list(feature_columns)]),
                index=test_df.index,
                name="ranking_value",
            )
            lightgbm_panel_df = _build_scored_ranked_panel(
                test_df,
                ranking_feature=_walkforward_lightgbm_ranking_feature(horizon_key),
                ranking_feature_label=_walkforward_lightgbm_ranking_feature_label(
                    horizon_key
                ),
                predictions=predictions,
            )
            lightgbm_panel_df = _assign_feature_deciles(
                lightgbm_panel_df,
                known_feature_order=_walkforward_lightgbm_feature_order(),
            )
            lightgbm_panel_df["selected_horizon_key"] = horizon_key
            lightgbm_panel_df["split_index"] = split_index
            lightgbm_panel_df["train_start"] = split.train_start
            lightgbm_panel_df["train_end"] = split.train_end
            lightgbm_panel_df["test_start"] = split.test_start
            lightgbm_panel_df["test_end"] = split.test_end
            lightgbm_ranked_frames.append(lightgbm_panel_df)

            importance_values = pd.Series(
                getattr(ranker, "feature_importances_", []),
                dtype=float,
            )
            if len(importance_values) != len(feature_columns):
                raise Topix100SmaRatioLightgbmResearchError(
                    "LightGBM research received unexpected feature importance output."
                )
            importance_df = pd.DataFrame(
                {
                    "split_index": split_index,
                    "train_start": split.train_start,
                    "train_end": split.train_end,
                    "test_start": split.test_start,
                    "test_end": split.test_end,
                    "selected_horizon_key": horizon_key,
                    "ranking_feature": _walkforward_lightgbm_ranking_feature(
                        horizon_key
                    ),
                    "ranking_feature_label": _walkforward_lightgbm_ranking_feature_label(
                        horizon_key
                    ),
                    "feature_name": feature_columns,
                    "feature_label": [
                        RANKING_FEATURE_LABEL_MAP[cast(Any, feature_name)]
                        for feature_name in feature_columns
                    ],
                    "importance_gain": importance_values.to_numpy(),
                }
            )
            importance_df = importance_df.sort_values(
                ["importance_gain", "feature_name"],
                ascending=[False, True],
                kind="stable",
            ).reset_index(drop=True)
            importance_df["importance_rank"] = range(1, len(importance_df) + 1)
            feature_importance_records.extend(_records_from_frame(importance_df))

            if baseline_selected_row is None:
                continue

            composite_name = str(baseline_selected_row["ranking_feature"])
            composite_ranked_panel_df = composite_panel_lookup.get(composite_name)
            if composite_ranked_panel_df is None:
                continue
            baseline_test_panel_df = _filter_df_by_date_range(
                composite_ranked_panel_df,
                start_date=split.test_start,
                end_date=split.test_end,
            )
            baseline_test_panel_df = baseline_test_panel_df.dropna(
                subset=[f"{horizon_key}_return"]
            ).copy()
            if baseline_test_panel_df.empty:
                continue
            baseline_test_panel_df["selected_ranking_feature"] = (
                baseline_test_panel_df["ranking_feature"].astype(str)
            )
            baseline_test_panel_df["selected_ranking_feature_label"] = (
                baseline_test_panel_df["ranking_feature_label"].astype(str)
            )
            baseline_test_panel_df["ranking_feature"] = _walkforward_baseline_ranking_feature(
                horizon_key
            )
            baseline_test_panel_df["ranking_feature_label"] = (
                _walkforward_baseline_ranking_feature_label(horizon_key)
            )
            baseline_test_panel_df["selected_horizon_key"] = horizon_key
            baseline_test_panel_df["split_index"] = split_index
            baseline_test_panel_df["train_start"] = split.train_start
            baseline_test_panel_df["train_end"] = split.train_end
            baseline_test_panel_df["test_start"] = split.test_start
            baseline_test_panel_df["test_end"] = split.test_end
            for metadata_column in (
                "price_feature",
                "price_feature_label",
                "price_direction",
                "volume_feature",
                "volume_feature_label",
                "volume_direction",
                "score_method",
            ):
                baseline_test_panel_df[metadata_column] = baseline_selected_row[
                    metadata_column
                ]
            baseline_ranked_frames.append(baseline_test_panel_df)

    split_coverage_df = pd.DataFrame.from_records(coverage_records)
    if split_coverage_df.empty:
        raise Topix100SmaRatioLightgbmResearchError(
            "Walk-forward LightGBM research could not build split coverage rows."
        )
    split_coverage_df["selected_horizon_key"] = pd.Categorical(
        split_coverage_df["selected_horizon_key"],
        categories=list(HORIZON_ORDER),
        ordered=True,
    )
    split_coverage_df = split_coverage_df.sort_values(
        ["selected_horizon_key", "split_index"],
        kind="stable",
    ).reset_index(drop=True)
    split_coverage_df["selected_horizon_key"] = split_coverage_df[
        "selected_horizon_key"
    ].astype(str)

    if not lightgbm_ranked_frames:
        raise Topix100SmaRatioLightgbmResearchError(
            "Walk-forward LightGBM research produced no test-window predictions."
        )
    if not baseline_ranked_frames:
        raise Topix100SmaRatioLightgbmResearchError(
            "Walk-forward baseline comparison produced no selected test-window rows."
        )

    lightgbm_ranked_panel_df = _core_sort_frame(
        pd.concat(lightgbm_ranked_frames, ignore_index=True),
        known_feature_order=_walkforward_lightgbm_feature_order(),
    )
    baseline_ranked_panel_df = _core_sort_frame(
        pd.concat(baseline_ranked_frames, ignore_index=True),
        known_feature_order=_walkforward_feature_order(),
    )
    _validate_no_overlapping_oos_rows(
        lightgbm_ranked_panel_df,
        model_name="lightgbm",
    )
    _validate_no_overlapping_oos_rows(
        baseline_ranked_panel_df,
        model_name="baseline",
    )

    lightgbm_analysis = _analyze_ranked_panel(lightgbm_ranked_panel_df)
    baseline_analysis = _analyze_ranked_panel(baseline_ranked_panel_df)
    feature_importance_split_df = pd.DataFrame.from_records(feature_importance_records)
    if not feature_importance_split_df.empty:
        feature_importance_split_df["selected_horizon_key"] = pd.Categorical(
            feature_importance_split_df["selected_horizon_key"],
            categories=list(HORIZON_ORDER),
            ordered=True,
        )
        feature_importance_split_df = feature_importance_split_df.sort_values(
            ["selected_horizon_key", "split_index", "importance_rank"],
            kind="stable",
        ).reset_index(drop=True)
        feature_importance_split_df["selected_horizon_key"] = (
            feature_importance_split_df["selected_horizon_key"].astype(str)
        )
    feature_importance_df = _summarize_walkforward_feature_importance(
        feature_importance_split_df
    )
    split_spread_df = _build_walkforward_split_spread_df(
        baseline_ranked_panel_df=baseline_ranked_panel_df,
        lightgbm_ranked_panel_df=lightgbm_ranked_panel_df,
    )
    exploratory_gate_df, overall_gate_status = _build_walkforward_gate_df(
        lightgbm_analysis=lightgbm_analysis,
        split_spread_df=split_spread_df,
    )
    comparison_summary_df = _build_walkforward_comparison_summary_df(
        baseline_analysis=baseline_analysis,
        lightgbm_analysis=lightgbm_analysis,
        split_spread_df=split_spread_df,
        exploratory_gate_df=exploratory_gate_df,
    )
    selected_model_df = _summarize_walkforward_models(split_coverage_df)

    baseline_selected_feature_df = (
        pd.concat(baseline_selected_feature_frames, ignore_index=True)
        if baseline_selected_feature_frames
        else pd.DataFrame()
    )
    baseline_selected_composite_df = (
        pd.concat(baseline_selected_composite_frames, ignore_index=True)
        if baseline_selected_composite_frames
        else pd.DataFrame()
    )
    if not baseline_selected_feature_df.empty:
        baseline_selected_feature_df["horizon_key"] = pd.Categorical(
            baseline_selected_feature_df["horizon_key"],
            categories=list(HORIZON_ORDER),
            ordered=True,
        )
        baseline_selected_feature_df = baseline_selected_feature_df.sort_values(
            ["split_index", "feature_family", "horizon_key"],
            kind="stable",
        ).reset_index(drop=True)
        baseline_selected_feature_df["horizon_key"] = baseline_selected_feature_df[
            "horizon_key"
        ].astype(str)
    if not baseline_selected_composite_df.empty:
        baseline_selected_composite_df["selected_horizon_key"] = pd.Categorical(
            baseline_selected_composite_df["selected_horizon_key"],
            categories=list(HORIZON_ORDER),
            ordered=True,
        )
        baseline_selected_composite_df = baseline_selected_composite_df.sort_values(
            ["split_index", "selected_horizon_key"],
            kind="stable",
        ).reset_index(drop=True)
        baseline_selected_composite_df["selected_horizon_key"] = (
            baseline_selected_composite_df["selected_horizon_key"].astype(str)
        )

    return Topix100SmaRatioLightgbmWalkforwardResearchResult(
        train_window=train_window,
        test_window=test_window,
        step=step,
        min_split_count_for_gate=MIN_SPLIT_COUNT_FOR_GATE,
        positive_split_share_gate=POSITIVE_SPLIT_SHARE_GATE,
        overall_gate_status=overall_gate_status,
        split_config_df=split_config_df,
        split_coverage_df=split_coverage_df,
        selected_model_df=selected_model_df,
        baseline_selected_feature_df=baseline_selected_feature_df,
        baseline_selected_composite_df=baseline_selected_composite_df,
        comparison_summary_df=comparison_summary_df,
        split_spread_df=split_spread_df,
        feature_importance_df=feature_importance_df,
        feature_importance_split_df=feature_importance_split_df,
        ranked_panel_df=lightgbm_ranked_panel_df,
        ranking_feature_summary_df=lightgbm_analysis["ranking_feature_summary_df"],
        decile_future_summary_df=lightgbm_analysis["decile_future_summary_df"],
        daily_group_means_df=lightgbm_analysis["daily_group_means_df"],
        global_significance_df=lightgbm_analysis["global_significance_df"],
        pairwise_significance_df=lightgbm_analysis["pairwise_significance_df"],
        exploratory_gate_df=exploratory_gate_df,
    )


def run_topix100_sma_ratio_rank_future_close_lightgbm_research(
    base_result: Topix100SmaRatioRankFutureCloseResearchResult,
    *,
    train_window: int = DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    test_window: int = DEFAULT_WALKFORWARD_TEST_WINDOW,
    step: int = DEFAULT_WALKFORWARD_STEP,
    include_diagnostic: bool = True,
) -> Topix100SmaRatioLightgbmResearchResult:
    if base_result.event_panel_df.empty:
        raise Topix100SmaRatioLightgbmResearchError(
            "LightGBM research requires a non-empty TOPIX100 SMA event panel."
        )

    ranker_cls = _load_lightgbm_ranker_cls()
    feature_columns = tuple(RANKING_FEATURE_ORDER)
    walkforward = _run_walkforward_research(
        base_result,
        ranker_cls=ranker_cls,
        feature_columns=feature_columns,
        train_window=train_window,
        test_window=test_window,
        step=step,
    )

    diagnostic: Topix100SmaRatioLightgbmFixedSplitDiagnostic | None
    diagnostic_error_message: str | None
    if not include_diagnostic:
        diagnostic = None
        diagnostic_error_message = None
        return Topix100SmaRatioLightgbmResearchResult(
            feature_columns=feature_columns,
            walkforward=walkforward,
            diagnostic=diagnostic,
            diagnostic_error_message=diagnostic_error_message,
        )

    try:
        diagnostic = _run_fixed_split_diagnostic(
            base_result,
            ranker_cls=ranker_cls,
            feature_columns=feature_columns,
        )
        diagnostic_error_message = None
    except Topix100SmaRatioLightgbmResearchError as exc:
        diagnostic = None
        diagnostic_error_message = str(exc)

    return Topix100SmaRatioLightgbmResearchResult(
        feature_columns=feature_columns,
        walkforward=walkforward,
        diagnostic=diagnostic,
        diagnostic_error_message=diagnostic_error_message,
    )
