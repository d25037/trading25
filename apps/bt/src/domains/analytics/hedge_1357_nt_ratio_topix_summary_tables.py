from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.hedge_1357_nt_ratio_topix_support import (
    CLOSE_BUCKET_ORDER,
    NT_RATIO_BUCKET_ORDER,
    RULE_ORDER,
    SPLIT_ORDER,
    TARGET_ORDER,
    NtRatioReturnStats,
    TopixCloseReturnStats,
    _TARGET_COLUMN_MAP,
    format_close_bucket_label,
    format_nt_ratio_bucket_label,
)


def _run_lengths(signal: pd.Series) -> list[int]:
    run_lengths: list[int] = []
    current_run = 0
    for value in signal.fillna(False).astype(bool):
        if value:
            current_run += 1
            continue
        if current_run:
            run_lengths.append(current_run)
            current_run = 0
    if current_run:
        run_lengths.append(current_run)
    return run_lengths


def _build_joint_forward_summary(
    daily_market_df: pd.DataFrame,
    *,
    topix_close_stats: TopixCloseReturnStats | None,
    nt_ratio_stats: NtRatioReturnStats | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        if split_name == "overall":
            split_df = daily_market_df.copy()
        else:
            split_df = daily_market_df[daily_market_df["split"] == split_name].copy()
        for target_name in TARGET_ORDER:
            etf_column = _TARGET_COLUMN_MAP[target_name][1]
            valid = split_df.dropna(
                subset=[
                    "topix_close_bucket_key",
                    "nt_ratio_bucket_key",
                    etf_column,
                ]
            )
            grouped = (
                valid.groupby(
                    ["nt_ratio_bucket_key", "topix_close_bucket_key"],
                    as_index=False,
                )[etf_column]
                .agg(["count", "mean", "median"])
                .reset_index()
            )
            grouped = grouped.rename(
                columns={
                    "count": "event_count",
                    "mean": "mean_etf_return",
                    "median": "median_etf_return",
                }
            )
            positive_rate = (
                valid.groupby(
                    ["nt_ratio_bucket_key", "topix_close_bucket_key"]
                )[etf_column]
                .apply(lambda values: float((values > 0).mean()))
                .reset_index(name="positive_rate")
            )
            merged = grouped.merge(
                positive_rate,
                on=["nt_ratio_bucket_key", "topix_close_bucket_key"],
                how="left",
            )
            for nt_bucket in NT_RATIO_BUCKET_ORDER:
                for topix_bucket in CLOSE_BUCKET_ORDER:
                    row = merged[
                        (merged["nt_ratio_bucket_key"] == nt_bucket)
                        & (merged["topix_close_bucket_key"] == topix_bucket)
                    ]
                    if row.empty:
                        rows.append(
                            {
                                "split": split_name,
                                "target_name": target_name,
                                "nt_ratio_bucket_key": nt_bucket,
                                "nt_ratio_bucket_label": (
                                    format_nt_ratio_bucket_label(
                                        nt_bucket,
                                        sigma_threshold_1=nt_ratio_stats.sigma_threshold_1,
                                        sigma_threshold_2=nt_ratio_stats.sigma_threshold_2,
                                    )
                                    if nt_ratio_stats is not None
                                    else None
                                ),
                                "topix_close_bucket_key": topix_bucket,
                                "topix_close_bucket_label": (
                                    format_close_bucket_label(
                                        topix_bucket,
                                        close_threshold_1=topix_close_stats.threshold_1,
                                        close_threshold_2=topix_close_stats.threshold_2,
                                    )
                                    if topix_close_stats is not None
                                    else None
                                ),
                                "event_count": 0,
                                "mean_etf_return": np.nan,
                                "median_etf_return": np.nan,
                                "positive_rate": np.nan,
                            }
                        )
                        continue
                    first = row.iloc[0]
                    rows.append(
                        {
                            "split": split_name,
                            "target_name": target_name,
                            "nt_ratio_bucket_key": nt_bucket,
                            "nt_ratio_bucket_label": first.get("nt_ratio_bucket_label"),
                            "topix_close_bucket_key": topix_bucket,
                            "topix_close_bucket_label": first.get("topix_close_bucket_label"),
                            "event_count": int(first["event_count"]),
                            "mean_etf_return": float(first["mean_etf_return"]),
                            "median_etf_return": float(first["median_etf_return"]),
                            "positive_rate": float(first["positive_rate"]),
                        }
                    )
    summary_df = pd.DataFrame(rows)
    label_lookup = (
        daily_market_df.dropna(
            subset=["nt_ratio_bucket_key", "nt_ratio_bucket_label"]
        )
        .drop_duplicates(subset=["nt_ratio_bucket_key"])[
            ["nt_ratio_bucket_key", "nt_ratio_bucket_label"]
        ]
    )
    topix_label_lookup = (
        daily_market_df.dropna(
            subset=["topix_close_bucket_key", "topix_close_bucket_label"]
        )
        .drop_duplicates(subset=["topix_close_bucket_key"])[
            ["topix_close_bucket_key", "topix_close_bucket_label"]
        ]
    )
    summary_df = summary_df.drop(columns=["nt_ratio_bucket_label", "topix_close_bucket_label"])
    summary_df = summary_df.merge(label_lookup, on="nt_ratio_bucket_key", how="left")
    summary_df = summary_df.merge(
        topix_label_lookup,
        on="topix_close_bucket_key",
        how="left",
    )
    return summary_df


def _build_rule_signal_summary(daily_market_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        if split_name == "overall":
            split_df = daily_market_df.copy()
        else:
            split_df = daily_market_df[daily_market_df["split"] == split_name].copy()
        total_days = int(split_df.shape[0])
        for rule_name in RULE_ORDER:
            signal = split_df[rule_name].fillna(False).astype(bool)
            run_lengths = _run_lengths(signal)
            signal_int = signal.astype(int)
            initial_transition = int(signal_int.iloc[0]) if not signal_int.empty else 0
            transitions = int(signal_int.diff().abs().fillna(initial_transition).sum())
            rows.append(
                {
                    "split": split_name,
                    "rule_name": rule_name,
                    "total_days": total_days,
                    "active_day_count": int(signal.sum()),
                    "active_ratio": float(signal.mean()) if total_days else np.nan,
                    "transitions": transitions,
                    "average_run_length": (
                        float(np.mean(run_lengths)) if run_lengths else np.nan
                    ),
                    "max_run_length": int(max(run_lengths)) if run_lengths else 0,
                }
            )
    return pd.DataFrame(rows)


def _build_etf_strategy_split_comparison(
    etf_strategy_metrics_df: pd.DataFrame,
) -> pd.DataFrame:
    split_df = etf_strategy_metrics_df[
        etf_strategy_metrics_df["split"].isin(["discovery", "validation"])
    ].copy()
    index_columns = ["target_name", "rule_name"]
    value_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_return_when_active",
        "strategy_mean_return",
        "strategy_total_return",
        "expected_shortfall_5",
        "max_drawdown",
        "positive_rate_when_active",
    ]
    pivot = split_df.pivot_table(
        index=index_columns,
        columns="split",
        values=value_columns,
        aggfunc="first",
    )
    if pivot.empty:
        return pd.DataFrame(columns=index_columns)
    pivot.columns = [f"{metric}_{split_name}" for metric, split_name in pivot.columns]
    pivot = pivot.reset_index()
    for metric in value_columns:
        for split_name in ("discovery", "validation"):
            column_name = f"{metric}_{split_name}"
            if column_name not in pivot.columns:
                pivot[column_name] = np.nan
    return pivot


def _build_split_comparison(hedge_metrics_df: pd.DataFrame) -> pd.DataFrame:
    split_df = hedge_metrics_df[hedge_metrics_df["split"].isin(["discovery", "validation"])].copy()
    index_columns = ["stock_group", "target_name", "rule_name", "weight_label"]
    value_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_weight_when_active",
        "stress_day_count",
        "stress_mean_loss_improvement",
        "expected_shortfall_improvement",
        "max_drawdown_improvement",
        "down_day_hit_rate",
        "carry_cost_non_stress",
    ]
    pivot = split_df.pivot_table(
        index=index_columns,
        columns="split",
        values=value_columns,
        aggfunc="first",
    )
    if pivot.empty:
        return pd.DataFrame(columns=index_columns)
    flattened_columns: list[str] = []
    for left, right in pivot.columns:
        if left in {"discovery", "validation"}:
            flattened_columns.append(f"{right}_{left}")
        else:
            flattened_columns.append(f"{left}_{right}")
    pivot.columns = flattened_columns
    pivot = pivot.reset_index()
    expected_metric_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_weight_when_active",
        "stress_day_count",
        "stress_mean_loss_improvement",
        "expected_shortfall_improvement",
        "max_drawdown_improvement",
        "down_day_hit_rate",
        "carry_cost_non_stress",
    ]
    for metric in expected_metric_columns:
        for split_name in ("discovery", "validation"):
            column_name = f"{metric}_{split_name}"
            if column_name not in pivot.columns:
                pivot[column_name] = np.nan
    return pivot


def _build_shortlist(split_comparison_df: pd.DataFrame) -> pd.DataFrame:
    if split_comparison_df.empty:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "target_name",
                "rule_name",
                "weight_label",
                "score",
                "selection_status",
            ]
        )
    shortlist = split_comparison_df.copy()
    shortlist["qualified"] = (
        shortlist["active_day_count_discovery"].fillna(0).ge(20)
        & shortlist["active_day_count_validation"].fillna(0).ge(20)
        & shortlist["stress_mean_loss_improvement_discovery"].fillna(-np.inf).gt(0)
        & shortlist["stress_mean_loss_improvement_validation"].fillna(-np.inf).gt(0)
        & shortlist["expected_shortfall_improvement_discovery"].fillna(-np.inf).gt(0)
        & shortlist["expected_shortfall_improvement_validation"].fillna(-np.inf).gt(0)
        & shortlist["carry_cost_non_stress_discovery"].fillna(-np.inf).ge(
            -0.5 * shortlist["stress_mean_loss_improvement_discovery"].fillna(np.inf)
        )
        & shortlist["carry_cost_non_stress_validation"].fillna(-np.inf).ge(
            -0.5 * shortlist["stress_mean_loss_improvement_validation"].fillna(np.inf)
        )
    )
    qualified = shortlist[shortlist["qualified"]].copy()
    if qualified.empty:
        qualified = shortlist[
            shortlist["active_day_count_discovery"].fillna(0).ge(10)
            & shortlist["active_day_count_validation"].fillna(0).ge(10)
        ].copy()
        selection_status = "fallback"
    else:
        selection_status = "qualified"
    if qualified.empty:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "target_name",
                "rule_name",
                "weight_label",
                "score",
                "selection_status",
            ]
        )
    qualified["score"] = (
        qualified["expected_shortfall_improvement_discovery"].fillna(0)
        + qualified["expected_shortfall_improvement_validation"].fillna(0)
        + qualified["stress_mean_loss_improvement_discovery"].fillna(0)
        + qualified["stress_mean_loss_improvement_validation"].fillna(0)
        + 0.5 * qualified["max_drawdown_improvement_discovery"].fillna(0)
        + 0.5 * qualified["max_drawdown_improvement_validation"].fillna(0)
    )
    qualified["selection_status"] = selection_status
    columns = [
        "stock_group",
        "target_name",
        "rule_name",
        "weight_label",
        "score",
        "selection_status",
        "active_day_count_discovery",
        "active_day_count_validation",
        "stress_mean_loss_improvement_discovery",
        "stress_mean_loss_improvement_validation",
        "expected_shortfall_improvement_discovery",
        "expected_shortfall_improvement_validation",
        "carry_cost_non_stress_discovery",
        "carry_cost_non_stress_validation",
    ]
    return qualified.sort_values("score", ascending=False)[columns].head(3).reset_index(drop=True)


__all__ = [
    "_build_etf_strategy_split_comparison",
    "_build_joint_forward_summary",
    "_build_rule_signal_summary",
    "_build_shortlist",
    "_build_split_comparison",
]
