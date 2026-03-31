# pyright: reportUnusedFunction=false
"""
TOPIX100 VI-change regime conditioning research analytics.

This module reuses the TOPIX100 price-vs-20SMA / volume research panel and
conditions the primary price/volume bucket discussion on same-day Nikkei VI
change regimes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path
from itertools import combinations
from typing import Any, Literal

import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioBucketKey,
    NtRatioReturnStats,
    format_nt_ratio_bucket_label,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma20_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    COMBINED_BUCKET_ORDER,
    SPLIT_HYPOTHESIS_LABELS as _PRICE_VS_SPLIT_HYPOTHESIS_SPECS,
    Topix100PriceVsSma20RankFutureCloseResearchResult,
    run_topix100_price_vs_sma20_rank_future_close_research,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    HORIZON_ORDER,
    METRIC_ORDER,
    _holm_adjust,
    _safe_paired_t_test,
    _safe_wilcoxon,
)
from src.domains.analytics.topix_regime_conditioning_core import (
    DEFAULT_SIGMA_THRESHOLD_1 as _CORE_DEFAULT_SIGMA_THRESHOLD_1,
    DEFAULT_SIGMA_THRESHOLD_2 as _CORE_DEFAULT_SIGMA_THRESHOLD_2,
    REGIME_GROUP_LABEL_MAP as _CORE_REGIME_GROUP_LABEL_MAP,
    REGIME_GROUP_ORDER as _CORE_REGIME_GROUP_ORDER,
    _build_horizon_panel as _core_build_horizon_panel,
    _build_regime_daily_means as _core_build_regime_daily_means,
    _build_regime_group_daily_means as _core_build_regime_group_daily_means,
    _summarize_regime_daily_means as _core_summarize_regime_daily_means,
    _summarize_regime_group_daily_means as _core_summarize_regime_group_daily_means,
)

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
RegimeType = Literal["vi_change"]
RegimeGroupKey = Literal["weak", "neutral", "strong"]

REGIME_TYPE_ORDER: tuple[RegimeType, ...] = ("vi_change",)
REGIME_LABEL_MAP: dict[RegimeType, str] = {
    "vi_change": "Nikkei VI Change",
}
REGIME_GROUP_ORDER: tuple[RegimeGroupKey, ...] = _CORE_REGIME_GROUP_ORDER
REGIME_GROUP_LABEL_MAP: dict[RegimeGroupKey, str] = _CORE_REGIME_GROUP_LABEL_MAP
DEFAULT_SIGMA_THRESHOLD_1 = _CORE_DEFAULT_SIGMA_THRESHOLD_1
DEFAULT_SIGMA_THRESHOLD_2 = _CORE_DEFAULT_SIGMA_THRESHOLD_2
HYPOTHESIS_SPECS = _PRICE_VS_SPLIT_HYPOTHESIS_SPECS
TOPIX100_VI_CHANGE_REGIME_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-vi-change-regime-conditioning"
)


@dataclass(frozen=True)
class Topix100ViChangeRegimeConditioningResearchResult:
    db_path: str
    source_mode: str
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    sigma_threshold_1: float
    sigma_threshold_2: float
    universe_constituent_count: int
    valid_date_count: int
    vi_change_stats: NtRatioReturnStats | None
    regime_market_df: pd.DataFrame
    regime_day_counts_df: pd.DataFrame
    regime_group_day_counts_df: pd.DataFrame
    split_panel_df: pd.DataFrame
    horizon_panel_df: pd.DataFrame
    regime_daily_means_df: pd.DataFrame
    regime_summary_df: pd.DataFrame
    regime_pairwise_significance_df: pd.DataFrame
    regime_hypothesis_df: pd.DataFrame
    regime_group_daily_means_df: pd.DataFrame
    regime_group_summary_df: pd.DataFrame
    regime_group_pairwise_significance_df: pd.DataFrame
    regime_group_hypothesis_df: pd.DataFrame


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    ordered = df.copy()
    if "regime_type" in ordered.columns:
        ordered["_regime_type_order"] = ordered["regime_type"].map(
            {key: index for index, key in enumerate(REGIME_TYPE_ORDER, start=1)}
        )
    if "regime_bucket_key" in ordered.columns:
        ordered["_regime_bucket_order"] = ordered["regime_bucket_key"].map(
            {key: index for index, key in enumerate(NT_RATIO_BUCKET_ORDER, start=1)}
        )
    if "regime_group_key" in ordered.columns:
        ordered["_regime_group_order"] = ordered["regime_group_key"].map(
            {key: index for index, key in enumerate(REGIME_GROUP_ORDER, start=1)}
        )
    if "horizon_key" in ordered.columns:
        ordered["_horizon_order"] = ordered["horizon_key"].map(
            {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
        )
    if "combined_bucket" in ordered.columns:
        ordered["_combined_bucket_order"] = ordered["combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "left_combined_bucket" in ordered.columns:
        ordered["_left_combined_bucket_order"] = ordered["left_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "right_combined_bucket" in ordered.columns:
        ordered["_right_combined_bucket_order"] = ordered["right_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "date" in ordered.columns:
        ordered["_date_order"] = pd.to_datetime(ordered["date"], errors="coerce")

    sort_columns = [column for column in ordered.columns if column.startswith("_")]
    if sort_columns:
        ordered = ordered.sort_values(sort_columns, kind="stable").drop(columns=sort_columns)
    return ordered.reset_index(drop=True)


def _vi_per_day_cte() -> str:
    return """
        WITH per_day AS (
            SELECT
                date,
                COUNT(DISTINCT CASE WHEN base_volatility > 0 THEN CAST(base_volatility AS DOUBLE) END)
                    AS positive_value_count,
                MIN(CASE WHEN base_volatility > 0 THEN CAST(base_volatility AS DOUBLE) END)
                    AS vi_close
            FROM options_225_data
            GROUP BY date
        )
    """


def _query_vi_available_date_range(conn: Any) -> tuple[str | None, str | None]:
    row = conn.execute(
        f"""
        {_vi_per_day_cte()}
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM per_day
        WHERE positive_value_count = 1
          AND vi_close IS NOT NULL
        """
    ).fetchone()
    return (
        str(row[0]) if row and row[0] else None,
        str(row[1]) if row and row[1] else None,
    )


def _query_vi_market_history(conn: Any, *, end_date: str | None) -> pd.DataFrame:
    params: list[Any] = []
    end_date_sql = ""
    if end_date:
        end_date_sql = " AND date <= ?"
        params.append(end_date)

    return conn.execute(
        f"""
        {_vi_per_day_cte()}
        SELECT date, vi_close
        FROM per_day
        WHERE positive_value_count = 1
          AND vi_close IS NOT NULL
          {end_date_sql}
        ORDER BY date
        """,
        tuple(params),
    ).fetchdf()


def _build_vi_change_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> NtRatioReturnStats | None:
    valid = market_df["vi_change"].dropna()
    if valid.empty:
        return None

    mean_return = float(valid.mean())
    std_return = float(valid.std(ddof=1))
    if std_return <= 0:
        raise ValueError("vi_change standard deviation must be positive")

    return NtRatioReturnStats(
        sample_count=int(valid.shape[0]),
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        lower_threshold_2=mean_return - sigma_threshold_2 * std_return,
        lower_threshold_1=mean_return - sigma_threshold_1 * std_return,
        upper_threshold_1=mean_return + sigma_threshold_1 * std_return,
        upper_threshold_2=mean_return + sigma_threshold_2 * std_return,
        min_return=float(valid.min()),
        q25_return=float(valid.quantile(0.25)),
        median_return=float(valid.median()),
        q75_return=float(valid.quantile(0.75)),
        max_return=float(valid.max()),
    )


def _bucket_vi_change(
    value: float | None,
    *,
    stats: NtRatioReturnStats | None,
) -> NtRatioBucketKey | None:
    if value is None or stats is None or pd.isna(value):
        return None
    if value <= stats.lower_threshold_2:
        return "return_le_mean_minus_2sd"
    if value <= stats.lower_threshold_1:
        return "return_mean_minus_2sd_to_minus_1sd"
    if value < stats.upper_threshold_1:
        return "return_mean_minus_1sd_to_plus_1sd"
    if value < stats.upper_threshold_2:
        return "return_mean_plus_1sd_to_plus_2sd"
    return "return_ge_mean_plus_2sd"


def _collapse_regime_bucket(regime_bucket_key: str | None) -> RegimeGroupKey | None:
    if regime_bucket_key is None or pd.isna(regime_bucket_key):
        return None
    if regime_bucket_key in {
        "return_le_mean_minus_2sd",
        "return_mean_minus_2sd_to_minus_1sd",
    }:
        return "weak"
    if regime_bucket_key == "return_mean_minus_1sd_to_plus_1sd":
        return "neutral"
    return "strong"


def _build_regime_market_df(
    raw_market_df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[pd.DataFrame, NtRatioReturnStats | None]:
    if raw_market_df.empty:
        return pd.DataFrame(), None

    ordered = raw_market_df.copy()
    ordered["date"] = ordered["date"].astype(str)
    ordered["vi_change"] = ordered["vi_close"].pct_change()

    vi_change_stats = _build_vi_change_stats(
        ordered,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )
    ordered["regime_bucket_key"] = ordered["vi_change"].map(
        lambda value: _bucket_vi_change(value, stats=vi_change_stats)
    )
    ordered["regime_bucket_label"] = ordered["regime_bucket_key"].map(
        lambda value: (
            format_nt_ratio_bucket_label(
                value,
                sigma_threshold_1=vi_change_stats.sigma_threshold_1,
                sigma_threshold_2=vi_change_stats.sigma_threshold_2,
            )
            if value is not None and vi_change_stats is not None
            else None
        )
    )

    if start_date:
        ordered = ordered.loc[ordered["date"] >= start_date].copy()
    if end_date:
        ordered = ordered.loc[ordered["date"] <= end_date].copy()
    return _sort_frame(ordered), vi_change_stats


def _build_regime_assignments_df(regime_market_df: pd.DataFrame) -> pd.DataFrame:
    if regime_market_df.empty:
        return pd.DataFrame()

    assignments_df = regime_market_df[
        ["date", "vi_close", "vi_change", "regime_bucket_key", "regime_bucket_label"]
    ].rename(columns={"vi_change": "regime_return"})
    assignments_df["regime_type"] = "vi_change"
    assignments_df["regime_label"] = REGIME_LABEL_MAP["vi_change"]
    assignments_df = assignments_df.dropna(subset=["regime_bucket_key"]).copy()
    assignments_df["regime_group_key"] = assignments_df["regime_bucket_key"].map(_collapse_regime_bucket)
    assignments_df["regime_group_label"] = assignments_df["regime_group_key"].map(
        REGIME_GROUP_LABEL_MAP
    )
    return _sort_frame(assignments_df)


def _build_regime_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    if regime_assignments_df.empty:
        return pd.DataFrame()
    return _sort_frame(
        regime_assignments_df.groupby(
            ["regime_type", "regime_label", "regime_bucket_key", "regime_bucket_label"],
            as_index=False,
        ).agg(
            date_count=("date", "nunique"),
            mean_regime_return=("regime_return", "mean"),
            median_regime_return=("regime_return", "median"),
        )
    )


def _build_regime_group_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    if regime_assignments_df.empty:
        return pd.DataFrame()
    return _sort_frame(
        regime_assignments_df.groupby(
            ["regime_type", "regime_label", "regime_group_key", "regime_group_label"],
            as_index=False,
        ).agg(
            date_count=("date", "nunique"),
            mean_regime_return=("regime_return", "mean"),
            median_regime_return=("regime_return", "median"),
        )
    )


def _build_horizon_panel(split_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _sort_frame(_core_build_horizon_panel(split_panel_df))


def _build_regime_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    return _sort_frame(_core_build_regime_daily_means(horizon_panel_df, regime_assignments_df))


def _summarize_regime_daily_means(regime_daily_means_df: pd.DataFrame) -> pd.DataFrame:
    return _sort_frame(_core_summarize_regime_daily_means(regime_daily_means_df))


def _build_regime_group_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    return _sort_frame(_core_build_regime_group_daily_means(horizon_panel_df, regime_assignments_df))


def _summarize_regime_group_daily_means(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    return _sort_frame(_core_summarize_regime_group_daily_means(regime_group_daily_means_df))


def _aligned_bucket_pivot(
    daily_means_df: pd.DataFrame,
    *,
    bucket_column: str,
    bucket_key: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = daily_means_df[
        (daily_means_df["regime_type"] == "vi_change")
        & (daily_means_df[bucket_column] == bucket_key)
        & (daily_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(index="date", columns="combined_bucket", values=value_column)
        .reindex(columns=list(COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _apply_holm_adjustments(
    pairwise_df: pd.DataFrame,
    *,
    group_columns: tuple[str, ...],
) -> pd.DataFrame:
    if pairwise_df.empty:
        return pairwise_df

    adjusted_df = pairwise_df.copy()
    adjusted_df["paired_t_p_value_holm"] = None
    adjusted_df["wilcoxon_p_value_holm"] = None
    for _, group_df in adjusted_df.groupby(list(group_columns), dropna=False, sort=False):
        adjusted_df.loc[group_df.index, "paired_t_p_value_holm"] = _holm_adjust(
            group_df["paired_t_p_value"].tolist()
        )
        adjusted_df.loc[group_df.index, "wilcoxon_p_value_holm"] = _holm_adjust(
            group_df["wilcoxon_p_value"].tolist()
        )
    return adjusted_df


def _build_regime_pairwise_significance(regime_daily_means_df: pd.DataFrame) -> pd.DataFrame:
    if regime_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    label_lookup = (
        regime_daily_means_df[["regime_bucket_key", "regime_bucket_label"]]
        .drop_duplicates()
        .set_index("regime_bucket_key")["regime_bucket_label"]
        .to_dict()
    )

    for regime_bucket_key in NT_RATIO_BUCKET_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_bucket_pivot(
                    regime_daily_means_df,
                    bucket_column="regime_bucket_key",
                    bucket_key=regime_bucket_key,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                        records.append(
                            {
                                "regime_type": "vi_change",
                                "regime_label": REGIME_LABEL_MAP["vi_change"],
                                "regime_bucket_key": regime_bucket_key,
                                "regime_bucket_label": label_lookup.get(regime_bucket_key),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "left_combined_bucket": left_bucket,
                                "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                                "right_combined_bucket": right_bucket,
                                "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                                "n_dates": 0,
                                "mean_difference": None,
                                "paired_t_statistic": None,
                                "paired_t_p_value": None,
                                "wilcoxon_statistic": None,
                                "wilcoxon_p_value": None,
                            }
                        )
                    continue

                for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                    left = pivot_df[left_bucket].to_numpy(dtype=float)
                    right = pivot_df[right_bucket].to_numpy(dtype=float)
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                    records.append(
                        {
                            "regime_type": "vi_change",
                            "regime_label": REGIME_LABEL_MAP["vi_change"],
                            "regime_bucket_key": regime_bucket_key,
                            "regime_bucket_label": label_lookup.get(regime_bucket_key),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_combined_bucket": left_bucket,
                            "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                            "right_combined_bucket": right_bucket,
                            "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": float((left - right).mean()),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )

    pairwise_df = pd.DataFrame.from_records(records)
    return _sort_frame(
        _apply_holm_adjustments(
            pairwise_df,
            group_columns=("regime_bucket_key", "horizon_key", "metric_key"),
        )
    )


def _build_regime_group_pairwise_significance(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    label_lookup = (
        regime_group_daily_means_df[["regime_group_key", "regime_group_label"]]
        .drop_duplicates()
        .set_index("regime_group_key")["regime_group_label"]
        .to_dict()
    )

    for regime_group_key in REGIME_GROUP_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_bucket_pivot(
                    regime_group_daily_means_df,
                    bucket_column="regime_group_key",
                    bucket_key=regime_group_key,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                        records.append(
                            {
                                "regime_type": "vi_change",
                                "regime_label": REGIME_LABEL_MAP["vi_change"],
                                "regime_group_key": regime_group_key,
                                "regime_group_label": label_lookup.get(regime_group_key),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "left_combined_bucket": left_bucket,
                                "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                                "right_combined_bucket": right_bucket,
                                "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                                "n_dates": 0,
                                "mean_difference": None,
                                "paired_t_statistic": None,
                                "paired_t_p_value": None,
                                "wilcoxon_statistic": None,
                                "wilcoxon_p_value": None,
                            }
                        )
                    continue

                for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                    left = pivot_df[left_bucket].to_numpy(dtype=float)
                    right = pivot_df[right_bucket].to_numpy(dtype=float)
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                    records.append(
                        {
                            "regime_type": "vi_change",
                            "regime_label": REGIME_LABEL_MAP["vi_change"],
                            "regime_group_key": regime_group_key,
                            "regime_group_label": label_lookup.get(regime_group_key),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_combined_bucket": left_bucket,
                            "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                            "right_combined_bucket": right_bucket,
                            "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": float((left - right).mean()),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )

    pairwise_df = pd.DataFrame.from_records(records)
    return _sort_frame(
        _apply_holm_adjustments(
            pairwise_df,
            group_columns=("regime_group_key", "horizon_key", "metric_key"),
        )
    )


def _build_regime_hypothesis(
    regime_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for regime_bucket_key in NT_RATIO_BUCKET_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                scoped_df = regime_pairwise_significance_df[
                    (regime_pairwise_significance_df["regime_bucket_key"] == regime_bucket_key)
                    & (regime_pairwise_significance_df["horizon_key"] == horizon_key)
                    & (regime_pairwise_significance_df["metric_key"] == metric_key)
                ]
                for left_bucket, right_bucket, hypothesis_label in HYPOTHESIS_SPECS:
                    row = scoped_df[
                        (scoped_df["left_combined_bucket"] == left_bucket)
                        & (scoped_df["right_combined_bucket"] == right_bucket)
                    ]
                    sign = 1.0
                    if row.empty:
                        row = scoped_df[
                            (scoped_df["left_combined_bucket"] == right_bucket)
                            & (scoped_df["right_combined_bucket"] == left_bucket)
                        ]
                        sign = -1.0
                    if row.empty:
                        records.append(
                            {
                                "regime_type": "vi_change",
                                "regime_label": REGIME_LABEL_MAP["vi_change"],
                                "regime_bucket_key": regime_bucket_key,
                                "regime_bucket_label": None,
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": None,
                                "paired_t_p_value_holm": None,
                                "wilcoxon_p_value_holm": None,
                            }
                        )
                        continue
                    record = row.iloc[0].to_dict()
                    records.append(
                        {
                            "regime_type": "vi_change",
                            "regime_label": REGIME_LABEL_MAP["vi_change"],
                            "regime_bucket_key": regime_bucket_key,
                            "regime_bucket_label": record.get("regime_bucket_label"),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_label": hypothesis_label,
                            "left_combined_bucket": left_bucket,
                            "right_combined_bucket": right_bucket,
                            "mean_difference": (
                                float(record["mean_difference"]) * sign
                                if record.get("mean_difference") is not None
                                else None
                            ),
                            "paired_t_p_value_holm": record.get("paired_t_p_value_holm"),
                            "wilcoxon_p_value_holm": record.get("wilcoxon_p_value_holm"),
                        }
                    )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_regime_group_hypothesis(
    regime_group_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for regime_group_key in REGIME_GROUP_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                scoped_df = regime_group_pairwise_significance_df[
                    (regime_group_pairwise_significance_df["regime_group_key"] == regime_group_key)
                    & (regime_group_pairwise_significance_df["horizon_key"] == horizon_key)
                    & (regime_group_pairwise_significance_df["metric_key"] == metric_key)
                ]
                for left_bucket, right_bucket, hypothesis_label in HYPOTHESIS_SPECS:
                    row = scoped_df[
                        (scoped_df["left_combined_bucket"] == left_bucket)
                        & (scoped_df["right_combined_bucket"] == right_bucket)
                    ]
                    sign = 1.0
                    if row.empty:
                        row = scoped_df[
                            (scoped_df["left_combined_bucket"] == right_bucket)
                            & (scoped_df["right_combined_bucket"] == left_bucket)
                        ]
                        sign = -1.0
                    if row.empty:
                        records.append(
                            {
                                "regime_type": "vi_change",
                                "regime_label": REGIME_LABEL_MAP["vi_change"],
                                "regime_group_key": regime_group_key,
                                "regime_group_label": REGIME_GROUP_LABEL_MAP[regime_group_key],
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": None,
                                "paired_t_p_value_holm": None,
                                "wilcoxon_p_value_holm": None,
                            }
                        )
                        continue
                    record = row.iloc[0].to_dict()
                    records.append(
                        {
                            "regime_type": "vi_change",
                            "regime_label": REGIME_LABEL_MAP["vi_change"],
                            "regime_group_key": regime_group_key,
                            "regime_group_label": record.get("regime_group_label"),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_label": hypothesis_label,
                            "left_combined_bucket": left_bucket,
                            "right_combined_bucket": right_bucket,
                            "mean_difference": (
                                float(record["mean_difference"]) * sign
                                if record.get("mean_difference") is not None
                                else None
                            ),
                            "paired_t_p_value_holm": record.get("paired_t_p_value_holm"),
                            "wilcoxon_p_value_holm": record.get("wilcoxon_p_value_holm"),
                        }
                    )
    return _sort_frame(pd.DataFrame.from_records(records))


def get_topix100_vi_change_available_date_range(db_path: str) -> tuple[str | None, str | None]:
    """Return the available VI date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _query_vi_available_date_range(ctx.connection)


def run_topix100_vi_change_regime_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
    sigma_threshold_1: float = DEFAULT_SIGMA_THRESHOLD_1,
    sigma_threshold_2: float = DEFAULT_SIGMA_THRESHOLD_2,
) -> Topix100ViChangeRegimeConditioningResearchResult:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")

    base_result: Topix100PriceVsSma20RankFutureCloseResearchResult = (
        run_topix100_price_vs_sma20_rank_future_close_research(
            db_path,
            start_date=start_date,
            end_date=end_date,
            lookback_years=lookback_years,
            min_constituents_per_day=min_constituents_per_day,
        )
    )

    with _open_analysis_connection(db_path) as ctx:
        available_start_date, available_end_date = _query_vi_available_date_range(ctx.connection)
        raw_market_df = _query_vi_market_history(
            ctx.connection,
            end_date=base_result.analysis_end_date,
        )
        regime_market_df, vi_change_stats = _build_regime_market_df(
            raw_market_df,
            start_date=base_result.analysis_start_date,
            end_date=base_result.analysis_end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )

    regime_assignments_df = _build_regime_assignments_df(regime_market_df)
    regime_day_counts_df = _build_regime_day_counts(regime_assignments_df)
    regime_group_day_counts_df = _build_regime_group_day_counts(regime_assignments_df)
    split_panel_df = base_result.price_volume_split_panel_df.copy()
    horizon_panel_df = _build_horizon_panel(split_panel_df)
    regime_daily_means_df = _build_regime_daily_means(horizon_panel_df, regime_assignments_df)
    regime_summary_df = _summarize_regime_daily_means(regime_daily_means_df)
    regime_pairwise_significance_df = _build_regime_pairwise_significance(regime_daily_means_df)
    regime_hypothesis_df = _build_regime_hypothesis(regime_pairwise_significance_df)
    regime_group_daily_means_df = _build_regime_group_daily_means(horizon_panel_df, regime_assignments_df)
    regime_group_summary_df = _summarize_regime_group_daily_means(regime_group_daily_means_df)
    regime_group_pairwise_significance_df = _build_regime_group_pairwise_significance(
        regime_group_daily_means_df
    )
    regime_group_hypothesis_df = _build_regime_group_hypothesis(
        regime_group_pairwise_significance_df
    )

    split_panel_dates = (
        set(split_panel_df["date"].astype(str).tolist())
        if "date" in split_panel_df.columns
        else set()
    )
    regime_dates = (
        set(regime_assignments_df["date"].astype(str).tolist())
        if "date" in regime_assignments_df.columns
        else set()
    )
    valid_date_count = len(split_panel_dates.intersection(regime_dates))

    return Topix100ViChangeRegimeConditioningResearchResult(
        db_path=db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        universe_constituent_count=base_result.topix100_constituent_count,
        valid_date_count=valid_date_count,
        vi_change_stats=vi_change_stats,
        regime_market_df=regime_market_df,
        regime_day_counts_df=regime_day_counts_df,
        regime_group_day_counts_df=regime_group_day_counts_df,
        split_panel_df=split_panel_df,
        horizon_panel_df=horizon_panel_df,
        regime_daily_means_df=regime_daily_means_df,
        regime_summary_df=regime_summary_df,
        regime_pairwise_significance_df=regime_pairwise_significance_df,
        regime_hypothesis_df=regime_hypothesis_df,
        regime_group_daily_means_df=regime_group_daily_means_df,
        regime_group_summary_df=regime_group_summary_df,
        regime_group_pairwise_significance_df=regime_group_pairwise_significance_df,
        regime_group_hypothesis_df=regime_group_hypothesis_df,
    )


def write_topix100_vi_change_regime_conditioning_research_bundle(
    result: Topix100ViChangeRegimeConditioningResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_vi_change_regime_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_VI_CHANGE_REGIME_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_vi_change_regime_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "sigma_threshold_1": result.sigma_threshold_1,
            "sigma_threshold_2": result.sigma_threshold_2,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_vi_change_regime_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_vi_change_regime_conditioning_research_bundle(
    bundle_path: str | Path,
) -> Topix100ViChangeRegimeConditioningResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_vi_change_regime_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_vi_change_regime_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_VI_CHANGE_REGIME_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_VI_CHANGE_REGIME_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_vi_change_regime_result_payload(
    result: Topix100ViChangeRegimeConditioningResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata: dict[str, Any] = {}
    tables: dict[str, pd.DataFrame] = {}
    for field in fields(result):
        value = getattr(result, field.name)
        if isinstance(value, pd.DataFrame):
            tables[field.name] = value
            continue
        if field.name == "vi_change_stats" and value is not None:
            metadata[field.name] = asdict(value)
            continue
        metadata[field.name] = value
    return metadata, tables


def _build_vi_change_regime_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100ViChangeRegimeConditioningResearchResult:
    vi_change_stats_payload = metadata.get("vi_change_stats")
    return Topix100ViChangeRegimeConditioningResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=str(metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        sigma_threshold_1=float(metadata["sigma_threshold_1"]),
        sigma_threshold_2=float(metadata["sigma_threshold_2"]),
        universe_constituent_count=int(metadata["universe_constituent_count"]),
        valid_date_count=int(metadata["valid_date_count"]),
        vi_change_stats=(
            NtRatioReturnStats(**vi_change_stats_payload) if vi_change_stats_payload else None
        ),
        regime_market_df=tables["regime_market_df"],
        regime_day_counts_df=tables["regime_day_counts_df"],
        regime_group_day_counts_df=tables["regime_group_day_counts_df"],
        split_panel_df=tables["split_panel_df"],
        horizon_panel_df=tables["horizon_panel_df"],
        regime_daily_means_df=tables["regime_daily_means_df"],
        regime_summary_df=tables["regime_summary_df"],
        regime_pairwise_significance_df=tables["regime_pairwise_significance_df"],
        regime_hypothesis_df=tables["regime_hypothesis_df"],
        regime_group_daily_means_df=tables["regime_group_daily_means_df"],
        regime_group_summary_df=tables["regime_group_summary_df"],
        regime_group_pairwise_significance_df=tables[
            "regime_group_pairwise_significance_df"
        ],
        regime_group_hypothesis_df=tables["regime_group_hypothesis_df"],
    )


def _build_vi_change_regime_bundle_summary_markdown(
    result: Topix100ViChangeRegimeConditioningResearchResult,
) -> str:
    summary_lines = [
        "# TOPIX100 VI Change Regime Conditioning",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Sigma thresholds: `{result.sigma_threshold_1}, {result.sigma_threshold_2}`",
        f"- TOPIX100 constituents: `{result.universe_constituent_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        "",
        "## Current Read",
        "",
    ]
    strongest_rows = result.regime_group_hypothesis_df[
        (result.regime_group_hypothesis_df["metric_key"] == "future_return")
        & (result.regime_group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_group_hypothesis_df["hypothesis_label"] == "Q10 Low vs Middle High")
        & result.regime_group_hypothesis_df["mean_difference"].notna()
    ].copy()
    if strongest_rows.empty:
        summary_lines.append("- No grouped `Q10 Low vs Middle High` rows were available.")
    else:
        strongest_row = strongest_rows.sort_values("mean_difference", ascending=False).iloc[0]
        summary_lines.append(
            "- Strongest grouped `Q10 Low vs Middle High` read: "
            f"`{strongest_row['regime_group_key']}` at "
            f"`{float(strongest_row['mean_difference']):+.4f}%`."
        )
    summary_lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_vi_change_regime_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)
