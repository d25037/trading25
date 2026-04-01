# pyright: reportUnusedFunction=false
"""
Shared core helpers for TOPIX rank / future-close research modules.

These helpers are intentionally generic so different price-feature studies can
reuse the same universe query, warmup, decile assignment, horizon expansion,
and significance logic without depending on one another's concrete modules.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any, Literal, Sequence, cast

import numpy as np
import pandas as pd
from scipy import stats

from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _normalize_code_sql,
)

DecileKey = Literal[
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
]
HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
UniverseKey = Literal["topix100", "prime_ex_topix500"]

DECILE_ORDER: tuple[DecileKey, ...] = (
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
)
HORIZON_ORDER: tuple[HorizonKey, ...] = ("t_plus_1", "t_plus_5", "t_plus_10")
METRIC_ORDER: tuple[MetricKey, ...] = ("future_close", "future_return")
_HORIZON_DAY_MAP: dict[HorizonKey, int] = {
    "t_plus_1": 1,
    "t_plus_5": 5,
    "t_plus_10": 10,
}
_DECILE_LABEL_MAP: dict[DecileKey, str] = {
    "Q1": "Q1 Highest Ratio",
    "Q2": "Q2",
    "Q3": "Q3",
    "Q4": "Q4",
    "Q5": "Q5",
    "Q6": "Q6",
    "Q7": "Q7",
    "Q8": "Q8",
    "Q9": "Q9",
    "Q10": "Q10 Lowest Ratio",
}
_TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = ("TOPIX Core30", "TOPIX Large70")
_TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
_PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "prime")
_DEFAULT_LOOKBACK_YEARS = 10
_DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY = 80
_DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY = 400


def _universe_membership_sql_and_params(
    universe_key: UniverseKey,
) -> tuple[str, list[str]]:
    if universe_key == "topix100":
        return (
            "scale_category IN (?, ?)",
            list(_TOPIX100_SCALE_CATEGORIES),
        )
    if universe_key == "prime_ex_topix500":
        return (
            "market_code IN (?, ?) AND scale_category NOT IN (?, ?, ?)",
            [*_PRIME_MARKET_CODES, *_TOPIX500_SCALE_CATEGORIES],
        )
    raise ValueError(f"Unsupported universe_key: {universe_key}")


def _query_universe_stock_history(
    conn: Any,
    *,
    universe_key: UniverseKey,
    end_date: str | None,
) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    membership_sql, membership_params = _universe_membership_sql_and_params(
        universe_key
    )
    date_filter_sql = ""
    params: list[str] = []
    if end_date:
        date_filter_sql = " AND date <= ?"
        params.append(end_date)

    sql = f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                coalesce(company_name, code) AS company_name,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        latest_universe AS (
            SELECT
                normalized_code,
                company_name
            FROM latest_universe_raw
            WHERE row_priority = 1
              AND {membership_sql}
        ),
        stock_rows_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE close IS NOT NULL
              AND close > 0
              AND volume IS NOT NULL
              {date_filter_sql}
        ),
        stock_rows AS (
            SELECT
                date,
                normalized_code,
                open,
                high,
                low,
                close,
                volume
            FROM stock_rows_raw
            WHERE row_priority = 1
        )
        SELECT
            s.normalized_code AS code,
            s.company_name,
            r.date,
            r.open,
            r.high,
            r.low,
            r.close,
            r.volume
        FROM stock_rows r
        JOIN latest_universe s
          ON s.normalized_code = r.normalized_code
        ORDER BY s.normalized_code, r.date
    """
    return conn.execute(sql, [*membership_params, *params]).fetchdf()


def _query_universe_date_range(
    conn: Any,
    *,
    universe_key: UniverseKey,
) -> tuple[str | None, str | None]:
    normalized_code_sql = _normalize_code_sql("code")
    membership_sql, membership_params = _universe_membership_sql_and_params(
        universe_key
    )
    row = conn.execute(
        f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        latest_universe AS (
            SELECT normalized_code
            FROM latest_universe_raw
            WHERE row_priority = 1
              AND {membership_sql}
        ),
        stock_rows_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE close IS NOT NULL
              AND close > 0
        ),
        stock_rows AS (
            SELECT date, normalized_code
            FROM stock_rows_raw
            WHERE row_priority = 1
        )
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM stock_rows r
        JOIN latest_universe s
          ON s.normalized_code = r.normalized_code
        """,
        membership_params,
    ).fetchone()
    if not row:
        return None, None
    min_date = str(row[0]) if row[0] else None
    max_date = str(row[1]) if row[1] else None
    return min_date, max_date


def _query_topix100_stock_history(
    conn: Any,
    *,
    end_date: str | None,
) -> pd.DataFrame:
    return _query_universe_stock_history(
        conn,
        universe_key="topix100",
        end_date=end_date,
    )


def _query_topix100_date_range(conn: Any) -> tuple[str | None, str | None]:
    return _query_universe_date_range(conn, universe_key="topix100")


def _query_prime_ex_topix500_date_range(conn: Any) -> tuple[str | None, str | None]:
    return _query_universe_date_range(conn, universe_key="prime_ex_topix500")


def _default_start_date(
    *,
    available_start_date: str | None,
    available_end_date: str | None,
    lookback_years: int,
) -> str | None:
    if available_end_date is None:
        return available_start_date
    candidate = (
        pd.Timestamp(available_end_date)
        - pd.DateOffset(years=lookback_years)
        + pd.Timedelta(days=1)
    ).strftime("%Y-%m-%d")
    if available_start_date is None:
        return candidate
    return max(available_start_date, candidate)


def _rolling_mean(
    df: pd.DataFrame,
    *,
    column_name: str,
    window: int,
) -> pd.Series:
    return (
        df.groupby("code", sort=False)[column_name]
        .rolling(window=window, min_periods=window)
        .mean()
        .reset_index(level=0, drop=True)
    )


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    ratio = numerator / denominator
    return ratio.where(np.isfinite(ratio))


def _ordered_feature_values(
    values: list[str] | pd.Series,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> list[str]:
    unique_values = list(dict.fromkeys(pd.Series(values).dropna().astype(str).tolist()))
    if not known_feature_order:
        return unique_values
    known_values = [value for value in known_feature_order if value in unique_values]
    extra_values = sorted(value for value in unique_values if value not in known_values)
    return [*known_values, *extra_values]


def _sort_frame(
    df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    if "ranking_feature" in sorted_df.columns:
        feature_order = {
            key: index
            for index, key in enumerate(
                _ordered_feature_values(
                    sorted_df["ranking_feature"],
                    known_feature_order=known_feature_order,
                ),
                start=1,
            )
        }
        sorted_df["_feature_order"] = sorted_df["ranking_feature"].map(feature_order)
    if "selected_horizon_key" in sorted_df.columns:
        sorted_df["_selected_horizon_order"] = sorted_df["selected_horizon_key"].map(
            {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
        )
    if "horizon_key" in sorted_df.columns:
        sorted_df["_horizon_order"] = sorted_df["horizon_key"].map(
            {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
        )
    if "feature_decile" in sorted_df.columns:
        sorted_df["_decile_order"] = sorted_df["feature_decile"].map(
            {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
        )
    if "left_decile" in sorted_df.columns:
        sorted_df["_left_decile_order"] = sorted_df["left_decile"].map(
            {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
        )
    if "right_decile" in sorted_df.columns:
        sorted_df["_right_decile_order"] = sorted_df["right_decile"].map(
            {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
        )

    sort_columns = [
        column
        for column in [
            "_selected_horizon_order",
            "_feature_order",
            "_horizon_order",
            "_decile_order",
            "_left_decile_order",
            "_right_decile_order",
            "date",
            "metric_key",
            "left_decile",
            "right_decile",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns).reset_index(drop=True)

    return sorted_df.drop(
        columns=[
            column
            for column in [
                "_selected_horizon_order",
                "_feature_order",
                "_horizon_order",
                "_decile_order",
                "_left_decile_order",
                "_right_decile_order",
            ]
            if column in sorted_df.columns
        ]
    )


def _ranking_feature_label_lookup(df: pd.DataFrame) -> dict[str, str]:
    if df.empty or "ranking_feature" not in df.columns:
        return {}
    scoped_df = df.dropna(subset=["ranking_feature"]).copy()
    if "ranking_feature_label" not in scoped_df.columns:
        return {
            feature: feature
            for feature in scoped_df["ranking_feature"].astype(str).unique().tolist()
        }
    return (
        scoped_df.groupby("ranking_feature")["ranking_feature_label"].first().to_dict()
    )


def _assign_feature_deciles(
    ranked_panel_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ranked_panel_df.empty:
        return ranked_panel_df
    ranked_panel_df["feature_rank_desc"] = (
        ranked_panel_df.groupby(["ranking_feature", "date"])["ranking_value"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    group_count = len(DECILE_ORDER)
    ranked_panel_df["feature_decile_index"] = (
        ((ranked_panel_df["feature_rank_desc"] - 1) * group_count)
        // ranked_panel_df["date_constituent_count"]
    ) + 1
    ranked_panel_df["feature_decile_index"] = ranked_panel_df[
        "feature_decile_index"
    ].clip(1, group_count)
    ranked_panel_df["feature_decile"] = ranked_panel_df["feature_decile_index"].map(
        {index: f"Q{index}" for index in range(1, group_count + 1)}
    )
    ranked_panel_df["feature_decile_label"] = ranked_panel_df["feature_decile"].map(
        _DECILE_LABEL_MAP
    )
    return _sort_frame(
        ranked_panel_df.reset_index(drop=True),
        known_feature_order=known_feature_order,
    )


def _build_horizon_panel(
    ranked_panel_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ranked_panel_df.empty:
        return pd.DataFrame()

    common_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "ranking_feature",
        "ranking_feature_label",
        "ranking_value",
        "feature_rank_desc",
        "feature_decile_index",
        "feature_decile",
        "feature_decile_label",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = ranked_panel_df[
            common_columns + [f"{horizon_key}_close", f"{horizon_key}_return"]
        ].copy()
        frame["horizon_key"] = horizon_key
        frame["horizon_days"] = _HORIZON_DAY_MAP[horizon_key]
        frame["future_close"] = frame.pop(f"{horizon_key}_close")
        frame["future_return"] = frame.pop(f"{horizon_key}_return")
        frame = frame.dropna(subset=["future_close", "future_return"]).copy()
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=common_columns + ["horizon_key", "horizon_days"])
    return _sort_frame(
        pd.concat(frames, ignore_index=True),
        known_feature_order=known_feature_order,
    )


def _summarize_ranking_features(
    ranked_panel_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ranked_panel_df.empty:
        return pd.DataFrame()

    summary_df = ranked_panel_df.groupby(
        [
            "ranking_feature",
            "ranking_feature_label",
            "feature_decile",
            "feature_decile_label",
        ],
        as_index=False,
    ).agg(
        sample_count=("code", "size"),
        date_count=("date", "nunique"),
        mean_ranking_value=("ranking_value", "mean"),
        median_ranking_value=("ranking_value", "median"),
        mean_event_close=("close", "mean"),
        median_event_close=("close", "median"),
    )
    return _sort_frame(summary_df, known_feature_order=known_feature_order)


def _summarize_future_targets(
    horizon_panel_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if horizon_panel_df.empty:
        return pd.DataFrame()

    summary_df = horizon_panel_df.groupby(
        [
            "ranking_feature",
            "ranking_feature_label",
            "horizon_key",
            "horizon_days",
            "feature_decile",
            "feature_decile_label",
        ],
        as_index=False,
    ).agg(
        sample_count=("code", "size"),
        date_count=("date", "nunique"),
        mean_ranking_value=("ranking_value", "mean"),
        mean_event_close=("close", "mean"),
        mean_future_close=("future_close", "mean"),
        median_future_close=("future_close", "median"),
        mean_future_return=("future_return", "mean"),
        median_future_return=("future_return", "median"),
        std_future_return=("future_return", "std"),
    )
    return _sort_frame(summary_df, known_feature_order=known_feature_order)


def _build_daily_group_means(
    horizon_panel_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if horizon_panel_df.empty:
        return pd.DataFrame()

    daily_group_means_df = horizon_panel_df.groupby(
        [
            "ranking_feature",
            "ranking_feature_label",
            "horizon_key",
            "horizon_days",
            "date",
            "feature_decile",
            "feature_decile_label",
        ],
        as_index=False,
    ).agg(
        group_sample_count=("code", "size"),
        group_mean_ranking_value=("ranking_value", "mean"),
        group_mean_event_close=("close", "mean"),
        group_mean_future_close=("future_close", "mean"),
        group_mean_future_return=("future_return", "mean"),
        group_median_future_return=("future_return", "median"),
    )
    return _sort_frame(daily_group_means_df, known_feature_order=known_feature_order)


def _aligned_decile_pivot(
    daily_group_means_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = daily_group_means_df.loc[
        (daily_group_means_df["ranking_feature"] == ranking_feature)
        & (daily_group_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(DECILE_ORDER))
    pivot_df = (
        scoped_df.pivot(index="date", columns="feature_decile", values=value_column)
        .reindex(columns=list(DECILE_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _safe_kruskal(samples: list[np.ndarray]) -> tuple[float | None, float | None]:
    if any(len(sample) == 0 for sample in samples):
        return None, None
    statistic, p_value = stats.kruskal(*samples)
    return float(cast(float, statistic)), float(cast(float, p_value))


def _safe_friedman(samples: list[np.ndarray]) -> tuple[float | None, float | None]:
    if not samples or len(samples[0]) < 2:
        return None, None
    statistic, p_value = stats.friedmanchisquare(*samples)
    return float(np.asarray(statistic).item()), float(np.asarray(p_value).item())


def _kendalls_w(
    *,
    friedman_statistic: float | None,
    n_dates: int,
    n_groups: int,
) -> float | None:
    if friedman_statistic is None or n_dates <= 0 or n_groups <= 1:
        return None
    return float(friedman_statistic / (n_dates * (n_groups - 1)))


def _holm_adjust(p_values: list[float | None]) -> list[float | None]:
    indexed = [
        (index, float(p_value))
        for index, p_value in enumerate(p_values)
        if p_value is not None and not pd.isna(p_value)
    ]
    adjusted: list[float | None] = [None] * len(p_values)
    if not indexed:
        return adjusted

    indexed.sort(key=lambda item: item[1])
    running_max = 0.0
    total = len(indexed)
    for rank, (index, p_value) in enumerate(indexed):
        adjusted_value = min(1.0, p_value * (total - rank))
        running_max = max(running_max, adjusted_value)
        adjusted[index] = float(running_max)
    return adjusted


def _safe_paired_t_test(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    diff = left - right
    if len(diff) < 2:
        return None, None
    if np.allclose(diff, 0.0):
        return 0.0, 1.0
    statistic, p_value = stats.ttest_rel(left, right, nan_policy="omit")
    return float(statistic), float(p_value)


def _safe_wilcoxon(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    diff = left - right
    if len(diff) == 0:
        return None, None
    if np.allclose(diff, 0.0):
        return 0.0, 1.0
    try:
        result = stats.wilcoxon(left, right, zero_method="wilcox")
    except ValueError:
        return None, None
    return float(result.statistic), float(result.pvalue)


def _build_global_significance(
    daily_group_means_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if (
        daily_group_means_df.empty
        or "ranking_feature" not in daily_group_means_df.columns
    ):
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    feature_values = _ordered_feature_values(
        daily_group_means_df["ranking_feature"],
        known_feature_order=known_feature_order,
    )
    label_lookup = _ranking_feature_label_lookup(daily_group_means_df)
    for ranking_feature in feature_values:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_decile_pivot(
                    daily_group_means_df,
                    ranking_feature=ranking_feature,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    records.append(
                        {
                            "ranking_feature": ranking_feature,
                            "ranking_feature_label": label_lookup.get(
                                ranking_feature,
                                ranking_feature,
                            ),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "n_dates": 0,
                            "q1_mean": None,
                            "q10_mean": None,
                            "q1_minus_q10_mean": None,
                            "friedman_statistic": None,
                            "friedman_p_value": None,
                            "kendalls_w": None,
                            "kruskal_statistic": None,
                            "kruskal_p_value": None,
                        }
                    )
                    continue

                samples = [
                    pivot_df[decile].to_numpy(dtype=float) for decile in DECILE_ORDER
                ]
                friedman_statistic, friedman_p_value = _safe_friedman(samples)
                kruskal_statistic, kruskal_p_value = _safe_kruskal(samples)
                q1_mean = float(samples[0].mean())
                q10_mean = float(samples[-1].mean())
                records.append(
                    {
                        "ranking_feature": ranking_feature,
                        "ranking_feature_label": label_lookup.get(
                            ranking_feature,
                            ranking_feature,
                        ),
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": int(len(pivot_df)),
                        "q1_mean": q1_mean,
                        "q10_mean": q10_mean,
                        "q1_minus_q10_mean": q1_mean - q10_mean,
                        "friedman_statistic": friedman_statistic,
                        "friedman_p_value": friedman_p_value,
                        "kendalls_w": _kendalls_w(
                            friedman_statistic=friedman_statistic,
                            n_dates=len(pivot_df),
                            n_groups=len(DECILE_ORDER),
                        ),
                        "kruskal_statistic": kruskal_statistic,
                        "kruskal_p_value": kruskal_p_value,
                    }
                )
    return _sort_frame(
        pd.DataFrame.from_records(records), known_feature_order=known_feature_order
    )


def _build_pairwise_significance(
    daily_group_means_df: pd.DataFrame,
    *,
    known_feature_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    if (
        daily_group_means_df.empty
        or "ranking_feature" not in daily_group_means_df.columns
    ):
        return pd.DataFrame()
    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }

    feature_values = _ordered_feature_values(
        daily_group_means_df["ranking_feature"],
        known_feature_order=known_feature_order,
    )
    label_lookup = _ranking_feature_label_lookup(daily_group_means_df)
    for ranking_feature in feature_values:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_decile_pivot(
                    daily_group_means_df,
                    ranking_feature=ranking_feature,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    for left_decile, right_decile in combinations(DECILE_ORDER, 2):
                        records.append(
                            {
                                "ranking_feature": ranking_feature,
                                "ranking_feature_label": label_lookup.get(
                                    ranking_feature,
                                    ranking_feature,
                                ),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "left_decile": left_decile,
                                "right_decile": right_decile,
                                "n_dates": 0,
                                "mean_difference": None,
                                "paired_t_statistic": None,
                                "paired_t_p_value": None,
                                "wilcoxon_statistic": None,
                                "wilcoxon_p_value": None,
                            }
                        )
                    continue

                for left_decile, right_decile in combinations(DECILE_ORDER, 2):
                    left = pivot_df[left_decile].to_numpy(dtype=float)
                    right = pivot_df[right_decile].to_numpy(dtype=float)
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                        left, right
                    )
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                    records.append(
                        {
                            "ranking_feature": ranking_feature,
                            "ranking_feature_label": label_lookup.get(
                                ranking_feature,
                                ranking_feature,
                            ),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_decile": left_decile,
                            "right_decile": right_decile,
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": float((left - right).mean()),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )

    pairwise_df = pd.DataFrame.from_records(records)
    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None
    for ranking_feature in feature_values:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                mask = (
                    (pairwise_df["ranking_feature"] == ranking_feature)
                    & (pairwise_df["horizon_key"] == horizon_key)
                    & (pairwise_df["metric_key"] == metric_key)
                )
                pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                    pairwise_df.loc[mask, "paired_t_p_value"].tolist()
                )
                pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                    pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
                )
    return _sort_frame(pairwise_df, known_feature_order=known_feature_order)
