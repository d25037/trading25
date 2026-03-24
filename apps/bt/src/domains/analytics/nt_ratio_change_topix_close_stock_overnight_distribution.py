"""
NT ratio change + TOPIX close-conditioned stock overnight return analysis.

The market.duckdb file is the source of truth for both metadata and time-series
tables. This module performs read-only analytics and returns pandas DataFrames
for notebook visualization.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioReturnStats,
    _nt_ratio_available_date_range,
    _query_nt_ratio_stats,
    format_nt_ratio_bucket_label,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CLOSE_BUCKET_ORDER,
    STOCK_GROUP_ORDER as _STOCK_GROUP_ORDER,
    CloseBucketKey,
    StockGroup,
    _date_where_clause,
    _normalize_code_sql,
    _open_analysis_connection,
    _validate_selected_groups,
    format_close_bucket_label,
)

NtRatioBucketKey = Literal[
    "return_le_mean_minus_2sd",
    "return_mean_minus_2sd_to_minus_1sd",
    "return_mean_minus_1sd_to_plus_1sd",
    "return_mean_plus_1sd_to_plus_2sd",
    "return_ge_mean_plus_2sd",
]
SourceMode = Literal["live", "snapshot"]

STOCK_GROUP_ORDER: tuple[StockGroup, ...] = _STOCK_GROUP_ORDER
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"


@dataclass(frozen=True)
class NtRatioChangeTopixCloseStockOvernightDistributionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    sigma_threshold_1: float
    sigma_threshold_2: float
    topix_close_threshold_1: float
    topix_close_threshold_2: float
    nt_ratio_stats: NtRatioReturnStats | None
    selected_groups: tuple[StockGroup, ...]
    excluded_nt_ratio_days_without_prev_ratio: int
    excluded_topix_days_without_prev_close: int
    excluded_joint_days_without_next_session: int
    joint_day_counts_df: pd.DataFrame
    summary_df: pd.DataFrame
    samples_df: pd.DataFrame
    clipped_samples_df: pd.DataFrame
    clip_bounds_df: pd.DataFrame
    daily_group_returns_df: pd.DataFrame


def _map_nt_ratio_bucket_labels(
    bucket_keys: pd.Series,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> pd.Series:
    return bucket_keys.map(
        lambda value: format_nt_ratio_bucket_label(
            cast(NtRatioBucketKey, value),
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
    )


def _map_topix_bucket_labels(
    bucket_keys: pd.Series,
    *,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> pd.Series:
    return bucket_keys.map(
        lambda value: format_close_bucket_label(
            cast(CloseBucketKey, value),
            close_threshold_1=topix_close_threshold_1,
            close_threshold_2=topix_close_threshold_2,
        )
    )


def _topix_close_bucket_case_sql() -> str:
    return (
        "CASE "
        "WHEN topix_close_return IS NULL THEN NULL "
        "WHEN topix_close_return <= -? THEN 'close_le_negative_threshold_2' "
        "WHEN topix_close_return <= -? THEN 'close_negative_threshold_2_to_1' "
        "WHEN topix_close_return < ? THEN 'close_negative_threshold_1_to_threshold_1' "
        "WHEN topix_close_return < ? THEN 'close_threshold_1_to_2' "
        "ELSE 'close_ge_threshold_2' "
        "END AS topix_close_bucket_key"
    )


def _joint_event_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats | None,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> tuple[str, list[Any]]:
    date_where_sql, date_params = _date_where_clause("date", start_date, end_date)
    params: list[Any] = [_NIKKEI_SYNTHETIC_INDEX_CODE]

    nt_ratio_bucket_case_sql = "NULL AS nt_ratio_bucket_key"
    if nt_ratio_stats is not None:
        nt_ratio_bucket_case_sql = (
            "CASE "
            "WHEN nt_ratio_return IS NULL THEN NULL "
            "WHEN nt_ratio_return <= ? THEN 'return_le_mean_minus_2sd' "
            "WHEN nt_ratio_return <= ? THEN 'return_mean_minus_2sd_to_minus_1sd' "
            "WHEN nt_ratio_return < ? THEN 'return_mean_minus_1sd_to_plus_1sd' "
            "WHEN nt_ratio_return < ? THEN 'return_mean_plus_1sd_to_plus_2sd' "
            "ELSE 'return_ge_mean_plus_2sd' "
            "END AS nt_ratio_bucket_key"
        )
        params.extend(
            [
                nt_ratio_stats.lower_threshold_2,
                nt_ratio_stats.lower_threshold_1,
                nt_ratio_stats.upper_threshold_1,
                nt_ratio_stats.upper_threshold_2,
            ]
        )

    params.extend(
        [
            topix_close_threshold_2,
            topix_close_threshold_1,
            topix_close_threshold_1,
            topix_close_threshold_2,
        ]
    )
    params.extend(date_params)

    sql = f"""
        WITH event_ordered AS (
            SELECT
                t.date,
                LEAD(t.date) OVER (ORDER BY t.date) AS next_date,
                CAST(t.close AS DOUBLE) AS topix_close,
                LAG(CAST(t.close AS DOUBLE)) OVER (ORDER BY t.date) AS prev_topix_close,
                CAST(n.close AS DOUBLE) / CAST(t.close AS DOUBLE) AS nt_ratio,
                LAG(CAST(n.close AS DOUBLE) / CAST(t.close AS DOUBLE))
                    OVER (ORDER BY t.date) AS prev_nt_ratio
            FROM topix_data t
            JOIN indices_data n
              ON n.date = t.date
             AND n.code = ?
            WHERE t.close IS NOT NULL
              AND t.close > 0
              AND n.close IS NOT NULL
              AND n.close > 0
        ),
        joint_event_base AS (
            SELECT
                date,
                next_date,
                topix_close,
                prev_topix_close,
                nt_ratio,
                prev_nt_ratio,
                CASE
                    WHEN prev_nt_ratio IS NULL OR prev_nt_ratio = 0 THEN NULL
                    ELSE (nt_ratio - prev_nt_ratio) / prev_nt_ratio
                END AS nt_ratio_return,
                CASE
                    WHEN prev_topix_close IS NULL OR prev_topix_close = 0 THEN NULL
                    ELSE (topix_close - prev_topix_close) / prev_topix_close
                END AS topix_close_return
            FROM event_ordered
        ),
        joint_event_days AS (
            SELECT
                date,
                next_date,
                nt_ratio,
                prev_nt_ratio,
                nt_ratio_return,
                topix_close,
                prev_topix_close,
                topix_close_return,
                {nt_ratio_bucket_case_sql},
                {_topix_close_bucket_case_sql()}
            FROM joint_event_base
            {date_where_sql}
        )
    """
    return sql, params


def _query_joint_analysis_date_range(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> tuple[str | None, str | None]:
    cte_sql, params = _joint_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=None,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM joint_event_days
        WHERE nt_ratio_return IS NOT NULL
          AND topix_close_return IS NOT NULL
          AND next_date IS NOT NULL
        """,
        params,
    ).fetchone()
    analysis_start = str(row[0]) if row and row[0] else None
    analysis_end = str(row[1]) if row and row[1] else None
    return analysis_start, analysis_end


def _query_exclusions(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> tuple[int, int, int]:
    cte_sql, params = _joint_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=None,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT
            COUNT(*) FILTER (
                WHERE prev_nt_ratio IS NULL OR prev_nt_ratio = 0
            ) AS excluded_without_prev_nt_ratio,
            COUNT(*) FILTER (
                WHERE prev_topix_close IS NULL OR prev_topix_close = 0
            ) AS excluded_without_prev_topix_close,
            COUNT(*) FILTER (
                WHERE nt_ratio_return IS NOT NULL
                  AND topix_close_return IS NOT NULL
                  AND next_date IS NULL
            ) AS excluded_without_next_session
        FROM joint_event_days
        """,
        params,
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def _grouped_stock_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> tuple[str, list[Any]]:
    cte_sql, params = _joint_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    normalized_code_sql = _normalize_code_sql("code")
    stock_event_conditions: list[str] = ["close IS NOT NULL"]
    stock_event_params: list[str] = []
    if start_date:
        stock_event_conditions.insert(0, "date >= ?")
        stock_event_params.append(start_date)
    if end_date:
        stock_event_conditions.insert(1 if start_date else 0, "date <= ?")
        stock_event_params.append(end_date)
    stock_event_where_sql = " WHERE " + " AND ".join(stock_event_conditions)

    group_selects = {
        "PRIME": """
            SELECT
                date,
                next_date,
                normalized_code,
                overnight_diff,
                overnight_return,
                direction,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                nt_ratio_return,
                topix_close_return,
                'PRIME' AS stock_group
            FROM stock_days_joined
            WHERE is_prime
        """,
        "TOPIX100": """
            SELECT
                date,
                next_date,
                normalized_code,
                overnight_diff,
                overnight_return,
                direction,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                nt_ratio_return,
                topix_close_return,
                'TOPIX100' AS stock_group
            FROM stock_days_joined
            WHERE is_topix100
        """,
        "TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                overnight_diff,
                overnight_return,
                direction,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                nt_ratio_return,
                topix_close_return,
                'TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_topix500
        """,
        "PRIME ex TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                overnight_diff,
                overnight_return,
                direction,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                nt_ratio_return,
                topix_close_return,
                'PRIME ex TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_prime_ex_topix500
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)

    sql = f"""
        {cte_sql},
        stocks_snapshot_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                lower(trim(market_code)) AS market_code_norm,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        stocks_snapshot AS (
            SELECT
                normalized_code,
                market_code_norm IN ('prime', '0111') AS is_prime,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70') AS is_topix100,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400') AS is_topix500,
                market_code_norm IN ('prime', '0111')
                    AND scale_category NOT IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400') AS is_prime_ex_topix500
            FROM stocks_snapshot_raw
            WHERE row_priority = 1
        ),
        stock_event_close_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            {stock_event_where_sql}
        ),
        stock_event_close AS (
            SELECT
                date,
                normalized_code,
                close AS event_close
            FROM stock_event_close_raw
            WHERE row_priority = 1
        ),
        stock_next_open_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                open,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE open IS NOT NULL
        ),
        stock_next_open AS (
            SELECT
                date AS next_date,
                normalized_code,
                open AS next_open
            FROM stock_next_open_raw
            WHERE row_priority = 1
        ),
        stock_days_joined AS (
            SELECT
                t.date,
                t.next_date,
                s.normalized_code,
                n.next_open - s.event_close AS overnight_diff,
                CASE
                    WHEN s.event_close = 0 THEN NULL
                    ELSE (n.next_open - s.event_close) / s.event_close
                END AS overnight_return,
                CASE
                    WHEN n.next_open - s.event_close > 0 THEN 'up'
                    WHEN n.next_open - s.event_close < 0 THEN 'down'
                    ELSE 'flat'
                END AS direction,
                t.nt_ratio_bucket_key,
                t.topix_close_bucket_key,
                t.nt_ratio_return,
                t.topix_close_return,
                m.is_prime,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500
            FROM joint_event_days t
            JOIN stock_event_close s ON s.date = t.date
            JOIN stock_next_open n
              ON n.next_date = t.next_date
             AND n.normalized_code = s.normalized_code
            JOIN stocks_snapshot m
              ON m.normalized_code = s.normalized_code
            WHERE t.nt_ratio_bucket_key IS NOT NULL
              AND t.topix_close_bucket_key IS NOT NULL
              AND t.next_date IS NOT NULL
        ),
        grouped_stock_days AS (
            {union_sql}
        )
    """
    return sql, [*params, *stock_event_params]


def _query_joint_day_counts(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> pd.DataFrame:
    cte_sql, params = _joint_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                COUNT(*) AS day_count
            FROM joint_event_days
            WHERE nt_ratio_bucket_key IS NOT NULL
              AND topix_close_bucket_key IS NOT NULL
              AND next_date IS NOT NULL
            GROUP BY nt_ratio_bucket_key, topix_close_bucket_key
            """,
            params,
        ).fetchdf(),
    )


def _query_summary(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
        selected_groups=selected_groups,
    )
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                stock_group,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                COUNT(*) AS sample_count,
                COUNT(*) FILTER (WHERE direction = 'up') AS up_count,
                COUNT(*) FILTER (WHERE direction = 'down') AS down_count,
                COUNT(*) FILTER (WHERE direction = 'flat') AS flat_count,
                AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS up_ratio,
                AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS down_ratio,
                AVG(CASE WHEN direction = 'flat' THEN 1.0 ELSE 0.0 END) AS flat_ratio,
                AVG(nt_ratio_return) AS mean_nt_ratio_return,
                AVG(topix_close_return) AS mean_topix_close_return,
                AVG(overnight_return) AS mean_overnight_return,
                AVG(overnight_diff) AS mean_overnight_diff,
                median(overnight_diff) AS median_overnight_diff,
                quantile_cont(overnight_diff, 0.05) AS p05_overnight_diff,
                quantile_cont(overnight_diff, 0.25) AS p25_overnight_diff,
                quantile_cont(overnight_diff, 0.50) AS p50_overnight_diff,
                quantile_cont(overnight_diff, 0.75) AS p75_overnight_diff,
                quantile_cont(overnight_diff, 0.95) AS p95_overnight_diff
            FROM grouped_stock_days
            GROUP BY stock_group, nt_ratio_bucket_key, topix_close_bucket_key
            """,
            params,
        ).fetchdf(),
    )


def _query_daily_group_returns(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
        selected_groups=selected_groups,
    )
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                stock_group,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                date,
                next_date,
                AVG(nt_ratio_return) AS nt_ratio_return,
                AVG(topix_close_return) AS topix_close_return,
                AVG(overnight_return) AS day_mean_overnight_return,
                AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS day_up_ratio,
                AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS day_down_ratio,
                COUNT(*) AS constituent_count
            FROM grouped_stock_days
            GROUP BY stock_group, nt_ratio_bucket_key, topix_close_bucket_key, date, next_date
            ORDER BY date, stock_group
            """,
            params,
        ).fetchdf(),
    )


def _query_samples(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
    sample_size: int,
) -> pd.DataFrame:
    if sample_size <= 0:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "nt_ratio_bucket_key",
                "topix_close_bucket_key",
                "date",
                "next_date",
                "code",
                "nt_ratio_return",
                "topix_close_return",
                "overnight_diff",
                "overnight_return",
                "direction",
                "sample_rank",
            ]
        )

    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
        selected_groups=selected_groups,
    )
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                stock_group,
                nt_ratio_bucket_key,
                topix_close_bucket_key,
                date,
                next_date,
                normalized_code AS code,
                nt_ratio_return,
                topix_close_return,
                overnight_diff,
                overnight_return,
                direction,
                sample_rank
            FROM (
                SELECT
                    stock_group,
                    nt_ratio_bucket_key,
                    topix_close_bucket_key,
                    date,
                    next_date,
                    normalized_code,
                    nt_ratio_return,
                    topix_close_return,
                    overnight_diff,
                    overnight_return,
                    direction,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_group, nt_ratio_bucket_key, topix_close_bucket_key
                        ORDER BY md5(
                            stock_group
                            || '|'
                            || normalized_code
                            || '|'
                            || date
                            || '|'
                            || next_date
                            || '|'
                            || nt_ratio_bucket_key
                            || '|'
                            || topix_close_bucket_key
                        )
                    ) AS sample_rank
                FROM grouped_stock_days
            ) ranked_samples
            WHERE sample_rank <= ?
            ORDER BY stock_group, nt_ratio_bucket_key, topix_close_bucket_key, sample_rank
            """,
            [*params, sample_size],
        ).fetchdf(),
    )


def _apply_joint_sample_clipping(
    samples_df: pd.DataFrame,
    *,
    clip_percentiles: tuple[float, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if samples_df.empty:
        empty_bounds = pd.DataFrame(
            columns=[
                "stock_group",
                "nt_ratio_bucket_key",
                "topix_close_bucket_key",
                "clip_lower",
                "clip_upper",
            ]
        )
        return samples_df.copy(), empty_bounds

    lower_pct, upper_pct = clip_percentiles
    bounds_df = (
        samples_df.groupby(
            ["stock_group", "nt_ratio_bucket_key", "topix_close_bucket_key"],
            as_index=False,
        )
        .agg(
            clip_lower=(
                "overnight_diff",
                lambda values: values.quantile(lower_pct / 100.0),
            ),
            clip_upper=(
                "overnight_diff",
                lambda values: values.quantile(upper_pct / 100.0),
            ),
        )
    )
    clipped = samples_df.merge(
        bounds_df,
        how="left",
        on=["stock_group", "nt_ratio_bucket_key", "topix_close_bucket_key"],
    )
    clipped = clipped[
        clipped["overnight_diff"].between(
            clipped["clip_lower"],
            clipped["clip_upper"],
            inclusive="both",
        )
    ].copy()
    return clipped, bounds_df


def _complete_joint_day_counts(
    day_counts_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.MultiIndex.from_product(
        [list(NT_RATIO_BUCKET_ORDER), list(CLOSE_BUCKET_ORDER)],
        names=["nt_ratio_bucket_key", "topix_close_bucket_key"],
    ).to_frame(index=False)
    merged = expected.merge(
        day_counts_df,
        how="left",
        on=["nt_ratio_bucket_key", "topix_close_bucket_key"],
    )
    merged["day_count"] = merged["day_count"].fillna(0).astype(int)
    merged["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
        merged["nt_ratio_bucket_key"],
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )
    merged["topix_close_bucket_label"] = _map_topix_bucket_labels(
        merged["topix_close_bucket_key"],
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    return merged


def _complete_summary_grid(
    summary_df: pd.DataFrame,
    *,
    selected_groups: Sequence[StockGroup],
    sigma_threshold_1: float,
    sigma_threshold_2: float,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.MultiIndex.from_product(
        [list(selected_groups), list(NT_RATIO_BUCKET_ORDER), list(CLOSE_BUCKET_ORDER)],
        names=["stock_group", "nt_ratio_bucket_key", "topix_close_bucket_key"],
    ).to_frame(index=False)
    merged = expected.merge(
        summary_df,
        how="left",
        on=["stock_group", "nt_ratio_bucket_key", "topix_close_bucket_key"],
    )

    count_columns = ["sample_count", "up_count", "down_count", "flat_count"]
    ratio_columns = [
        "up_ratio",
        "down_ratio",
        "flat_ratio",
        "mean_nt_ratio_return",
        "mean_topix_close_return",
        "mean_overnight_return",
        "mean_overnight_diff",
        "median_overnight_diff",
        "p05_overnight_diff",
        "p25_overnight_diff",
        "p50_overnight_diff",
        "p75_overnight_diff",
        "p95_overnight_diff",
    ]
    for column in count_columns:
        if column not in merged.columns:
            merged[column] = 0
        merged[column] = merged[column].fillna(0).astype(int)
    for column in ratio_columns:
        if column not in merged.columns:
            merged[column] = 0.0
        merged[column] = merged[column].astype(float)

    merged["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
        merged["nt_ratio_bucket_key"],
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )
    merged["topix_close_bucket_label"] = _map_topix_bucket_labels(
        merged["topix_close_bucket_key"],
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
    )
    return merged


def _validate_inputs(
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
    topix_close_threshold_1: float,
    topix_close_threshold_2: float,
    sample_size: int,
    clip_percentiles: tuple[float, float],
) -> None:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")
    if topix_close_threshold_1 <= 0:
        raise ValueError("topix_close_threshold_1 must be positive")
    if topix_close_threshold_2 <= topix_close_threshold_1:
        raise ValueError("topix_close_threshold_2 must be greater than topix_close_threshold_1")
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    lower, upper = clip_percentiles
    if lower < 0 or upper > 100 or lower >= upper:
        raise ValueError("clip_percentiles must satisfy 0 <= lower < upper <= 100")


def get_nt_ratio_change_topix_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    """Return the available joint NT ratio / TOPIX date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _nt_ratio_available_date_range(ctx.connection)


def run_nt_ratio_change_topix_close_stock_overnight_distribution(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
    topix_close_threshold_1: float = 0.01,
    topix_close_threshold_2: float = 0.02,
    selected_groups: Sequence[str] | None = None,
    sample_size: int = 2000,
    clip_percentiles: tuple[float, float] = (1.0, 99.0),
) -> NtRatioChangeTopixCloseStockOvernightDistributionResult:
    """Run the joint NT ratio change / TOPIX close-conditioned overnight analysis."""
    _validate_inputs(
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
        sample_size=sample_size,
        clip_percentiles=clip_percentiles,
    )
    validated_groups = _validate_selected_groups(selected_groups)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start, available_end = _nt_ratio_available_date_range(conn)
        nt_ratio_stats, _, _ = _query_nt_ratio_stats(
            conn,
            start_date=start_date,
            end_date=end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        analysis_start, analysis_end = _query_joint_analysis_date_range(
            conn,
            start_date=start_date,
            end_date=end_date,
            topix_close_threshold_1=topix_close_threshold_1,
            topix_close_threshold_2=topix_close_threshold_2,
        )
        (
            excluded_without_prev_nt_ratio,
            excluded_without_prev_topix_close,
            excluded_without_next_session,
        ) = _query_exclusions(
            conn,
            start_date=start_date,
            end_date=end_date,
            topix_close_threshold_1=topix_close_threshold_1,
            topix_close_threshold_2=topix_close_threshold_2,
        )

        if nt_ratio_stats is None or analysis_start is None:
            joint_day_counts_df = _complete_joint_day_counts(
                pd.DataFrame(
                    columns=["nt_ratio_bucket_key", "topix_close_bucket_key", "day_count"]
                ),
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            summary_df = _complete_summary_grid(
                pd.DataFrame(
                    columns=[
                        "stock_group",
                        "nt_ratio_bucket_key",
                        "topix_close_bucket_key",
                    ]
                ),
                selected_groups=validated_groups,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            samples_df = pd.DataFrame(
                columns=[
                    "stock_group",
                    "nt_ratio_bucket_key",
                    "topix_close_bucket_key",
                    "date",
                    "next_date",
                    "code",
                    "nt_ratio_return",
                    "topix_close_return",
                    "overnight_diff",
                    "overnight_return",
                    "direction",
                    "sample_rank",
                ]
            )
            clipped_samples_df, clip_bounds_df = _apply_joint_sample_clipping(
                samples_df,
                clip_percentiles=clip_percentiles,
            )
            daily_group_returns_df = pd.DataFrame(
                columns=[
                    "stock_group",
                    "nt_ratio_bucket_key",
                    "topix_close_bucket_key",
                    "date",
                    "next_date",
                    "nt_ratio_return",
                    "topix_close_return",
                    "day_mean_overnight_return",
                    "day_up_ratio",
                    "day_down_ratio",
                    "constituent_count",
                    "nt_ratio_bucket_label",
                    "topix_close_bucket_label",
                ]
            )
        else:
            joint_day_counts_df = _query_joint_day_counts(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            summary_df = _query_summary(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
                selected_groups=validated_groups,
            )
            daily_group_returns_df = _query_daily_group_returns(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
                selected_groups=validated_groups,
            )
            samples_df = _query_samples(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
                selected_groups=validated_groups,
                sample_size=sample_size,
            )
            joint_day_counts_df = _complete_joint_day_counts(
                joint_day_counts_df,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            summary_df = _complete_summary_grid(
                summary_df,
                selected_groups=validated_groups,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            samples_df = samples_df.copy()
            samples_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                samples_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            samples_df["topix_close_bucket_label"] = _map_topix_bucket_labels(
                samples_df["topix_close_bucket_key"],
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            clipped_samples_df, clip_bounds_df = _apply_joint_sample_clipping(
                samples_df,
                clip_percentiles=clip_percentiles,
            )
            clip_bounds_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                clip_bounds_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            clip_bounds_df["topix_close_bucket_label"] = _map_topix_bucket_labels(
                clip_bounds_df["topix_close_bucket_key"],
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
            daily_group_returns_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                daily_group_returns_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            daily_group_returns_df["topix_close_bucket_label"] = _map_topix_bucket_labels(
                daily_group_returns_df["topix_close_bucket_key"],
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )

    if nt_ratio_stats is None or analysis_start is None:
        clip_bounds_df = clip_bounds_df.copy()
        if "nt_ratio_bucket_key" in clip_bounds_df.columns:
            clip_bounds_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                clip_bounds_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
        if "topix_close_bucket_key" in clip_bounds_df.columns:
            clip_bounds_df["topix_close_bucket_label"] = _map_topix_bucket_labels(
                clip_bounds_df["topix_close_bucket_key"],
                topix_close_threshold_1=topix_close_threshold_1,
                topix_close_threshold_2=topix_close_threshold_2,
            )
        if "nt_ratio_bucket_label" not in samples_df.columns:
            samples_df["nt_ratio_bucket_label"] = pd.Series(dtype="object")
        if "topix_close_bucket_label" not in samples_df.columns:
            samples_df["topix_close_bucket_label"] = pd.Series(dtype="object")

    return NtRatioChangeTopixCloseStockOvernightDistributionResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start,
        available_end_date=available_end,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        topix_close_threshold_1=topix_close_threshold_1,
        topix_close_threshold_2=topix_close_threshold_2,
        nt_ratio_stats=nt_ratio_stats,
        selected_groups=validated_groups,
        excluded_nt_ratio_days_without_prev_ratio=excluded_without_prev_nt_ratio,
        excluded_topix_days_without_prev_close=excluded_without_prev_topix_close,
        excluded_joint_days_without_next_session=excluded_without_next_session,
        joint_day_counts_df=joint_day_counts_df,
        summary_df=summary_df,
        samples_df=samples_df,
        clipped_samples_df=clipped_samples_df,
        clip_bounds_df=clip_bounds_df,
        daily_group_returns_df=daily_group_returns_df,
    )
