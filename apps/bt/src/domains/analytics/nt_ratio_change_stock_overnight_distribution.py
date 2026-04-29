"""
NT ratio change-conditioned stock overnight return distribution analysis.

The market.duckdb file is the source of truth for both metadata and time-series
tables. This module performs read-only analytics and returns pandas DataFrames
for notebook visualization.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.deterministic_sampling import select_deterministic_samples
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    STOCK_GROUP_ORDER as _STOCK_GROUP_ORDER,
    StockGroup,
    _date_where_clause,
    _normalize_code_sql,
    _open_analysis_connection,
    _validate_selected_groups,
)

NtRatioBucketKey = Literal[
    "return_le_mean_minus_2sd",
    "return_mean_minus_2sd_to_minus_1sd",
    "return_mean_minus_1sd_to_plus_1sd",
    "return_mean_plus_1sd_to_plus_2sd",
    "return_ge_mean_plus_2sd",
]
SourceMode = Literal["live", "snapshot"]

NT_RATIO_BUCKET_ORDER: tuple[NtRatioBucketKey, ...] = (
    "return_le_mean_minus_2sd",
    "return_mean_minus_2sd_to_minus_1sd",
    "return_mean_minus_1sd_to_plus_1sd",
    "return_mean_plus_1sd_to_plus_2sd",
    "return_ge_mean_plus_2sd",
)
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = _STOCK_GROUP_ORDER
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/nt-ratio-change-stock-overnight-distribution"
)


@dataclass(frozen=True)
class NtRatioReturnStats:
    sample_count: int
    mean_return: float
    std_return: float
    sigma_threshold_1: float
    sigma_threshold_2: float
    lower_threshold_2: float
    lower_threshold_1: float
    upper_threshold_1: float
    upper_threshold_2: float
    min_return: float | None
    q25_return: float | None
    median_return: float | None
    q75_return: float | None
    max_return: float | None


@dataclass(frozen=True)
class NtRatioChangeStockOvernightDistributionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    sigma_threshold_1: float
    sigma_threshold_2: float
    nt_ratio_stats: NtRatioReturnStats | None
    selected_groups: tuple[StockGroup, ...]
    sample_size: int
    clip_percentiles: tuple[float, float]
    excluded_nt_ratio_days_without_prev_ratio: int
    excluded_nt_ratio_days_without_next_session: int
    day_counts_df: pd.DataFrame
    summary_df: pd.DataFrame
    samples_df: pd.DataFrame
    clipped_samples_df: pd.DataFrame
    clip_bounds_df: pd.DataFrame
    daily_group_returns_df: pd.DataFrame


def _serialize_nt_ratio_stats(
    stats: NtRatioReturnStats | None,
) -> dict[str, Any] | None:
    if stats is None:
        return None
    return {field.name: getattr(stats, field.name) for field in fields(NtRatioReturnStats)}


def _deserialize_nt_ratio_stats(
    payload: dict[str, Any] | None,
) -> NtRatioReturnStats | None:
    if payload is None:
        return None
    return NtRatioReturnStats(
        sample_count=int(payload["sample_count"]),
        mean_return=float(payload["mean_return"]),
        std_return=float(payload["std_return"]),
        sigma_threshold_1=float(payload["sigma_threshold_1"]),
        sigma_threshold_2=float(payload["sigma_threshold_2"]),
        lower_threshold_2=float(payload["lower_threshold_2"]),
        lower_threshold_1=float(payload["lower_threshold_1"]),
        upper_threshold_1=float(payload["upper_threshold_1"]),
        upper_threshold_2=float(payload["upper_threshold_2"]),
        min_return=cast(float | None, payload.get("min_return")),
        q25_return=cast(float | None, payload.get("q25_return")),
        median_return=cast(float | None, payload.get("median_return")),
        q75_return=cast(float | None, payload.get("q75_return")),
        max_return=cast(float | None, payload.get("max_return")),
    )


def _format_sigma(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def format_nt_ratio_bucket_label(
    bucket_key: NtRatioBucketKey,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> str:
    sigma_1 = _format_sigma(sigma_threshold_1)
    sigma_2 = _format_sigma(sigma_threshold_2)
    if bucket_key == "return_le_mean_minus_2sd":
        return f"return <= μ-{sigma_2}σ"
    if bucket_key == "return_mean_minus_2sd_to_minus_1sd":
        return f"μ-{sigma_2}σ < return <= μ-{sigma_1}σ"
    if bucket_key == "return_mean_minus_1sd_to_plus_1sd":
        return f"μ-{sigma_1}σ < return < μ+{sigma_1}σ"
    if bucket_key == "return_mean_plus_1sd_to_plus_2sd":
        return f"μ+{sigma_1}σ <= return < μ+{sigma_2}σ"
    return f"return >= μ+{sigma_2}σ"


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


def _nt_ratio_available_date_range(conn: Any) -> tuple[str | None, str | None]:
    row = conn.execute(
        """
        SELECT MIN(t.date) AS min_date, MAX(t.date) AS max_date
        FROM topix_data t
        JOIN indices_data n
          ON n.date = t.date
         AND n.code = ?
        WHERE t.close IS NOT NULL
          AND t.close > 0
          AND n.close IS NOT NULL
          AND n.close > 0
        """,
        (_NIKKEI_SYNTHETIC_INDEX_CODE,),
    ).fetchone()
    min_date = str(row[0]) if row and row[0] else None
    max_date = str(row[1]) if row and row[1] else None
    return min_date, max_date


def _nt_ratio_event_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats | None = None,
) -> tuple[str, list[Any]]:
    date_where_sql, date_params = _date_where_clause("date", start_date, end_date)
    params: list[Any] = [_NIKKEI_SYNTHETIC_INDEX_CODE]

    bucket_case_sql = "NULL AS nt_ratio_bucket_key"
    if nt_ratio_stats is not None:
        bucket_case_sql = (
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

    params.extend(date_params)
    sql = f"""
        WITH nt_ratio_ordered AS (
            SELECT
                t.date,
                LEAD(t.date) OVER (ORDER BY t.date) AS next_date,
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
        nt_ratio_event_base AS (
            SELECT
                date,
                next_date,
                nt_ratio,
                prev_nt_ratio,
                CASE
                    WHEN prev_nt_ratio IS NULL OR prev_nt_ratio = 0 THEN NULL
                    ELSE (nt_ratio - prev_nt_ratio) / prev_nt_ratio
                END AS nt_ratio_return
            FROM nt_ratio_ordered
        ),
        nt_ratio_event_days AS (
            SELECT
                date,
                next_date,
                nt_ratio,
                prev_nt_ratio,
                nt_ratio_return,
                {bucket_case_sql}
            FROM nt_ratio_event_base
            {date_where_sql}
        )
    """
    return sql, params


def _query_nt_ratio_stats(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[NtRatioReturnStats | None, str | None, str | None]:
    cte_sql, params = _nt_ratio_event_days_cte(start_date=start_date, end_date=end_date)
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT
            COUNT(*) AS sample_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            AVG(nt_ratio_return) AS mean_return,
            STDDEV_SAMP(nt_ratio_return) AS std_return,
            MIN(nt_ratio_return) AS min_return,
            quantile_cont(nt_ratio_return, 0.25) AS q25_return,
            median(nt_ratio_return) AS median_return,
            quantile_cont(nt_ratio_return, 0.75) AS q75_return,
            MAX(nt_ratio_return) AS max_return
        FROM nt_ratio_event_days
        WHERE nt_ratio_return IS NOT NULL
          AND next_date IS NOT NULL
        """,
        params,
    ).fetchone()
    sample_count = int(row[0] or 0) if row else 0
    analysis_start = str(row[1]) if row and row[1] else None
    analysis_end = str(row[2]) if row and row[2] else None
    if sample_count == 0:
        return None, analysis_start, analysis_end

    mean_return = float(row[3])
    std_return = float(row[4]) if row[4] is not None else 0.0
    if std_return <= 0:
        raise ValueError("nt_ratio_return standard deviation must be positive")

    stats = NtRatioReturnStats(
        sample_count=sample_count,
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        lower_threshold_2=mean_return - sigma_threshold_2 * std_return,
        lower_threshold_1=mean_return - sigma_threshold_1 * std_return,
        upper_threshold_1=mean_return + sigma_threshold_1 * std_return,
        upper_threshold_2=mean_return + sigma_threshold_2 * std_return,
        min_return=float(row[5]) if row[5] is not None else None,
        q25_return=float(row[6]) if row[6] is not None else None,
        median_return=float(row[7]) if row[7] is not None else None,
        q75_return=float(row[8]) if row[8] is not None else None,
        max_return=float(row[9]) if row[9] is not None else None,
    )
    return stats, analysis_start, analysis_end


def _query_nt_ratio_exclusions(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> tuple[int, int]:
    cte_sql, params = _nt_ratio_event_days_cte(start_date=start_date, end_date=end_date)
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT
            COUNT(*) FILTER (
                WHERE prev_nt_ratio IS NULL OR prev_nt_ratio = 0
            ) AS excluded_without_prev_ratio,
            COUNT(*) FILTER (
                WHERE nt_ratio_return IS NOT NULL
                  AND next_date IS NULL
            ) AS excluded_without_next_session
        FROM nt_ratio_event_days
        """,
        params,
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _grouped_stock_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
    selected_groups: Sequence[StockGroup],
) -> tuple[str, list[Any]]:
    cte_sql, params = _nt_ratio_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
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
                nt_ratio_return,
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
                nt_ratio_return,
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
                nt_ratio_return,
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
                nt_ratio_return,
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
                market_code_norm IN ('prime', '0111', '0101') AS is_prime,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70') AS is_topix100,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400') AS is_topix500,
                market_code_norm IN ('prime', '0111', '0101')
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
                s.normalized_code AS normalized_code,
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
                t.nt_ratio_return,
                m.is_prime,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500
            FROM nt_ratio_event_days t
            JOIN stock_event_close s ON s.date = t.date
            JOIN stock_next_open n
              ON n.next_date = t.next_date
             AND n.normalized_code = s.normalized_code
            JOIN stocks_snapshot m
              ON m.normalized_code = s.normalized_code
            WHERE t.nt_ratio_bucket_key IS NOT NULL
              AND t.next_date IS NOT NULL
        ),
        grouped_stock_days AS (
            {union_sql}
        )
    """
    return sql, [*params, *stock_event_params]


def _query_day_counts(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    nt_ratio_stats: NtRatioReturnStats,
) -> pd.DataFrame:
    cte_sql, params = _nt_ratio_event_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
    )
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT nt_ratio_bucket_key, COUNT(*) AS day_count
            FROM nt_ratio_event_days
            WHERE nt_ratio_bucket_key IS NOT NULL
              AND next_date IS NOT NULL
            GROUP BY nt_ratio_bucket_key
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
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
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
                COUNT(*) AS sample_count,
                COUNT(*) FILTER (WHERE direction = 'up') AS up_count,
                COUNT(*) FILTER (WHERE direction = 'down') AS down_count,
                COUNT(*) FILTER (WHERE direction = 'flat') AS flat_count,
                AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS up_ratio,
                AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS down_ratio,
                AVG(CASE WHEN direction = 'flat' THEN 1.0 ELSE 0.0 END) AS flat_ratio,
                AVG(nt_ratio_return) AS mean_nt_ratio_return,
                AVG(overnight_return) AS mean_overnight_return,
                AVG(overnight_diff) AS mean_overnight_diff,
                median(overnight_diff) AS median_overnight_diff,
                quantile_cont(overnight_diff, 0.05) AS p05_overnight_diff,
                quantile_cont(overnight_diff, 0.25) AS p25_overnight_diff,
                quantile_cont(overnight_diff, 0.50) AS p50_overnight_diff,
                quantile_cont(overnight_diff, 0.75) AS p75_overnight_diff,
                quantile_cont(overnight_diff, 0.95) AS p95_overnight_diff
            FROM grouped_stock_days
            GROUP BY stock_group, nt_ratio_bucket_key
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
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        nt_ratio_stats=nt_ratio_stats,
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
                date,
                next_date,
                AVG(nt_ratio_return) AS nt_ratio_return,
                AVG(overnight_return) AS day_mean_overnight_return,
                AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS day_up_ratio,
                AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS day_down_ratio,
                COUNT(*) AS constituent_count
            FROM grouped_stock_days
            GROUP BY stock_group, nt_ratio_bucket_key, date, next_date
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
    selected_groups: Sequence[StockGroup],
    sample_size: int,
) -> pd.DataFrame:
    if sample_size <= 0:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "nt_ratio_bucket_key",
                "date",
                "next_date",
                "code",
                "nt_ratio_return",
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
        selected_groups=selected_groups,
    )
    samples_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                stock_group,
                nt_ratio_bucket_key,
                date,
                next_date,
                normalized_code AS code,
                nt_ratio_return,
                overnight_diff,
                overnight_return,
                direction
            FROM grouped_stock_days
            """,
            params,
        ).fetchdf(),
    )
    return select_deterministic_samples(
        samples_df,
        sample_size=sample_size,
        partition_columns=["stock_group", "nt_ratio_bucket_key"],
        hash_columns=["stock_group", "code", "date", "next_date"],
        final_order_columns=["stock_group", "nt_ratio_bucket_key", "sample_rank"],
    )


def _apply_nt_ratio_sample_clipping(
    samples_df: pd.DataFrame,
    *,
    clip_percentiles: tuple[float, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if samples_df.empty:
        empty_bounds = pd.DataFrame(
            columns=["stock_group", "nt_ratio_bucket_key", "clip_lower", "clip_upper"]
        )
        return samples_df.copy(), empty_bounds

    lower_pct, upper_pct = clip_percentiles
    bounds_df = (
        samples_df.groupby(["stock_group", "nt_ratio_bucket_key"], as_index=False)
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
        on=["stock_group", "nt_ratio_bucket_key"],
    )
    clipped = clipped[
        clipped["overnight_diff"].between(
            clipped["clip_lower"],
            clipped["clip_upper"],
            inclusive="both",
        )
    ].copy()
    return clipped, bounds_df


def _complete_day_counts(
    day_counts_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.DataFrame({"nt_ratio_bucket_key": list(NT_RATIO_BUCKET_ORDER)})
    merged = expected.merge(day_counts_df, how="left", on="nt_ratio_bucket_key")
    merged["day_count"] = merged["day_count"].fillna(0).astype(int)
    merged["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
        merged["nt_ratio_bucket_key"],
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )
    return merged


def _complete_summary_grid(
    summary_df: pd.DataFrame,
    *,
    selected_groups: Sequence[StockGroup],
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.MultiIndex.from_product(
        [list(selected_groups), list(NT_RATIO_BUCKET_ORDER)],
        names=["stock_group", "nt_ratio_bucket_key"],
    ).to_frame(index=False)
    merged = expected.merge(
        summary_df,
        how="left",
        on=["stock_group", "nt_ratio_bucket_key"],
    )

    count_columns = ["sample_count", "up_count", "down_count", "flat_count"]
    ratio_columns = [
        "up_ratio",
        "down_ratio",
        "flat_ratio",
        "mean_nt_ratio_return",
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
    return merged


def _validate_inputs(
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
    sample_size: int,
    clip_percentiles: tuple[float, float],
) -> None:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    lower, upper = clip_percentiles
    if lower < 0 or upper > 100 or lower >= upper:
        raise ValueError("clip_percentiles must satisfy 0 <= lower < upper <= 100")


def get_nt_ratio_available_date_range(db_path: str) -> tuple[str | None, str | None]:
    """Return the available NT ratio date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _nt_ratio_available_date_range(ctx.connection)


def run_nt_ratio_change_stock_overnight_distribution(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
    selected_groups: Sequence[str] | None = None,
    sample_size: int = 2000,
    clip_percentiles: tuple[float, float] = (1.0, 99.0),
) -> NtRatioChangeStockOvernightDistributionResult:
    """Run the NT ratio change-conditioned stock overnight analysis from market.duckdb."""
    _validate_inputs(
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        sample_size=sample_size,
        clip_percentiles=clip_percentiles,
    )
    validated_groups = _validate_selected_groups(selected_groups)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start, available_end = _nt_ratio_available_date_range(conn)
        nt_ratio_stats, analysis_start, analysis_end = _query_nt_ratio_stats(
            conn,
            start_date=start_date,
            end_date=end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        excluded_without_prev_ratio, excluded_without_next_session = _query_nt_ratio_exclusions(
            conn,
            start_date=start_date,
            end_date=end_date,
        )

        if nt_ratio_stats is None:
            day_counts_df = _complete_day_counts(
                pd.DataFrame(columns=["nt_ratio_bucket_key", "day_count"]),
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            summary_df = _complete_summary_grid(
                pd.DataFrame(columns=["stock_group", "nt_ratio_bucket_key"]),
                selected_groups=validated_groups,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            samples_df = pd.DataFrame(
                columns=[
                    "stock_group",
                    "nt_ratio_bucket_key",
                    "date",
                    "next_date",
                    "code",
                    "nt_ratio_return",
                    "overnight_diff",
                    "overnight_return",
                    "direction",
                    "sample_rank",
                ]
            )
            clipped_samples_df, clip_bounds_df = _apply_nt_ratio_sample_clipping(
                samples_df,
                clip_percentiles=clip_percentiles,
            )
            daily_group_returns_df = pd.DataFrame(
                columns=[
                    "stock_group",
                    "nt_ratio_bucket_key",
                    "date",
                    "next_date",
                    "nt_ratio_return",
                    "day_mean_overnight_return",
                    "day_up_ratio",
                    "day_down_ratio",
                    "constituent_count",
                    "nt_ratio_bucket_label",
                ]
            )
        else:
            day_counts_df = _query_day_counts(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
            )
            summary_df = _query_summary(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                selected_groups=validated_groups,
            )
            daily_group_returns_df = _query_daily_group_returns(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                selected_groups=validated_groups,
            )
            samples_df = _query_samples(
                conn,
                start_date=start_date,
                end_date=end_date,
                nt_ratio_stats=nt_ratio_stats,
                selected_groups=validated_groups,
                sample_size=sample_size,
            )
            day_counts_df = _complete_day_counts(
                day_counts_df,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            summary_df = _complete_summary_grid(
                summary_df,
                selected_groups=validated_groups,
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            samples_df = samples_df.copy()
            samples_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                samples_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            clipped_samples_df, clip_bounds_df = _apply_nt_ratio_sample_clipping(
                samples_df,
                clip_percentiles=clip_percentiles,
            )
            clip_bounds_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                clip_bounds_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
            daily_group_returns_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                daily_group_returns_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )

    if nt_ratio_stats is None:
        clip_bounds_df = clip_bounds_df.copy()
        if "nt_ratio_bucket_key" in clip_bounds_df.columns:
            clip_bounds_df["nt_ratio_bucket_label"] = _map_nt_ratio_bucket_labels(
                clip_bounds_df["nt_ratio_bucket_key"],
                sigma_threshold_1=sigma_threshold_1,
                sigma_threshold_2=sigma_threshold_2,
            )
        if "nt_ratio_bucket_label" not in samples_df.columns:
            samples_df["nt_ratio_bucket_label"] = pd.Series(dtype="object")

    return NtRatioChangeStockOvernightDistributionResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start,
        available_end_date=available_end,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        nt_ratio_stats=nt_ratio_stats,
        selected_groups=validated_groups,
        sample_size=sample_size,
        clip_percentiles=clip_percentiles,
        excluded_nt_ratio_days_without_prev_ratio=excluded_without_prev_ratio,
        excluded_nt_ratio_days_without_next_session=excluded_without_next_session,
        day_counts_df=day_counts_df,
        summary_df=summary_df,
        samples_df=samples_df,
        clipped_samples_df=clipped_samples_df,
        clip_bounds_df=clip_bounds_df,
        daily_group_returns_df=daily_group_returns_df,
    )


def write_nt_ratio_change_stock_overnight_distribution_research_bundle(
    result: NtRatioChangeStockOvernightDistributionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_nt_ratio_change_stock_overnight_distribution",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "sigma_threshold_1": result.sigma_threshold_1,
            "sigma_threshold_2": result.sigma_threshold_2,
            "selected_groups": list(result.selected_groups),
            "sample_size": result.sample_size,
            "clip_percentiles": list(result.clip_percentiles),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_nt_ratio_change_stock_overnight_distribution_research_bundle(
    bundle_path: str | Path,
) -> NtRatioChangeStockOvernightDistributionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_result_payload(
    result: NtRatioChangeStockOvernightDistributionResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata = {
        "db_path": result.db_path,
        "source_mode": result.source_mode,
        "source_detail": result.source_detail,
        "available_start_date": result.available_start_date,
        "available_end_date": result.available_end_date,
        "analysis_start_date": result.analysis_start_date,
        "analysis_end_date": result.analysis_end_date,
        "sigma_threshold_1": result.sigma_threshold_1,
        "sigma_threshold_2": result.sigma_threshold_2,
        "nt_ratio_stats": _serialize_nt_ratio_stats(result.nt_ratio_stats),
        "selected_groups": list(result.selected_groups),
        "sample_size": result.sample_size,
        "clip_percentiles": list(result.clip_percentiles),
        "excluded_nt_ratio_days_without_prev_ratio": result.excluded_nt_ratio_days_without_prev_ratio,
        "excluded_nt_ratio_days_without_next_session": result.excluded_nt_ratio_days_without_next_session,
    }
    tables = {
        "day_counts_df": result.day_counts_df,
        "summary_df": result.summary_df,
        "samples_df": result.samples_df,
        "clipped_samples_df": result.clipped_samples_df,
        "clip_bounds_df": result.clip_bounds_df,
        "daily_group_returns_df": result.daily_group_returns_df,
    }
    return metadata, tables


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> NtRatioChangeStockOvernightDistributionResult:
    return NtRatioChangeStockOvernightDistributionResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        sigma_threshold_1=float(metadata["sigma_threshold_1"]),
        sigma_threshold_2=float(metadata["sigma_threshold_2"]),
        nt_ratio_stats=_deserialize_nt_ratio_stats(
            cast(dict[str, Any] | None, metadata.get("nt_ratio_stats"))
        ),
        selected_groups=cast(
            tuple[StockGroup, ...],
            tuple(str(value) for value in metadata["selected_groups"]),
        ),
        sample_size=int(metadata["sample_size"]),
        clip_percentiles=(
            float(cast(list[Any], metadata["clip_percentiles"])[0]),
            float(cast(list[Any], metadata["clip_percentiles"])[1]),
        ),
        excluded_nt_ratio_days_without_prev_ratio=int(
            metadata["excluded_nt_ratio_days_without_prev_ratio"]
        ),
        excluded_nt_ratio_days_without_next_session=int(
            metadata["excluded_nt_ratio_days_without_next_session"]
        ),
        day_counts_df=tables["day_counts_df"],
        summary_df=tables["summary_df"],
        samples_df=tables["samples_df"],
        clipped_samples_df=tables["clipped_samples_df"],
        clip_bounds_df=tables["clip_bounds_df"],
        daily_group_returns_df=tables["daily_group_returns_df"],
    )


def _build_research_bundle_summary_markdown(
    result: NtRatioChangeStockOvernightDistributionResult,
) -> str:
    summary_lines = [
        "# NT Ratio Change / Stock Overnight Distribution",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Sigma thresholds: `{result.sigma_threshold_1:g}σ / {result.sigma_threshold_2:g}σ`",
        f"- Selected groups: `{', '.join(result.selected_groups)}`",
        f"- Excluded NT days without previous ratio: `{result.excluded_nt_ratio_days_without_prev_ratio}`",
        f"- Excluded NT days without next session: `{result.excluded_nt_ratio_days_without_next_session}`",
        "",
        "## Current Read",
        "",
    ]
    stats = result.nt_ratio_stats
    if stats is None:
        summary_lines.append("- No analyzable NT ratio rows were available in this run.")
    else:
        strongest = result.summary_df[
            result.summary_df["mean_overnight_return"].notna()
        ].copy()
        summary_lines.extend(
            [
                f"- NT ratio sample count: `{stats.sample_count}`",
                f"- Mean / std: `{stats.mean_return * 100:+.4f}% / {stats.std_return * 100:.4f}%`",
            ]
        )
        if strongest.empty:
            summary_lines.append("- Group summary was empty after filtering.")
        else:
            strongest_row = strongest.sort_values(
                "mean_overnight_return",
                ascending=False,
            ).iloc[0]
            summary_lines.append(
                "- Highest mean overnight return bucket was "
                f"`{strongest_row['stock_group']}` x "
                f"`{strongest_row['nt_ratio_bucket_label']}` at "
                f"`{float(strongest_row['mean_overnight_return']) * 100:+.4f}%`."
            )
    summary_lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)
