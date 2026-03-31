"""
TOPIX close-conditioned stock overnight return distribution analysis.

The market.duckdb file is the source of truth for both metadata and time-series
tables. This module performs read-only analytics and returns pandas DataFrames
for notebook visualization.
"""

from __future__ import annotations

import importlib
import shutil
import tempfile
from collections.abc import Sequence
from contextlib import contextmanager
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

CloseBucketKey = Literal[
    "close_le_negative_threshold_2",
    "close_negative_threshold_2_to_1",
    "close_negative_threshold_1_to_threshold_1",
    "close_threshold_1_to_2",
    "close_ge_threshold_2",
]
StockGroup = Literal[
    "PRIME",
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
]
SourceMode = Literal["live", "snapshot"]

CLOSE_BUCKET_ORDER: tuple[CloseBucketKey, ...] = (
    "close_le_negative_threshold_2",
    "close_negative_threshold_2_to_1",
    "close_negative_threshold_1_to_threshold_1",
    "close_threshold_1_to_2",
    "close_ge_threshold_2",
)
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = (
    "PRIME",
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
)

_LOCK_ERROR_PATTERNS: tuple[str, ...] = (
    "conflicting lock is held",
    "could not set lock",
)
TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix-close-stock-overnight-distribution"
)


@dataclass(frozen=True)
class TopixCloseStockOvernightDistributionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    close_threshold_1: float
    close_threshold_2: float
    close_return_stats: TopixCloseReturnStats | None
    selected_groups: tuple[StockGroup, ...]
    sample_size: int
    clip_percentiles: tuple[float, float]
    excluded_topix_days_without_prev_close: int
    excluded_topix_days_without_next_session: int
    day_counts_df: pd.DataFrame
    summary_df: pd.DataFrame
    samples_df: pd.DataFrame
    clipped_samples_df: pd.DataFrame
    clip_bounds_df: pd.DataFrame
    daily_group_returns_df: pd.DataFrame


@dataclass(frozen=True)
class TopixCloseReturnStats:
    sample_count: int
    mean_return: float
    std_return: float
    sigma_threshold_1: float
    sigma_threshold_2: float
    threshold_1: float
    threshold_2: float
    min_return: float | None
    q25_return: float | None
    median_return: float | None
    q75_return: float | None
    max_return: float | None


def _serialize_topix_close_return_stats(
    stats: TopixCloseReturnStats | None,
) -> dict[str, Any] | None:
    if stats is None:
        return None
    return {
        field.name: getattr(stats, field.name) for field in fields(TopixCloseReturnStats)
    }


def _deserialize_topix_close_return_stats(
    payload: dict[str, Any] | None,
) -> TopixCloseReturnStats | None:
    if payload is None:
        return None
    return TopixCloseReturnStats(
        sample_count=int(payload["sample_count"]),
        mean_return=float(payload["mean_return"]),
        std_return=float(payload["std_return"]),
        sigma_threshold_1=float(payload["sigma_threshold_1"]),
        sigma_threshold_2=float(payload["sigma_threshold_2"]),
        threshold_1=float(payload["threshold_1"]),
        threshold_2=float(payload["threshold_2"]),
        min_return=cast(float | None, payload.get("min_return")),
        q25_return=cast(float | None, payload.get("q25_return")),
        median_return=cast(float | None, payload.get("median_return")),
        q75_return=cast(float | None, payload.get("q75_return")),
        max_return=cast(float | None, payload.get("max_return")),
    )


@dataclass(frozen=True)
class _ConnectionContext:
    connection: Any
    source_mode: SourceMode
    source_detail: str


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    duckdb = importlib.import_module("duckdb")
    return duckdb.connect(db_path, read_only=read_only)


def _is_lock_error(error: Exception) -> bool:
    message = str(error).lower()
    return any(pattern in message for pattern in _LOCK_ERROR_PATTERNS)


@contextmanager
def _open_analysis_connection(db_path: str):
    conn: Any | None = None
    tmpdir: tempfile.TemporaryDirectory[str] | None = None
    try:
        try:
            conn = _connect_duckdb(db_path, read_only=True)
            yield _ConnectionContext(
                connection=conn,
                source_mode="live",
                source_detail=f"live DuckDB: {db_path}",
            )
            return
        except Exception as exc:
            if not _is_lock_error(exc):
                raise

        tmpdir = tempfile.TemporaryDirectory(
            prefix="topix-close-stock-overnight-",
            dir="/tmp",
        )
        snapshot_dir = Path(tmpdir.name)
        db_path_obj = Path(db_path)
        snapshot_path = snapshot_dir / db_path_obj.name
        shutil.copy2(db_path_obj, snapshot_path)

        wal_path = Path(f"{db_path}.wal")
        if wal_path.exists():
            shutil.copy2(wal_path, Path(f"{snapshot_path}.wal"))

        conn = _connect_duckdb(str(snapshot_path), read_only=True)
        yield _ConnectionContext(
            connection=conn,
            source_mode="snapshot",
            source_detail=f"temporary snapshot copied from {db_path}",
        )
    finally:
        if conn is not None:
            conn.close()
        if tmpdir is not None:
            tmpdir.cleanup()


def _date_where_clause(
    column_name: str,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list[str]]:
    conditions: list[str] = []
    params: list[str] = []
    if start_date:
        conditions.append(f"{column_name} >= ?")
        params.append(start_date)
    if end_date:
        conditions.append(f"{column_name} <= ?")
        params.append(end_date)
    if not conditions:
        return "", []
    return " WHERE " + " AND ".join(conditions), params


def _fetch_date_range(
    conn: Any,
    *,
    table_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str | None, str | None]:
    where_sql, params = _date_where_clause("date", start_date, end_date)
    row = conn.execute(
        f"SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {table_name}{where_sql}",
        params,
    ).fetchone()
    min_date = str(row[0]) if row and row[0] else None
    max_date = str(row[1]) if row and row[1] else None
    return min_date, max_date


def _format_threshold(value: float) -> str:
    percent = value * 100.0
    if percent.is_integer():
        return f"{int(percent)}%"
    formatted = f"{percent:.2f}".rstrip("0").rstrip(".")
    return f"{formatted}%"


def format_close_bucket_label(
    bucket_key: CloseBucketKey,
    *,
    close_threshold_1: float,
    close_threshold_2: float,
) -> str:
    threshold_1 = _format_threshold(close_threshold_1)
    threshold_2 = _format_threshold(close_threshold_2)
    if bucket_key == "close_le_negative_threshold_2":
        return f"close <= -{threshold_2}"
    if bucket_key == "close_negative_threshold_2_to_1":
        return f"-{threshold_2} < close <= -{threshold_1}"
    if bucket_key == "close_negative_threshold_1_to_threshold_1":
        return f"-{threshold_1} < close < {threshold_1}"
    if bucket_key == "close_threshold_1_to_2":
        return f"{threshold_1} <= close < {threshold_2}"
    return f"close >= {threshold_2}"


def _map_close_bucket_labels(
    bucket_keys: pd.Series,
    *,
    close_threshold_1: float,
    close_threshold_2: float,
) -> pd.Series:
    return bucket_keys.map(
        lambda value: format_close_bucket_label(
            cast(CloseBucketKey, value),
            close_threshold_1=close_threshold_1,
            close_threshold_2=close_threshold_2,
        )
    )


def _close_return_sql() -> str:
    return "(close - prev_close) / prev_close"


def _query_topix_close_return_stats(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[TopixCloseReturnStats | None, str | None, str | None]:
    topix_where_sql, topix_params = _date_where_clause("date", start_date, end_date)
    close_return_sql = _close_return_sql()
    row = conn.execute(
        f"""
        WITH topix_event_days AS (
            SELECT
                date,
                next_date,
                CASE
                    WHEN close IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE {close_return_sql}
                END AS topix_close_return
            FROM (
                SELECT
                    date,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close,
                    LEAD(date) OVER (ORDER BY date) AS next_date
                FROM topix_data
            ) ordered_topix
            {topix_where_sql}
        )
        SELECT
            COUNT(*) AS sample_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            AVG(topix_close_return) AS mean_return,
            STDDEV_SAMP(topix_close_return) AS std_return,
            MIN(topix_close_return) AS min_return,
            quantile_cont(topix_close_return, 0.25) AS q25_return,
            median(topix_close_return) AS median_return,
            quantile_cont(topix_close_return, 0.75) AS q75_return,
            MAX(topix_close_return) AS max_return
        FROM topix_event_days
        WHERE topix_close_return IS NOT NULL
          AND next_date IS NOT NULL
        """,
        topix_params,
    ).fetchone()
    sample_count = int(row[0] or 0) if row else 0
    analysis_start = str(row[1]) if row and row[1] else None
    analysis_end = str(row[2]) if row and row[2] else None
    if sample_count == 0:
        return None, analysis_start, analysis_end

    mean_return = float(row[3])
    std_return = float(row[4]) if row[4] is not None else 0.0
    if std_return <= 0:
        raise ValueError("topix_close_return standard deviation must be positive")

    stats = TopixCloseReturnStats(
        sample_count=sample_count,
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        threshold_1=sigma_threshold_1 * std_return,
        threshold_2=sigma_threshold_2 * std_return,
        min_return=float(row[5]) if row[5] is not None else None,
        q25_return=float(row[6]) if row[6] is not None else None,
        median_return=float(row[7]) if row[7] is not None else None,
        q75_return=float(row[8]) if row[8] is not None else None,
        max_return=float(row[9]) if row[9] is not None else None,
    )
    return stats, analysis_start, analysis_end


def _close_bucket_case_sql() -> str:
    close_return_sql = _close_return_sql()
    return (
        "CASE "
        "WHEN close IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL "
        f"WHEN {close_return_sql} <= -? THEN 'close_le_negative_threshold_2' "
        f"WHEN {close_return_sql} <= -? THEN 'close_negative_threshold_2_to_1' "
        f"WHEN {close_return_sql} < ? THEN 'close_negative_threshold_1_to_threshold_1' "
        f"WHEN {close_return_sql} < ? THEN 'close_threshold_1_to_2' "
        "ELSE 'close_ge_threshold_2' "
        "END"
    )


def _normalize_code_sql(column_name: str) -> str:
    return (
        "CASE "
        f"WHEN length({column_name}) IN (5, 6) AND right({column_name}, 1) = '0' "
        f"THEN left({column_name}, length({column_name}) - 1) "
        f"ELSE {column_name} "
        "END"
    )


def _grouped_stock_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> tuple[str, list[Any]]:
    topix_where_sql, topix_params = _date_where_clause("date", start_date, end_date)
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
                close_bucket_key,
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
                close_bucket_key,
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
                close_bucket_key,
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
                close_bucket_key,
                topix_close_return,
                'PRIME ex TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_prime_ex_topix500
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)
    close_return_sql = _close_return_sql()
    close_bucket_case_sql = _close_bucket_case_sql()

    sql = f"""
        WITH topix_event_days AS (
            SELECT
                date,
                next_date,
                close,
                prev_close,
                {close_bucket_case_sql} AS close_bucket_key,
                CASE
                    WHEN close IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE {close_return_sql}
                END AS topix_close_return
            FROM (
                SELECT
                    date,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close,
                    LEAD(date) OVER (ORDER BY date) AS next_date
                FROM topix_data
            ) ordered_topix
            {topix_where_sql}
        ),
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
                t.close_bucket_key,
                t.topix_close_return,
                m.is_prime,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500
            FROM topix_event_days t
            JOIN stock_event_close s ON s.date = t.date
            JOIN stock_next_open n
                ON n.next_date = t.next_date
                AND n.normalized_code = s.normalized_code
            JOIN stocks_snapshot m
                ON m.normalized_code = s.normalized_code
            WHERE t.close_bucket_key IS NOT NULL
              AND t.next_date IS NOT NULL
        ),
        grouped_stock_days AS (
            {union_sql}
        )
    """
    return sql, [
        close_threshold_2,
        close_threshold_1,
        close_threshold_1,
        close_threshold_2,
        *topix_params,
        *stock_event_params,
    ]


def _query_day_counts(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
) -> tuple[pd.DataFrame, int, int]:
    close_bucket_case_sql = _close_bucket_case_sql()
    topix_where_sql, topix_params = _date_where_clause("date", start_date, end_date)
    sql = f"""
        WITH topix_event_days AS (
            SELECT
                date,
                next_date,
                {close_bucket_case_sql} AS close_bucket_key
            FROM (
                SELECT
                    date,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close,
                    LEAD(date) OVER (ORDER BY date) AS next_date
                FROM topix_data
            ) ordered_topix
            {topix_where_sql}
        )
        SELECT close_bucket_key, COUNT(*) AS day_count
        FROM topix_event_days
        WHERE close_bucket_key IS NOT NULL
          AND next_date IS NOT NULL
        GROUP BY close_bucket_key
    """
    params = [
        close_threshold_2,
        close_threshold_1,
        close_threshold_1,
        close_threshold_2,
        *topix_params,
    ]
    day_counts_df = cast(pd.DataFrame, conn.execute(sql, params).fetchdf())

    excluded_where_sql, excluded_params = _date_where_clause("date", start_date, end_date)
    excluded_row = conn.execute(
        f"""
        SELECT
            COUNT(*) FILTER (
                WHERE close IS NULL OR prev_close IS NULL OR prev_close = 0
            ) AS excluded_without_prev_close,
            COUNT(*) FILTER (
                WHERE close IS NOT NULL
                  AND prev_close IS NOT NULL
                  AND prev_close <> 0
                  AND next_date IS NULL
            ) AS excluded_without_next_session
        FROM (
            SELECT
                date,
                close,
                LAG(close) OVER (ORDER BY date) AS prev_close,
                LEAD(date) OVER (ORDER BY date) AS next_date
            FROM topix_data
        ) ordered_topix
        {excluded_where_sql}
        """,
        excluded_params,
    ).fetchone()
    excluded_without_prev_close = int(excluded_row[0] or 0) if excluded_row else 0
    excluded_without_next_session = int(excluded_row[1] or 0) if excluded_row else 0
    return day_counts_df, excluded_without_prev_close, excluded_without_next_session


def _query_summary(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            close_bucket_key,
            COUNT(*) AS sample_count,
            COUNT(*) FILTER (WHERE direction = 'up') AS up_count,
            COUNT(*) FILTER (WHERE direction = 'down') AS down_count,
            COUNT(*) FILTER (WHERE direction = 'flat') AS flat_count,
            AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS up_ratio,
            AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS down_ratio,
            AVG(CASE WHEN direction = 'flat' THEN 1.0 ELSE 0.0 END) AS flat_ratio,
            AVG(overnight_return) AS mean_overnight_return,
            AVG(overnight_diff) AS mean_overnight_diff,
            median(overnight_diff) AS median_overnight_diff,
            quantile_cont(overnight_diff, 0.05) AS p05_overnight_diff,
            quantile_cont(overnight_diff, 0.25) AS p25_overnight_diff,
            quantile_cont(overnight_diff, 0.50) AS p50_overnight_diff,
            quantile_cont(overnight_diff, 0.75) AS p75_overnight_diff,
            quantile_cont(overnight_diff, 0.95) AS p95_overnight_diff
        FROM grouped_stock_days
        GROUP BY stock_group, close_bucket_key
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_daily_group_returns(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            close_bucket_key,
            date,
            next_date,
            AVG(topix_close_return) AS topix_close_return,
            AVG(overnight_return) AS day_mean_overnight_return,
            AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS day_up_ratio,
            AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS day_down_ratio,
            COUNT(*) AS constituent_count
        FROM grouped_stock_days
        GROUP BY stock_group, close_bucket_key, date, next_date
        ORDER BY date, stock_group
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_analysis_date_range(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> tuple[str | None, str | None]:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        selected_groups=selected_groups,
    )
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM grouped_stock_days
        """,
        params,
    ).fetchone()
    min_date = str(row[0]) if row and row[0] else None
    max_date = str(row[1]) if row and row[1] else None
    return min_date, max_date


def _query_samples(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    close_threshold_1: float,
    close_threshold_2: float,
    selected_groups: Sequence[StockGroup],
    sample_size: int,
) -> pd.DataFrame:
    if sample_size <= 0:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "close_bucket_key",
                "date",
                "next_date",
                "code",
                "overnight_diff",
                "overnight_return",
                "direction",
                "sample_rank",
            ]
        )

    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            close_bucket_key,
            date,
            next_date,
            normalized_code AS code,
            overnight_diff,
            overnight_return,
            direction
        FROM grouped_stock_days
    """
    samples_df = cast(pd.DataFrame, conn.execute(sql, params).fetchdf())
    return select_deterministic_samples(
        samples_df,
        sample_size=sample_size,
        partition_columns=["stock_group", "close_bucket_key"],
        hash_columns=["stock_group", "code", "date", "next_date"],
        final_order_columns=["stock_group", "close_bucket_key", "sample_rank"],
    )


def _complete_day_counts(
    day_counts_df: pd.DataFrame,
    *,
    close_threshold_1: float,
    close_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.DataFrame({"close_bucket_key": list(CLOSE_BUCKET_ORDER)})
    merged = expected.merge(day_counts_df, how="left", on="close_bucket_key")
    merged["day_count"] = merged["day_count"].fillna(0).astype(int)
    merged["close_bucket_label"] = _map_close_bucket_labels(
        merged["close_bucket_key"],
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    return merged


def _complete_summary_grid(
    summary_df: pd.DataFrame,
    *,
    selected_groups: Sequence[StockGroup],
    close_threshold_1: float,
    close_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.MultiIndex.from_product(
        [list(selected_groups), list(CLOSE_BUCKET_ORDER)],
        names=["stock_group", "close_bucket_key"],
    ).to_frame(index=False)
    merged = expected.merge(
        summary_df,
        how="left",
        on=["stock_group", "close_bucket_key"],
    )

    count_columns = ["sample_count", "up_count", "down_count", "flat_count"]
    ratio_columns = ["up_ratio", "down_ratio", "flat_ratio"]
    for column in count_columns:
        merged[column] = merged[column].fillna(0).astype(int)
    for column in ratio_columns:
        merged[column] = merged[column].fillna(0.0).astype(float)

    merged["close_bucket_label"] = _map_close_bucket_labels(
        merged["close_bucket_key"],
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    return merged


def _apply_sample_clipping(
    samples_df: pd.DataFrame,
    *,
    clip_percentiles: tuple[float, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if samples_df.empty:
        empty_bounds = pd.DataFrame(
            columns=["stock_group", "close_bucket_key", "clip_lower", "clip_upper"]
        )
        return samples_df.copy(), empty_bounds

    lower_pct, upper_pct = clip_percentiles
    bounds_df = (
        samples_df.groupby(["stock_group", "close_bucket_key"], as_index=False)
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
        on=["stock_group", "close_bucket_key"],
    )
    clipped = clipped[
        clipped["overnight_diff"].between(
            clipped["clip_lower"],
            clipped["clip_upper"],
            inclusive="both",
        )
    ].copy()
    return clipped, bounds_df


def _validate_selected_groups(selected_groups: Sequence[str] | None) -> tuple[StockGroup, ...]:
    if selected_groups is None:
        return STOCK_GROUP_ORDER
    validated: list[StockGroup] = []
    seen: set[str] = set()
    allowed = set(STOCK_GROUP_ORDER)
    for group in selected_groups:
        if group not in allowed:
            raise ValueError(f"Unsupported stock group: {group}")
        if group in seen:
            continue
        validated.append(cast(StockGroup, group))
        seen.add(group)
    if not validated:
        raise ValueError("selected_groups must contain at least one supported group")
    return tuple(validated)


def _validate_inputs(
    *,
    close_threshold_1: float,
    close_threshold_2: float,
    sample_size: int,
    clip_percentiles: tuple[float, float],
) -> None:
    if close_threshold_1 <= 0:
        raise ValueError("close_threshold_1 must be positive")
    if close_threshold_2 <= close_threshold_1:
        raise ValueError("close_threshold_2 must be greater than close_threshold_1")
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    lower, upper = clip_percentiles
    if lower < 0 or upper > 100 or lower >= upper:
        raise ValueError("clip_percentiles must satisfy 0 <= lower < upper <= 100")


def get_topix_available_date_range(db_path: str) -> tuple[str | None, str | None]:
    """Return the available TOPIX date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def get_topix_close_return_stats(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
) -> TopixCloseReturnStats | None:
    """Return TOPIX close-return distribution stats for sigma-derived bucketing."""
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")
    with _open_analysis_connection(db_path) as ctx:
        stats, _, _ = _query_topix_close_return_stats(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        return stats


def run_topix_close_stock_overnight_distribution(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    close_threshold_1: float = 0.01,
    close_threshold_2: float = 0.02,
    close_return_stats: TopixCloseReturnStats | None = None,
    selected_groups: Sequence[str] | None = None,
    sample_size: int = 2000,
    clip_percentiles: tuple[float, float] = (1.0, 99.0),
) -> TopixCloseStockOvernightDistributionResult:
    """Run the TOPIX close-conditioned stock overnight analysis from market.duckdb."""
    _validate_inputs(
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        sample_size=sample_size,
        clip_percentiles=clip_percentiles,
    )
    validated_groups = _validate_selected_groups(selected_groups)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start, available_end = _fetch_date_range(conn, table_name="topix_data")
        analysis_start, analysis_end = _query_analysis_date_range(
            conn,
            start_date=start_date,
            end_date=end_date,
            close_threshold_1=close_threshold_1,
            close_threshold_2=close_threshold_2,
            selected_groups=validated_groups,
        )
        day_counts_df, excluded_without_prev_close, excluded_without_next_session = (
            _query_day_counts(
                conn,
                start_date=start_date,
                end_date=end_date,
                close_threshold_1=close_threshold_1,
                close_threshold_2=close_threshold_2,
            )
        )
        summary_df = _query_summary(
            conn,
            start_date=start_date,
            end_date=end_date,
            close_threshold_1=close_threshold_1,
            close_threshold_2=close_threshold_2,
            selected_groups=validated_groups,
        )
        daily_group_returns_df = _query_daily_group_returns(
            conn,
            start_date=start_date,
            end_date=end_date,
            close_threshold_1=close_threshold_1,
            close_threshold_2=close_threshold_2,
            selected_groups=validated_groups,
        )
        samples_df = _query_samples(
            conn,
            start_date=start_date,
            end_date=end_date,
            close_threshold_1=close_threshold_1,
            close_threshold_2=close_threshold_2,
            selected_groups=validated_groups,
            sample_size=sample_size,
        )

    day_counts_df = _complete_day_counts(
        day_counts_df,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    summary_df = _complete_summary_grid(
        summary_df,
        selected_groups=validated_groups,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    samples_df = samples_df.copy()
    samples_df["close_bucket_label"] = _map_close_bucket_labels(
        samples_df["close_bucket_key"],
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    clipped_samples_df, clip_bounds_df = _apply_sample_clipping(
        samples_df,
        clip_percentiles=clip_percentiles,
    )
    clip_bounds_df["close_bucket_label"] = _map_close_bucket_labels(
        clip_bounds_df["close_bucket_key"],
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )
    daily_group_returns_df["close_bucket_label"] = _map_close_bucket_labels(
        daily_group_returns_df["close_bucket_key"],
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
    )

    return TopixCloseStockOvernightDistributionResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start,
        available_end_date=available_end,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        close_threshold_1=close_threshold_1,
        close_threshold_2=close_threshold_2,
        close_return_stats=close_return_stats,
        selected_groups=validated_groups,
        sample_size=sample_size,
        clip_percentiles=clip_percentiles,
        excluded_topix_days_without_prev_close=excluded_without_prev_close,
        excluded_topix_days_without_next_session=excluded_without_next_session,
        day_counts_df=day_counts_df,
        summary_df=summary_df,
        samples_df=samples_df,
        clipped_samples_df=clipped_samples_df,
        clip_bounds_df=clip_bounds_df,
        daily_group_returns_df=daily_group_returns_df,
    )


def write_topix_close_stock_overnight_distribution_research_bundle(
    result: TopixCloseStockOvernightDistributionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_close_stock_overnight_distribution",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "close_threshold_1": result.close_threshold_1,
            "close_threshold_2": result.close_threshold_2,
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


def load_topix_close_stock_overnight_distribution_research_bundle(
    bundle_path: str | Path,
) -> TopixCloseStockOvernightDistributionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix_close_stock_overnight_distribution_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_close_stock_overnight_distribution_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_result_payload(
    result: TopixCloseStockOvernightDistributionResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata = {
        "db_path": result.db_path,
        "source_mode": result.source_mode,
        "source_detail": result.source_detail,
        "available_start_date": result.available_start_date,
        "available_end_date": result.available_end_date,
        "analysis_start_date": result.analysis_start_date,
        "analysis_end_date": result.analysis_end_date,
        "close_threshold_1": result.close_threshold_1,
        "close_threshold_2": result.close_threshold_2,
        "close_return_stats": _serialize_topix_close_return_stats(
            result.close_return_stats
        ),
        "selected_groups": list(result.selected_groups),
        "sample_size": result.sample_size,
        "clip_percentiles": list(result.clip_percentiles),
        "excluded_topix_days_without_prev_close": result.excluded_topix_days_without_prev_close,
        "excluded_topix_days_without_next_session": result.excluded_topix_days_without_next_session,
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
) -> TopixCloseStockOvernightDistributionResult:
    return TopixCloseStockOvernightDistributionResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        close_threshold_1=float(metadata["close_threshold_1"]),
        close_threshold_2=float(metadata["close_threshold_2"]),
        close_return_stats=_deserialize_topix_close_return_stats(
            cast(dict[str, Any] | None, metadata.get("close_return_stats"))
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
        excluded_topix_days_without_prev_close=int(
            metadata["excluded_topix_days_without_prev_close"]
        ),
        excluded_topix_days_without_next_session=int(
            metadata["excluded_topix_days_without_next_session"]
        ),
        day_counts_df=tables["day_counts_df"],
        summary_df=tables["summary_df"],
        samples_df=tables["samples_df"],
        clipped_samples_df=tables["clipped_samples_df"],
        clip_bounds_df=tables["clip_bounds_df"],
        daily_group_returns_df=tables["daily_group_returns_df"],
    )


def _build_research_bundle_summary_markdown(
    result: TopixCloseStockOvernightDistributionResult,
) -> str:
    summary_lines = [
        "# TOPIX Close / Stock Overnight Distribution",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Close thresholds: `{result.close_threshold_1 * 100:.4f}% / {result.close_threshold_2 * 100:.4f}%`",
        f"- Selected groups: `{', '.join(result.selected_groups)}`",
        f"- Excluded TOPIX days without previous close: `{result.excluded_topix_days_without_prev_close}`",
        f"- Excluded TOPIX days without next session: `{result.excluded_topix_days_without_next_session}`",
        "",
        "## Current Read",
        "",
    ]
    stats = result.close_return_stats
    if stats is not None:
        summary_lines.extend(
            [
                f"- TOPIX close sample count: `{stats.sample_count}`",
                f"- Mean / std: `{stats.mean_return * 100:+.4f}% / {stats.std_return * 100:.4f}%`",
                f"- Sigma thresholds: `{stats.sigma_threshold_1:g}σ / {stats.sigma_threshold_2:g}σ`",
            ]
        )
    strongest = result.summary_df[result.summary_df["mean_overnight_return"].notna()].copy()
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
            f"`{strongest_row['close_bucket_label']}` at "
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
