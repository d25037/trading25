"""
TOPIX gap / stock intraday distribution analysis.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

GapBucketKey = Literal[
    "gap_le_negative_threshold_2",
    "gap_negative_threshold_2_to_1",
    "gap_negative_threshold_1_to_threshold_1",
    "gap_threshold_1_to_2",
    "gap_ge_threshold_2",
]
StockGroup = Literal[
    "PRIME",
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
]
SourceMode = Literal["live", "snapshot"]
RotationSignalLabel = Literal["weak", "strong", "neutral"]

GAP_BUCKET_ORDER: tuple[GapBucketKey, ...] = (
    "gap_le_negative_threshold_2",
    "gap_negative_threshold_2_to_1",
    "gap_negative_threshold_1_to_threshold_1",
    "gap_threshold_1_to_2",
    "gap_ge_threshold_2",
)
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = (
    "PRIME",
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
)
TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
)
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)

_LOCK_ERROR_PATTERNS: tuple[str, ...] = (
    "conflicting lock is held",
    "could not set lock",
)
ROTATION_WEAK_GROUP: StockGroup = "TOPIX500"
ROTATION_STRONG_GROUP: StockGroup = "PRIME ex TOPIX500"
ROTATION_REQUIRED_GROUPS: tuple[StockGroup, StockGroup] = (
    ROTATION_WEAK_GROUP,
    ROTATION_STRONG_GROUP,
)


@dataclass(frozen=True)
class TopixGapIntradayDistributionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    gap_threshold_1: float
    gap_threshold_2: float
    selected_groups: tuple[StockGroup, ...]
    excluded_topix_days_without_prev_close: int
    day_counts_df: pd.DataFrame
    summary_df: pd.DataFrame
    samples_df: pd.DataFrame
    clipped_samples_df: pd.DataFrame
    clip_bounds_df: pd.DataFrame
    rotation_daily_df: pd.DataFrame
    rotation_signal_summary_df: pd.DataFrame
    rotation_overall_summary_df: pd.DataFrame


@dataclass(frozen=True)
class TopixGapReturnStats:
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
            prefix="topix-gap-intraday-",
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


def _date_where_clause(column_name: str, start_date: str | None, end_date: str | None) -> tuple[str, list[str]]:
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


def _format_threshold(value: float) -> str:
    percent = value * 100.0
    if percent.is_integer():
        return f"{int(percent)}%"
    formatted = f"{percent:.2f}".rstrip("0").rstrip(".")
    return f"{formatted}%"


def _gap_return_sql() -> str:
    return "(open - prev_close) / prev_close"


def _query_topix_gap_return_stats(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[TopixGapReturnStats | None, str | None, str | None]:
    gap_where_sql, gap_params = _date_where_clause("date", start_date, end_date)
    gap_return_sql = _gap_return_sql()
    row = conn.execute(
        f"""
        WITH topix_gap_days AS (
            SELECT
                date,
                CASE
                    WHEN open IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE {gap_return_sql}
                END AS gap_return
            FROM (
                SELECT
                    date,
                    open,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close
                FROM topix_data
            ) ordered_topix
            {gap_where_sql}
        )
        SELECT
            COUNT(*) AS sample_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            AVG(gap_return) AS mean_return,
            STDDEV_SAMP(gap_return) AS std_return,
            MIN(gap_return) AS min_return,
            quantile_cont(gap_return, 0.25) AS q25_return,
            median(gap_return) AS median_return,
            quantile_cont(gap_return, 0.75) AS q75_return,
            MAX(gap_return) AS max_return
        FROM topix_gap_days
        WHERE gap_return IS NOT NULL
        """,
        gap_params,
    ).fetchone()
    sample_count = int(row[0] or 0) if row else 0
    analysis_start = str(row[1]) if row and row[1] else None
    analysis_end = str(row[2]) if row and row[2] else None
    if sample_count == 0:
        return None, analysis_start, analysis_end

    mean_return = float(row[3])
    std_return = float(row[4]) if row[4] is not None else 0.0
    if std_return <= 0:
        raise ValueError("topix_gap_return standard deviation must be positive")

    stats = TopixGapReturnStats(
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


def _gap_bucket_case_sql() -> str:
    gap_return_sql = _gap_return_sql()
    return (
        "CASE "
        "WHEN open IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL "
        f"WHEN {gap_return_sql} <= -? THEN 'gap_le_negative_threshold_2' "
        f"WHEN {gap_return_sql} <= -? THEN 'gap_negative_threshold_2_to_1' "
        f"WHEN {gap_return_sql} < ? THEN 'gap_negative_threshold_1_to_threshold_1' "
        f"WHEN {gap_return_sql} < ? THEN 'gap_threshold_1_to_2' "
        "ELSE 'gap_ge_threshold_2' "
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


def format_gap_bucket_label(
    bucket_key: GapBucketKey,
    *,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> str:
    threshold_1 = _format_threshold(gap_threshold_1)
    threshold_2 = _format_threshold(gap_threshold_2)
    if bucket_key == "gap_le_negative_threshold_2":
        return f"gap <= -{threshold_2}"
    if bucket_key == "gap_negative_threshold_2_to_1":
        return f"-{threshold_2} < gap <= -{threshold_1}"
    if bucket_key == "gap_negative_threshold_1_to_threshold_1":
        return f"-{threshold_1} < gap < {threshold_1}"
    if bucket_key == "gap_threshold_1_to_2":
        return f"{threshold_1} <= gap < {threshold_2}"
    return f"gap >= {threshold_2}"


def _grouped_stock_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> tuple[str, list[Any]]:
    gap_where_sql, gap_params = _date_where_clause("date", start_date, end_date)
    normalized_code_sql = _normalize_code_sql("code")
    stock_conditions: list[str] = ["open IS NOT NULL", "close IS NOT NULL"]
    stock_params: list[str] = []
    if start_date:
        stock_conditions.insert(0, "date >= ?")
        stock_params.append(start_date)
    if end_date:
        stock_conditions.insert(1 if start_date else 0, "date <= ?")
        stock_params.append(end_date)
    stock_where_sql = " WHERE " + " AND ".join(stock_conditions)

    group_selects = {
        "PRIME": """
            SELECT date, normalized_code, intraday_diff, intraday_return, direction, gap_bucket_key, gap_return, 'PRIME' AS stock_group
            FROM stock_days_joined
            WHERE is_prime
        """,
        "TOPIX100": """
            SELECT date, normalized_code, intraday_diff, intraday_return, direction, gap_bucket_key, gap_return, 'TOPIX100' AS stock_group
            FROM stock_days_joined
            WHERE is_topix100
        """,
        "TOPIX500": """
            SELECT date, normalized_code, intraday_diff, intraday_return, direction, gap_bucket_key, gap_return, 'TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_topix500
        """,
        "PRIME ex TOPIX500": """
            SELECT date, normalized_code, intraday_diff, intraday_return, direction, gap_bucket_key, gap_return, 'PRIME ex TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_prime_ex_topix500
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)
    gap_return_sql = _gap_return_sql()
    gap_bucket_case_sql = _gap_bucket_case_sql()

    sql = f"""
        WITH topix_with_gap AS (
            SELECT
                date,
                open,
                close,
                prev_close,
                {gap_bucket_case_sql} AS gap_bucket_key,
                CASE
                    WHEN open IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE {gap_return_sql}
                END AS gap_return
                FROM (
                    SELECT
                        date,
                        open,
                        close,
                        LAG(close) OVER (ORDER BY date) AS prev_close
                    FROM topix_data
                ) ordered_topix
                {gap_where_sql}
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
        stock_data_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                open,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            {stock_where_sql}
        ),
        stock_data_dedup AS (
            SELECT
                date,
                normalized_code,
                close - open AS intraday_diff,
                CASE
                    WHEN open = 0 THEN NULL
                    ELSE (close - open) / open
                END AS intraday_return,
                CASE
                    WHEN close - open > 0 THEN 'up'
                    WHEN close - open < 0 THEN 'down'
                    ELSE 'flat'
                END AS direction
            FROM stock_data_raw
            WHERE row_priority = 1
        ),
        stock_days_joined AS (
            SELECT
                s.date,
                s.normalized_code,
                s.intraday_diff,
                s.intraday_return,
                s.direction,
                t.gap_bucket_key,
                t.gap_return,
                m.is_prime,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500
            FROM stock_data_dedup s
            JOIN topix_with_gap t USING (date)
            JOIN stocks_snapshot m USING (normalized_code)
            WHERE t.gap_bucket_key IS NOT NULL
        ),
        grouped_stock_days AS (
            {union_sql}
        )
    """
    return sql, [
        gap_threshold_2,
        gap_threshold_1,
        gap_threshold_1,
        gap_threshold_2,
        *gap_params,
        *stock_params,
    ]


def _query_day_counts(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> tuple[pd.DataFrame, int]:
    gap_where_sql, gap_params = _date_where_clause("date", start_date, end_date)
    gap_bucket_case_sql = _gap_bucket_case_sql()
    excluded_conditions: list[str] = [
        "open IS NULL OR prev_close IS NULL OR prev_close = 0"
    ]
    excluded_params: list[str] = []
    if start_date:
        excluded_conditions.insert(0, "date >= ?")
        excluded_params.append(start_date)
    if end_date:
        excluded_conditions.insert(1 if start_date else 0, "date <= ?")
        excluded_params.append(end_date)
    excluded_where_sql = " WHERE " + " AND ".join(excluded_conditions)
    sql = f"""
        WITH topix_with_gap AS (
            SELECT
                {gap_bucket_case_sql} AS gap_bucket_key
            FROM (
                SELECT
                    date,
                    open,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close
                FROM topix_data
            ) ordered_topix
            {gap_where_sql}
        )
        SELECT gap_bucket_key, COUNT(*) AS day_count
        FROM topix_with_gap
        WHERE gap_bucket_key IS NOT NULL
        GROUP BY gap_bucket_key
    """
    params = [
        gap_threshold_2,
        gap_threshold_1,
        gap_threshold_1,
        gap_threshold_2,
        *gap_params,
    ]
    df = cast(pd.DataFrame, conn.execute(sql, params).fetchdf())

    excluded_row = conn.execute(
        f"""
        SELECT COUNT(*) AS excluded_count
        FROM (
            SELECT
                open,
                LAG(close) OVER (ORDER BY date) AS prev_close,
                date
            FROM topix_data
        ) ordered_topix
        {excluded_where_sql}
        """,
        excluded_params,
    ).fetchone()
    excluded_count = int(excluded_row[0] or 0) if excluded_row else 0
    return df, excluded_count


def _query_topix_gap_days(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> pd.DataFrame:
    gap_where_sql, gap_params = _date_where_clause("date", start_date, end_date)
    gap_return_sql = _gap_return_sql()
    gap_bucket_case_sql = _gap_bucket_case_sql()
    sql = f"""
        WITH topix_with_gap AS (
            SELECT
                date,
                {gap_bucket_case_sql} AS gap_bucket_key,
                CASE
                    WHEN open IS NULL OR prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE {gap_return_sql}
                END AS gap_return
            FROM (
                SELECT
                    date,
                    open,
                    close,
                    LAG(close) OVER (ORDER BY date) AS prev_close
                FROM topix_data
            ) ordered_topix
            {gap_where_sql}
        )
        SELECT
            date,
            gap_bucket_key,
            gap_return
        FROM topix_with_gap
        WHERE gap_bucket_key IS NOT NULL
        ORDER BY date
    """
    params = [
        gap_threshold_2,
        gap_threshold_1,
        gap_threshold_1,
        gap_threshold_2,
        *gap_params,
    ]
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_summary(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            gap_bucket_key,
            COUNT(*) AS sample_count,
            COUNT(*) FILTER (WHERE direction = 'up') AS up_count,
            COUNT(*) FILTER (WHERE direction = 'down') AS down_count,
            COUNT(*) FILTER (WHERE direction = 'flat') AS flat_count,
            AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS up_ratio,
            AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS down_ratio,
            AVG(CASE WHEN direction = 'flat' THEN 1.0 ELSE 0.0 END) AS flat_ratio,
            AVG(intraday_return) AS mean_intraday_return,
            AVG(intraday_diff) AS mean_intraday_diff,
            median(intraday_diff) AS median_intraday_diff,
            quantile_cont(intraday_diff, 0.05) AS p05_intraday_diff,
            quantile_cont(intraday_diff, 0.25) AS p25_intraday_diff,
            quantile_cont(intraday_diff, 0.50) AS p50_intraday_diff,
            quantile_cont(intraday_diff, 0.75) AS p75_intraday_diff,
            quantile_cont(intraday_diff, 0.95) AS p95_intraday_diff
        FROM grouped_stock_days
        GROUP BY stock_group, gap_bucket_key
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_daily_group_returns(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            gap_bucket_key,
            date,
            AVG(intraday_return) AS day_mean_intraday_return,
            AVG(CASE WHEN direction = 'up' THEN 1.0 ELSE 0.0 END) AS day_up_ratio,
            AVG(CASE WHEN direction = 'down' THEN 1.0 ELSE 0.0 END) AS day_down_ratio,
            COUNT(*) AS constituent_count
        FROM grouped_stock_days
        GROUP BY stock_group, gap_bucket_key, date
        ORDER BY date, stock_group
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_analysis_date_range(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
    selected_groups: Sequence[StockGroup],
) -> tuple[str | None, str | None]:
    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
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
    gap_threshold_1: float,
    gap_threshold_2: float,
    selected_groups: Sequence[StockGroup],
    sample_size: int,
) -> pd.DataFrame:
    if sample_size <= 0:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "gap_bucket_key",
                "date",
                "code",
                "intraday_diff",
                "intraday_return",
                "direction",
                "sample_rank",
            ]
        )

    cte_sql, params = _grouped_stock_days_cte(
        start_date=start_date,
        end_date=end_date,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
        selected_groups=selected_groups,
    )
    sql = f"""
        {cte_sql}
        SELECT
            stock_group,
            gap_bucket_key,
            date,
            normalized_code AS code,
            intraday_diff,
            intraday_return,
            direction,
            sample_rank
        FROM (
            SELECT
                stock_group,
                gap_bucket_key,
                date,
                normalized_code,
                intraday_diff,
                intraday_return,
                direction,
                ROW_NUMBER() OVER (
                    PARTITION BY stock_group, gap_bucket_key
                    ORDER BY md5(
                        stock_group || '|' || normalized_code || '|' || date || '|' || CAST(intraday_diff AS VARCHAR)
                    )
                ) AS sample_rank
            FROM grouped_stock_days
        ) ranked_samples
        WHERE sample_rank <= ?
        ORDER BY stock_group, gap_bucket_key, sample_rank
    """
    return cast(pd.DataFrame, conn.execute(sql, [*params, sample_size]).fetchdf())


def _complete_day_counts(
    day_counts_df: pd.DataFrame,
    *,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.DataFrame({"gap_bucket_key": list(GAP_BUCKET_ORDER)})
    merged = expected.merge(day_counts_df, how="left", on="gap_bucket_key")
    merged["day_count"] = merged["day_count"].fillna(0).astype(int)
    merged["gap_bucket_label"] = merged["gap_bucket_key"].map(
        lambda value: format_gap_bucket_label(
            cast(GapBucketKey, value),
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
        )
    )
    return merged


def _complete_summary_grid(
    summary_df: pd.DataFrame,
    *,
    selected_groups: Sequence[StockGroup],
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> pd.DataFrame:
    expected = pd.MultiIndex.from_product(
        [list(selected_groups), list(GAP_BUCKET_ORDER)],
        names=["stock_group", "gap_bucket_key"],
    ).to_frame(index=False)
    merged = expected.merge(
        summary_df,
        how="left",
        on=["stock_group", "gap_bucket_key"],
    )

    count_columns = ["sample_count", "up_count", "down_count", "flat_count"]
    ratio_columns = ["up_ratio", "down_ratio", "flat_ratio"]
    for column in count_columns:
        merged[column] = merged[column].fillna(0).astype(int)
    for column in ratio_columns:
        merged[column] = merged[column].fillna(0.0).astype(float)

    merged["gap_bucket_label"] = merged["gap_bucket_key"].map(
        lambda value: format_gap_bucket_label(
            cast(GapBucketKey, value),
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
        )
    )
    return merged


def _rotation_signal_for_bucket(bucket_key: GapBucketKey) -> RotationSignalLabel:
    if bucket_key in (
        "gap_le_negative_threshold_2",
        "gap_negative_threshold_2_to_1",
    ):
        return "weak"
    if bucket_key in ("gap_threshold_1_to_2", "gap_ge_threshold_2"):
        return "strong"
    return "neutral"


def _rotation_group_for_signal(signal_label: RotationSignalLabel) -> StockGroup | None:
    if signal_label == "weak":
        return ROTATION_WEAK_GROUP
    if signal_label == "strong":
        return ROTATION_STRONG_GROUP
    return None


def _max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    anchored_equity = pd.concat(
        [pd.Series([1.0], dtype=float), equity_curve.reset_index(drop=True)],
        ignore_index=True,
    )
    drawdown = anchored_equity / anchored_equity.cummax() - 1.0
    return float(drawdown.min())


def _build_rotation_strategy_outputs(
    *,
    topix_gap_days_df: pd.DataFrame,
    rotation_group_daily_df: pd.DataFrame,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    strategy_daily_df = topix_gap_days_df.copy()
    if strategy_daily_df.empty:
        empty_daily = pd.DataFrame(
            columns=[
                "date",
                "gap_bucket_key",
                "gap_bucket_label",
                "gap_return",
                "signal_label",
                "selected_group",
                "position",
                "selected_group_return",
                "selected_group_up_ratio",
                "selected_group_down_ratio",
                "selected_group_constituent_count",
                "missing_selected_group_return",
                "strategy_return",
                "equity_curve",
                "cumulative_return",
            ]
        )
        empty_signal_summary = pd.DataFrame(
            columns=[
                "signal_label",
                "selected_group",
                "position",
                "day_count",
                "mean_strategy_return",
                "median_strategy_return",
                "win_ratio",
                "loss_ratio",
                "cumulative_return",
            ]
        )
        empty_overall_summary = pd.DataFrame(
            columns=[
                "strategy_name",
                "total_days",
                "trade_days",
                "flat_days",
                "weak_trade_days",
                "strong_trade_days",
                "missing_trade_days",
                "mean_trade_return",
                "median_trade_return",
                "mean_daily_return",
                "win_trade_ratio",
                "loss_trade_ratio",
                "cumulative_return",
                "final_equity",
                "max_drawdown",
            ]
        )
        return empty_daily, empty_signal_summary, empty_overall_summary

    strategy_daily_df["gap_bucket_label"] = strategy_daily_df["gap_bucket_key"].map(
        lambda value: format_gap_bucket_label(
            cast(GapBucketKey, value),
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
        )
    )
    strategy_daily_df["signal_label"] = strategy_daily_df["gap_bucket_key"].map(
        lambda value: _rotation_signal_for_bucket(cast(GapBucketKey, value))
    )
    strategy_daily_df["selected_group"] = strategy_daily_df["signal_label"].map(
        _rotation_group_for_signal
    )
    strategy_daily_df["position"] = strategy_daily_df["selected_group"].map(
        lambda value: "long" if value else "flat"
    )

    group_daily = rotation_group_daily_df.rename(
        columns={
            "stock_group": "selected_group",
            "day_mean_intraday_return": "selected_group_return",
            "day_up_ratio": "selected_group_up_ratio",
            "day_down_ratio": "selected_group_down_ratio",
            "constituent_count": "selected_group_constituent_count",
        }
    )
    strategy_daily_df = strategy_daily_df.merge(
        group_daily[
            [
                "date",
                "gap_bucket_key",
                "selected_group",
                "selected_group_return",
                "selected_group_up_ratio",
                "selected_group_down_ratio",
                "selected_group_constituent_count",
            ]
        ],
        how="left",
        on=["date", "gap_bucket_key", "selected_group"],
    )
    strategy_daily_df["missing_selected_group_return"] = (
        strategy_daily_df["position"].eq("long")
        & strategy_daily_df["selected_group_return"].isna()
    )
    strategy_daily_df["strategy_return"] = strategy_daily_df["selected_group_return"]
    strategy_daily_df.loc[
        strategy_daily_df["position"].eq("flat"), "strategy_return"
    ] = 0.0
    strategy_daily_df.loc[
        strategy_daily_df["missing_selected_group_return"], "strategy_return"
    ] = 0.0
    strategy_daily_df["equity_curve"] = (
        1.0 + strategy_daily_df["strategy_return"]
    ).cumprod()
    strategy_daily_df["cumulative_return"] = strategy_daily_df["equity_curve"] - 1.0

    strategy_signal_summary_df = (
        strategy_daily_df.groupby(
            ["signal_label", "selected_group", "position"],
            as_index=False,
            dropna=False,
        )
        .agg(
            day_count=("date", "count"),
            mean_strategy_return=("strategy_return", "mean"),
            median_strategy_return=("strategy_return", "median"),
            win_ratio=("strategy_return", lambda values: (values > 0).mean()),
            loss_ratio=("strategy_return", lambda values: (values < 0).mean()),
            cumulative_return=(
                "strategy_return",
                lambda values: (1.0 + values).prod() - 1.0,
            ),
        )
    )

    trade_mask = strategy_daily_df["position"].eq("long")
    trade_returns = strategy_daily_df.loc[trade_mask, "strategy_return"]
    final_equity = float(strategy_daily_df["equity_curve"].iloc[-1])
    strategy_overall_summary_df = pd.DataFrame(
        [
            {
                "strategy_name": (
                    "weak=>TOPIX500 long / strong=>PRIME ex TOPIX500 long / "
                    "neutral=>flat"
                ),
                "total_days": int(len(strategy_daily_df)),
                "trade_days": int(trade_mask.sum()),
                "flat_days": int((~trade_mask).sum()),
                "weak_trade_days": int(
                    strategy_daily_df["signal_label"].eq("weak").sum()
                ),
                "strong_trade_days": int(
                    strategy_daily_df["signal_label"].eq("strong").sum()
                ),
                "missing_trade_days": int(
                    strategy_daily_df["missing_selected_group_return"].sum()
                ),
                "mean_trade_return": float(trade_returns.mean())
                if not trade_returns.empty
                else 0.0,
                "median_trade_return": float(trade_returns.median())
                if not trade_returns.empty
                else 0.0,
                "mean_daily_return": float(strategy_daily_df["strategy_return"].mean()),
                "win_trade_ratio": float((trade_returns > 0).mean())
                if not trade_returns.empty
                else 0.0,
                "loss_trade_ratio": float((trade_returns < 0).mean())
                if not trade_returns.empty
                else 0.0,
                "cumulative_return": final_equity - 1.0,
                "final_equity": final_equity,
                "max_drawdown": _max_drawdown(strategy_daily_df["equity_curve"]),
            }
        ]
    )

    return (
        strategy_daily_df,
        strategy_signal_summary_df,
        strategy_overall_summary_df,
    )


def _apply_sample_clipping(
    samples_df: pd.DataFrame,
    *,
    clip_percentiles: tuple[float, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if samples_df.empty:
        empty_bounds = pd.DataFrame(
            columns=["stock_group", "gap_bucket_key", "clip_lower", "clip_upper"]
        )
        return samples_df.copy(), empty_bounds

    lower_pct, upper_pct = clip_percentiles
    bounds_df = (
        samples_df.groupby(["stock_group", "gap_bucket_key"], as_index=False)
        .agg(
            clip_lower=("intraday_diff", lambda values: values.quantile(lower_pct / 100.0)),
            clip_upper=("intraday_diff", lambda values: values.quantile(upper_pct / 100.0)),
        )
    )
    clipped = samples_df.merge(
        bounds_df,
        how="left",
        on=["stock_group", "gap_bucket_key"],
    )
    clipped = clipped[
        clipped["intraday_diff"].between(
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
    gap_threshold_1: float,
    gap_threshold_2: float,
    sample_size: int,
    clip_percentiles: tuple[float, float],
) -> None:
    if gap_threshold_1 <= 0:
        raise ValueError("gap_threshold_1 must be positive")
    if gap_threshold_2 <= gap_threshold_1:
        raise ValueError("gap_threshold_2 must be greater than gap_threshold_1")
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    lower, upper = clip_percentiles
    if lower < 0 or upper > 100 or lower >= upper:
        raise ValueError("clip_percentiles must satisfy 0 <= lower < upper <= 100")


def get_topix_available_date_range(db_path: str) -> tuple[str | None, str | None]:
    """Return the available TOPIX date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def get_topix_gap_return_stats(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
) -> TopixGapReturnStats | None:
    """Return TOPIX gap-return distribution stats for sigma-derived bucketing."""
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")
    with _open_analysis_connection(db_path) as ctx:
        stats, _, _ = _query_topix_gap_return_stats(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        return stats


def run_topix_gap_intraday_distribution(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    gap_threshold_1: float = 0.01,
    gap_threshold_2: float = 0.02,
    selected_groups: Sequence[str] | None = None,
    sample_size: int = 2000,
    clip_percentiles: tuple[float, float] = (1.0, 99.0),
) -> TopixGapIntradayDistributionResult:
    """Run the TOPIX gap / intraday distribution analysis from market.duckdb."""
    _validate_inputs(
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
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
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
            selected_groups=validated_groups,
        )

        day_counts_df, excluded_count = _query_day_counts(
            conn,
            start_date=start_date,
            end_date=end_date,
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
        )
        rotation_topix_gap_days_df = _query_topix_gap_days(
            conn,
            start_date=start_date,
            end_date=end_date,
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
        )
        summary_df = _query_summary(
            conn,
            start_date=start_date,
            end_date=end_date,
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
            selected_groups=validated_groups,
        )
        rotation_group_daily_df = _query_daily_group_returns(
            conn,
            start_date=start_date,
            end_date=end_date,
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
            selected_groups=ROTATION_REQUIRED_GROUPS,
        )
        samples_df = _query_samples(
            conn,
            start_date=start_date,
            end_date=end_date,
            gap_threshold_1=gap_threshold_1,
            gap_threshold_2=gap_threshold_2,
            selected_groups=validated_groups,
            sample_size=sample_size,
        )

    day_counts_df = _complete_day_counts(
        day_counts_df,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
    )
    summary_df = _complete_summary_grid(
        summary_df,
        selected_groups=validated_groups,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
    )
    samples_df = samples_df.copy()
    if samples_df.empty:
        samples_df["gap_bucket_label"] = pd.Series(dtype="object")
    else:
        samples_df["gap_bucket_label"] = samples_df["gap_bucket_key"].map(
            lambda value: format_gap_bucket_label(
                cast(GapBucketKey, value),
                gap_threshold_1=gap_threshold_1,
                gap_threshold_2=gap_threshold_2,
            )
        )
    clipped_samples_df, clip_bounds_df = _apply_sample_clipping(
        samples_df,
        clip_percentiles=clip_percentiles,
    )
    if not clip_bounds_df.empty:
        clip_bounds_df["gap_bucket_label"] = clip_bounds_df["gap_bucket_key"].map(
            lambda value: format_gap_bucket_label(
                cast(GapBucketKey, value),
                gap_threshold_1=gap_threshold_1,
                gap_threshold_2=gap_threshold_2,
            )
        )
    (
        rotation_daily_df,
        rotation_signal_summary_df,
        rotation_overall_summary_df,
    ) = _build_rotation_strategy_outputs(
        topix_gap_days_df=rotation_topix_gap_days_df,
        rotation_group_daily_df=rotation_group_daily_df,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
    )

    return TopixGapIntradayDistributionResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start,
        available_end_date=available_end,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        gap_threshold_1=gap_threshold_1,
        gap_threshold_2=gap_threshold_2,
        selected_groups=validated_groups,
        excluded_topix_days_without_prev_close=excluded_count,
        day_counts_df=day_counts_df,
        summary_df=summary_df,
        samples_df=samples_df,
        clipped_samples_df=clipped_samples_df,
        clip_bounds_df=clip_bounds_df,
        rotation_daily_df=rotation_daily_df,
        rotation_signal_summary_df=rotation_signal_summary_df,
        rotation_overall_summary_df=rotation_overall_summary_df,
    )
