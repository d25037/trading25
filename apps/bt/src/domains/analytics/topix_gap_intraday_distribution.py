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
    "gap_lt_threshold_1",
    "gap_threshold_1_to_2",
    "gap_ge_threshold_2",
]
StockGroup = Literal[
    "PRIME",
    "STANDARD",
    "GROWTH",
    "TOPIX500",
    "PRIME ex TOPIX500",
]
SourceMode = Literal["live", "snapshot"]

GAP_BUCKET_ORDER: tuple[GapBucketKey, ...] = (
    "gap_lt_threshold_1",
    "gap_threshold_1_to_2",
    "gap_ge_threshold_2",
)
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = (
    "PRIME",
    "STANDARD",
    "GROWTH",
    "TOPIX500",
    "PRIME ex TOPIX500",
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
    if bucket_key == "gap_lt_threshold_1":
        return f"|gap| < {threshold_1}"
    if bucket_key == "gap_threshold_1_to_2":
        return f"{threshold_1} <= |gap| < {threshold_2}"
    return f"|gap| >= {threshold_2}"


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
            SELECT date, normalized_code, intraday_diff, direction, gap_bucket_key, gap_return, 'PRIME' AS stock_group
            FROM stock_days_joined
            WHERE is_prime
        """,
        "STANDARD": """
            SELECT date, normalized_code, intraday_diff, direction, gap_bucket_key, gap_return, 'STANDARD' AS stock_group
            FROM stock_days_joined
            WHERE is_standard
        """,
        "GROWTH": """
            SELECT date, normalized_code, intraday_diff, direction, gap_bucket_key, gap_return, 'GROWTH' AS stock_group
            FROM stock_days_joined
            WHERE is_growth
        """,
        "TOPIX500": """
            SELECT date, normalized_code, intraday_diff, direction, gap_bucket_key, gap_return, 'TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_topix500
        """,
        "PRIME ex TOPIX500": """
            SELECT date, normalized_code, intraday_diff, direction, gap_bucket_key, gap_return, 'PRIME ex TOPIX500' AS stock_group
            FROM stock_days_joined
            WHERE is_prime_ex_topix500
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)

    sql = f"""
        WITH topix_with_gap AS (
            SELECT
                date,
                open,
                close,
                prev_close,
                CASE
                    WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                    WHEN abs((open - prev_close) / prev_close) >= ? THEN 'gap_ge_threshold_2'
                    WHEN abs((open - prev_close) / prev_close) >= ? THEN 'gap_threshold_1_to_2'
                    ELSE 'gap_lt_threshold_1'
                END AS gap_bucket_key,
                CASE
                    WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                    ELSE (open - prev_close) / prev_close
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
                market_code_norm IN ('standard', '0112') AS is_standard,
                market_code_norm IN ('growth', '0113') AS is_growth,
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
                s.direction,
                t.gap_bucket_key,
                t.gap_return,
                m.is_prime,
                m.is_standard,
                m.is_growth,
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
    return sql, [gap_threshold_2, gap_threshold_1, *gap_params, *stock_params]


def _query_day_counts(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    gap_threshold_1: float,
    gap_threshold_2: float,
) -> tuple[pd.DataFrame, int]:
    gap_where_sql, gap_params = _date_where_clause("date", start_date, end_date)
    excluded_conditions: list[str] = ["prev_close IS NULL OR prev_close = 0"]
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
                CASE
                    WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                    WHEN abs((open - prev_close) / prev_close) >= ? THEN 'gap_ge_threshold_2'
                    WHEN abs((open - prev_close) / prev_close) >= ? THEN 'gap_threshold_1_to_2'
                    ELSE 'gap_lt_threshold_1'
                END AS gap_bucket_key
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
    params = [gap_threshold_2, gap_threshold_1, *gap_params]
    df = cast(pd.DataFrame, conn.execute(sql, params).fetchdf())

    excluded_row = conn.execute(
        f"""
        SELECT COUNT(*) AS excluded_count
        FROM (
            SELECT
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
            direction,
            sample_rank
        FROM (
            SELECT
                stock_group,
                gap_bucket_key,
                date,
                normalized_code,
                intraday_diff,
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
        analysis_start, analysis_end = _fetch_date_range(
            conn,
            table_name="topix_data",
            start_date=start_date,
            end_date=end_date,
        )

        day_counts_df, excluded_count = _query_day_counts(
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
    )
