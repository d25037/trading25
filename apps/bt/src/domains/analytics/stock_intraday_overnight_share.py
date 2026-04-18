"""
Stock intraday / overnight absolute log-return share analysis.

The market.duckdb file is the source of truth for metadata and adjusted stock
time-series. This module performs read-only analytics and returns pandas
DataFrames for notebook visualization.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    _connect_duckdb as _shared_connect_duckdb,
    date_where_clause as _date_where_clause,
    fetch_date_range as _fetch_date_range,
    normalize_code_sql as _normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_payload_research_bundle,
    write_payload_research_bundle,
)

StockGroup = Literal[
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
    "STANDARD",
    "GROWTH",
]

STOCK_GROUP_ORDER: tuple[StockGroup, ...] = (
    "TOPIX100",
    "TOPIX500",
    "PRIME ex TOPIX500",
    "STANDARD",
    "GROWTH",
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
_PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "prime")
_STANDARD_MARKET_CODES: tuple[str, ...] = ("0112", "standard")
_GROWTH_MARKET_CODES: tuple[str, ...] = ("0113", "growth")

_STOCK_METRICS_COLUMNS: tuple[str, ...] = (
    "stock_group",
    "code",
    "company_name",
    "market_code",
    "market_name",
    "scale_category",
    "analysis_start_date",
    "analysis_end_date",
    "session_count",
    "intraday_abs_log_return_sum",
    "overnight_abs_log_return_sum",
    "total_abs_log_return_sum",
    "intraday_share",
    "overnight_share",
    "mean_intraday_log_return",
    "mean_overnight_log_return",
    "mean_intraday_abs_log_return",
    "mean_overnight_abs_log_return",
)
_DAILY_GROUP_SHARE_COLUMNS: tuple[str, ...] = (
    "stock_group",
    "date",
    "constituent_count",
    "intraday_abs_log_return_sum",
    "overnight_abs_log_return_sum",
    "total_abs_log_return_sum",
    "intraday_share",
    "overnight_share",
)
STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/stock-intraday-overnight-share"
)


@dataclass(frozen=True)
class StockIntradayOvernightShareResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_groups: tuple[StockGroup, ...]
    min_session_count: int
    stock_metrics_df: pd.DataFrame
    group_summary_df: pd.DataFrame
    daily_group_shares_df: pd.DataFrame


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="stock-intraday-overnight-share-",
        connect_fn=_connect_duckdb,
    )


def _group_order_key(series: pd.Series, selected_groups: Sequence[StockGroup]) -> pd.Categorical:
    return pd.Categorical(series, categories=list(selected_groups), ordered=True)


def _stock_group_sessions_cte(
    *,
    start_date: str | None,
    end_date: str | None,
    selected_groups: Sequence[StockGroup],
) -> tuple[str, list[Any]]:
    paired_where_sql, paired_params = _date_where_clause("date", start_date, end_date)
    normalized_code_sql = _normalize_code_sql("code")
    group_selects = {
        "TOPIX100": """
            SELECT
                date,
                next_date,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category,
                intraday_log_return,
                overnight_log_return,
                intraday_abs_log_return,
                overnight_abs_log_return,
                total_abs_log_return,
                'TOPIX100' AS stock_group
            FROM stock_sessions_joined
            WHERE is_topix100
        """,
        "TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category,
                intraday_log_return,
                overnight_log_return,
                intraday_abs_log_return,
                overnight_abs_log_return,
                total_abs_log_return,
                'TOPIX500' AS stock_group
            FROM stock_sessions_joined
            WHERE is_topix500
        """,
        "PRIME ex TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category,
                intraday_log_return,
                overnight_log_return,
                intraday_abs_log_return,
                overnight_abs_log_return,
                total_abs_log_return,
                'PRIME ex TOPIX500' AS stock_group
            FROM stock_sessions_joined
            WHERE is_prime_ex_topix500
        """,
        "STANDARD": """
            SELECT
                date,
                next_date,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category,
                intraday_log_return,
                overnight_log_return,
                intraday_abs_log_return,
                overnight_abs_log_return,
                total_abs_log_return,
                'STANDARD' AS stock_group
            FROM stock_sessions_joined
            WHERE is_standard
        """,
        "GROWTH": """
            SELECT
                date,
                next_date,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category,
                intraday_log_return,
                overnight_log_return,
                intraday_abs_log_return,
                overnight_abs_log_return,
                total_abs_log_return,
                'GROWTH' AS stock_group
            FROM stock_sessions_joined
            WHERE is_growth
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)
    sql = f"""
        WITH stocks_snapshot AS (
            SELECT
                normalized_code,
                company_name,
                market_code,
                market_name,
                coalesce(scale_category, '') AS scale_category,
                market_code IN {cast(Any, _PRIME_MARKET_CODES)} AS is_prime,
                coalesce(scale_category, '') IN {cast(Any, TOPIX100_SCALE_CATEGORIES)}
                    AS is_topix100,
                coalesce(scale_category, '') IN {cast(Any, TOPIX500_SCALE_CATEGORIES)}
                    AS is_topix500,
                market_code IN {cast(Any, _PRIME_MARKET_CODES)}
                    AND coalesce(scale_category, '') NOT IN {cast(Any, TOPIX500_SCALE_CATEGORIES)}
                    AS is_prime_ex_topix500,
                market_code IN {cast(Any, _STANDARD_MARKET_CODES)} AS is_standard,
                market_code IN {cast(Any, _GROWTH_MARKET_CODES)} AS is_growth
            FROM (
                SELECT
                    {normalized_code_sql} AS normalized_code,
                    company_name,
                    market_code,
                    market_name,
                    scale_category,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS row_priority
                FROM stocks
            ) stock_candidates
            WHERE row_priority = 1
        ),
        stock_daily_raw AS (
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
            WHERE open IS NOT NULL
              AND close IS NOT NULL
              AND open > 0
              AND close > 0
        ),
        stock_daily AS (
            SELECT
                date,
                normalized_code,
                open,
                close
            FROM stock_daily_raw
            WHERE row_priority = 1
        ),
        stock_with_next AS (
            SELECT
                date,
                normalized_code,
                open,
                close,
                LEAD(date) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_date,
                LEAD(open) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_open
            FROM stock_daily
        ),
        stock_paired_sessions AS (
            SELECT
                date,
                next_date,
                normalized_code,
                ln(close / open) AS intraday_log_return,
                ln(next_open / close) AS overnight_log_return,
                abs(ln(close / open)) AS intraday_abs_log_return,
                abs(ln(next_open / close)) AS overnight_abs_log_return,
                abs(ln(close / open)) + abs(ln(next_open / close)) AS total_abs_log_return
            FROM stock_with_next
            {paired_where_sql}
              {("AND" if paired_where_sql else "WHERE")} next_date IS NOT NULL
              AND next_open IS NOT NULL
              AND next_open > 0
        ),
        stock_sessions_joined AS (
            SELECT
                s.date,
                s.next_date,
                s.normalized_code,
                s.intraday_log_return,
                s.overnight_log_return,
                s.intraday_abs_log_return,
                s.overnight_abs_log_return,
                s.total_abs_log_return,
                m.company_name,
                m.market_code,
                m.market_name,
                m.scale_category,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500,
                m.is_standard,
                m.is_growth
            FROM stock_paired_sessions s
            JOIN stocks_snapshot m USING (normalized_code)
        ),
        stock_group_sessions AS (
            {union_sql}
        )
    """
    return sql, paired_params


def _query_analysis_date_range(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    selected_groups: Sequence[StockGroup],
) -> tuple[str | None, str | None]:
    cte_sql, params = _stock_group_sessions_cte(
        start_date=start_date,
        end_date=end_date,
        selected_groups=selected_groups,
    )
    row = conn.execute(
        f"""
        {cte_sql}
        SELECT
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM stock_group_sessions
        """,
        params,
    ).fetchone()
    min_date = str(row[0]) if row and row[0] else None
    max_date = str(row[1]) if row and row[1] else None
    return min_date, max_date


def _query_stock_metrics(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    selected_groups: Sequence[StockGroup],
    min_session_count: int,
) -> pd.DataFrame:
    cte_sql, params = _stock_group_sessions_cte(
        start_date=start_date,
        end_date=end_date,
        selected_groups=selected_groups,
    )
    stock_metrics_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            SELECT
                stock_group,
                normalized_code AS code,
                company_name,
                market_code,
                market_name,
                scale_category,
                MIN(date) AS analysis_start_date,
                MAX(date) AS analysis_end_date,
                COUNT(*) AS session_count,
                SUM(intraday_abs_log_return) AS intraday_abs_log_return_sum,
                SUM(overnight_abs_log_return) AS overnight_abs_log_return_sum,
                SUM(total_abs_log_return) AS total_abs_log_return_sum,
                CASE
                    WHEN SUM(total_abs_log_return) = 0 THEN NULL
                    ELSE SUM(intraday_abs_log_return) / SUM(total_abs_log_return)
                END AS intraday_share,
                CASE
                    WHEN SUM(total_abs_log_return) = 0 THEN NULL
                    ELSE SUM(overnight_abs_log_return) / SUM(total_abs_log_return)
                END AS overnight_share,
                AVG(intraday_log_return) AS mean_intraday_log_return,
                AVG(overnight_log_return) AS mean_overnight_log_return,
                AVG(intraday_abs_log_return) AS mean_intraday_abs_log_return,
                AVG(overnight_abs_log_return) AS mean_overnight_abs_log_return
            FROM stock_group_sessions
            GROUP BY
                stock_group,
                normalized_code,
                company_name,
                market_code,
                market_name,
                scale_category
            HAVING COUNT(*) >= ?
            """,
            [*params, min_session_count],
        ).fetchdf(),
    )
    if stock_metrics_df.empty:
        return pd.DataFrame(columns=_STOCK_METRICS_COLUMNS)

    stock_metrics_df["stock_group"] = _group_order_key(
        stock_metrics_df["stock_group"],
        selected_groups,
    )
    stock_metrics_df = stock_metrics_df.sort_values(
        by=[
            "stock_group",
            "overnight_share",
            "total_abs_log_return_sum",
            "code",
        ],
        ascending=[True, False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    stock_metrics_df["stock_group"] = stock_metrics_df["stock_group"].astype(str)
    return stock_metrics_df


def _query_daily_group_shares(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    selected_groups: Sequence[StockGroup],
    min_session_count: int,
) -> pd.DataFrame:
    cte_sql, params = _stock_group_sessions_cte(
        start_date=start_date,
        end_date=end_date,
        selected_groups=selected_groups,
    )
    daily_group_shares_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            {cte_sql}
            , eligible_stocks AS (
                SELECT
                    stock_group,
                    normalized_code
                FROM stock_group_sessions
                GROUP BY stock_group, normalized_code
                HAVING COUNT(*) >= ?
            )
            SELECT
                s.stock_group,
                s.date,
                COUNT(*) AS constituent_count,
                SUM(s.intraday_abs_log_return) AS intraday_abs_log_return_sum,
                SUM(s.overnight_abs_log_return) AS overnight_abs_log_return_sum,
                SUM(s.total_abs_log_return) AS total_abs_log_return_sum,
                CASE
                    WHEN SUM(s.total_abs_log_return) = 0 THEN NULL
                    ELSE SUM(s.intraday_abs_log_return) / SUM(s.total_abs_log_return)
                END AS intraday_share,
                CASE
                    WHEN SUM(s.total_abs_log_return) = 0 THEN NULL
                    ELSE SUM(s.overnight_abs_log_return) / SUM(s.total_abs_log_return)
                END AS overnight_share
            FROM stock_group_sessions s
            JOIN eligible_stocks e
              ON e.stock_group = s.stock_group
             AND e.normalized_code = s.normalized_code
            GROUP BY s.stock_group, s.date
            ORDER BY s.date, s.stock_group
            """,
            [*params, min_session_count],
        ).fetchdf(),
    )
    if daily_group_shares_df.empty:
        return pd.DataFrame(columns=_DAILY_GROUP_SHARE_COLUMNS)

    daily_group_shares_df["stock_group"] = _group_order_key(
        daily_group_shares_df["stock_group"],
        selected_groups,
    )
    daily_group_shares_df = daily_group_shares_df.sort_values(
        by=["date", "stock_group"],
        ascending=[True, True],
    ).reset_index(drop=True)
    daily_group_shares_df["stock_group"] = daily_group_shares_df["stock_group"].astype(
        str
    )
    return daily_group_shares_df


def _build_group_summary(
    stock_metrics_df: pd.DataFrame,
    *,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group in selected_groups:
        group_df = stock_metrics_df.loc[stock_metrics_df["stock_group"] == group]
        if group_df.empty:
            rows.append(
                {
                    "stock_group": group,
                    "stock_count": 0,
                    "share_defined_stock_count": 0,
                    "mean_session_count": 0.0,
                    "median_session_count": 0.0,
                    "mean_intraday_share": None,
                    "median_intraday_share": None,
                    "p25_intraday_share": None,
                    "p75_intraday_share": None,
                    "mean_overnight_share": None,
                    "median_overnight_share": None,
                    "p25_overnight_share": None,
                    "p75_overnight_share": None,
                    "mean_total_abs_log_return_sum": None,
                    "median_total_abs_log_return_sum": None,
                }
            )
            continue

        intraday_share = group_df["intraday_share"].dropna()
        overnight_share = group_df["overnight_share"].dropna()
        total_abs_sum = group_df["total_abs_log_return_sum"].dropna()
        session_count = group_df["session_count"]

        rows.append(
            {
                "stock_group": group,
                "stock_count": int(len(group_df)),
                "share_defined_stock_count": int(len(intraday_share)),
                "mean_session_count": float(session_count.mean()),
                "median_session_count": float(session_count.median()),
                "mean_intraday_share": float(intraday_share.mean())
                if not intraday_share.empty
                else None,
                "median_intraday_share": float(intraday_share.median())
                if not intraday_share.empty
                else None,
                "p25_intraday_share": float(intraday_share.quantile(0.25))
                if not intraday_share.empty
                else None,
                "p75_intraday_share": float(intraday_share.quantile(0.75))
                if not intraday_share.empty
                else None,
                "mean_overnight_share": float(overnight_share.mean())
                if not overnight_share.empty
                else None,
                "median_overnight_share": float(overnight_share.median())
                if not overnight_share.empty
                else None,
                "p25_overnight_share": float(overnight_share.quantile(0.25))
                if not overnight_share.empty
                else None,
                "p75_overnight_share": float(overnight_share.quantile(0.75))
                if not overnight_share.empty
                else None,
                "mean_total_abs_log_return_sum": float(total_abs_sum.mean())
                if not total_abs_sum.empty
                else None,
                "median_total_abs_log_return_sum": float(total_abs_sum.median())
                if not total_abs_sum.empty
                else None,
            }
        )

    summary_df = pd.DataFrame(rows)
    summary_df["stock_group"] = _group_order_key(summary_df["stock_group"], selected_groups)
    summary_df = summary_df.sort_values("stock_group").reset_index(drop=True)
    summary_df["stock_group"] = summary_df["stock_group"].astype(str)
    return summary_df


def _validate_selected_groups(selected_groups: Sequence[str] | None) -> tuple[StockGroup, ...]:
    if selected_groups is None:
        return STOCK_GROUP_ORDER

    allowed = set(STOCK_GROUP_ORDER)
    deduped_groups: list[StockGroup] = []
    for raw_group in selected_groups:
        group = raw_group.strip()
        if not group:
            continue
        if group not in allowed:
            raise ValueError(
                f"Unsupported stock group: {group}. Supported groups: {', '.join(STOCK_GROUP_ORDER)}"
            )
        typed_group = cast(StockGroup, group)
        if typed_group not in deduped_groups:
            deduped_groups.append(typed_group)

    if not deduped_groups:
        raise ValueError("selected_groups must contain at least one supported group")
    return tuple(deduped_groups)


def _validate_inputs(*, min_session_count: int) -> None:
    if min_session_count < 0:
        raise ValueError("min_session_count must be non-negative")


def get_stock_available_date_range(db_path: str) -> tuple[str | None, str | None]:
    """Return the available stock_data date range from market.duckdb."""
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="stock_data")


def run_stock_intraday_overnight_share_analysis(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    selected_groups: Sequence[str] | None = None,
    min_session_count: int = 60,
) -> StockIntradayOvernightShareResult:
    """
    Compute per-stock intraday / overnight absolute log-return shares.

    The shares are defined on analyzable sessions t where all of
    O_t, C_t, and O_{t+1} exist:

    - intraday_share = sum(|log(C_t / O_t)|) / sum(|log(C_t / O_t)| + |log(O_{t+1} / C_t)|)
    - overnight_share = sum(|log(O_{t+1} / C_t)|) / sum(|log(C_t / O_t)| + |log(O_{t+1} / C_t)|)
    """
    _validate_inputs(min_session_count=min_session_count)
    validated_groups = _validate_selected_groups(selected_groups)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start, available_end = _fetch_date_range(conn, table_name="stock_data")
        analysis_start, analysis_end = _query_analysis_date_range(
            conn,
            start_date=start_date,
            end_date=end_date,
            selected_groups=validated_groups,
        )
        stock_metrics_df = _query_stock_metrics(
            conn,
            start_date=start_date,
            end_date=end_date,
            selected_groups=validated_groups,
            min_session_count=min_session_count,
        )
        group_summary_df = _build_group_summary(
            stock_metrics_df,
            selected_groups=validated_groups,
        )
        daily_group_shares_df = _query_daily_group_shares(
            conn,
            start_date=start_date,
            end_date=end_date,
            selected_groups=validated_groups,
            min_session_count=min_session_count,
        )

    return StockIntradayOvernightShareResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start,
        available_end_date=available_end,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        selected_groups=validated_groups,
        min_session_count=min_session_count,
        stock_metrics_df=stock_metrics_df,
        group_summary_df=group_summary_df,
        daily_group_shares_df=daily_group_shares_df,
    )


def write_stock_intraday_overnight_share_research_bundle(
    result: StockIntradayOvernightShareResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_payload_research_bundle(
        experiment_id=STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_stock_intraday_overnight_share_analysis",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "selected_groups": list(result.selected_groups),
            "min_session_count": result.min_session_count,
        },
        result=result,
        split_result_payload=_split_result_payload,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_stock_intraday_overnight_share_research_bundle(
    bundle_path: str | Path,
) -> StockIntradayOvernightShareResult:
    return load_payload_research_bundle(
        bundle_path,
        build_result_from_payload=_build_result_from_payload,
    )


def get_stock_intraday_overnight_share_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_stock_intraday_overnight_share_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_result_payload(
    result: StockIntradayOvernightShareResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata = {
        "db_path": result.db_path,
        "source_mode": result.source_mode,
        "source_detail": result.source_detail,
        "available_start_date": result.available_start_date,
        "available_end_date": result.available_end_date,
        "analysis_start_date": result.analysis_start_date,
        "analysis_end_date": result.analysis_end_date,
        "selected_groups": list(result.selected_groups),
        "min_session_count": result.min_session_count,
    }
    tables = {
        "stock_metrics_df": result.stock_metrics_df,
        "group_summary_df": result.group_summary_df,
        "daily_group_shares_df": result.daily_group_shares_df,
    }
    return metadata, tables


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> StockIntradayOvernightShareResult:
    return StockIntradayOvernightShareResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        selected_groups=cast(
            tuple[StockGroup, ...],
            tuple(str(value) for value in metadata["selected_groups"]),
        ),
        min_session_count=int(metadata["min_session_count"]),
        stock_metrics_df=tables["stock_metrics_df"],
        group_summary_df=tables["group_summary_df"],
        daily_group_shares_df=tables["daily_group_shares_df"],
    )


def _build_research_bundle_summary_markdown(
    result: StockIntradayOvernightShareResult,
) -> str:
    summary_lines = [
        "# Stock Intraday / Overnight Share",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Selected groups: `{', '.join(result.selected_groups)}`",
        f"- Minimum sessions per stock: `{result.min_session_count}`",
        f"- Included stock rows: `{len(result.stock_metrics_df)}`",
        "",
        "## Current Read",
        "",
    ]
    strongest = result.group_summary_df[
        result.group_summary_df["mean_overnight_share"].notna()
    ].copy()
    if strongest.empty:
        summary_lines.append("- Group summary was empty after filtering.")
    else:
        strongest_row = strongest.sort_values(
            "mean_overnight_share",
            ascending=False,
        ).iloc[0]
        summary_lines.append(
            "- Highest mean overnight share group was "
            f"`{strongest_row['stock_group']}` at "
            f"`{float(strongest_row['mean_overnight_share']) * 100:.2f}%`."
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
