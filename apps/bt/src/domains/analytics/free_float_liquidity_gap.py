"""Free-float liquidity gap research over local market.duckdb."""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.free_float_liquidity_adjustment import (
    apply_adjusted_free_float_market_cap,
    load_adjustment_events_by_code,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.shared.utils.market_code_alias import normalize_market_scope

FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID = "market-behavior/free-float-liquidity-gap"
DEFAULT_ADV_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HORIZONS: tuple[int, ...] = (20, 60)
DEFAULT_CHANGE_WINDOW = 20
DEFAULT_OBSERVATION_STRIDE_SESSIONS = 20
DEFAULT_BUCKET_COUNT = 5
DEFAULT_MIN_REGRESSION_OBSERVATIONS = 30
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "observation_df",
    "market_regression_df",
    "residual_bucket_forward_return_df",
    "residual_change_bucket_forward_return_df",
    "market_sample_diagnostics_df",
)
_MARKET_ORDER: dict[str, int] = {
    "all": 0,
    "prime": 1,
    "standard": 2,
    "growth": 3,
    "unknown": 99,
}
_BUCKET_LABELS: dict[int, str] = {
    1: "low",
    2: "mid_low",
    3: "middle",
    4: "mid_high",
    5: "high",
}


@dataclass(frozen=True)
class FreeFloatLiquidityGapResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    adv_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    change_window: int
    observation_stride_sessions: int
    bucket_count: int
    min_regression_observations: int
    feature_policy: str
    observation_df: pd.DataFrame
    market_regression_df: pd.DataFrame
    residual_bucket_forward_return_df: pd.DataFrame
    residual_change_bucket_forward_return_df: pd.DataFrame
    market_sample_diagnostics_df: pd.DataFrame


def run_free_float_liquidity_gap_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    adv_windows: Iterable[int] = DEFAULT_ADV_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    change_window: int = DEFAULT_CHANGE_WINDOW,
    observation_stride_sessions: int = DEFAULT_OBSERVATION_STRIDE_SESSIONS,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    min_regression_observations: int = DEFAULT_MIN_REGRESSION_OBSERVATIONS,
) -> FreeFloatLiquidityGapResult:
    resolved_adv_windows = tuple(sorted({int(window) for window in adv_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    _validate_params(
        adv_windows=resolved_adv_windows,
        horizons=resolved_horizons,
        change_window=change_window,
        observation_stride_sessions=observation_stride_sessions,
        bucket_count=bucket_count,
        min_regression_observations=min_regression_observations,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="free-float-liquidity-gap-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = (
            "stock_master_daily_exact_date"
            if _table_exists(ctx.connection, "stock_master_daily")
            else "stocks_latest_fallback"
        )
        source_df = _query_observation_source(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            adv_windows=resolved_adv_windows,
            horizons=resolved_horizons,
            change_window=change_window,
            observation_stride_sessions=observation_stride_sessions,
            market_source=market_source,
        )
        adjustment_events_by_code = load_adjustment_events_by_code(
            ctx.connection,
            codes=sorted(source_df["code"].astype(str).unique().tolist())
            if not source_df.empty
            else [],
            end_date=str(source_df["date"].max()) if not source_df.empty else end_date,
        )
        source_df = apply_adjusted_free_float_market_cap(
            source_df,
            adjustment_events_by_code=adjustment_events_by_code,
        )
        if not source_df.empty:
            source_df["prior_free_float_market_cap_jpy"] = source_df[
                "free_float_market_cap_jpy"
            ]
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    observation_df = _build_observation_df(
        source_df,
        adv_windows=resolved_adv_windows,
        horizons=resolved_horizons,
        change_window=change_window,
    )
    market_regression_df = _build_market_regression_df(
        observation_df,
        min_observations=min_regression_observations,
    )
    observation_df = _apply_regression_residuals(observation_df, market_regression_df)
    residual_bucket_forward_return_df = _build_bucket_forward_return_df(
        observation_df,
        bucket_column="liquidity_residual_z",
        output_bucket_name="residual_bucket",
        horizons=resolved_horizons,
        bucket_count=bucket_count,
    )
    residual_change_bucket_forward_return_df = _build_bucket_forward_return_df(
        observation_df,
        bucket_column="liquidity_residual_change",
        output_bucket_name="residual_change_bucket",
        horizons=resolved_horizons,
        bucket_count=bucket_count,
    )
    market_sample_diagnostics_df = _build_market_sample_diagnostics_df(observation_df)

    return FreeFloatLiquidityGapResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(observation_df["date"].min())
        if "date" in observation_df
        else None,
        analysis_end_date=_str_or_none(observation_df["date"].max())
        if "date" in observation_df
        else None,
        adv_windows=resolved_adv_windows,
        horizons=resolved_horizons,
        change_window=int(change_window),
        observation_stride_sessions=int(observation_stride_sessions),
        bucket_count=int(bucket_count),
        min_regression_observations=int(min_regression_observations),
        feature_policy=(
            "ADV_N uses close*volume rolling mean through the observation close; "
            "free_float_market_cap uses the latest disclosed shares_outstanding and treasury_shares as of the observation date; "
            "market split uses stock_master_daily exact-date PIT rows when available; "
            "forward returns are close-to-close after the observation date; "
            "ADV is treated as liquidity/capacity diagnostic, not standalone alpha"
        ),
        observation_df=observation_df,
        market_regression_df=market_regression_df,
        residual_bucket_forward_return_df=residual_bucket_forward_return_df,
        residual_change_bucket_forward_return_df=residual_change_bucket_forward_return_df,
        market_sample_diagnostics_df=market_sample_diagnostics_df,
    )


def write_free_float_liquidity_gap_bundle(
    result: FreeFloatLiquidityGapResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID,
        module=__name__,
        function="run_free_float_liquidity_gap_research",
        params={
            "adv_windows": list(result.adv_windows),
            "horizons": list(result.horizons),
            "change_window": result.change_window,
            "observation_stride_sessions": result.observation_stride_sessions,
            "bucket_count": result.bucket_count,
            "min_regression_observations": result.min_regression_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_free_float_liquidity_gap_bundle(
    bundle_path: str | Path,
) -> FreeFloatLiquidityGapResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FreeFloatLiquidityGapResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_free_float_liquidity_gap_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_free_float_liquidity_gap_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def build_summary_markdown(result: FreeFloatLiquidityGapResult) -> str:
    diagnostics = _top_rows_for_markdown(
        result.market_sample_diagnostics_df,
        sort_columns=["market_scope", "adv_window"],
        ascending=[True, True],
        limit=18,
    )
    regression = _top_rows_for_markdown(
        result.market_regression_df,
        sort_columns=["market_scope", "adv_window"],
        ascending=[True, True],
        limit=18,
    )
    residual = _top_rows_for_markdown(
        result.residual_bucket_forward_return_df,
        sort_columns=["market_scope", "adv_window", "horizon", "residual_bucket_rank"],
        ascending=[True, True, True, True],
        limit=36,
    )
    change = _top_rows_for_markdown(
        result.residual_change_bucket_forward_return_df,
        sort_columns=[
            "market_scope",
            "adv_window",
            "horizon",
            "residual_change_bucket_rank",
        ],
        ascending=[True, True, True, True],
        limit=36,
    )
    return "\n".join(
        [
            "# Free-Float Liquidity Gap",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- ADV windows: `{list(result.adv_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Change window: `{result.change_window}` sessions",
            f"- Observation stride: `{result.observation_stride_sessions}` sessions",
            "",
            "## Market Sample Diagnostics",
            "",
            diagnostics,
            "",
            "## Market Regression",
            "",
            regression,
            "",
            "## Residual Bucket Forward Returns",
            "",
            residual,
            "",
            "## Residual Change Bucket Forward Returns",
            "",
            change,
            "",
        ]
    )


def _validate_params(
    *,
    adv_windows: Sequence[int],
    horizons: Sequence[int],
    change_window: int,
    observation_stride_sessions: int,
    bucket_count: int,
    min_regression_observations: int,
) -> None:
    if not adv_windows or any(window <= 1 for window in adv_windows):
        raise ValueError("adv_windows must contain integers greater than 1")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if change_window <= 0:
        raise ValueError("change_window must be positive")
    if observation_stride_sessions <= 0:
        raise ValueError("observation_stride_sessions must be positive")
    if bucket_count < 2:
        raise ValueError("bucket_count must be at least 2")
    if min_regression_observations < 3:
        raise ValueError("min_regression_observations must be at least 3")


def _assert_required_tables(conn: Any) -> None:
    missing = [
        table
        for table in ("stock_data", "statements", "topix_data")
        if not _table_exists(conn, table)
    ]
    if missing:
        raise RuntimeError(f"market.duckdb is missing required tables: {missing}")
    if not _table_exists(conn, "stocks") and not _table_exists(
        conn, "stock_master_daily"
    ):
        raise RuntimeError("market.duckdb requires stocks or stock_master_daily")


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _query_observation_source(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    adv_windows: Sequence[int],
    horizons: Sequence[int],
    change_window: int,
    observation_stride_sessions: int,
    market_source: str,
) -> pd.DataFrame:
    normalized_code = normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    date_clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        date_clauses.append("date >= ?")
        params.append(start_date)
    if end_date:
        date_clauses.append("date <= ?")
        params.append(end_date)
    date_sql = " AND " + " AND ".join(date_clauses) if date_clauses else ""
    adv_columns = ",\n".join(
        [
            f"AVG(trading_value_jpy) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS adv_{window}_jpy,\n"
            f"COUNT(trading_value_jpy) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS adv_{window}_sessions"
            for window in adv_windows
        ]
    )
    prior_adv_columns = ",\n".join(
        [
            f"LAG(adv_{window}_jpy, {change_window}) OVER (PARTITION BY code ORDER BY date) AS prior_adv_{window}_jpy"
            for window in adv_windows
        ]
    )
    forward_columns = ",\n".join(
        [
            f"LEAD(close, {horizon}) OVER (PARTITION BY code ORDER BY date) AS forward_close_{horizon}d"
            for horizon in horizons
        ]
    )
    topix_forward_columns = ",\n".join(
        [
            f"LEAD(close, {horizon}) OVER (ORDER BY date) AS forward_topix_close_{horizon}d"
            for horizon in horizons
        ]
    )
    market_cte = _market_source_cte(market_source)
    market_join = _market_join_sql(market_source)
    df = conn.execute(
        f"""
        WITH price_base AS (
            SELECT *
            FROM (
                SELECT
                    {normalized_code} AS code,
                    date,
                    close,
                    volume,
                    close * volume AS trading_value_jpy,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}, date
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM stock_data
                WHERE close > 0 AND volume >= 0 {date_sql}
            )
            WHERE rn = 1
        ),
        price_feature AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date) AS session_idx,
                {adv_columns},
                {forward_columns}
            FROM price_base
        ),
        price_lagged AS (
            SELECT
                *,
                {prior_adv_columns}
            FROM price_feature
        ),
        statement_base AS (
            SELECT *
            FROM (
                SELECT
                    {normalize_code_sql("code")} AS code,
                    disclosed_date,
                    shares_outstanding,
                    treasury_shares,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalize_code_sql("code")}, disclosed_date
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM statements
                WHERE shares_outstanding > 0
            )
            WHERE rn = 1
        ),
        statement_interval AS (
            SELECT
                code,
                disclosed_date AS valid_from,
                LEAD(disclosed_date) OVER (PARTITION BY code ORDER BY disclosed_date) AS valid_to,
                shares_outstanding,
                treasury_shares
            FROM statement_base
        ),
        topix_feature AS (
            SELECT
                date,
                close AS topix_close,
                {topix_forward_columns}
            FROM topix_data
            WHERE close > 0 {date_sql}
        ),
        {market_cte}
        sampled_price AS (
            SELECT *
            FROM price_lagged
            WHERE (session_idx % ?) = 0
        ),
        enriched AS (
            SELECT
                p.*,
                st.shares_outstanding,
                st.treasury_shares,
                st.valid_from AS share_disclosed_date,
                NULL AS free_float_market_cap_jpy,
                NULL AS prior_free_float_market_cap_jpy,
                t.topix_close,
                {", ".join(f"t.forward_topix_close_{horizon}d" for horizon in horizons)},
                m.company_name,
                m.market_code,
                m.market_name,
                m.sector_33_name
            FROM sampled_price p
            LEFT JOIN statement_interval st
              ON st.code = p.code
             AND st.valid_from <= p.date
             AND (st.valid_to IS NULL OR p.date < st.valid_to)
            LEFT JOIN topix_feature t ON t.date = p.date
            {market_join}
        )
        SELECT *
        FROM enriched
        ORDER BY code, date
        """,
        [*params, *params, observation_stride_sessions],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    df["market_scope"] = [
        normalize_market_scope(code, market_name=name, default="unknown")
        for code, name in zip(df["market_code"], df["market_name"], strict=False)
    ]
    return df


def _market_source_cte(market_source: str) -> str:
    if market_source == "stock_master_daily_exact_date":
        return f"""
        market_source AS (
            SELECT *
            FROM (
                SELECT
                    {normalize_code_sql("code")} AS code,
                    date,
                    company_name,
                    market_code,
                    market_name,
                    sector_33_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalize_code_sql("code")}, date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM stock_master_daily
            )
            WHERE rn = 1
        ),
        """
    return f"""
        market_source AS (
            SELECT *
            FROM (
                SELECT
                    {normalize_code_sql("code")} AS code,
                    NULL AS date,
                    company_name,
                    market_code,
                    market_name,
                    sector_33_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalize_code_sql("code")}
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM stocks
            )
            WHERE rn = 1
        ),
        """


def _market_join_sql(market_source: str) -> str:
    if market_source == "stock_master_daily_exact_date":
        return "LEFT JOIN market_source m ON m.code = p.code AND m.date = p.date"
    return "LEFT JOIN market_source m ON m.code = p.code"


def _build_observation_df(
    source_df: pd.DataFrame,
    *,
    adv_windows: Sequence[int],
    horizons: Sequence[int],
    change_window: int,
) -> pd.DataFrame:
    columns = _observation_columns(horizons)
    if source_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    for row in source_df.itertuples(index=False):
        free_float_cap = _to_float(getattr(row, "free_float_market_cap_jpy", None))
        if free_float_cap is None or free_float_cap <= 0:
            continue
        for adv_window in adv_windows:
            adv = _to_float(getattr(row, f"adv_{adv_window}_jpy", None))
            adv_sessions = _to_int(getattr(row, f"adv_{adv_window}_sessions", None))
            if adv is None or adv <= 0 or adv_sessions != adv_window:
                continue
            prior_adv = _to_float(getattr(row, f"prior_adv_{adv_window}_jpy", None))
            prior_cap = _to_float(getattr(row, "prior_free_float_market_cap_jpy", None))
            record: dict[str, Any] = {
                "code": str(row.code),
                "date": str(row.date),
                "company_name": getattr(row, "company_name", None),
                "market_scope": str(
                    getattr(row, "market_scope", "unknown") or "unknown"
                ),
                "market_code": getattr(row, "market_code", None),
                "market_name": getattr(row, "market_name", None),
                "sector_33_name": getattr(row, "sector_33_name", None),
                "adv_window": int(adv_window),
                "change_window": int(change_window),
                "close": _to_float(getattr(row, "close", None)),
                "volume": _to_float(getattr(row, "volume", None)),
                "adv_jpy": adv,
                "adv_mil_jpy": adv / 1_000_000.0,
                "prior_adv_jpy": prior_adv,
                "adv_log_change": _log_change(adv, prior_adv),
                "free_float_market_cap_jpy": free_float_cap,
                "free_float_market_cap_bil_jpy": free_float_cap / 1_000_000_000.0,
                "prior_free_float_market_cap_jpy": prior_cap,
                "shares_outstanding": _to_float(
                    getattr(row, "shares_outstanding", None)
                ),
                "treasury_shares": _to_float(getattr(row, "treasury_shares", None)),
                "free_float_ratio_pct": _free_float_ratio_pct(
                    _to_float(getattr(row, "shares_outstanding", None)),
                    _to_float(getattr(row, "treasury_shares", None)),
                ),
                "log_adv": math.log(adv),
                "log_free_float_market_cap": math.log(free_float_cap),
            }
            close = _to_float(getattr(row, "close", None))
            topix_close = _to_float(getattr(row, "topix_close", None))
            for horizon in horizons:
                forward_close = _to_float(
                    getattr(row, f"forward_close_{horizon}d", None)
                )
                forward_topix_close = _to_float(
                    getattr(row, f"forward_topix_close_{horizon}d", None)
                )
                forward_return = _return_pct(close, forward_close)
                topix_return = _return_pct(topix_close, forward_topix_close)
                record[f"forward_return_{horizon}d_pct"] = forward_return
                record[f"forward_topix_return_{horizon}d_pct"] = topix_return
                record[f"forward_excess_return_{horizon}d_pct"] = (
                    forward_return - topix_return
                    if _is_finite(forward_return) and _is_finite(topix_return)
                    else np.nan
                )
            records.append(record)
    if not records:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame.from_records(records, columns=columns)
        .sort_values(
            ["market_scope", "adv_window", "date", "code"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _build_market_regression_df(
    observation_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "adv_window",
        "observation_count",
        "code_count",
        "intercept",
        "beta_log_free_float_market_cap",
        "r_squared",
        "residual_std",
        "median_adv_mil_jpy",
        "median_free_float_market_cap_bil_jpy",
    ]
    if observation_df.empty:
        return pd.DataFrame(columns=columns)
    scoped = _expand_market_scope(observation_df)
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(["market_scope", "adv_window"], sort=False):
        market_scope, adv_window = keys
        valid = (
            group[
                [
                    "code",
                    "log_adv",
                    "log_free_float_market_cap",
                    "adv_mil_jpy",
                    "free_float_market_cap_bil_jpy",
                ]
            ]
            .replace(
                [np.inf, -np.inf],
                np.nan,
            )
            .dropna()
        )
        if (
            len(valid) < min_observations
            or valid["log_free_float_market_cap"].nunique() < 2
        ):
            continue
        x = valid["log_free_float_market_cap"].to_numpy(dtype=float)
        y = valid["log_adv"].to_numpy(dtype=float)
        x_matrix = np.column_stack([np.ones(len(x)), x])
        intercept, beta = np.linalg.lstsq(x_matrix, y, rcond=None)[0]
        fitted = intercept + beta * x
        residuals = y - fitted
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
        records.append(
            {
                "market_scope": str(market_scope),
                "adv_window": int(adv_window),
                "observation_count": int(len(valid)),
                "code_count": int(valid["code"].nunique()),
                "intercept": float(intercept),
                "beta_log_free_float_market_cap": float(beta),
                "r_squared": _round_float(r_squared, 6),
                "residual_std": _round_float(float(np.std(residuals, ddof=1)), 6)
                if len(residuals) > 1
                else np.nan,
                "median_adv_mil_jpy": _median(valid["adv_mil_jpy"]),
                "median_free_float_market_cap_bil_jpy": _median(
                    valid["free_float_market_cap_bil_jpy"]
                ),
            }
        )
    return _sort_market_frame(
        pd.DataFrame.from_records(records, columns=columns), ["adv_window"]
    )


def _apply_regression_residuals(
    observation_df: pd.DataFrame,
    market_regression_df: pd.DataFrame,
) -> pd.DataFrame:
    columns_to_add = [
        "liquidity_residual",
        "liquidity_residual_z",
        "liquidity_implied_free_float_market_cap_bil_jpy",
        "liquidity_implied_ffcap_gap_pct",
        "prior_liquidity_residual",
        "liquidity_residual_change",
    ]
    if observation_df.empty:
        result = observation_df.copy()
        for column in columns_to_add:
            result[column] = pd.Series(dtype="float64")
        return result
    model_map = {
        (str(row.market_scope), _to_int(row.adv_window) or 0): row
        for row in market_regression_df.itertuples(index=False)
    }
    records: list[dict[str, Any]] = []
    for row in observation_df.to_dict(orient="records"):
        model = model_map.get((str(row["market_scope"]), int(row["adv_window"])))
        if model is None:
            model = model_map.get(("all", int(row["adv_window"])))
        enriched: dict[str, Any] = {str(key): value for key, value in row.items()}
        if model is None:
            for column in columns_to_add:
                enriched[column] = np.nan
            records.append(enriched)
            continue
        intercept = _to_float(model.intercept) or 0.0
        beta = _to_float(model.beta_log_free_float_market_cap) or 0.0
        residual_std = _to_float(model.residual_std) or np.nan
        log_adv = _to_float(row.get("log_adv"))
        log_cap = _to_float(row.get("log_free_float_market_cap"))
        residual = (
            log_adv - (intercept + beta * log_cap)
            if log_adv is not None and log_cap is not None
            else np.nan
        )
        implied_log_cap = (
            (log_adv - intercept) / beta
            if log_adv is not None and abs(beta) > 1e-12
            else np.nan
        )
        prior_adv = _to_float(row.get("prior_adv_jpy"))
        prior_cap = _to_float(row.get("prior_free_float_market_cap_jpy"))
        prior_residual = (
            math.log(prior_adv) - (intercept + beta * math.log(prior_cap))
            if prior_adv is not None
            and prior_adv > 0
            and prior_cap is not None
            and prior_cap > 0
            else np.nan
        )
        enriched["liquidity_residual"] = _round_float(residual, 6)
        enriched["liquidity_residual_z"] = (
            _round_float(residual / residual_std, 6)
            if _is_finite(residual_std) and residual_std > 0
            else np.nan
        )
        enriched["liquidity_implied_free_float_market_cap_bil_jpy"] = (
            _round_float(math.exp(implied_log_cap) / 1_000_000_000.0, 6)
            if _is_finite(implied_log_cap)
            else np.nan
        )
        enriched["liquidity_implied_ffcap_gap_pct"] = (
            _round_float((math.exp(implied_log_cap - log_cap) - 1.0) * 100.0, 6)
            if _is_finite(implied_log_cap) and log_cap is not None
            else np.nan
        )
        enriched["prior_liquidity_residual"] = _round_float(prior_residual, 6)
        enriched["liquidity_residual_change"] = (
            _round_float(residual - prior_residual, 6)
            if _is_finite(residual) and _is_finite(prior_residual)
            else np.nan
        )
        records.append(enriched)
    return pd.DataFrame.from_records(records)


def _build_bucket_forward_return_df(
    observation_df: pd.DataFrame,
    *,
    bucket_column: str,
    output_bucket_name: str,
    horizons: Sequence[int],
    bucket_count: int,
) -> pd.DataFrame:
    rank_column = f"{output_bucket_name}_rank"
    label_column = output_bucket_name
    columns = [
        "market_scope",
        "adv_window",
        "horizon",
        rank_column,
        label_column,
        "observation_count",
        "code_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "win_rate_pct",
        "median_liquidity_residual_z",
        "median_liquidity_residual_change",
        "median_liquidity_implied_ffcap_gap_pct",
        "median_adv_log_change",
        "median_adv_mil_jpy",
        "median_free_float_market_cap_bil_jpy",
    ]
    if observation_df.empty:
        return pd.DataFrame(columns=columns)
    scoped = _expand_market_scope(observation_df)
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(["market_scope", "adv_window"], sort=False):
        market_scope, adv_window = keys
        valid = group[
            pd.to_numeric(group[bucket_column], errors="coerce").notna()
        ].copy()
        if valid.empty:
            continue
        valid[rank_column] = _bucket_rank(
            valid, bucket_column, bucket_count=bucket_count
        )
        for horizon in horizons:
            return_col = f"forward_return_{horizon}d_pct"
            excess_col = f"forward_excess_return_{horizon}d_pct"
            for bucket_rank, bucket_frame in valid.groupby(rank_column, sort=True):
                resolved_bucket_rank = _to_int(bucket_rank)
                if resolved_bucket_rank is None:
                    continue
                returns = pd.to_numeric(bucket_frame[return_col], errors="coerce")
                excess = pd.to_numeric(bucket_frame[excess_col], errors="coerce")
                records.append(
                    {
                        "market_scope": str(market_scope),
                        "adv_window": int(adv_window),
                        "horizon": int(horizon),
                        rank_column: resolved_bucket_rank,
                        label_column: _bucket_label(resolved_bucket_rank, bucket_count),
                        "observation_count": int(returns.notna().sum()),
                        "code_count": int(bucket_frame["code"].nunique()),
                        "mean_forward_return_pct": _mean(returns),
                        "median_forward_return_pct": _median(returns),
                        "mean_forward_excess_return_pct": _mean(excess),
                        "median_forward_excess_return_pct": _median(excess),
                        "win_rate_pct": _win_rate(returns),
                        "median_liquidity_residual_z": _median(
                            bucket_frame["liquidity_residual_z"]
                        ),
                        "median_liquidity_residual_change": _median(
                            bucket_frame["liquidity_residual_change"]
                        ),
                        "median_liquidity_implied_ffcap_gap_pct": _median(
                            bucket_frame["liquidity_implied_ffcap_gap_pct"]
                        ),
                        "median_adv_log_change": _median(
                            bucket_frame["adv_log_change"]
                        ),
                        "median_adv_mil_jpy": _median(bucket_frame["adv_mil_jpy"]),
                        "median_free_float_market_cap_bil_jpy": _median(
                            bucket_frame["free_float_market_cap_bil_jpy"]
                        ),
                    }
                )
    return _sort_market_frame(
        pd.DataFrame.from_records(records, columns=columns),
        ["adv_window", "horizon", rank_column],
    )


def _build_market_sample_diagnostics_df(observation_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "adv_window",
        "observation_count",
        "code_count",
        "start_date",
        "end_date",
        "median_adv_mil_jpy",
        "median_free_float_market_cap_bil_jpy",
        "median_free_float_ratio_pct",
        "missing_residual_count",
    ]
    if observation_df.empty:
        return pd.DataFrame(columns=columns)
    scoped = _expand_market_scope(observation_df)
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(["market_scope", "adv_window"], sort=False):
        market_scope, adv_window = keys
        records.append(
            {
                "market_scope": str(market_scope),
                "adv_window": int(adv_window),
                "observation_count": int(len(group)),
                "code_count": int(group["code"].nunique()),
                "start_date": _str_or_none(group["date"].min()),
                "end_date": _str_or_none(group["date"].max()),
                "median_adv_mil_jpy": _median(group["adv_mil_jpy"]),
                "median_free_float_market_cap_bil_jpy": _median(
                    group["free_float_market_cap_bil_jpy"]
                ),
                "median_free_float_ratio_pct": _median(group["free_float_ratio_pct"]),
                "missing_residual_count": int(
                    pd.to_numeric(group["liquidity_residual"], errors="coerce")
                    .isna()
                    .sum()
                ),
            }
        )
    return _sort_market_frame(
        pd.DataFrame.from_records(records, columns=columns), ["adv_window"]
    )


def _expand_market_scope(observation_df: pd.DataFrame) -> pd.DataFrame:
    if observation_df.empty:
        return observation_df.copy()
    actual = observation_df.copy()
    all_scope = observation_df.copy()
    all_scope["market_scope"] = "all"
    return pd.concat([all_scope, actual], ignore_index=True)


def _bucket_rank(df: pd.DataFrame, column: str, *, bucket_count: int) -> pd.Series:
    values = pd.to_numeric(df[column], errors="coerce")
    if values.notna().sum() == 0:
        return pd.Series(np.nan, index=df.index)
    pct = values.rank(pct=True, method="average")
    ranked = np.ceil(pct * bucket_count).clip(1, bucket_count)
    return cast(pd.Series, pd.Series(ranked, index=df.index).astype(int))


def _bucket_label(rank: int, bucket_count: int) -> str:
    if bucket_count == 5:
        return _BUCKET_LABELS.get(rank, str(rank))
    if rank == 1:
        return "low"
    if rank == bucket_count:
        return "high"
    return f"bucket_{rank}"


def _observation_columns(horizons: Sequence[int]) -> list[str]:
    base = [
        "code",
        "date",
        "company_name",
        "market_scope",
        "market_code",
        "market_name",
        "sector_33_name",
        "adv_window",
        "change_window",
        "close",
        "volume",
        "adv_jpy",
        "adv_mil_jpy",
        "prior_adv_jpy",
        "adv_log_change",
        "free_float_market_cap_jpy",
        "free_float_market_cap_bil_jpy",
        "prior_free_float_market_cap_jpy",
        "shares_outstanding",
        "treasury_shares",
        "free_float_ratio_pct",
        "log_adv",
        "log_free_float_market_cap",
        "liquidity_residual",
        "liquidity_residual_z",
        "liquidity_implied_free_float_market_cap_bil_jpy",
        "liquidity_implied_ffcap_gap_pct",
        "prior_liquidity_residual",
        "liquidity_residual_change",
    ]
    for horizon in horizons:
        base.extend(
            [
                f"forward_return_{horizon}d_pct",
                f"forward_topix_return_{horizon}d_pct",
                f"forward_excess_return_{horizon}d_pct",
            ]
        )
    return base


def _sort_market_frame(df: pd.DataFrame, extra_columns: Sequence[str]) -> pd.DataFrame:
    if df.empty or "market_scope" not in df:
        return df
    ordered = df.copy()
    ordered["_market_order"] = (
        ordered["market_scope"].map(_MARKET_ORDER).fillna(50).astype(int)
    )
    return (
        ordered.sort_values(["_market_order", *extra_columns], kind="stable")
        .drop(columns=["_market_order"])
        .reset_index(drop=True)
    )


def _top_rows_for_markdown(
    df: pd.DataFrame,
    *,
    sort_columns: Sequence[str],
    ascending: Sequence[bool],
    limit: int,
) -> str:
    if df.empty:
        return "_No rows._"
    frame = df.copy()
    existing_sort = [column for column in sort_columns if column in frame.columns]
    if existing_sort:
        frame = frame.sort_values(
            existing_sort,
            ascending=list(ascending)[: len(existing_sort)],
            kind="stable",
        )
    return _frame_to_markdown(frame.head(limit))


def _frame_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in df.itertuples(index=False, name=None):
        values = [_format_markdown_cell(value) for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _format_markdown_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        return str(_round_float(value, 6))
    return str(value).replace("|", "\\|")


def _return_pct(start: float | None, end: float | None) -> float:
    if start is None or end is None or start <= 0:
        return np.nan
    return (end / start - 1.0) * 100.0


def _log_change(current: float | None, previous: float | None) -> float:
    if current is None or previous is None or current <= 0 or previous <= 0:
        return np.nan
    return math.log(current) - math.log(previous)


def _free_float_ratio_pct(shares: float | None, treasury_shares: float | None) -> float:
    if shares is None or shares <= 0:
        return np.nan
    free_float = shares - (treasury_shares or 0.0)
    if free_float <= 0:
        return np.nan
    return free_float / shares * 100.0


def _to_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _mean(values: Iterable[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.mean())) if not series.empty else np.nan


def _median(values: Iterable[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.median())) if not series.empty else np.nan


def _win_rate(values: Iterable[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return (
        _round_float(float((series > 0).mean() * 100.0)) if not series.empty else np.nan
    )


def _round_float(value: Any, digits: int = 4) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return np.nan
    if not math.isfinite(numeric):
        return np.nan
    return round(numeric, digits)


def _is_finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _str_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)
