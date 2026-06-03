"""PIT-safe convergence study for free-float liquidity-implied price gaps."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
from src.shared.utils.market_code_alias import (
    expand_market_codes,
    normalize_market_scope,
)

FREE_FLOAT_LIQUIDITY_IMPLIED_PRICE_CONVERGENCE_EXPERIMENT_ID = (
    "market-behavior/free-float-liquidity-implied-price-convergence"
)
DEFAULT_ADV_WINDOWS: tuple[int, ...] = (60,)
DEFAULT_HORIZONS: tuple[int, ...] = (20, 60, 120, 250, 500)
DEFAULT_RECENT_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_OBSERVATION_STRIDE_SESSIONS = 20
DEFAULT_MIN_DAILY_REGRESSION_OBSERVATIONS = 300
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "observation_df",
    "convergence_by_gap_bucket_df",
    "convergence_by_regime_df",
    "daily_regression_diagnostics_df",
    "latest_extreme_gap_df",
)
_REGIME_ORDER: dict[str, int] = {
    "rerating_participation": 0,
    "distribution_stress": 1,
    "stale_liquidity": 2,
    "neutral": 3,
}
_GAP_BUCKET_ORDER: dict[str, int] = {
    "<=25%": 0,
    "25-50%": 1,
    "50-100%": 2,
    "100-200%": 3,
    ">200%": 4,
}


@dataclass(frozen=True)
class FreeFloatLiquidityImpliedPriceConvergenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    adv_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    recent_return_windows: tuple[int, ...]
    observation_stride_sessions: int
    min_daily_regression_observations: int
    feature_policy: str
    observation_df: pd.DataFrame
    convergence_by_gap_bucket_df: pd.DataFrame
    convergence_by_regime_df: pd.DataFrame
    daily_regression_diagnostics_df: pd.DataFrame
    latest_extreme_gap_df: pd.DataFrame


def run_free_float_liquidity_implied_price_convergence(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    adv_windows: Iterable[int] = DEFAULT_ADV_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    recent_return_windows: Iterable[int] = DEFAULT_RECENT_RETURN_WINDOWS,
    observation_stride_sessions: int = DEFAULT_OBSERVATION_STRIDE_SESSIONS,
    min_daily_regression_observations: int = DEFAULT_MIN_DAILY_REGRESSION_OBSERVATIONS,
) -> FreeFloatLiquidityImpliedPriceConvergenceResult:
    resolved_adv_windows = tuple(sorted({int(window) for window in adv_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_recent_return_windows = tuple(
        sorted({int(window) for window in recent_return_windows})
    )
    _validate_params(
        adv_windows=resolved_adv_windows,
        horizons=resolved_horizons,
        recent_return_windows=resolved_recent_return_windows,
        observation_stride_sessions=observation_stride_sessions,
        min_daily_regression_observations=min_daily_regression_observations,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    max_lookback = max((*resolved_adv_windows, *resolved_recent_return_windows))
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="free-float-liquidity-implied-price-convergence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        source_df = _query_prime_daily_source(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            adv_windows=resolved_adv_windows,
            recent_return_windows=resolved_recent_return_windows,
            max_lookback=max_lookback,
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
        topix_df = _query_topix_source(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    daily_panel, daily_regression_diagnostics_df = _build_daily_panel(
        source_df,
        adv_windows=resolved_adv_windows,
        recent_return_windows=resolved_recent_return_windows,
        min_daily_regression_observations=min_daily_regression_observations,
    )
    observation_df = _build_observation_df(
        daily_panel,
        topix_df,
        horizons=resolved_horizons,
        observation_stride_sessions=observation_stride_sessions,
    )
    convergence_by_gap_bucket_df = _build_convergence_summary_df(
        observation_df,
        horizons=resolved_horizons,
        group_columns=("adv_window", "gap_direction", "gap_abs_bucket"),
        extra_sort_columns=("gap_direction", "gap_abs_bucket"),
    )
    convergence_by_regime_df = _build_convergence_summary_df(
        observation_df,
        horizons=resolved_horizons,
        group_columns=("adv_window", "liquidity_regime", "gap_direction"),
        extra_sort_columns=("liquidity_regime", "gap_direction"),
    )
    latest_extreme_gap_df = _build_latest_extreme_gap_df(daily_panel)

    return FreeFloatLiquidityImpliedPriceConvergenceResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=_str_or_none(observation_df["date"].min())
        if "date" in observation_df
        else None,
        analysis_end_date=_str_or_none(observation_df["date"].max())
        if "date" in observation_df
        else None,
        adv_windows=resolved_adv_windows,
        horizons=resolved_horizons,
        recent_return_windows=resolved_recent_return_windows,
        observation_stride_sessions=int(observation_stride_sessions),
        min_daily_regression_observations=int(min_daily_regression_observations),
        feature_policy=(
            "Prime-only daily cross-sectional regression: each observation date fits "
            "log(ADV_N) ~ log(free_float_market_cap) using only same-date Prime rows. "
            "ADV_N, recent returns, free-float market cap, residuals, regimes, and "
            "liquidity-implied price are computed as of the observation close. Future "
            "close, future TOPIX close, and future implied-price rows are joined only "
            "for outcome measurement."
        ),
        observation_df=observation_df,
        convergence_by_gap_bucket_df=convergence_by_gap_bucket_df,
        convergence_by_regime_df=convergence_by_regime_df,
        daily_regression_diagnostics_df=daily_regression_diagnostics_df,
        latest_extreme_gap_df=latest_extreme_gap_df,
    )


def write_free_float_liquidity_implied_price_convergence_bundle(
    result: FreeFloatLiquidityImpliedPriceConvergenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FREE_FLOAT_LIQUIDITY_IMPLIED_PRICE_CONVERGENCE_EXPERIMENT_ID,
        module=__name__,
        function="run_free_float_liquidity_implied_price_convergence",
        params={
            "adv_windows": list(result.adv_windows),
            "horizons": list(result.horizons),
            "recent_return_windows": list(result.recent_return_windows),
            "observation_stride_sessions": result.observation_stride_sessions,
            "min_daily_regression_observations": result.min_daily_regression_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_free_float_liquidity_implied_price_convergence_bundle(
    bundle_path: str | Path,
) -> FreeFloatLiquidityImpliedPriceConvergenceResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FreeFloatLiquidityImpliedPriceConvergenceResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_free_float_liquidity_implied_price_convergence_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FREE_FLOAT_LIQUIDITY_IMPLIED_PRICE_CONVERGENCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_free_float_liquidity_implied_price_convergence_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FREE_FLOAT_LIQUIDITY_IMPLIED_PRICE_CONVERGENCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def build_summary_markdown(
    result: FreeFloatLiquidityImpliedPriceConvergenceResult,
) -> str:
    bucket = _top_rows_for_markdown(
        result.convergence_by_gap_bucket_df,
        sort_columns=["adv_window", "horizon", "gap_direction", "gap_abs_bucket"],
        ascending=[True, True, True, True],
        limit=60,
    )
    regime = _top_rows_for_markdown(
        result.convergence_by_regime_df,
        sort_columns=["adv_window", "horizon", "liquidity_regime", "gap_direction"],
        ascending=[True, True, True, True],
        limit=60,
    )
    diagnostics = _top_rows_for_markdown(
        result.daily_regression_diagnostics_df,
        sort_columns=["adv_window", "date"],
        ascending=[True, False],
        limit=20,
    )
    latest = _top_rows_for_markdown(
        result.latest_extreme_gap_df,
        sort_columns=["adv_window", "gap_direction", "liquidity_implied_price_gap_pct"],
        ascending=[True, True, False],
        limit=40,
    )
    return "\n".join(
        [
            "# Free-Float Liquidity-Implied Price Convergence",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- ADV windows: `{list(result.adv_windows)}`",
            f"- Horizons: `{list(result.horizons)}` sessions",
            f"- Observation stride: `{result.observation_stride_sessions}` sessions",
            f"- Min daily regression observations: `{result.min_daily_regression_observations}`",
            "",
            "## Feature Policy",
            "",
            result.feature_policy,
            "",
            "## Convergence By Gap Bucket",
            "",
            bucket,
            "",
            "## Convergence By Regime",
            "",
            regime,
            "",
            "## Daily Regression Diagnostics",
            "",
            diagnostics,
            "",
            "## Latest Extreme Gap Snapshot",
            "",
            latest,
            "",
        ]
    )


def _validate_params(
    *,
    adv_windows: Sequence[int],
    horizons: Sequence[int],
    recent_return_windows: Sequence[int],
    observation_stride_sessions: int,
    min_daily_regression_observations: int,
) -> None:
    if not adv_windows or any(window <= 1 for window in adv_windows):
        raise ValueError("adv_windows must contain integers greater than 1")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not recent_return_windows or any(
        window <= 0 for window in recent_return_windows
    ):
        raise ValueError("recent_return_windows must contain positive integers")
    if observation_stride_sessions <= 0:
        raise ValueError("observation_stride_sessions must be positive")
    if min_daily_regression_observations < 30:
        raise ValueError("min_daily_regression_observations must be at least 30")


def _assert_required_tables(conn: Any) -> None:
    missing = [
        table
        for table in ("stock_data", "statements", "topix_data")
        if not _table_exists(conn, table)
    ]
    if missing:
        raise RuntimeError(f"market.duckdb is missing required tables: {missing}")
    if not _table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stock_master_daily for PIT universe scope")


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


def _query_prime_daily_source(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    adv_windows: Sequence[int],
    recent_return_windows: Sequence[int],
    max_lookback: int,
    market_source: str,
) -> pd.DataFrame:
    normalized_code = normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    date_clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        lookback_start = (
            pd.Timestamp(start_date) - pd.Timedelta(days=max_lookback * 4 + 30)
        ).strftime("%Y-%m-%d")
        date_clauses.append("date >= ?")
        params.append(lookback_start)
    if end_date:
        date_clauses.append("date <= ?")
        params.append(end_date)
    date_sql = " AND " + " AND ".join(date_clauses) if date_clauses else ""
    prime_codes = tuple(expand_market_codes(["prime"]))
    prime_placeholders = ",".join("?" for _ in prime_codes)
    adv_columns = ",\n".join(
        [
            f"AVG(trading_value_jpy) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS adv_{window}_jpy,\n"
            f"COUNT(trading_value_jpy) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS adv_{window}_sessions"
            for window in adv_windows
        ]
    )
    recent_return_columns = ",\n".join(
        [
            f"LAG(close, {window}) OVER (PARTITION BY code ORDER BY date) AS prior_close_{window}d"
            for window in recent_return_windows
        ]
    )
    market_cte = _market_source_cte(market_source)
    market_join = _market_join_sql(market_source)
    df = conn.execute(
        f"""
        WITH {market_cte}
        prime_codes AS (
            SELECT DISTINCT code
            FROM market_source
            WHERE lower(trim(market_code)) IN ({prime_placeholders})
        ),
        price_base AS (
            SELECT *
            FROM (
                SELECT
                    p.code,
                    p.date,
                    p.close,
                    p.volume,
                    p.close * p.volume AS trading_value_jpy,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.code, p.date
                        ORDER BY p.prefer_rank
                    ) AS rn
                FROM (
                    SELECT
                        {normalized_code} AS code,
                        date,
                        close,
                        volume,
                        {prefer_4digit} AS prefer_rank
                    FROM stock_data
                    WHERE close > 0 AND volume >= 0 {date_sql}
                ) p
                JOIN prime_codes pc ON pc.code = p.code
            )
            WHERE rn = 1
        ),
        price_feature AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date) AS session_idx,
                {adv_columns},
                {recent_return_columns}
            FROM price_base
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
        enriched AS (
            SELECT
                p.*,
                st.shares_outstanding,
                st.treasury_shares,
                st.valid_from AS share_disclosed_date,
                NULL AS free_float_market_cap_jpy,
                m.company_name,
                m.market_code,
                m.market_name,
                m.sector_33_name
            FROM price_feature p
            LEFT JOIN statement_interval st
              ON st.code = p.code
             AND st.valid_from <= p.date
             AND (st.valid_to IS NULL OR p.date < st.valid_to)
            {market_join}
        )
        SELECT *
        FROM enriched
        WHERE shares_outstanding - COALESCE(treasury_shares, 0) > 0
        ORDER BY code, date
        """,
        [*prime_codes, *params],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    df["market_scope"] = [
        normalize_market_scope(code, market_name=name, default="unknown")
        for code, name in zip(df["market_code"], df["market_name"], strict=False)
    ]
    df = df[df["market_scope"] == "prime"].copy()
    if start_date:
        df = df[df["date"] >= start_date].copy()
    return df.reset_index(drop=True)


def _query_topix_source(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    conditions: list[str] = ["close > 0"]
    params: list[Any] = []
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    where_sql = " AND ".join(conditions)
    df = conn.execute(
        f"""
        SELECT date, close AS topix_close
        FROM topix_data
        WHERE {where_sql}
        ORDER BY date
        """,
        params,
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(columns=["date", "topix_close"])
    df["date"] = df["date"].astype(str)
    return df


def _market_source_cte(market_source: str) -> str:
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
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


def _market_join_sql(market_source: str) -> str:
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    return "LEFT JOIN market_source m ON m.code = p.code AND m.date = p.date"


def _build_daily_panel(
    source_df: pd.DataFrame,
    *,
    adv_windows: Sequence[int],
    recent_return_windows: Sequence[int],
    min_daily_regression_observations: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = _daily_panel_columns(recent_return_windows)
    diagnostics_columns = [
        "date",
        "adv_window",
        "observation_count",
        "intercept",
        "beta_log_free_float_market_cap",
        "r_squared",
        "residual_std",
    ]
    if source_df.empty:
        return (
            pd.DataFrame(columns=columns),
            pd.DataFrame(columns=diagnostics_columns),
        )
    records: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for adv_window in adv_windows:
        adv_column = f"adv_{adv_window}_jpy"
        count_column = f"adv_{adv_window}_sessions"
        frame = source_df.copy()
        frame[adv_column] = pd.to_numeric(frame[adv_column], errors="coerce")
        frame[count_column] = pd.to_numeric(frame[count_column], errors="coerce")
        frame["free_float_market_cap_jpy"] = pd.to_numeric(
            frame["free_float_market_cap_jpy"],
            errors="coerce",
        )
        frame = frame[
            (frame[adv_column] > 0)
            & (frame[count_column] >= adv_window)
            & (frame["free_float_market_cap_jpy"] > 0)
            & (pd.to_numeric(frame["close"], errors="coerce") > 0)
        ].copy()
        if frame.empty:
            continue
        frame["log_adv"] = np.log(frame[adv_column].astype(float))
        frame["log_free_float_market_cap"] = np.log(
            frame["free_float_market_cap_jpy"].astype(float)
        )
        for date, group in frame.groupby("date", sort=True):
            valid = (
                group[["log_adv", "log_free_float_market_cap", "code"]]
                .replace([np.inf, -np.inf], np.nan)
                .dropna()
            )
            if (
                len(valid) < min_daily_regression_observations
                or valid["log_free_float_market_cap"].nunique() < 2
            ):
                continue
            x = valid["log_free_float_market_cap"].to_numpy(dtype=float)
            y = valid["log_adv"].to_numpy(dtype=float)
            design = np.column_stack([np.ones(len(x)), x])
            intercept, beta = np.linalg.lstsq(design, y, rcond=None)[0]
            fitted = intercept + beta * x
            residuals = y - fitted
            residual_std = float(np.std(residuals, ddof=1))
            ss_res = float(np.sum(residuals**2))
            ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
            r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
            diagnostics.append(
                {
                    "date": str(date),
                    "adv_window": int(adv_window),
                    "observation_count": int(len(valid)),
                    "intercept": float(intercept),
                    "beta_log_free_float_market_cap": float(beta),
                    "r_squared": _round_float(r_squared, 6),
                    "residual_std": _round_float(residual_std, 6),
                }
            )
            if abs(float(beta)) <= 1e-12 or residual_std <= 0:
                continue
            rows = group.copy()
            rows["liquidity_residual"] = rows["log_adv"] - (
                float(intercept) + float(beta) * rows["log_free_float_market_cap"]
            )
            rows["liquidity_residual_z"] = rows["liquidity_residual"] / residual_std
            rows["liquidity_implied_free_float_market_cap_jpy"] = np.exp(
                (rows["log_adv"] - float(intercept)) / float(beta)
            )
            rows["liquidity_implied_price"] = rows["close"].astype(float) * (
                rows["liquidity_implied_free_float_market_cap_jpy"]
                / rows["free_float_market_cap_jpy"].astype(float)
            )
            rows["liquidity_implied_price_gap_pct"] = (
                rows["liquidity_implied_price"] / rows["close"].astype(float) - 1.0
            ) * 100.0
            for recent_window in recent_return_windows:
                prior_column = f"prior_close_{recent_window}d"
                rows[f"recent_return_{recent_window}d_pct"] = _return_pct_series(
                    rows["close"],
                    rows[prior_column],
                )
            rows["liquidity_regime"] = [
                _classify_liquidity_regime(
                    _to_float(residual_z),
                    _to_float(row.get("recent_return_20d_pct")),
                    _to_float(row.get("recent_return_60d_pct")),
                )
                for residual_z, row in zip(
                    rows["liquidity_residual_z"],
                    rows.to_dict(orient="records"),
                    strict=False,
                )
            ]
            rows["gap_direction"] = np.where(
                rows["liquidity_implied_price_gap_pct"] >= 0,
                "positive",
                "negative",
            )
            rows["gap_abs_bucket"] = [
                _gap_abs_bucket(value)
                for value in rows["liquidity_implied_price_gap_pct"]
            ]
            rows["adv_window"] = int(adv_window)
            rows["adv_jpy"] = rows[adv_column]
            rows["adv_mil_jpy"] = rows[adv_column] / 1_000_000.0
            rows["free_float_market_cap_bil_jpy"] = (
                rows["free_float_market_cap_jpy"] / 1_000_000_000.0
            )
            records.extend(
                {str(key): value for key, value in row.items()}
                for row in rows.reindex(columns=columns).to_dict(orient="records")
            )
    daily_panel = (
        pd.DataFrame.from_records(records, columns=columns)
        if records
        else pd.DataFrame(columns=columns)
    )
    diagnostics_df = (
        pd.DataFrame.from_records(diagnostics, columns=diagnostics_columns)
        if diagnostics
        else pd.DataFrame(columns=diagnostics_columns)
    )
    if daily_panel.empty:
        return daily_panel, diagnostics_df
    return (
        daily_panel.sort_values(
            ["adv_window", "code", "date"], kind="stable"
        ).reset_index(drop=True),
        diagnostics_df.sort_values(["adv_window", "date"], kind="stable").reset_index(
            drop=True
        ),
    )


def _build_observation_df(
    daily_panel: pd.DataFrame,
    topix_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    observation_stride_sessions: int,
) -> pd.DataFrame:
    base_columns = [
        *list(daily_panel.columns),
        *[
            item
            for horizon in horizons
            for item in (
                f"future_date_{horizon}d",
                f"future_close_{horizon}d",
                f"future_return_{horizon}d_pct",
                f"future_topix_return_{horizon}d_pct",
                f"future_excess_return_{horizon}d_pct",
                f"future_implied_price_{horizon}d",
                f"future_implied_price_gap_{horizon}d_pct",
                f"fixed_target_closure_{horizon}d_ratio",
                f"rolling_gap_abs_reduction_{horizon}d_pct",
                f"implied_price_change_{horizon}d_pct",
                f"direction_hit_{horizon}d",
            )
        ],
    ]
    if daily_panel.empty:
        return pd.DataFrame(columns=base_columns)
    topix_map = {
        str(row.date): _to_float(row.topix_close)
        for row in topix_df.itertuples(index=False)
    }
    frames: list[pd.DataFrame] = []
    for adv_window, window_frame in daily_panel.groupby("adv_window", sort=True):
        frame = window_frame.sort_values(["code", "date"], kind="stable").copy()
        frame["_session_idx"] = frame.groupby("code").cumcount() + 1
        for horizon in horizons:
            frame[f"future_date_{horizon}d"] = frame.groupby("code")["date"].shift(
                -horizon
            )
            frame[f"future_close_{horizon}d"] = frame.groupby("code")["close"].shift(
                -horizon
            )
            frame[f"future_implied_price_{horizon}d"] = frame.groupby("code")[
                "liquidity_implied_price"
            ].shift(-horizon)
            frame[f"future_implied_price_gap_{horizon}d_pct"] = frame.groupby("code")[
                "liquidity_implied_price_gap_pct"
            ].shift(-horizon)
            frame[f"future_return_{horizon}d_pct"] = _return_pct_series(
                frame[f"future_close_{horizon}d"],
                frame["close"],
            )
            current_topix = frame["date"].map(topix_map)
            future_topix = frame[f"future_date_{horizon}d"].map(topix_map)
            frame[f"future_topix_return_{horizon}d_pct"] = _return_pct_series(
                future_topix,
                current_topix,
            )
            frame[f"future_excess_return_{horizon}d_pct"] = (
                frame[f"future_return_{horizon}d_pct"]
                - frame[f"future_topix_return_{horizon}d_pct"]
            )
            denominator = frame["liquidity_implied_price"] - frame["close"]
            numerator = frame[f"future_close_{horizon}d"] - frame["close"]
            frame[f"fixed_target_closure_{horizon}d_ratio"] = np.where(
                denominator.abs() > 1e-12,
                numerator / denominator,
                np.nan,
            )
            current_abs_gap = frame["liquidity_implied_price_gap_pct"].abs()
            future_abs_gap = frame[f"future_implied_price_gap_{horizon}d_pct"].abs()
            frame[f"rolling_gap_abs_reduction_{horizon}d_pct"] = np.where(
                current_abs_gap > 1e-12,
                (current_abs_gap - future_abs_gap) / current_abs_gap * 100.0,
                np.nan,
            )
            frame[f"implied_price_change_{horizon}d_pct"] = _return_pct_series(
                frame[f"future_implied_price_{horizon}d"],
                frame["liquidity_implied_price"],
            )
            frame[f"direction_hit_{horizon}d"] = np.where(
                denominator.abs() > 1e-12,
                (np.sign(numerator) == np.sign(denominator)).astype(float),
                np.nan,
            )
        sampled = frame[(frame["_session_idx"] % observation_stride_sessions) == 0]
        frames.append(sampled.drop(columns=["_session_idx"]))
        _ = adv_window
    if not frames:
        return pd.DataFrame(columns=base_columns)
    return (
        pd.concat(frames, ignore_index=True)
        .reindex(columns=base_columns)
        .sort_values(["adv_window", "date", "code"], kind="stable")
        .reset_index(drop=True)
    )


def _build_convergence_summary_df(
    observation_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    group_columns: Sequence[str],
    extra_sort_columns: Sequence[str],
) -> pd.DataFrame:
    columns = [
        *group_columns,
        "horizon",
        "observation_count",
        "median_gap_pct",
        "median_abs_gap_pct",
        "mean_future_return_pct",
        "median_future_return_pct",
        "mean_future_excess_return_pct",
        "median_future_excess_return_pct",
        "mean_fixed_target_closure_ratio",
        "median_fixed_target_closure_ratio",
        "direction_hit_rate_pct",
        "median_rolling_gap_abs_reduction_pct",
        "median_implied_price_change_pct",
        "median_future_implied_gap_pct",
    ]
    if observation_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    for horizon in horizons:
        needed = [
            *group_columns,
            "liquidity_implied_price_gap_pct",
            f"future_return_{horizon}d_pct",
            f"future_excess_return_{horizon}d_pct",
            f"fixed_target_closure_{horizon}d_ratio",
            f"direction_hit_{horizon}d",
            f"rolling_gap_abs_reduction_{horizon}d_pct",
            f"implied_price_change_{horizon}d_pct",
            f"future_implied_price_gap_{horizon}d_pct",
        ]
        if any(column not in observation_df.columns for column in needed):
            continue
        scoped = (
            observation_df[needed]
            .replace([np.inf, -np.inf], np.nan)
            .dropna(subset=[f"future_return_{horizon}d_pct"])
        )
        if scoped.empty:
            continue
        for keys, group in scoped.groupby(list(group_columns), sort=False):
            key_tuple = keys if isinstance(keys, tuple) else (keys,)
            record: dict[str, Any] = dict(zip(group_columns, key_tuple, strict=True))
            record["horizon"] = int(horizon)
            record["observation_count"] = int(len(group))
            gap = pd.to_numeric(
                group["liquidity_implied_price_gap_pct"],
                errors="coerce",
            )
            record["median_gap_pct"] = _median(gap)
            record["median_abs_gap_pct"] = _median(gap.abs())
            future_return = pd.to_numeric(
                group[f"future_return_{horizon}d_pct"],
                errors="coerce",
            )
            future_excess = pd.to_numeric(
                group[f"future_excess_return_{horizon}d_pct"],
                errors="coerce",
            )
            closure = pd.to_numeric(
                group[f"fixed_target_closure_{horizon}d_ratio"],
                errors="coerce",
            )
            direction_hit = pd.to_numeric(
                group[f"direction_hit_{horizon}d"],
                errors="coerce",
            )
            record["mean_future_return_pct"] = _mean(future_return)
            record["median_future_return_pct"] = _median(future_return)
            record["mean_future_excess_return_pct"] = _mean(future_excess)
            record["median_future_excess_return_pct"] = _median(future_excess)
            record["mean_fixed_target_closure_ratio"] = _mean(closure)
            record["median_fixed_target_closure_ratio"] = _median(closure)
            record["direction_hit_rate_pct"] = _mean(direction_hit) * 100.0
            record["median_rolling_gap_abs_reduction_pct"] = _median(
                group[f"rolling_gap_abs_reduction_{horizon}d_pct"]
            )
            record["median_implied_price_change_pct"] = _median(
                group[f"implied_price_change_{horizon}d_pct"]
            )
            record["median_future_implied_gap_pct"] = _median(
                group[f"future_implied_price_gap_{horizon}d_pct"]
            )
            records.append(record)
    if not records:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame.from_records(records)
    sort_columns = ["adv_window", "horizon", *extra_sort_columns]
    for column in ("liquidity_regime", "gap_abs_bucket"):
        if column in frame.columns:
            order_map = (
                _REGIME_ORDER if column == "liquidity_regime" else _GAP_BUCKET_ORDER
            )
            frame[f"_{column}_order"] = frame[column].map(order_map).fillna(99)
            sort_columns = [
                f"_{column}_order" if item == column else item for item in sort_columns
            ]
    frame = frame.sort_values(sort_columns, kind="stable")
    return frame[[column for column in columns if column in frame.columns]].reset_index(
        drop=True
    )


def _build_latest_extreme_gap_df(daily_panel: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "adv_window",
        "gap_direction",
        "date",
        "code",
        "company_name",
        "sector_33_name",
        "close",
        "liquidity_implied_price",
        "liquidity_implied_price_gap_pct",
        "liquidity_residual_z",
        "liquidity_regime",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "adv_mil_jpy",
        "free_float_market_cap_bil_jpy",
    ]
    if daily_panel.empty:
        return pd.DataFrame(columns=columns)
    latest_date = str(daily_panel["date"].max())
    latest = daily_panel[daily_panel["date"].astype(str) == latest_date].copy()
    if latest.empty:
        return pd.DataFrame(columns=columns)
    latest["gap_direction"] = np.where(
        latest["liquidity_implied_price_gap_pct"] >= 0,
        "positive",
        "negative",
    )
    frames: list[pd.DataFrame] = []
    for _, group in latest.groupby(["adv_window", "gap_direction"], sort=True):
        ascending = bool(str(group["gap_direction"].iloc[0]) == "negative")
        frames.append(
            group.sort_values(
                "liquidity_implied_price_gap_pct",
                ascending=ascending,
                kind="stable",
            ).head(20)
        )
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True).reindex(columns=columns)


def _daily_panel_columns(recent_return_windows: Sequence[int]) -> list[str]:
    return [
        "date",
        "code",
        "company_name",
        "sector_33_name",
        "adv_window",
        "session_idx",
        "close",
        "volume",
        "adv_jpy",
        "adv_mil_jpy",
        "free_float_market_cap_jpy",
        "free_float_market_cap_bil_jpy",
        "shares_outstanding",
        "treasury_shares",
        "liquidity_residual",
        "liquidity_residual_z",
        "liquidity_implied_free_float_market_cap_jpy",
        "liquidity_implied_price",
        "liquidity_implied_price_gap_pct",
        *[f"recent_return_{window}d_pct" for window in recent_return_windows],
        "liquidity_regime",
        "gap_direction",
        "gap_abs_bucket",
    ]


def _return_pct_series(numerator: Any, denominator: Any) -> pd.Series:
    current = pd.to_numeric(pd.Series(numerator), errors="coerce")
    prior = pd.to_numeric(pd.Series(denominator), errors="coerce")
    return pd.Series(
        np.where(
            (current > 0) & (prior > 0),
            (current / prior - 1.0) * 100.0,
            np.nan,
        ),
        index=current.index,
        dtype="float64",
    )


def _classify_liquidity_regime(
    residual_z: float | None,
    recent_return_20d_pct: float | None,
    recent_return_60d_pct: float | None,
) -> str:
    if residual_z is None:
        return "neutral"
    returns = [recent_return_20d_pct, recent_return_60d_pct]
    valid_returns = [value for value in returns if value is not None]
    if residual_z >= 1.0 and len(valid_returns) == 2:
        if all(value >= 0 for value in valid_returns):
            return "rerating_participation"
        if any(value < 0 for value in valid_returns):
            return "distribution_stress"
    if residual_z <= -1.0:
        return "stale_liquidity"
    return "neutral"


def _gap_abs_bucket(value: Any) -> str:
    numeric = abs(_to_float(value) or np.nan)
    if not np.isfinite(numeric):
        return "unknown"
    if numeric <= 25:
        return "<=25%"
    if numeric <= 50:
        return "25-50%"
    if numeric <= 100:
        return "50-100%"
    if numeric <= 200:
        return "100-200%"
    return ">200%"


def _top_rows_for_markdown(
    df: pd.DataFrame,
    *,
    sort_columns: list[str],
    ascending: list[bool],
    limit: int,
) -> str:
    if df.empty:
        return "_No rows._"
    available_sort_columns = [column for column in sort_columns if column in df.columns]
    if available_sort_columns:
        ascending_values = ascending[: len(available_sort_columns)]
        frame = df.sort_values(
            available_sort_columns,
            ascending=ascending_values,
            kind="stable",
        ).head(limit)
    else:
        frame = df.head(limit)
    return _frame_to_markdown(frame)


def _frame_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(column) for column in df.columns]
    rows = [
        [_format_markdown_cell(row[column]) for column in df.columns]
        for _, row in df.iterrows()
    ]
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header, separator, *body])


def _format_markdown_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value).replace("|", "\\|")


def _to_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if np.isfinite(numeric) else None


def _mean(values: Any) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.mean()), 6) if not series.empty else np.nan


def _median(values: Any) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.median()), 6) if not series.empty else np.nan


def _round_float(value: Any, digits: int = 6) -> float:
    numeric = _to_float(value)
    return round(numeric, digits) if numeric is not None else np.nan


def _str_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)
