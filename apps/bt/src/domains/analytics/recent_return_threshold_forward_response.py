"""General recent-return threshold forward-response research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    _sort_summary_df,
    _str_or_none,
    _table_exists,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

RECENT_RETURN_THRESHOLD_FORWARD_RESPONSE_EXPERIMENT_ID = (
    "market-behavior/recent-return-threshold-forward-response"
)
DEFAULT_PRE_WINDOWS: tuple[int, ...] = (20, 60, 120, 150)
DEFAULT_LONG_TREND_WINDOWS: tuple[int, ...] = (120, 150)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20)
DEFAULT_20D_THRESHOLDS: tuple[float, ...] = (0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0)
DEFAULT_60D_THRESHOLDS: tuple[float, ...] = (
    0.0,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
    40.0,
    50.0,
)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth", "unknown")
_LIQUIDITY_SCOPE_ORDER: tuple[str, ...] = (
    "all_liquidity",
    "rerating_participation",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
    "missing",
)
_PERCENTILE_BUCKET_ORDER: tuple[str, ...] = (
    "top_10pct",
    "top_20pct",
    "middle_60pct",
    "bottom_20pct",
    "bottom_10pct",
    "missing",
)
_VALUATION_BUCKET_ORDER: tuple[str, ...] = (
    "cheapest_10pct",
    "cheapest_20pct",
    "middle_60pct",
    "expensive_20pct",
    "expensive_10pct",
)
_VALUATION_INTERACTION_BUCKET_ORDER: tuple[str, ...] = (
    "both_low",
    "low_pbr_only",
    "low_forward_per_only",
    "neither_low",
)
_LONG_TREND_QUADRANT_ORDER: tuple[str, ...] = (
    "persistent_rerating",
    "relief_bounce",
    "uptrend_pullback",
    "short_bounce",
)
_ENTRY_MODES: tuple[str, ...] = ("close_to_close", "next_open_to_close")
_SAMPLE_SCOPES: tuple[str, ...] = ("daily", "weekly", "monthly")


@dataclass(frozen=True)
class RecentReturnThresholdForwardResponseResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pre_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    thresholds_20d: tuple[float, ...]
    thresholds_60d: tuple[float, ...]
    long_trend_windows: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    threshold_response_df: pd.DataFrame
    joint_threshold_response_df: pd.DataFrame
    percentile_response_df: pd.DataFrame
    valuation_response_df: pd.DataFrame
    valuation_interaction_df: pd.DataFrame
    long_trend_quadrant_response_df: pd.DataFrame
    nonoverlap_response_df: pd.DataFrame
    annual_threshold_response_df: pd.DataFrame
    liquidity_interaction_df: pd.DataFrame


def run_recent_return_threshold_forward_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    pre_windows: Iterable[int] = DEFAULT_PRE_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    thresholds_20d: Sequence[float] = DEFAULT_20D_THRESHOLDS,
    thresholds_60d: Sequence[float] = DEFAULT_60D_THRESHOLDS,
    long_trend_windows: Iterable[int] = DEFAULT_LONG_TREND_WINDOWS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RecentReturnThresholdForwardResponseResult:
    resolved_long_trend_windows = tuple(
        sorted({int(window) for window in long_trend_windows})
    )
    resolved_pre_windows = tuple(
        sorted({int(window) for window in pre_windows} | set(resolved_long_trend_windows))
    )
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_thresholds_20d = _normalize_thresholds(
        thresholds_20d, name="thresholds_20d"
    )
    resolved_thresholds_60d = _normalize_thresholds(
        thresholds_60d, name="thresholds_60d"
    )
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
    _validate_params(
        pre_windows=resolved_pre_windows,
        long_trend_windows=resolved_long_trend_windows,
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = _offset_calendar_date(
        start_date, days=-(max(resolved_pre_windows) * 4 + 30)
    )
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="recent-return-threshold-forward-response-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        _create_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            pre_windows=resolved_pre_windows,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM recent_return_threshold_panel"
            ).fetchone()[0]
        )
        coverage_diagnostics_df = _build_coverage_diagnostics_df(ctx.connection)
        threshold_response_df = _build_threshold_response_df(
            ctx.connection,
            pre_windows=resolved_pre_windows,
            horizons=resolved_horizons,
            thresholds_by_window={
                20: resolved_thresholds_20d,
                60: resolved_thresholds_60d,
            },
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        joint_threshold_response_df = _build_joint_threshold_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            thresholds_20d=resolved_thresholds_20d,
            thresholds_60d=resolved_thresholds_60d,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        percentile_response_df = _build_percentile_response_df(
            ctx.connection,
            pre_windows=resolved_pre_windows,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        valuation_response_df = _build_valuation_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        valuation_interaction_df = _build_valuation_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        long_trend_quadrant_response_df = _build_long_trend_quadrant_response_df(
            ctx.connection,
            long_trend_windows=resolved_long_trend_windows,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            sample_scope="daily",
        )
        nonoverlap_response_df = _build_nonoverlap_response_df(
            ctx.connection,
            pre_windows=resolved_pre_windows,
            horizons=resolved_horizons,
            thresholds_by_window={
                20: resolved_thresholds_20d,
                60: resolved_thresholds_60d,
            },
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        annual_threshold_response_df = _build_annual_threshold_response_df(
            ctx.connection,
            pre_windows=resolved_pre_windows,
            horizons=resolved_horizons,
            thresholds_by_window={
                20: resolved_thresholds_20d,
                60: resolved_thresholds_60d,
            },
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        liquidity_interaction_df = _build_liquidity_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        observation_sample_df = _query_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return RecentReturnThresholdForwardResponseResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(observation_sample_df["date"].min())
        if "date" in observation_sample_df and not observation_sample_df.empty
        else start_date,
        analysis_end_date=end_date,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        thresholds_20d=resolved_thresholds_20d,
        thresholds_60d=resolved_thresholds_60d,
        long_trend_windows=resolved_long_trend_windows,
        market_scopes=resolved_market_scopes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_count=observation_count,
        observation_sample_df=observation_sample_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
        threshold_response_df=threshold_response_df,
        joint_threshold_response_df=joint_threshold_response_df,
        percentile_response_df=percentile_response_df,
        valuation_response_df=valuation_response_df,
        valuation_interaction_df=valuation_interaction_df,
        long_trend_quadrant_response_df=long_trend_quadrant_response_df,
        nonoverlap_response_df=nonoverlap_response_df,
        annual_threshold_response_df=annual_threshold_response_df,
        liquidity_interaction_df=liquidity_interaction_df,
    )


def write_recent_return_threshold_forward_response_bundle(
    result: RecentReturnThresholdForwardResponseResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RECENT_RETURN_THRESHOLD_FORWARD_RESPONSE_EXPERIMENT_ID,
        module=__name__,
        function="run_recent_return_threshold_forward_response_research",
        params={
            "pre_windows": list(result.pre_windows),
            "horizons": list(result.horizons),
            "thresholds_20d": list(result.thresholds_20d),
            "thresholds_60d": list(result.thresholds_60d),
            "long_trend_windows": list(result.long_trend_windows),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "observation_sample_count": int(len(result.observation_sample_df)),
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "threshold_response_df": result.threshold_response_df,
            "joint_threshold_response_df": result.joint_threshold_response_df,
            "percentile_response_df": result.percentile_response_df,
            "valuation_response_df": result.valuation_response_df,
            "valuation_interaction_df": result.valuation_interaction_df,
            "long_trend_quadrant_response_df": result.long_trend_quadrant_response_df,
            "nonoverlap_response_df": result.nonoverlap_response_df,
            "annual_threshold_response_df": result.annual_threshold_response_df,
            "liquidity_interaction_df": result.liquidity_interaction_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RecentReturnThresholdForwardResponseResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=24)
    thresholds = _top_rows_for_markdown(
        result.threshold_response_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "pre_window",
            "direction",
            "threshold_pct",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    joint = _top_rows_for_markdown(
        result.joint_threshold_response_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "threshold_20d_pct",
            "threshold_60d_pct",
            "entry_mode",
            "horizon",
        ],
        limit=60,
    )
    percentile = _top_rows_for_markdown(
        result.percentile_response_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "pre_window",
            "percentile_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=60,
    )
    valuation = _top_rows_for_markdown(
        result.valuation_response_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "valuation_feature",
            "valuation_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    valuation_interaction = _top_rows_for_markdown(
        result.valuation_interaction_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "interaction_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=60,
    )
    long_trend_quadrant = _top_rows_for_markdown(
        result.long_trend_quadrant_response_df,
        sort_columns=[
            "trend_window",
            "trend_quadrant_order",
            "entry_mode",
            "horizon",
            "market_scope",
            "liquidity_scope",
        ],
        limit=80,
    )
    nonoverlap = _top_rows_for_markdown(
        result.nonoverlap_response_df,
        sort_columns=[
            "sample_scope",
            "market_scope",
            "pre_window",
            "direction",
            "threshold_pct",
            "entry_mode",
            "horizon",
        ],
        limit=60,
    )
    liquidity = _top_rows_for_markdown(
        result.liquidity_interaction_df,
        sort_columns=[
            "market_scope",
            "liquidity_scope",
            "momentum_state",
            "entry_mode",
            "horizon",
        ],
        limit=60,
    )
    return "\n".join(
        [
            "# Recent Return Threshold Forward Response",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Observation count: `{result.observation_count}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- 20d thresholds: `{list(result.thresholds_20d)}`",
            f"- 60d thresholds: `{list(result.thresholds_60d)}`",
            f"- Long trend windows: `{list(result.long_trend_windows)}`",
            f"- Market scopes: `{list(result.market_scopes)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Coverage Diagnostics",
            "",
            coverage,
            "",
            "## Threshold Response",
            "",
            thresholds,
            "",
            "## Joint Threshold Response",
            "",
            joint,
            "",
            "## Percentile Response",
            "",
            percentile,
            "",
            "## Valuation Response",
            "",
            valuation,
            "",
            "## Valuation Interaction",
            "",
            valuation_interaction,
            "",
            "## Long Trend Quadrant Response",
            "",
            long_trend_quadrant,
            "",
            "## Non-Overlap Response",
            "",
            nonoverlap,
            "",
            "## Liquidity Interaction",
            "",
            liquidity,
            "",
        ]
    )


def _validate_params(
    *,
    pre_windows: Sequence[int],
    long_trend_windows: Sequence[int],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not pre_windows or any(window <= 0 for window in pre_windows):
        raise ValueError("pre_windows must be positive")
    if not long_trend_windows or any(window <= 0 for window in long_trend_windows):
        raise ValueError("long_trend_windows must be positive")
    unsupported_long_windows = [
        window for window in long_trend_windows if window not in DEFAULT_LONG_TREND_WINDOWS
    ]
    if unsupported_long_windows:
        raise ValueError("long_trend_windows currently supports only 120 and 150")
    missing_required = [window for window in (20, 60) if window not in pre_windows]
    if missing_required:
        raise ValueError("pre_windows must include 20 and 60 for quadrant analysis")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _normalize_thresholds(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    normalized = tuple(sorted({float(value) for value in values}))
    if not normalized or any(
        not math.isfinite(value) or value < 0.0 for value in normalized
    ):
        raise ValueError(f"{name} must contain finite non-negative values")
    return normalized


def _normalize_market_scopes(values: Sequence[str]) -> tuple[str, ...]:
    allowed = set(_MARKET_SCOPE_ORDER)
    normalized = tuple(
        dict.fromkeys(
            str(value).strip().lower() for value in values if str(value).strip()
        )
    )
    if not normalized:
        raise ValueError("market_scopes must not be empty")
    invalid = [value for value in normalized if value not in allowed]
    if invalid:
        raise ValueError(f"unsupported market_scopes: {', '.join(invalid)}")
    return normalized


def _assert_required_tables(conn: Any) -> None:
    required = ("stock_data", "topix_data", "statements")
    missing = [table for table in required if not _table_exists(conn, table)]
    if missing:
        raise RuntimeError(
            f"market.duckdb missing required tables: {', '.join(missing)}"
        )
    if not _table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stock_master_daily for PIT universe scope")


def _create_daily_valuation_view(conn: Any) -> None:
    if not _table_exists(conn, "daily_valuation"):
        conn.execute(
            """
            CREATE OR REPLACE TEMP VIEW recent_return_daily_valuation AS
            SELECT
                NULL::VARCHAR AS code,
                NULL::VARCHAR AS date,
                NULL::DOUBLE AS per,
                NULL::DOUBLE AS forward_per,
                NULL::DOUBLE AS p_op,
                NULL::DOUBLE AS forward_p_op,
                NULL::DOUBLE AS pbr,
                NULL::DOUBLE AS market_cap
            WHERE FALSE
            """
        )
        return
    valuation_code = normalize_code_sql("dv.code")
    daily_p_op_expr = _optional_daily_valuation_double_expr(conn, "p_op")
    daily_forward_p_op_expr = _optional_daily_valuation_double_expr(conn, "forward_p_op")
    daily_pbr_expr = _optional_daily_valuation_double_expr(conn, "pbr")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW recent_return_daily_valuation AS
        SELECT code, date, per, forward_per, p_op, forward_p_op, pbr, market_cap
        FROM (
            SELECT
                {valuation_code} AS code,
                dv.date,
                CAST(dv.per AS DOUBLE) AS per,
                CAST(dv.forward_per AS DOUBLE) AS forward_per,
                {daily_p_op_expr} AS p_op,
                {daily_forward_p_op_expr} AS forward_p_op,
                {daily_pbr_expr} AS pbr,
                CAST(dv.market_cap AS DOUBLE) AS market_cap,
                row_number() OVER (
                    PARTITION BY {valuation_code}, dv.date
                    ORDER BY dv.price_basis_date DESC NULLS LAST,
                             dv.basis_version DESC NULLS LAST,
                             CASE WHEN length(dv.code) = 4 THEN 0 ELSE 1 END,
                             dv.code
                ) AS row_rank
            FROM daily_valuation dv
        )
        WHERE row_rank = 1
        """
    )


def _optional_statement_double_expr(conn: Any, column: str) -> str:
    if _statement_column_exists(conn, column):
        return f"CAST(st.{column} AS DOUBLE)"
    return "CAST(NULL AS DOUBLE)"


def _optional_statement_text_expr(conn: Any, column: str) -> str:
    if _statement_column_exists(conn, column):
        return f"CAST(st.{column} AS VARCHAR)"
    return "CAST(NULL AS VARCHAR)"


def _optional_daily_valuation_double_expr(conn: Any, column: str) -> str:
    if _daily_valuation_column_exists(conn, column):
        return f"CAST(dv.{column} AS DOUBLE)"
    return "CAST(NULL AS DOUBLE)"


def _statement_column_exists(conn: Any, column: str) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM pragma_table_info('statements') WHERE name = ?",
            [column],
        ).fetchone()[0]
    )


def _daily_valuation_column_exists(conn: Any, column: str) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM pragma_table_info('daily_valuation') WHERE name = ?",
            [column],
        ).fetchone()[0]
    )


def _create_observation_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    market_source: str,
    market_scopes: Sequence[str],
) -> None:
    _create_daily_valuation_view(conn)
    price_code = normalize_code_sql("sd.code")
    master_code = (
        normalize_code_sql("smd.code")
        if market_source == "stock_master_daily_exact_date"
        else normalize_code_sql("s.code")
    )
    statement_code = normalize_code_sql("st.code")
    statement_period_expr = _optional_statement_text_expr(
        conn, "type_of_current_period"
    )
    statement_document_expr = _optional_statement_text_expr(conn, "type_of_document")
    operating_profit_expr = _optional_statement_double_expr(conn, "operating_profit")
    forecast_operating_profit_expr = _optional_statement_double_expr(
        conn,
        "forecast_operating_profit",
    )
    next_year_forecast_operating_profit_expr = _optional_statement_double_expr(
        conn,
        "next_year_forecast_operating_profit",
    )
    lag_exprs = ",\n                ".join(
        f"lag(close, {window}) over (partition by code order by date) as close_lag_{window}d"
        for window in pre_windows
    )
    forward_exprs = ",\n                ".join(
        [
            "lead(open, 1) over (partition by code order by date) as next_open",
            *[
                f"lead(close, {horizon}) over (partition by code order by date) as future_close_{horizon}d"
                for horizon in horizons
            ],
            *[
                f"lead(date, {horizon}) over (partition by code order by date) as future_date_{horizon}d"
                for horizon in horizons
            ],
        ]
    )
    recent_exprs = ",\n            ".join(
        f"case when close_lag_{window}d > 0 then (close / close_lag_{window}d - 1.0) * 100.0 end "
        f"as recent_return_{window}d_pct"
        for window in pre_windows
    )
    return_exprs = ",\n            ".join(
        [
            *[
                f"case when close > 0 and future_close_{horizon}d > 0 then "
                f"(future_close_{horizon}d / close - 1.0) * 100.0 end "
                f"as forward_close_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"case when next_open > 0 and future_close_{horizon}d > 0 then "
                f"(future_close_{horizon}d / next_open - 1.0) * 100.0 end "
                f"as forward_next_open_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    topix_forward_exprs = ",\n                ".join(
        [
            "lead(open, 1) over (order by date) as topix_next_open",
            *[
                f"lead(close, {horizon}) over (order by date) as topix_future_close_{horizon}d"
                for horizon in horizons
            ],
        ]
    )
    topix_return_exprs = ",\n            ".join(
        [
            *[
                f"case when topix_close > 0 and topix_future_close_{horizon}d > 0 then "
                f"(topix_future_close_{horizon}d / topix_close - 1.0) * 100.0 end "
                f"as topix_close_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"case when topix_next_open > 0 and topix_future_close_{horizon}d > 0 then "
                f"(topix_future_close_{horizon}d / topix_next_open - 1.0) * 100.0 end "
                f"as topix_next_open_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    excess_exprs = ",\n            ".join(
        [
            *[
                f"forward_close_return_{horizon}d_pct - topix_close_return_{horizon}d_pct "
                f"as forward_close_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"forward_next_open_return_{horizon}d_pct - topix_next_open_return_{horizon}d_pct "
                f"as forward_next_open_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    raw_conditions: list[str] = []
    raw_params: list[str] = []
    if query_start is not None:
        raw_conditions.append("sd.date >= ?")
        raw_params.append(query_start)
    if query_end is not None:
        raw_conditions.append("sd.date <= ?")
        raw_params.append(query_end)
    raw_where = "" if not raw_conditions else "WHERE " + " AND ".join(raw_conditions)
    final_conditions: list[str] = []
    final_params: list[str] = []
    if analysis_start_date is not None:
        final_conditions.append("date >= ?")
        final_params.append(analysis_start_date)
    if analysis_end_date is not None:
        final_conditions.append("date <= ?")
        final_params.append(analysis_end_date)
    final_where = (
        "" if not final_conditions else "WHERE " + " AND ".join(final_conditions)
    )
    market_filter = (
        "TRUE"
        if "all" in market_scopes
        else f"m.market IN ({_sql_string_list(market_scopes)})"
    )
    master_cte = _market_master_cte(
        market_source=market_source, master_code=master_code
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE recent_return_threshold_panel AS
        WITH raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.open,
                sd.close,
                sd.volume,
                row_number() OVER (
                    PARTITION BY {price_code}, sd.date
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM stock_data sd
            {raw_where}
        ),
        prices AS (
            SELECT code, date, open, close, volume
            FROM raw_prices
            WHERE row_rank = 1
              AND open > 0
              AND close > 0
        ),
        {master_cte},
        statement_base AS (
            SELECT *
            FROM (
                SELECT
                    {statement_code} AS code,
                    st.disclosed_date,
                    {statement_period_expr} AS type_of_current_period,
                    {statement_document_expr} AS type_of_document,
                    st.shares_outstanding,
                    st.treasury_shares,
                    {operating_profit_expr} AS operating_profit,
                    {forecast_operating_profit_expr} AS forecast_operating_profit,
                    {next_year_forecast_operating_profit_expr}
                        AS next_year_forecast_operating_profit,
                    row_number() OVER (
                        PARTITION BY {statement_code}, st.disclosed_date
                        ORDER BY CASE WHEN length(st.code) = 4 THEN 0 ELSE 1 END, st.code
                    ) AS row_rank
                FROM statements st
            )
            WHERE row_rank = 1
        ),
        share_interval AS (
            SELECT
                code,
                disclosed_date AS share_disclosed_date,
                lead(disclosed_date) OVER (PARTITION BY code ORDER BY disclosed_date) AS valid_to,
                shares_outstanding,
                treasury_shares
            FROM statement_base
            WHERE shares_outstanding > 0
        ),
        operating_profit_interval AS (
            SELECT
                code,
                disclosed_date AS operating_profit_disclosed_date,
                lead(disclosed_date) OVER (PARTITION BY code ORDER BY disclosed_date) AS valid_to,
                operating_profit
            FROM statement_base
            WHERE upper(coalesce(type_of_current_period, '')) = 'FY'
              AND operating_profit > 0
        ),
        forecast_operating_profit_interval AS (
            SELECT
                code,
                disclosed_date AS forecast_operating_profit_disclosed_date,
                lead(disclosed_date) OVER (PARTITION BY code ORDER BY disclosed_date) AS valid_to,
                CASE
                    WHEN next_year_forecast_operating_profit > 0
                        THEN next_year_forecast_operating_profit
                    WHEN forecast_operating_profit > 0 THEN forecast_operating_profit
                END AS forecast_operating_profit
            FROM statement_base
            WHERE coalesce(next_year_forecast_operating_profit, forecast_operating_profit) > 0
        ),
        scoped AS (
            SELECT
                p.*,
                m.company_name,
                m.market,
                m.market_code,
                m.scale_category,
                share.share_disclosed_date,
                share.shares_outstanding,
                share.treasury_shares,
                op.operating_profit,
                fop.forecast_operating_profit,
                op.operating_profit_disclosed_date,
                fop.forecast_operating_profit_disclosed_date,
                dv.per,
                dv.forward_per,
                dv.pbr,
                CASE
                    WHEN dv.market_cap > 0 THEN dv.market_cap / 1000000000.0
                    WHEN p.close > 0 AND share.shares_outstanding > 0
                        THEN p.close * share.shares_outstanding / 1000000000.0
                END AS market_cap_bil_jpy,
                CASE
                    WHEN dv.p_op > 0 THEN dv.p_op
                    WHEN coalesce(dv.market_cap, p.close * share.shares_outstanding) > 0
                     AND op.operating_profit > 0
                        THEN coalesce(dv.market_cap, p.close * share.shares_outstanding)
                             / op.operating_profit
                END AS p_op,
                CASE
                    WHEN dv.forward_p_op > 0 THEN dv.forward_p_op
                    WHEN coalesce(dv.market_cap, p.close * share.shares_outstanding) > 0
                     AND fop.forecast_operating_profit > 0
                        THEN coalesce(dv.market_cap, p.close * share.shares_outstanding)
                             / fop.forecast_operating_profit
                END AS forward_p_op
            FROM prices p
            JOIN market_master m ON m.code = p.code AND m.date = p.date
            LEFT JOIN share_interval share
              ON share.code = p.code
             AND share.share_disclosed_date <= p.date
             AND (share.valid_to IS NULL OR p.date < share.valid_to)
            LEFT JOIN operating_profit_interval op
              ON op.code = p.code
             AND op.operating_profit_disclosed_date <= p.date
             AND (op.valid_to IS NULL OR p.date < op.valid_to)
            LEFT JOIN forecast_operating_profit_interval fop
              ON fop.code = p.code
             AND fop.forecast_operating_profit_disclosed_date <= p.date
             AND (fop.valid_to IS NULL OR p.date < fop.valid_to)
            LEFT JOIN recent_return_daily_valuation dv
              ON dv.code = p.code
             AND dv.date = p.date
            WHERE {market_filter}
        ),
        featured AS (
            SELECT
                *,
                median(close * volume) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS med_adv60_jpy,
                count(close * volume) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS med_adv60_sessions,
                {lag_exprs},
                {forward_exprs}
            FROM scoped
        ),
        topix_featured AS (
            SELECT
                date,
                close AS topix_close,
                {topix_forward_exprs}
            FROM topix_data
            WHERE close > 0
        ),
        computed AS (
            SELECT
                f.*,
                tf.topix_close,
                {recent_exprs},
                {return_exprs},
                {topix_return_exprs}
            FROM featured f
            LEFT JOIN topix_featured tf USING (date)
        ),
        excess AS (
            SELECT
                *,
                {excess_exprs},
                CASE
                    WHEN shares_outstanding - coalesce(treasury_shares, 0) > 0
                        THEN close * (shares_outstanding - coalesce(treasury_shares, 0))
                END AS free_float_market_cap_jpy
            FROM computed
        ),
        residual_source AS (
            SELECT
                *,
                CASE
                    WHEN med_adv60_sessions >= 60
                     AND med_adv60_jpy > 0
                     AND free_float_market_cap_jpy > 0
                        THEN ln(med_adv60_jpy)
                END AS log_adv60,
                CASE
                    WHEN med_adv60_sessions >= 60
                     AND med_adv60_jpy > 0
                     AND free_float_market_cap_jpy > 0
                        THEN ln(free_float_market_cap_jpy)
                END AS log_free_float_market_cap
            FROM excess
        ),
        residual_stats AS (
            SELECT
                date,
                market,
                regr_intercept(log_adv60, log_free_float_market_cap) AS residual_intercept,
                regr_slope(log_adv60, log_free_float_market_cap) AS residual_beta,
                count(log_adv60) AS residual_observations
            FROM residual_source
            GROUP BY date, market
        ),
        residual_values AS (
            SELECT
                rs.*,
                rstats.residual_intercept,
                rstats.residual_beta,
                rstats.residual_observations,
                CASE
                    WHEN rstats.residual_observations >= 50
                     AND rstats.residual_intercept IS NOT NULL
                     AND rstats.residual_beta IS NOT NULL
                        THEN rs.log_adv60 - (rstats.residual_intercept + rstats.residual_beta * rs.log_free_float_market_cap)
                END AS liquidity_residual
            FROM residual_source rs
            LEFT JOIN residual_stats rstats USING (date, market)
        ),
        residual_dispersion AS (
            SELECT
                date,
                market,
                stddev_samp(liquidity_residual) AS liquidity_residual_std
            FROM residual_values
            GROUP BY date, market
        ),
        residual_z AS (
            SELECT
                rv.*,
                rd.liquidity_residual_std
            FROM residual_values rv
            LEFT JOIN residual_dispersion rd USING (date, market)
        )
        SELECT
            *,
            CASE
                WHEN liquidity_residual_std > 0 THEN liquidity_residual / liquidity_residual_std
            END AS liquidity_residual_z,
            CASE
                WHEN liquidity_residual_std IS NULL OR liquidity_residual_std <= 0 THEN 'missing'
                WHEN liquidity_residual / liquidity_residual_std >= 1
                  AND recent_return_20d_pct >= 0
                  AND recent_return_60d_pct >= 0 THEN 'rerating_participation'
                WHEN liquidity_residual / liquidity_residual_std >= 1 THEN 'distribution_stress'
                WHEN liquidity_residual / liquidity_residual_std <= -1 THEN 'stale_liquidity'
                ELSE 'neutral'
            END AS liquidity_regime
        FROM residual_z
        {final_where}
        """,
        [*raw_params, *final_params],
    )
    _create_scoped_view(conn)


def _market_master_cte(*, market_source: str, master_code: str) -> str:
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    return f"""
    raw_market_master AS (
        SELECT
            {master_code} AS code,
            smd.date,
            smd.company_name,
            CASE
                WHEN lower(trim(smd.market_code)) IN ('0111', 'prime') THEN 'prime'
                WHEN lower(trim(smd.market_code)) IN ('0112', 'standard') THEN 'standard'
                WHEN lower(trim(smd.market_code)) IN ('0113', 'growth') THEN 'growth'
                ELSE 'unknown'
            END AS market,
            smd.market_code,
            smd.scale_category,
            row_number() OVER (
                PARTITION BY {master_code}, smd.date
                ORDER BY CASE WHEN length(smd.code) = 4 THEN 0 ELSE 1 END, smd.code
            ) AS row_rank
        FROM stock_master_daily smd
    ),
    market_master AS (
        SELECT code, date, company_name, market, market_code, scale_category
        FROM raw_market_master
        WHERE row_rank = 1
    )
    """


def _create_scoped_view(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW recent_return_threshold_scoped AS
        WITH base AS (
            SELECT
                p.*,
                strftime(CAST(p.date AS DATE), '%Y') AS anchor_year,
                strftime(CAST(p.date AS DATE), '%Y-%m') AS anchor_month,
                CASE
                    WHEN lead(strftime(CAST(p.date AS DATE), '%Y-%m')) OVER (
                        PARTITION BY p.code ORDER BY p.date
                    ) IS DISTINCT FROM strftime(CAST(p.date AS DATE), '%Y-%m') THEN TRUE
                    ELSE FALSE
                END AS is_month_end_anchor,
                CASE
                    WHEN strftime(CAST(p.date AS DATE), '%w') = '5' THEN TRUE
                    ELSE FALSE
                END AS is_weekly_anchor
            FROM recent_return_threshold_panel p
        )
        SELECT
            base.*,
            scope.market_scope,
            liq.liquidity_scope
        FROM base
        CROSS JOIN LATERAL (
            SELECT 'all' AS market_scope
            UNION ALL
            SELECT base.market AS market_scope
        ) scope
        CROSS JOIN LATERAL (
            SELECT 'all_liquidity' AS liquidity_scope
            UNION ALL
            SELECT base.liquidity_regime AS liquidity_scope
        ) liq
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            market_scope,
            liquidity_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN recent_return_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS recent_return_20d_coverage_pct,
            avg(CASE WHEN recent_return_60d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS recent_return_60d_coverage_pct,
            avg(CASE WHEN med_adv60_sessions >= 60 THEN 1.0 ELSE 0.0 END) * 100.0
                AS med_adv60_coverage_pct,
            avg(CASE WHEN liquidity_residual_z IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS liquidity_residual_z_coverage_pct,
            avg(CASE WHEN per IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS per_coverage_pct,
            avg(CASE WHEN forward_per IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_per_coverage_pct,
            avg(CASE WHEN pbr IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS pbr_coverage_pct,
            avg(CASE WHEN p_op IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS p_op_coverage_pct,
            avg(CASE WHEN forward_p_op IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_p_op_coverage_pct
        FROM recent_return_threshold_scoped
        GROUP BY market_scope, liquidity_scope
        """
    ).fetchdf()
    return _sort_summary_df(
        frame,
        columns=[
            "market_scope",
            "liquidity_scope",
            "observation_count",
            "code_count",
            "date_count",
            "recent_return_20d_coverage_pct",
            "recent_return_60d_coverage_pct",
            "med_adv60_coverage_pct",
            "liquidity_residual_z_coverage_pct",
            "per_coverage_pct",
            "forward_per_coverage_pct",
            "pbr_coverage_pct",
            "p_op_coverage_pct",
            "forward_p_op_coverage_pct",
        ],
    )


def _build_threshold_response_df(
    conn: Any,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    thresholds_by_window: dict[int, Sequence[float]],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in pre_windows:
        thresholds = thresholds_by_window.get(window, ())
        for direction in ("ge", "le"):
            for threshold in thresholds:
                condition = _threshold_condition(window, direction, threshold)
                for entry_mode in _ENTRY_MODES:
                    for horizon in horizons:
                        frames.append(
                            _aggregate_condition(
                                conn,
                                condition=condition,
                                condition_fields={
                                    "condition_family": "single_threshold",
                                    "sample_scope": sample_scope,
                                    "pre_window": int(window),
                                    "direction": direction,
                                    "threshold_pct": float(threshold),
                                    "condition_label": _condition_label(
                                        window, direction, threshold
                                    ),
                                    "entry_mode": entry_mode,
                                    "horizon": int(horizon),
                                },
                                return_column=_return_column(entry_mode, horizon),
                                sample_scope=sample_scope,
                                group_by_year=False,
                                min_observations=min_observations,
                                severe_loss_threshold_pct=severe_loss_threshold_pct,
                            )
                        )
    return _concat_sorted(frames, columns=_threshold_response_columns())


def _build_joint_threshold_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    thresholds_20d: Sequence[float],
    thresholds_60d: Sequence[float],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for threshold_20d in thresholds_20d:
        for threshold_60d in thresholds_60d:
            condition = (
                f"recent_return_20d_pct >= {float(threshold_20d)} "
                f"AND recent_return_60d_pct >= {float(threshold_60d)}"
            )
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            condition=condition,
                            condition_fields={
                                "condition_family": "joint_runup",
                                "sample_scope": sample_scope,
                                "threshold_20d_pct": float(threshold_20d),
                                "threshold_60d_pct": float(threshold_60d),
                                "condition_label": (
                                    f"recent_return_20d_ge_{_threshold_token(threshold_20d)}"
                                    f"__recent_return_60d_ge_{_threshold_token(threshold_60d)}"
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            sample_scope=sample_scope,
                            group_by_year=False,
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_joint_threshold_response_columns())


def _build_percentile_response_df(
    conn: Any,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    sample_filter = _sample_scope_filter(sample_scope)
    for window in pre_windows:
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW recent_return_percentile_work AS
            SELECT
                *,
                percent_rank() OVER (
                    PARTITION BY market_scope, anchor_year
                    ORDER BY recent_return_{window}d_pct
                ) AS recent_return_rank_pct
            FROM recent_return_threshold_scoped
            WHERE {sample_filter}
              AND recent_return_{window}d_pct IS NOT NULL
            """
        )
        for bucket in _PERCENTILE_BUCKET_ORDER:
            condition = _percentile_condition(bucket)
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="recent_return_percentile_work",
                            condition=condition,
                            condition_fields={
                                "condition_family": "annual_percentile_bucket",
                                "sample_scope": sample_scope,
                                "pre_window": int(window),
                                "percentile_bucket": bucket,
                                "percentile_bucket_order": _PERCENTILE_BUCKET_ORDER.index(
                                    bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            sample_scope=sample_scope,
                            group_by_year=False,
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                            sample_filter_override="TRUE",
                        )
                    )
    return _concat_sorted(frames, columns=_percentile_response_columns())


def _build_valuation_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    sample_filter = _sample_scope_filter(sample_scope)
    for feature in ("per", "forward_per", "pbr", "p_op", "forward_p_op"):
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW recent_return_valuation_work AS
            SELECT
                *,
                percent_rank() OVER (
                    PARTITION BY market_scope, anchor_year
                    ORDER BY {feature}
                ) AS valuation_rank_pct
            FROM recent_return_threshold_scoped
            WHERE {sample_filter}
              AND {feature} IS NOT NULL
              AND {feature} > 0
            """
        )
        for bucket in _VALUATION_BUCKET_ORDER:
            condition = _valuation_bucket_condition(bucket)
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="recent_return_valuation_work",
                            condition=condition,
                            condition_fields={
                                "condition_family": "valuation_percentile_bucket",
                                "sample_scope": sample_scope,
                                "valuation_feature": feature,
                                "valuation_bucket": bucket,
                                "valuation_bucket_order": _VALUATION_BUCKET_ORDER.index(
                                    bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            sample_scope=sample_scope,
                            group_by_year=False,
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                            sample_filter_override="TRUE",
                        )
                    )
    return _concat_sorted(frames, columns=_valuation_response_columns())


def _build_valuation_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    sample_filter = _sample_scope_filter(sample_scope)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW recent_return_valuation_interaction_work AS
        SELECT
            *,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY pbr
            ) AS pbr_rank_pct,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY forward_per
            ) AS forward_per_rank_pct
        FROM recent_return_threshold_scoped
        WHERE {sample_filter}
          AND pbr IS NOT NULL
          AND pbr > 0
          AND forward_per IS NOT NULL
          AND forward_per > 0
        """
    )
    for bucket in _VALUATION_INTERACTION_BUCKET_ORDER:
        condition = _valuation_interaction_condition(bucket)
        for entry_mode in _ENTRY_MODES:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="recent_return_valuation_interaction_work",
                        condition=condition,
                        condition_fields={
                            "condition_family": "pbr_forward_per_interaction",
                            "sample_scope": sample_scope,
                            "low_cutoff_pct": 20.0,
                            "interaction_bucket": bucket,
                            "interaction_bucket_order": _VALUATION_INTERACTION_BUCKET_ORDER.index(
                                bucket
                            ),
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_return_column(entry_mode, horizon),
                        sample_scope=sample_scope,
                        group_by_year=False,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                        sample_filter_override="TRUE",
                    )
                )
    return _concat_sorted(frames, columns=_valuation_interaction_columns())


def _build_long_trend_quadrant_response_df(
    conn: Any,
    *,
    long_trend_windows: Sequence[int],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_scope: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for trend_window in long_trend_windows:
        for trend_quadrant in _LONG_TREND_QUADRANT_ORDER:
            condition = _long_trend_quadrant_condition(
                trend_quadrant,
                trend_window=trend_window,
            )
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frame = _aggregate_condition(
                        conn,
                        condition=condition,
                        condition_fields={
                            "condition_family": "long_trend_quadrant",
                            "sample_scope": sample_scope,
                            "trend_window": int(trend_window),
                            "trend_quadrant": trend_quadrant,
                            "trend_quadrant_order": _LONG_TREND_QUADRANT_ORDER.index(
                                trend_quadrant
                            ),
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_return_column(entry_mode, horizon),
                        sample_scope=sample_scope,
                        group_by_year=False,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    if not frame.empty:
                        source_column = f"median_recent_return_{int(trend_window)}d_pct"
                        frame["median_recent_return_long_pct"] = frame.get(source_column)
                    frames.append(frame)
    return _concat_sorted(frames, columns=_long_trend_quadrant_response_columns())


def _build_nonoverlap_response_df(
    conn: Any,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    thresholds_by_window: dict[int, Sequence[float]],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for sample_scope in ("weekly", "monthly"):
        frames.append(
            _build_threshold_response_df(
                conn,
                pre_windows=pre_windows,
                horizons=horizons,
                thresholds_by_window=thresholds_by_window,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                sample_scope=sample_scope,
            )
        )
    return _concat_sorted(frames, columns=_threshold_response_columns())


def _build_annual_threshold_response_df(
    conn: Any,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    thresholds_by_window: dict[int, Sequence[float]],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in pre_windows:
        for threshold in thresholds_by_window.get(window, ()):
            condition = _threshold_condition(window, "ge", threshold)
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            condition=condition,
                            condition_fields={
                                "condition_family": "annual_single_threshold",
                                "sample_scope": "daily",
                                "pre_window": int(window),
                                "direction": "ge",
                                "threshold_pct": float(threshold),
                                "condition_label": _condition_label(
                                    window, "ge", threshold
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            sample_scope="daily",
                            group_by_year=True,
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(
        frames,
        columns=[
            *_threshold_response_columns()[:7],
            "anchor_year",
            *_threshold_response_columns()[7:],
        ],
    )


def _build_liquidity_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    states = {
        "both_positive": "recent_return_20d_pct >= 0 AND recent_return_60d_pct >= 0",
        "both_negative": "recent_return_20d_pct < 0 AND recent_return_60d_pct < 0",
        "20d_strong_runup": "recent_return_20d_pct >= 20",
        "60d_strong_runup": "recent_return_60d_pct >= 30",
        "mixed": "NOT ((recent_return_20d_pct >= 0 AND recent_return_60d_pct >= 0) OR (recent_return_20d_pct < 0 AND recent_return_60d_pct < 0))",
    }
    for momentum_state, condition in states.items():
        for entry_mode in _ENTRY_MODES:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        condition=condition,
                        condition_fields={
                            "condition_family": "liquidity_momentum_state",
                            "sample_scope": "daily",
                            "momentum_state": momentum_state,
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_return_column(entry_mode, horizon),
                        sample_scope="daily",
                        group_by_year=False,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_liquidity_interaction_columns())


def _aggregate_condition(
    conn: Any,
    *,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    sample_scope: str,
    group_by_year: bool,
    min_observations: int,
    severe_loss_threshold_pct: float,
    source_name: str = "recent_return_threshold_scoped",
    sample_filter_override: str | None = None,
) -> pd.DataFrame:
    sample_filter = sample_filter_override or _sample_scope_filter(sample_scope)
    year_select = ", anchor_year" if group_by_year else ""
    year_group = ", anchor_year" if group_by_year else ""
    frame = conn.execute(
        f"""
        SELECT
            market_scope,
            liquidity_scope
            {year_select},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({return_column}) AS mean_forward_excess_return_pct,
            median({return_column}) AS median_forward_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({return_column}, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont({return_column}, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(recent_return_120d_pct) AS median_recent_return_120d_pct,
            median(recent_return_150d_pct) AS median_recent_return_150d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per) AS median_per,
            median(forward_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op
        FROM {source_name}
        WHERE {sample_filter}
          AND {condition}
          AND {return_column} IS NOT NULL
        GROUP BY market_scope, liquidity_scope {year_group}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), "market_scope", "liquidity_scope"]
    if group_by_year:
        ordered.append("anchor_year")
    ordered.extend(
        [
            "observation_count",
            "code_count",
            "date_count",
            "mean_forward_excess_return_pct",
            "median_forward_excess_return_pct",
            "p10_forward_excess_return_pct",
            "p25_forward_excess_return_pct",
            "p75_forward_excess_return_pct",
            "p90_forward_excess_return_pct",
            "win_rate_pct",
            "severe_loss_rate_pct",
            "median_recent_return_20d_pct",
            "median_recent_return_60d_pct",
            "median_recent_return_120d_pct",
            "median_recent_return_150d_pct",
            "median_med_adv60_mil_jpy",
            "median_liquidity_residual_z",
            "median_per",
            "median_forward_per",
            "median_pbr",
            "median_p_op",
            "median_forward_p_op",
        ]
    )
    return frame.reindex(columns=ordered)


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            scale_category,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            recent_return_120d_pct,
            recent_return_150d_pct,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            free_float_market_cap_jpy / 1000000000.0 AS free_float_market_cap_bil_jpy,
            liquidity_residual_z,
            liquidity_regime,
            per,
            forward_per,
            pbr,
            p_op,
            forward_p_op,
            market_cap_bil_jpy,
            forward_close_excess_return_5d_pct,
            forward_close_excess_return_20d_pct,
            forward_next_open_excess_return_5d_pct,
            forward_next_open_excess_return_20d_pct
        FROM recent_return_threshold_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _sample_scope_filter(sample_scope: str) -> str:
    if sample_scope == "daily":
        return "TRUE"
    if sample_scope == "weekly":
        return "is_weekly_anchor"
    if sample_scope == "monthly":
        return "is_month_end_anchor"
    raise ValueError(f"unsupported sample_scope: {sample_scope}")


def _threshold_condition(window: int, direction: str, threshold: float) -> str:
    if direction == "ge":
        return f"recent_return_{window}d_pct >= {float(threshold)}"
    if direction == "le":
        return f"recent_return_{window}d_pct <= {-float(threshold)}"
    raise ValueError(f"unsupported direction: {direction}")


def _condition_label(window: int, direction: str, threshold: float) -> str:
    operator = "ge" if direction == "ge" else "le_negative"
    return f"recent_return_{window}d_{operator}_{_threshold_token(threshold)}"


def _percentile_condition(bucket: str) -> str:
    if bucket == "top_10pct":
        return "recent_return_rank_pct >= 0.9"
    if bucket == "top_20pct":
        return "recent_return_rank_pct >= 0.8 AND recent_return_rank_pct < 0.9"
    if bucket == "middle_60pct":
        return "recent_return_rank_pct > 0.2 AND recent_return_rank_pct < 0.8"
    if bucket == "bottom_20pct":
        return "recent_return_rank_pct > 0.1 AND recent_return_rank_pct <= 0.2"
    if bucket == "bottom_10pct":
        return "recent_return_rank_pct <= 0.1"
    if bucket == "missing":
        return "recent_return_rank_pct IS NULL"
    raise ValueError(f"unsupported percentile bucket: {bucket}")


def _valuation_bucket_condition(bucket: str) -> str:
    if bucket == "cheapest_10pct":
        return "valuation_rank_pct <= 0.1"
    if bucket == "cheapest_20pct":
        return "valuation_rank_pct > 0.1 AND valuation_rank_pct <= 0.2"
    if bucket == "middle_60pct":
        return "valuation_rank_pct > 0.2 AND valuation_rank_pct < 0.8"
    if bucket == "expensive_20pct":
        return "valuation_rank_pct >= 0.8 AND valuation_rank_pct < 0.9"
    if bucket == "expensive_10pct":
        return "valuation_rank_pct >= 0.9"
    raise ValueError(f"unsupported valuation bucket: {bucket}")


def _valuation_interaction_condition(bucket: str) -> str:
    pbr_low = "pbr_rank_pct <= 0.2"
    fper_low = "forward_per_rank_pct <= 0.2"
    if bucket == "both_low":
        return f"{pbr_low} AND {fper_low}"
    if bucket == "low_pbr_only":
        return f"{pbr_low} AND NOT ({fper_low})"
    if bucket == "low_forward_per_only":
        return f"NOT ({pbr_low}) AND {fper_low}"
    if bucket == "neither_low":
        return f"NOT ({pbr_low}) AND NOT ({fper_low})"
    raise ValueError(f"unsupported valuation interaction bucket: {bucket}")


def _long_trend_quadrant_condition(quadrant: str, *, trend_window: int) -> str:
    long_return = f"recent_return_{int(trend_window)}d_pct"
    if quadrant == "persistent_rerating":
        return (
            "recent_return_20d_pct > 0 "
            "AND recent_return_60d_pct > 0 "
            f"AND {long_return} > 0"
        )
    if quadrant == "relief_bounce":
        return (
            "recent_return_20d_pct > 0 "
            "AND recent_return_60d_pct > 0 "
            f"AND {long_return} <= 0"
        )
    if quadrant == "uptrend_pullback":
        return (
            "recent_return_20d_pct < 0 "
            "AND recent_return_60d_pct > 0 "
            f"AND {long_return} > 0"
        )
    if quadrant == "short_bounce":
        return (
            "recent_return_20d_pct > 0 "
            "AND recent_return_60d_pct <= 0 "
            f"AND {long_return} <= 0"
        )
    raise ValueError(f"unsupported long trend quadrant: {quadrant}")


def _return_column(entry_mode: str, horizon: int) -> str:
    if entry_mode == "close_to_close":
        return f"forward_close_excess_return_{horizon}d_pct"
    if entry_mode == "next_open_to_close":
        return f"forward_next_open_excess_return_{horizon}d_pct"
    raise ValueError(f"unsupported entry_mode: {entry_mode}")


def _threshold_token(value: float) -> str:
    return f"{float(value):.1f}".rstrip("0").rstrip(".").replace(".", "_")


def _sql_string_list(values: Sequence[str]) -> str:
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)


def _offset_calendar_date(date: str | None, *, days: int) -> str | None:
    if date is None:
        return None
    return (pd.Timestamp(date) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    result = pd.concat(non_empty, ignore_index=True, sort=False)
    for column in columns:
        if column not in result.columns:
            result[column] = np.nan
    return _sort_summary_df(result, columns=list(columns))


def _base_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_recent_return_120d_pct",
        "median_recent_return_150d_pct",
        "median_med_adv60_mil_jpy",
        "median_liquidity_residual_z",
        "median_per",
        "median_forward_per",
        "median_pbr",
        "median_p_op",
        "median_forward_p_op",
    ]


def _threshold_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "pre_window",
        "direction",
        "threshold_pct",
        "condition_label",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]


def _joint_threshold_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "threshold_20d_pct",
        "threshold_60d_pct",
        "condition_label",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]


def _percentile_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "pre_window",
        "percentile_bucket",
        "percentile_bucket_order",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]


def _valuation_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "valuation_feature",
        "valuation_bucket",
        "valuation_bucket_order",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]


def _valuation_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "low_cutoff_pct",
        "interaction_bucket",
        "interaction_bucket_order",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]


def _long_trend_quadrant_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "trend_window",
        "trend_quadrant",
        "trend_quadrant_order",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
        "median_recent_return_long_pct",
    ]


def _liquidity_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "momentum_state",
        "market_scope",
        "liquidity_scope",
        "entry_mode",
        "horizon",
        *_base_response_columns()[6:],
    ]
