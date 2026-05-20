"""Short-term shock and pullback forward-response research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    _str_or_none,
    _table_exists,
    _top_rows_for_markdown,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.recent_return_threshold_forward_response import (
    DEFAULT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    _assert_required_tables,
    _concat_sorted,
    _create_scoped_view,
    _daily_valuation_column_exists,
    _market_master_cte,
    _normalize_market_scopes,
    _optional_daily_valuation_double_expr,
    _offset_calendar_date,
    _return_column,
    _sql_string_list,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

SHORT_TERM_SHOCK_FORWARD_RESPONSE_EXPERIMENT_ID = (
    "market-behavior/short-term-shock-forward-response"
)
DEFAULT_PULLBACK_THRESHOLDS_20D: tuple[float, ...] = (0.0, 5.0, 10.0, 15.0, 20.0, 30.0)
DEFAULT_UPTREND_THRESHOLDS_60D: tuple[float, ...] = (0.0, 10.0, 20.0, 30.0)
DEFAULT_MARKET_SHOCK_THRESHOLDS: tuple[float, ...] = (-3.0, -5.0, -8.0)
DEFAULT_CASE_STUDY_DATES: tuple[str, ...] = ("2024-08-05", "2025-04-07")
DEFAULT_CASE_STUDY_WINDOW_SESSIONS = 5
_ENTRY_MODES: tuple[str, ...] = ("close_to_close", "next_open_to_close")
_SAMPLE_SCOPES: tuple[str, ...] = ("daily", "weekly", "monthly")
_RETURN_METRICS: tuple[str, ...] = ("topix_excess", "raw")
_PRICE_ACTION_STATES: dict[str, str] = {
    "pullback_in_uptrend": "recent_return_20d_pct < 0 AND recent_return_60d_pct >= 0",
    "downtrend_decline": "recent_return_20d_pct < 0 AND recent_return_60d_pct < 0",
    "persistent_runup": "recent_return_20d_pct >= 0 AND recent_return_60d_pct >= 0",
    "relief_bounce": "recent_return_20d_pct >= 0 AND recent_return_60d_pct < 0",
}
_MARKET_STATES: dict[str, str] = {
    "market_pullback_in_uptrend": "topix_return_20d_pct < 0 AND topix_return_60d_pct >= 0",
    "market_downtrend_decline": "topix_return_20d_pct < 0 AND topix_return_60d_pct < 0",
    "market_persistent_runup": "topix_return_20d_pct >= 0 AND topix_return_60d_pct >= 0",
    "market_relief_bounce": "topix_return_20d_pct >= 0 AND topix_return_60d_pct < 0",
}
_SHOCK_OFFSET_BUCKETS: dict[str, str] = {
    "shock_day": "days_since_market_shock = 0",
    "post_shock_1d": "days_since_market_shock = 1",
    "post_shock_2_5d": "days_since_market_shock BETWEEN 2 AND 5",
    "post_shock_6_10d": "days_since_market_shock BETWEEN 6 AND 10",
    "post_shock_11_20d": "days_since_market_shock BETWEEN 11 AND 20",
}


@dataclass(frozen=True)
class ShortTermShockForwardResponseResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    pullback_thresholds_20d: tuple[float, ...]
    uptrend_thresholds_60d: tuple[float, ...]
    market_shock_thresholds: tuple[float, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    market_shock_calendar_df: pd.DataFrame
    general_short_term_response_df: pd.DataFrame
    pullback_in_uptrend_response_df: pd.DataFrame
    market_shock_window_response_df: pd.DataFrame
    stock_market_interaction_df: pd.DataFrame
    liquidity_valuation_interaction_df: pd.DataFrame
    case_study_response_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_short_term_shock_forward_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    pullback_thresholds_20d: Sequence[float] = DEFAULT_PULLBACK_THRESHOLDS_20D,
    uptrend_thresholds_60d: Sequence[float] = DEFAULT_UPTREND_THRESHOLDS_60D,
    market_shock_thresholds: Sequence[float] = DEFAULT_MARKET_SHOCK_THRESHOLDS,
    sample_scopes: Sequence[str] = _SAMPLE_SCOPES,
    case_study_dates: Sequence[str] = DEFAULT_CASE_STUDY_DATES,
    case_study_window_sessions: int = DEFAULT_CASE_STUDY_WINDOW_SESSIONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    core_only: bool = False,
) -> ShortTermShockForwardResponseResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_pullback_thresholds = _normalize_non_negative_thresholds(
        pullback_thresholds_20d, name="pullback_thresholds_20d"
    )
    resolved_uptrend_thresholds = _normalize_non_negative_thresholds(
        uptrend_thresholds_60d, name="uptrend_thresholds_60d"
    )
    resolved_shock_thresholds = _normalize_negative_thresholds(
        market_shock_thresholds, name="market_shock_thresholds"
    )
    resolved_sample_scopes = _normalize_sample_scopes(sample_scopes)
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
    resolved_case_dates = tuple(
        dict.fromkeys(str(value).strip() for value in case_study_dates if str(value).strip())
    )
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
        case_study_window_sessions=case_study_window_sessions,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    pre_windows = (20, 60)
    query_start = _offset_calendar_date(start_date, days=-(max(pre_windows) * 4 + 30))
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="short-term-shock-forward-response-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = (
            "stock_master_daily_exact_date"
            if _table_exists(ctx.connection, "stock_master_daily")
            else "stocks_latest_fallback"
        )
        _create_fast_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
            include_valuation_liquidity=not core_only,
        )
        _create_short_term_shock_views(
            ctx.connection,
            shock_thresholds=resolved_shock_thresholds,
            case_study_dates=resolved_case_dates,
            case_study_window_sessions=case_study_window_sessions,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM short_term_shock_panel"
            ).fetchone()[0]
        )
        market_shock_calendar_df = _build_market_shock_calendar_df(
            ctx.connection,
            shock_thresholds=resolved_shock_thresholds,
        )
        general_short_term_response_df = _build_general_short_term_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            sample_scopes=resolved_sample_scopes,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        pullback_in_uptrend_response_df = _build_pullback_in_uptrend_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            pullback_thresholds_20d=resolved_pullback_thresholds,
            uptrend_thresholds_60d=resolved_uptrend_thresholds,
            sample_scopes=resolved_sample_scopes,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        market_shock_window_response_df = _build_market_shock_window_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            shock_thresholds=resolved_shock_thresholds,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        stock_market_interaction_df = (
            pd.DataFrame(columns=_stock_market_interaction_columns())
            if core_only
            else _build_stock_market_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
        liquidity_valuation_interaction_df = (
            pd.DataFrame(columns=_liquidity_valuation_interaction_columns())
            if core_only
            else _build_liquidity_valuation_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
        case_study_response_df = _build_case_study_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=max(1, min_observations // 10),
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        observation_sample_df = _query_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return ShortTermShockForwardResponseResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(observation_sample_df["date"].min())
        if "date" in observation_sample_df and not observation_sample_df.empty
        else start_date,
        analysis_end_date=end_date,
        horizons=resolved_horizons,
        pullback_thresholds_20d=resolved_pullback_thresholds,
        uptrend_thresholds_60d=resolved_uptrend_thresholds,
        market_shock_thresholds=resolved_shock_thresholds,
        market_scopes=resolved_market_scopes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_count=observation_count,
        market_shock_calendar_df=market_shock_calendar_df,
        general_short_term_response_df=general_short_term_response_df,
        pullback_in_uptrend_response_df=pullback_in_uptrend_response_df,
        market_shock_window_response_df=market_shock_window_response_df,
        stock_market_interaction_df=stock_market_interaction_df,
        liquidity_valuation_interaction_df=liquidity_valuation_interaction_df,
        case_study_response_df=case_study_response_df,
        observation_sample_df=observation_sample_df,
    )


def write_short_term_shock_forward_response_bundle(
    result: ShortTermShockForwardResponseResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=SHORT_TERM_SHOCK_FORWARD_RESPONSE_EXPERIMENT_ID,
        module=__name__,
        function="run_short_term_shock_forward_response_research",
        params={
            "horizons": list(result.horizons),
            "pullback_thresholds_20d": list(result.pullback_thresholds_20d),
            "uptrend_thresholds_60d": list(result.uptrend_thresholds_60d),
            "market_shock_thresholds": list(result.market_shock_thresholds),
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
            "market_shock_calendar_df": result.market_shock_calendar_df,
            "general_short_term_response_df": result.general_short_term_response_df,
            "pullback_in_uptrend_response_df": result.pullback_in_uptrend_response_df,
            "market_shock_window_response_df": result.market_shock_window_response_df,
            "stock_market_interaction_df": result.stock_market_interaction_df,
            "liquidity_valuation_interaction_df": result.liquidity_valuation_interaction_df,
            "case_study_response_df": result.case_study_response_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: ShortTermShockForwardResponseResult) -> str:
    return "\n".join(
        [
            "# Short-Term Shock Forward Response",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Observation count: `{result.observation_count}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- 20d pullback thresholds: `{list(result.pullback_thresholds_20d)}`",
            f"- 60d uptrend thresholds: `{list(result.uptrend_thresholds_60d)}`",
            f"- Market shock thresholds: `{list(result.market_shock_thresholds)}`",
            f"- Market scopes: `{list(result.market_scopes)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Market Shock Calendar",
            "",
            _top_rows_for_markdown(
                result.market_shock_calendar_df,
                sort_columns=["shock_threshold_pct", "date"],
                limit=80,
            ),
            "",
            "## General Short-Term Response",
            "",
            _top_rows_for_markdown(
                result.general_short_term_response_df,
                sort_columns=[
                    "sample_scope",
                    "price_action_state",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=80,
            ),
            "",
            "## Pullback In Uptrend Response",
            "",
            _top_rows_for_markdown(
                result.pullback_in_uptrend_response_df,
                sort_columns=[
                    "sample_scope",
                    "pullback_threshold_20d_pct",
                    "uptrend_threshold_60d_pct",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=100,
            ),
            "",
            "## Market Shock Window Response",
            "",
            _top_rows_for_markdown(
                result.market_shock_window_response_df,
                sort_columns=[
                    "shock_threshold_pct",
                    "shock_offset_bucket_order",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=100,
            ),
            "",
            "## Stock x Market Interaction",
            "",
            _top_rows_for_markdown(
                result.stock_market_interaction_df,
                sort_columns=[
                    "stock_state",
                    "market_state",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=100,
            ),
            "",
            "## Liquidity / Valuation Interaction",
            "",
            _top_rows_for_markdown(
                result.liquidity_valuation_interaction_df,
                sort_columns=[
                    "condition_name",
                    "valuation_bucket",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=100,
            ),
            "",
            "## Case Study Response",
            "",
            _top_rows_for_markdown(
                result.case_study_response_df,
                sort_columns=[
                    "case_anchor_date",
                    "case_offset_sessions",
                    "price_action_state",
                    "market_scope",
                    "liquidity_scope",
                    "return_metric",
                    "entry_mode",
                    "horizon",
                ],
                limit=120,
            ),
            "",
        ]
    )


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
    case_study_window_sessions: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")
    if case_study_window_sessions < 0:
        raise ValueError("case_study_window_sessions must be non-negative")


def _normalize_non_negative_thresholds(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    normalized = tuple(sorted({float(value) for value in values}))
    if not normalized or any(
        not math.isfinite(value) or value < 0.0 for value in normalized
    ):
        raise ValueError(f"{name} must contain finite non-negative values")
    return normalized


def _normalize_negative_thresholds(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    normalized = tuple(sorted({float(value) for value in values}))
    if not normalized or any(
        not math.isfinite(value) or value >= 0.0 for value in normalized
    ):
        raise ValueError(f"{name} must contain finite negative values")
    return normalized


def _normalize_sample_scopes(values: Sequence[str]) -> tuple[str, ...]:
    allowed = set(_SAMPLE_SCOPES)
    normalized = tuple(
        dict.fromkeys(
            str(value).strip().lower() for value in values if str(value).strip()
        )
    )
    if not normalized:
        raise ValueError("sample_scopes must not be empty")
    invalid = [value for value in normalized if value not in allowed]
    if invalid:
        raise ValueError(f"unsupported sample_scopes: {', '.join(invalid)}")
    return normalized


def _create_short_term_shock_views(
    conn: Any,
    *,
    shock_thresholds: Sequence[float],
    case_study_dates: Sequence[str],
    case_study_window_sessions: int,
) -> None:
    shock_floor = min(shock_thresholds)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE short_term_topix_features AS
        WITH base AS (
            SELECT
                date,
                open,
                close,
                row_number() OVER (ORDER BY date) AS topix_session_index,
                lag(close, 1) OVER (ORDER BY date) AS close_lag_1d,
                lag(close, 5) OVER (ORDER BY date) AS close_lag_5d,
                lag(close, 20) OVER (ORDER BY date) AS close_lag_20d,
                lag(close, 60) OVER (ORDER BY date) AS close_lag_60d
            FROM topix_data
            WHERE close > 0
        ),
        returns AS (
            SELECT
                *,
                CASE WHEN close_lag_1d > 0 THEN (close / close_lag_1d - 1.0) * 100.0 END
                    AS topix_return_1d_pct,
                CASE WHEN close_lag_5d > 0 THEN (close / close_lag_5d - 1.0) * 100.0 END
                    AS topix_return_5d_pct,
                CASE WHEN close_lag_20d > 0 THEN (close / close_lag_20d - 1.0) * 100.0 END
                    AS topix_return_20d_pct,
                CASE WHEN close_lag_60d > 0 THEN (close / close_lag_60d - 1.0) * 100.0 END
                    AS topix_return_60d_pct
            FROM base
        ),
        shock_marked AS (
            SELECT
                *,
                max(CASE WHEN topix_return_1d_pct <= {float(shock_floor)} THEN topix_session_index END)
                    OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    AS latest_market_shock_session_index
            FROM returns
        )
        SELECT
            *,
            topix_session_index - latest_market_shock_session_index AS days_since_market_shock
        FROM shock_marked
        """
    )
    case_dates_sql = _sql_string_list(case_study_dates) if case_study_dates else "NULL"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE short_term_case_dates AS
        SELECT
            anchor_date,
            anchor_session_index
        FROM (
            SELECT
                date AS anchor_date,
                topix_session_index AS anchor_session_index
            FROM short_term_topix_features
            WHERE date IN ({case_dates_sql})
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE short_term_shock_panel AS
        SELECT
            s.*,
            tf.topix_return_1d_pct,
            tf.topix_return_5d_pct,
            tf.topix_return_20d_pct,
            tf.topix_return_60d_pct,
            tf.days_since_market_shock,
            CASE
                WHEN s.recent_return_20d_pct < 0 AND s.recent_return_60d_pct >= 0
                    THEN 'pullback_in_uptrend'
                WHEN s.recent_return_20d_pct < 0 AND s.recent_return_60d_pct < 0
                    THEN 'downtrend_decline'
                WHEN s.recent_return_20d_pct >= 0 AND s.recent_return_60d_pct >= 0
                    THEN 'persistent_runup'
                WHEN s.recent_return_20d_pct >= 0 AND s.recent_return_60d_pct < 0
                    THEN 'relief_bounce'
                ELSE 'missing'
            END AS price_action_state,
            CASE
                WHEN tf.topix_return_20d_pct < 0 AND tf.topix_return_60d_pct >= 0
                    THEN 'market_pullback_in_uptrend'
                WHEN tf.topix_return_20d_pct < 0 AND tf.topix_return_60d_pct < 0
                    THEN 'market_downtrend_decline'
                WHEN tf.topix_return_20d_pct >= 0 AND tf.topix_return_60d_pct >= 0
                    THEN 'market_persistent_runup'
                WHEN tf.topix_return_20d_pct >= 0 AND tf.topix_return_60d_pct < 0
                    THEN 'market_relief_bounce'
                ELSE 'market_missing'
            END AS market_state
        FROM recent_return_threshold_scoped s
        LEFT JOIN short_term_topix_features tf USING (date)
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE short_term_case_panel AS
        SELECT
            p.*,
            c.anchor_date AS case_anchor_date,
            tf.topix_session_index - c.anchor_session_index AS case_offset_sessions
        FROM short_term_shock_panel p
        JOIN short_term_topix_features tf USING (date)
        JOIN short_term_case_dates c
          ON tf.topix_session_index BETWEEN c.anchor_session_index - {int(case_study_window_sessions)}
                                      AND c.anchor_session_index + {int(case_study_window_sessions)}
        """
    )


def _create_fast_observation_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: Sequence[int],
    market_source: str,
    market_scopes: Sequence[str],
    include_valuation_liquidity: bool,
) -> None:
    price_code = normalize_code_sql("sd.code")
    master_code = (
        normalize_code_sql("smd.code")
        if market_source == "stock_master_daily_exact_date"
        else normalize_code_sql("s.code")
    )
    lag_exprs = ",\n                ".join(
        f"lag(close, {window}) over (partition by code order by date) as close_lag_{window}d"
        for window in (20, 60)
    )
    forward_exprs = ",\n                ".join(
        [
            "lead(open, 1) over (partition by code order by date) as next_open",
            *[
                f"lead(close, {horizon}) over (partition by code order by date) as future_close_{horizon}d"
                for horizon in horizons
            ],
        ]
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
        market_source=market_source,
        master_code=master_code,
    )
    valuation_join = ""
    scoped_valuation_columns = """
                NULL::DOUBLE AS dv_per,
                NULL::DOUBLE AS dv_forward_per,
                NULL::DOUBLE AS dv_pbr,
                NULL::DOUBLE AS dv_p_op,
                NULL::DOUBLE AS dv_forward_p_op,
                NULL::DOUBLE AS dv_market_cap_bil_jpy,
                NULL::DOUBLE AS free_float_market_cap_jpy_source
    """
    valuation_select = """
            free_float_market_cap_jpy_source AS free_float_market_cap_jpy,
            NULL::DOUBLE AS liquidity_residual_z,
            'missing' AS liquidity_regime,
            dv_per AS per,
            dv_forward_per AS forward_per,
            dv_pbr AS pbr,
            dv_p_op AS p_op,
            dv_forward_p_op AS forward_p_op,
            dv_market_cap_bil_jpy AS market_cap_bil_jpy
    """
    residual_ctes = ""
    residual_from = "excess"
    if include_valuation_liquidity:
        _create_short_term_daily_valuation_view(conn)
        valuation_join = """
            LEFT JOIN short_term_daily_valuation dv
              ON dv.code = p.code
             AND dv.date = p.date
        """
        scoped_valuation_columns = """
                dv.per AS dv_per,
                dv.forward_per AS dv_forward_per,
                dv.pbr AS dv_pbr,
                dv.p_op AS dv_p_op,
                dv.forward_p_op AS dv_forward_p_op,
                dv.market_cap / 1000000000.0 AS dv_market_cap_bil_jpy,
                COALESCE(dv.free_float_market_cap, dv.market_cap) AS free_float_market_cap_jpy_source
        """
        valuation_select = """
            free_float_market_cap_jpy_source AS free_float_market_cap_jpy,
            liquidity_residual_z,
            CASE
                WHEN liquidity_residual_std IS NULL OR liquidity_residual_std <= 0 THEN 'missing'
                WHEN liquidity_residual_z >= 1
                  AND recent_return_20d_pct >= 0
                  AND recent_return_60d_pct >= 0 THEN 'rerating_participation'
                WHEN liquidity_residual_z >= 1 THEN 'distribution_stress'
                WHEN liquidity_residual_z <= -1 THEN 'stale_liquidity'
                ELSE 'neutral'
            END AS liquidity_regime,
            dv_per AS per,
            dv_forward_per AS forward_per,
            dv_pbr AS pbr,
            dv_p_op AS p_op,
            dv_forward_p_op AS forward_p_op,
            dv_market_cap_bil_jpy AS market_cap_bil_jpy
        """
        residual_ctes = """
        ,
        residual_source AS (
            SELECT
                *,
                CASE
                    WHEN med_adv60_sessions >= 60
                     AND med_adv60_jpy > 0
                     AND free_float_market_cap_jpy_source > 0
                        THEN ln(med_adv60_jpy)
                END AS log_adv60,
                CASE
                    WHEN med_adv60_sessions >= 60
                     AND med_adv60_jpy > 0
                     AND free_float_market_cap_jpy_source > 0
                        THEN ln(free_float_market_cap_jpy_source)
                END AS log_free_float_market_cap
            FROM excess
        ),
        residual_stats AS (
            SELECT
                *,
                regr_intercept(log_adv60, log_free_float_market_cap)
                    OVER (PARTITION BY date, market) AS residual_intercept,
                regr_slope(log_adv60, log_free_float_market_cap)
                    OVER (PARTITION BY date, market) AS residual_beta,
                count(log_adv60) OVER (PARTITION BY date, market) AS residual_observations
            FROM residual_source
        ),
        residual_values AS (
            SELECT
                *,
                CASE
                    WHEN residual_observations >= 50
                     AND residual_intercept IS NOT NULL
                     AND residual_beta IS NOT NULL
                        THEN log_adv60 - (residual_intercept + residual_beta * log_free_float_market_cap)
                END AS liquidity_residual
            FROM residual_stats
        ),
        residual_z_source AS (
            SELECT
                *,
                stddev_samp(liquidity_residual) OVER (PARTITION BY date, market)
                    AS liquidity_residual_std
            FROM residual_values
        ),
        residual_z AS (
            SELECT
                *,
                CASE
                    WHEN liquidity_residual_std > 0
                        THEN liquidity_residual / liquidity_residual_std
                END AS liquidity_residual_z
            FROM residual_z_source
        )
        """
        residual_from = "residual_z"
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
        scoped AS (
            SELECT
                p.*,
                m.company_name,
                m.market,
                m.market_code,
                m.scale_category,
                {scoped_valuation_columns}
            FROM prices p
            JOIN market_master m ON m.code = p.code AND m.date = p.date
            {valuation_join}
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
                case when close_lag_20d > 0 then (close / close_lag_20d - 1.0) * 100.0 end
                    as recent_return_20d_pct,
                case when close_lag_60d > 0 then (close / close_lag_60d - 1.0) * 100.0 end
                    as recent_return_60d_pct,
                {return_exprs},
                {topix_return_exprs}
            FROM featured f
            LEFT JOIN topix_featured tf USING (date)
        ),
        excess AS (
            SELECT
                *,
                {excess_exprs}
            FROM computed
        )
        {residual_ctes}
        SELECT
            *,
            {valuation_select}
        FROM {residual_from}
        {final_where}
        """,
        [*raw_params, *final_params],
    )
    _create_scoped_view(conn)


def _create_short_term_daily_valuation_view(conn: Any) -> None:
    if not _table_exists(conn, "daily_valuation"):
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE short_term_daily_valuation AS
            SELECT
                NULL::VARCHAR AS code,
                NULL::VARCHAR AS date,
                NULL::DOUBLE AS per,
                NULL::DOUBLE AS forward_per,
                NULL::DOUBLE AS pbr,
                NULL::DOUBLE AS p_op,
                NULL::DOUBLE AS forward_p_op,
                NULL::DOUBLE AS market_cap,
                NULL::DOUBLE AS free_float_market_cap
            WHERE FALSE
            """
        )
        return
    valuation_code = normalize_code_sql("dv.code")
    free_float_expr = (
        "CAST(dv.free_float_market_cap AS DOUBLE)"
        if _daily_valuation_column_exists(conn, "free_float_market_cap")
        else "CAST(NULL AS DOUBLE)"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE short_term_daily_valuation AS
        SELECT
            code,
            date,
            per,
            forward_per,
            pbr,
            p_op,
            forward_p_op,
            market_cap,
            free_float_market_cap
        FROM (
            SELECT
                {valuation_code} AS code,
                dv.date,
                CAST(dv.per AS DOUBLE) AS per,
                CAST(dv.forward_per AS DOUBLE) AS forward_per,
                {_optional_daily_valuation_double_expr(conn, "pbr")} AS pbr,
                {_optional_daily_valuation_double_expr(conn, "p_op")} AS p_op,
                {_optional_daily_valuation_double_expr(conn, "forward_p_op")} AS forward_p_op,
                CAST(dv.market_cap AS DOUBLE) AS market_cap,
                {free_float_expr} AS free_float_market_cap,
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


def _build_market_shock_calendar_df(
    conn: Any,
    *,
    shock_thresholds: Sequence[float],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for threshold in shock_thresholds:
        frame = conn.execute(
            """
            SELECT
                ? AS shock_threshold_pct,
                date,
                topix_return_1d_pct,
                topix_return_5d_pct,
                topix_return_20d_pct,
                topix_return_60d_pct,
                days_since_market_shock
            FROM short_term_topix_features
            WHERE topix_return_1d_pct <= ?
            ORDER BY date
            """,
            [float(threshold), float(threshold)],
        ).fetchdf()
        frames.append(frame)
    return _concat_sorted(
        frames,
        columns=[
            "shock_threshold_pct",
            "date",
            "topix_return_1d_pct",
            "topix_return_5d_pct",
            "topix_return_20d_pct",
            "topix_return_60d_pct",
            "days_since_market_shock",
        ],
    )


def _build_general_short_term_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    sample_scopes: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for sample_scope in sample_scopes:
        for state, condition in _PRICE_ACTION_STATES.items():
            frames.extend(
                _aggregate_all_returns(
                    conn,
                    condition=condition,
                    condition_fields={
                        "condition_family": "price_action_state",
                        "sample_scope": sample_scope,
                        "price_action_state": state,
                    },
                    source_name="short_term_shock_panel",
                    sample_scope=sample_scope,
                    horizons=horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_general_response_columns())


def _build_pullback_in_uptrend_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    pullback_thresholds_20d: Sequence[float],
    uptrend_thresholds_60d: Sequence[float],
    sample_scopes: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for sample_scope in sample_scopes:
        for pullback_threshold in pullback_thresholds_20d:
            for uptrend_threshold in uptrend_thresholds_60d:
                if pullback_threshold == 0:
                    condition = (
                        "recent_return_20d_pct < 0 "
                        f"AND recent_return_60d_pct >= {float(uptrend_threshold)}"
                    )
                else:
                    condition = (
                        f"recent_return_20d_pct <= {-float(pullback_threshold)} "
                        f"AND recent_return_60d_pct >= {float(uptrend_threshold)}"
                    )
                frames.extend(
                    _aggregate_all_returns(
                        conn,
                        condition=condition,
                        condition_fields={
                            "condition_family": "pullback_in_uptrend_threshold",
                            "sample_scope": sample_scope,
                            "pullback_threshold_20d_pct": float(pullback_threshold),
                            "uptrend_threshold_60d_pct": float(uptrend_threshold),
                            "condition_label": (
                                f"recent_return_20d_le_negative_{_threshold_token(pullback_threshold)}"
                                f"__recent_return_60d_ge_{_threshold_token(uptrend_threshold)}"
                            ),
                        },
                        source_name="short_term_shock_panel",
                        sample_scope=sample_scope,
                        horizons=horizons,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_pullback_response_columns())


def _build_market_shock_window_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    shock_thresholds: Sequence[float],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for shock_threshold in shock_thresholds:
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE short_term_shock_window_work AS
            WITH shock_dates AS (
                SELECT topix_session_index AS shock_session_index
                FROM short_term_topix_features
                WHERE topix_return_1d_pct <= {float(shock_threshold)}
            ),
            nearest AS (
                SELECT
                    p.*,
                    min(tf.topix_session_index - s.shock_session_index) AS shock_offset_sessions
                FROM short_term_shock_panel p
                JOIN short_term_topix_features tf USING (date)
                JOIN shock_dates s
                  ON tf.topix_session_index BETWEEN s.shock_session_index
                                              AND s.shock_session_index + 20
                GROUP BY ALL
            )
            SELECT *
            FROM nearest
            """
        )
        for order, (bucket, condition) in enumerate(_SHOCK_OFFSET_BUCKETS.items()):
            offset_condition = condition.replace(
                "days_since_market_shock", "shock_offset_sessions"
            )
            frames.extend(
                _aggregate_all_returns(
                    conn,
                    condition=offset_condition,
                    condition_fields={
                        "condition_family": "market_shock_window",
                        "sample_scope": "daily",
                        "shock_threshold_pct": float(shock_threshold),
                        "shock_offset_bucket": bucket,
                        "shock_offset_bucket_order": int(order),
                    },
                    source_name="short_term_shock_window_work",
                    sample_scope="daily",
                    horizons=horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_market_shock_window_columns())


def _build_stock_market_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for stock_state, stock_condition in _PRICE_ACTION_STATES.items():
        for market_state, market_condition in _MARKET_STATES.items():
            frames.extend(
                _aggregate_all_returns(
                    conn,
                    condition=f"({stock_condition}) AND ({market_condition})",
                    condition_fields={
                        "condition_family": "stock_market_price_action",
                        "sample_scope": "daily",
                        "stock_state": stock_state,
                        "market_state": market_state,
                    },
                    source_name="short_term_shock_panel",
                    sample_scope="daily",
                    horizons=horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_stock_market_interaction_columns())


def _build_liquidity_valuation_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE short_term_valuation_work AS
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
        FROM short_term_shock_panel
        WHERE pbr IS NOT NULL
          AND pbr > 0
          AND forward_per IS NOT NULL
          AND forward_per > 0
        """
    )
    conditions = {
        "pullback_in_uptrend": "recent_return_20d_pct < 0 AND recent_return_60d_pct >= 0",
        "deep_pullback_in_uptrend": "recent_return_20d_pct <= -10 AND recent_return_60d_pct >= 0",
        "recent_market_shock_pullback": (
            "days_since_market_shock BETWEEN 0 AND 5 "
            "AND recent_return_20d_pct < 0 AND recent_return_60d_pct >= 0"
        ),
    }
    valuation_buckets = {
        "both_low": "pbr_rank_pct <= 0.2 AND forward_per_rank_pct <= 0.2",
        "low_pbr_only": "pbr_rank_pct <= 0.2 AND NOT (forward_per_rank_pct <= 0.2)",
        "low_forward_per_only": "NOT (pbr_rank_pct <= 0.2) AND forward_per_rank_pct <= 0.2",
        "neither_low": "NOT (pbr_rank_pct <= 0.2) AND NOT (forward_per_rank_pct <= 0.2)",
    }
    frames: list[pd.DataFrame] = []
    for condition_name, base_condition in conditions.items():
        for valuation_bucket, valuation_condition in valuation_buckets.items():
            frames.extend(
                _aggregate_all_returns(
                    conn,
                    condition=f"({base_condition}) AND ({valuation_condition})",
                    condition_fields={
                        "condition_family": "liquidity_valuation_interaction",
                        "sample_scope": "daily",
                        "condition_name": condition_name,
                        "valuation_bucket": valuation_bucket,
                    },
                    source_name="short_term_valuation_work",
                    sample_scope="daily",
                    horizons=horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    sample_filter_override="TRUE",
                )
            )
    return _concat_sorted(frames, columns=_liquidity_valuation_interaction_columns())


def _build_case_study_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    case_offsets = [
        ("pre_5_1d", "case_offset_sessions BETWEEN -5 AND -1", -5),
        ("shock_day", "case_offset_sessions = 0", 0),
        ("post_1d", "case_offset_sessions = 1", 1),
        ("post_2_5d", "case_offset_sessions BETWEEN 2 AND 5", 2),
    ]
    case_dates = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT case_anchor_date FROM short_term_case_panel ORDER BY case_anchor_date"
        ).fetchall()
    ]
    for case_date in case_dates:
        for offset_name, offset_condition, offset_order in case_offsets:
            for state, state_condition in _PRICE_ACTION_STATES.items():
                frames.extend(
                    _aggregate_all_returns(
                        conn,
                        condition=(
                            f"case_anchor_date = '{str(case_date).replace("'", "''")}' "
                            f"AND ({offset_condition}) AND ({state_condition})"
                        ),
                        condition_fields={
                            "condition_family": "case_study",
                            "sample_scope": "daily",
                            "case_anchor_date": str(case_date),
                            "case_offset_bucket": offset_name,
                            "case_offset_sessions": int(offset_order),
                            "price_action_state": state,
                        },
                        source_name="short_term_case_panel",
                        sample_scope="daily",
                        horizons=horizons,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_case_study_columns())


def _aggregate_all_returns(
    conn: Any,
    *,
    condition: str,
    condition_fields: dict[str, Any],
    source_name: str,
    sample_scope: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_filter_override: str | None = None,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for return_metric in _RETURN_METRICS:
        for entry_mode in _ENTRY_MODES:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name=source_name,
                        condition=condition,
                        condition_fields={
                            **condition_fields,
                            "return_metric": return_metric,
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_metric_return_column(
                            return_metric, entry_mode, horizon
                        ),
                        sample_scope=sample_scope,
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                        sample_filter_override=sample_filter_override,
                    )
                )
    return frames


def _aggregate_condition(
    conn: Any,
    *,
    source_name: str,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    sample_scope: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
    sample_filter_override: str | None = None,
) -> pd.DataFrame:
    sample_filter = sample_filter_override or _sample_scope_filter(sample_scope)
    frame = conn.execute(
        f"""
        SELECT
            market_scope,
            liquidity_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({return_column}) AS mean_forward_return_pct,
            median({return_column}) AS median_forward_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_return_pct,
            quantile_cont({return_column}, 0.25) AS p25_forward_return_pct,
            quantile_cont({return_column}, 0.75) AS p75_forward_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(topix_return_1d_pct) AS median_topix_return_1d_pct,
            median(topix_return_20d_pct) AS median_topix_return_20d_pct,
            median(topix_return_60d_pct) AS median_topix_return_60d_pct,
            median(days_since_market_shock) AS median_days_since_market_shock,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(pbr) AS median_pbr,
            median(forward_per) AS median_forward_per,
            median(forward_p_op) AS median_forward_p_op
        FROM {source_name}
        WHERE {sample_filter}
          AND {condition}
          AND {return_column} IS NOT NULL
        GROUP BY market_scope, liquidity_scope
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    return frame.reindex(columns=[*condition_fields.keys(), *_response_metric_columns()])


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market,
            market_scope,
            liquidity_scope,
            price_action_state,
            market_state,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            topix_return_1d_pct,
            topix_return_20d_pct,
            topix_return_60d_pct,
            days_since_market_shock,
            liquidity_residual_z,
            pbr,
            forward_per,
            forward_p_op,
            forward_close_return_5d_pct,
            forward_close_excess_return_5d_pct,
            forward_close_return_20d_pct,
            forward_close_excess_return_20d_pct
        FROM short_term_shock_panel
        ORDER BY date, code, market_scope, liquidity_scope
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _metric_return_column(return_metric: str, entry_mode: str, horizon: int) -> str:
    if return_metric == "topix_excess":
        return _return_column(entry_mode, horizon)
    if return_metric == "raw":
        if entry_mode == "close_to_close":
            return f"forward_close_return_{horizon}d_pct"
        if entry_mode == "next_open_to_close":
            return f"forward_next_open_return_{horizon}d_pct"
    raise ValueError(f"unsupported return metric or entry mode: {return_metric} / {entry_mode}")


def _sample_scope_filter(sample_scope: str) -> str:
    if sample_scope == "daily":
        return "TRUE"
    if sample_scope == "weekly":
        return "is_weekly_anchor"
    if sample_scope == "monthly":
        return "is_month_end_anchor"
    raise ValueError(f"unsupported sample_scope: {sample_scope}")


def _threshold_token(value: float) -> str:
    return f"{float(value):.1f}".rstrip("0").rstrip(".").replace(".", "_")


def _response_metric_columns() -> list[str]:
    return [
        "market_scope",
        "liquidity_scope",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "p10_forward_return_pct",
        "p25_forward_return_pct",
        "p75_forward_return_pct",
        "p90_forward_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_topix_return_1d_pct",
        "median_topix_return_20d_pct",
        "median_topix_return_60d_pct",
        "median_days_since_market_shock",
        "median_liquidity_residual_z",
        "median_pbr",
        "median_forward_per",
        "median_forward_p_op",
    ]


def _base_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _general_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "price_action_state",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _pullback_response_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "pullback_threshold_20d_pct",
        "uptrend_threshold_60d_pct",
        "condition_label",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _market_shock_window_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "shock_threshold_pct",
        "shock_offset_bucket",
        "shock_offset_bucket_order",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _stock_market_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "stock_state",
        "market_state",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _liquidity_valuation_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "condition_name",
        "valuation_bucket",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


def _case_study_columns() -> list[str]:
    return [
        "condition_family",
        "sample_scope",
        "case_anchor_date",
        "case_offset_bucket",
        "case_offset_sessions",
        "price_action_state",
        "return_metric",
        "entry_mode",
        "horizon",
        *_response_metric_columns(),
    ]


__all__ = [
    "DEFAULT_CASE_STUDY_DATES",
    "DEFAULT_MARKET_SHOCK_THRESHOLDS",
    "DEFAULT_PULLBACK_THRESHOLDS_20D",
    "DEFAULT_UPTREND_THRESHOLDS_60D",
    "SHORT_TERM_SHOCK_FORWARD_RESPONSE_EXPERIMENT_ID",
    "ShortTermShockForwardResponseResult",
    "build_summary_markdown",
    "run_short_term_shock_forward_response_research",
    "write_short_term_shock_forward_response_bundle",
]
