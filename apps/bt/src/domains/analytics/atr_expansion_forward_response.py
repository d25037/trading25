"""ATR expansion forward-response research."""

from __future__ import annotations

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
from src.domains.analytics.ranking_color_evidence import (
    _create_observation_panel as _create_ranking_color_observation_panel,
)
from src.domains.analytics.recent_return_threshold_forward_response import (
    _market_master_cte,
    _sql_string_list,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

ATR_EXPANSION_FORWARD_RESPONSE_EXPERIMENT_ID = (
    "market-behavior/atr-expansion-forward-response"
)
DEFAULT_ATR_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth", "unknown")
_ENTRY_MODES: tuple[str, ...] = ("close_to_close", "next_open_to_close")
_EXPANSION_BUCKET_ORDER: tuple[str, ...] = (
    "top_10pct",
    "top_20pct",
    "middle_60pct",
    "bottom_20pct",
    "bottom_10pct",
)
_RETURN_REGIME_ORDER: tuple[str, ...] = (
    "persistent_runup",
    "short_pullback_in_uptrend",
    "short_bounce",
    "downtrend_decline",
)
_ATR_EXPANSION_STATE_ORDER: tuple[str, ...] = (
    "dual_expansion",
    "short_atr_expansion",
    "atr20_acceleration",
    "no_expansion",
)
_LIQUIDITY_COLOR_ATR_STATE_ORDER: tuple[str, ...] = (
    "all_atr",
    "overheat_excluded",
    "overheat_only",
    "atr20_acceleration_ex_overheat",
    *_ATR_EXPANSION_STATE_ORDER,
)
_ATR_PAIR_BUCKET_ORDER: tuple[str, ...] = ("high_20pct", "middle_60pct", "low_20pct")


@dataclass(frozen=True)
class AtrExpansionForwardResponseResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    atr_windows: tuple[int, ...]
    return_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    atr_expansion_response_df: pd.DataFrame
    return_regime_interaction_df: pd.DataFrame
    atr_pair_interaction_df: pd.DataFrame
    liquidity_color_atr_interaction_df: pd.DataFrame


def run_atr_expansion_forward_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    atr_windows: Iterable[int] = DEFAULT_ATR_WINDOWS,
    return_windows: Iterable[int] = DEFAULT_RETURN_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> AtrExpansionForwardResponseResult:
    resolved_atr_windows = tuple(sorted({int(window) for window in atr_windows}))
    resolved_return_windows = tuple(sorted({int(window) for window in return_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
    _validate_params(
        atr_windows=resolved_atr_windows,
        return_windows=resolved_return_windows,
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = _offset_calendar_date(
        start_date,
        days=-(max(max(resolved_atr_windows), max(resolved_return_windows)) * 4 + 30),
    )
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="atr-expansion-forward-response-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        _create_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            atr_windows=resolved_atr_windows,
            return_windows=resolved_return_windows,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_ranking_color_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_liquidity_color_atr_work(ctx.connection)
        observation_count = int(
            ctx.connection.execute("SELECT count(*) FROM atr_expansion_panel").fetchone()[0]
        )
        coverage_diagnostics_df = _build_coverage_diagnostics_df(ctx.connection)
        atr_expansion_response_df = _build_atr_expansion_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        return_regime_interaction_df = _build_return_regime_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        atr_pair_interaction_df = _build_atr_pair_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        liquidity_color_atr_interaction_df = _build_liquidity_color_atr_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        observation_sample_df = _query_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
            horizons=resolved_horizons,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return AtrExpansionForwardResponseResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(observation_sample_df["date"].min())
        if "date" in observation_sample_df and not observation_sample_df.empty
        else start_date,
        analysis_end_date=end_date,
        atr_windows=resolved_atr_windows,
        return_windows=resolved_return_windows,
        horizons=resolved_horizons,
        market_scopes=resolved_market_scopes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_count=observation_count,
        observation_sample_df=observation_sample_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
        atr_expansion_response_df=atr_expansion_response_df,
        return_regime_interaction_df=return_regime_interaction_df,
        atr_pair_interaction_df=atr_pair_interaction_df,
        liquidity_color_atr_interaction_df=liquidity_color_atr_interaction_df,
    )


def write_atr_expansion_forward_response_bundle(
    result: AtrExpansionForwardResponseResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=ATR_EXPANSION_FORWARD_RESPONSE_EXPERIMENT_ID,
        module=__name__,
        function="run_atr_expansion_forward_response_research",
        params={
            "atr_windows": list(result.atr_windows),
            "return_windows": list(result.return_windows),
            "horizons": list(result.horizons),
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
            "atr_expansion_response_df": result.atr_expansion_response_df,
            "return_regime_interaction_df": result.return_regime_interaction_df,
            "atr_pair_interaction_df": result.atr_pair_interaction_df,
            "liquidity_color_atr_interaction_df": (
                result.liquidity_color_atr_interaction_df
            ),
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: AtrExpansionForwardResponseResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=24)
    atr_response = _top_rows_for_markdown(
        result.atr_expansion_response_df,
        sort_columns=[
            "market_scope",
            "atr_feature",
            "expansion_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    return_regime = _top_rows_for_markdown(
        result.return_regime_interaction_df,
        sort_columns=[
            "market_scope",
            "return_regime_order",
            "atr_expansion_state_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    atr_pair = _top_rows_for_markdown(
        result.atr_pair_interaction_df,
        sort_columns=[
            "market_scope",
            "atr20_bucket_order",
            "atr60_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    liquidity_color_atr = _top_rows_for_markdown(
        result.liquidity_color_atr_interaction_df,
        sort_columns=[
            "market_scope",
            "liquidity_regime_order",
            "ui_color_order",
            "atr_expansion_state_order",
            "entry_mode",
            "horizon",
        ],
        limit=100,
    )
    return "\n".join(
        [
            "# ATR Expansion Forward Response",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Observation count: `{result.observation_count}`",
            f"- ATR windows: `{list(result.atr_windows)}`",
            f"- Return windows: `{list(result.return_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Market scopes: `{list(result.market_scopes)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Coverage Diagnostics",
            "",
            coverage,
            "",
            "## ATR Expansion Response",
            "",
            atr_response,
            "",
            "## Return Regime Interaction",
            "",
            return_regime,
            "",
            "## ATR Pair Interaction",
            "",
            atr_pair,
            "",
            "## Liquidity Color ATR Interaction",
            "",
            liquidity_color_atr,
            "",
        ]
    )


def _validate_params(
    *,
    atr_windows: Sequence[int],
    return_windows: Sequence[int],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not atr_windows or any(window <= 1 for window in atr_windows):
        raise ValueError("atr_windows must be greater than 1")
    if set(atr_windows) != {20, 60}:
        raise ValueError("atr_windows must include 20 and 60")
    if not return_windows or any(window <= 0 for window in return_windows):
        raise ValueError("return_windows must be positive")
    if not {20, 60}.issubset(set(return_windows)):
        raise ValueError("return_windows must include 20 and 60")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


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
    required = ("stock_data", "topix_data", "daily_valuation")
    missing = [table for table in required if not _table_exists(conn, table)]
    if missing:
        raise RuntimeError(
            f"market.duckdb missing required tables: {', '.join(missing)}"
        )
    if not _table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stock_master_daily for PIT universe scope")


def _create_observation_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    atr_windows: Sequence[int],
    return_windows: Sequence[int],
    horizons: Sequence[int],
    market_source: str,
    market_scopes: Sequence[str],
) -> None:
    price_code = normalize_code_sql("sd.code")
    master_code = (
        normalize_code_sql("smd.code")
        if market_source == "stock_master_daily_exact_date"
        else normalize_code_sql("s.code")
    )
    atr_exprs = ",\n                ".join(
        f"avg(true_range) over (partition by code order by date "
        f"rows between {window - 1} preceding and current row) as atr{window}"
        for window in atr_windows
    )
    atr_count_exprs = ",\n                ".join(
        f"count(true_range) over (partition by code order by date "
        f"rows between {window - 1} preceding and current row) as atr{window}_sessions"
        for window in atr_windows
    )
    lag_exprs = ",\n                ".join(
        f"lag(close, {window}) over (partition by code order by date) as close_lag_{window}d"
        for window in return_windows
    )
    forward_exprs = ",\n                ".join(
        [
            "lead(open, 1) over (partition by code order by date) as next_open",
            *[
                f"lead(close, {horizon}) over (partition by code order by date) "
                f"as future_close_{horizon}d"
                for horizon in horizons
            ],
        ]
    )
    completion_date_exprs = ",\n                ".join(
        f"lead(date, {horizon}) over (partition by code order by date) "
        f"as forward_outcome_completion_date_{horizon}d"
        for horizon in horizons
    )
    forward_value_selects = ",\n                ".join(
        [
            "pfv.next_open",
            *[
                f"pfv.future_close_{horizon}d"
                for horizon in horizons
            ],
            *[
                f"pfv.forward_outcome_completion_date_{horizon}d"
                for horizon in horizons
            ],
        ]
    )
    recent_exprs = ",\n            ".join(
        f"case when close_lag_{window}d > 0 then (close / close_lag_{window}d - 1.0) * 100.0 end "
        f"as recent_return_{window}d_pct"
        for window in return_windows
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
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE atr_expansion_panel AS
        WITH raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.open,
                sd.high,
                sd.low,
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
            SELECT code, date, open, high, low, close, volume
            FROM raw_prices
            WHERE row_rank = 1
              AND open > 0
              AND high > 0
              AND low > 0
              AND close > 0
        ),
        price_forward_values AS (
            SELECT
                code,
                date,
                {forward_exprs},
                {completion_date_exprs}
            FROM prices
        ),
        {master_cte},
        scoped AS (
            SELECT
                p.*,
                m.company_name,
                m.market,
                m.market_code,
                m.scale_category
            FROM prices p
            JOIN market_master m ON m.code = p.code AND m.date = p.date
            WHERE {market_filter}
        ),
        true_range_base AS (
            SELECT
                *,
                lag(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
            FROM scoped
        ),
        true_range AS (
            SELECT
                *,
                greatest(
                    high - low,
                    coalesce(abs(high - prev_close), 0.0),
                    coalesce(abs(low - prev_close), 0.0)
                ) AS true_range
            FROM true_range_base
        ),
        featured AS (
            SELECT
                *,
                median(close * volume) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS med_adv60_jpy,
                {atr_exprs},
                {atr_count_exprs},
                {lag_exprs}
            FROM true_range
        ),
        featured_with_forward AS (
            SELECT
                f.*,
                {forward_value_selects}
            FROM featured f
            JOIN price_forward_values pfv USING (code, date)
        ),
        featured_with_lag AS (
            SELECT
                *,
                lag(atr20, 20) over (partition by code order by date) as atr20_lag_20d
            FROM featured_with_forward
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
                case when close > 0 then atr20 / close * 100.0 end as atr20_pct,
                case when close > 0 then atr60 / close * 100.0 end as atr60_pct,
                case when atr60 > 0 then atr20 / atr60 end as atr20_to_atr60,
                case
                    when atr20_lag_20d > 0 then (atr20 / atr20_lag_20d - 1.0) * 100.0
                end as atr20_change_20d_pct,
                {recent_exprs},
                {return_exprs},
                {topix_return_exprs}
            FROM featured_with_lag f
            LEFT JOIN topix_featured tf USING (date)
        )
        SELECT
            *,
            {excess_exprs}
        FROM computed
        {final_where}
        """,
        [*raw_params, *final_params],
    )
    _create_scoped_view(conn, include_all="all" in market_scopes)


def _create_scoped_view(conn: Any, *, include_all: bool) -> None:
    all_union = (
        """
        UNION ALL
        SELECT *, 'all' AS market_scope
        FROM base
        """
        if include_all
        else ""
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW atr_expansion_scoped AS
        WITH base AS (
            SELECT
                p.*,
                strftime(CAST(p.date AS DATE), '%Y') AS anchor_year,
                strftime(CAST(p.date AS DATE), '%Y-%m') AS anchor_month
            FROM atr_expansion_panel p
        )
        SELECT *, market AS market_scope
        FROM base
        {all_union}
        """
    )


def _create_liquidity_color_atr_work(conn: Any) -> None:
    color_selects: list[str] = []
    for regime_order, (regime, ui_colors) in enumerate(_liquidity_color_sql().items()):
        for color_order, (ui_color, color_sql) in enumerate(ui_colors.items()):
            color_selects.append(
                f"""
                SELECT
                    a.*,
                    '{regime}' AS liquidity_regime,
                    {regime_order} AS liquidity_regime_order,
                    '{ui_color}' AS ui_color,
                    {color_order} AS ui_color_order
                FROM atr_expansion_scoped a
                JOIN ranking_color_liquidity_ranked r
                  ON r.code = a.code
                 AND r.date = a.date
                 AND r.market_scope = a.market_scope
                 AND r.liquidity_scope = '{regime}'
                WHERE {color_sql}
                """
            )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE atr_liquidity_color_work AS\n"
        + "\nUNION ALL\n".join(color_selects)
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            market AS market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            min(date) AS min_date,
            max(date) AS max_date,
            avg(CASE WHEN atr20_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_coverage_pct,
            avg(CASE WHEN atr60_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr60_coverage_pct,
            avg(CASE WHEN atr20_to_atr60 IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_to_atr60_coverage_pct,
            avg(CASE WHEN atr20_change_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_change_20d_coverage_pct
        FROM atr_expansion_panel
        GROUP BY market
        """
    ).fetchdf()
    return _sort_summary_df(
        frame,
        columns=[
            "market_scope",
            "observation_count",
            "code_count",
            "date_count",
            "min_date",
            "max_date",
            "atr20_coverage_pct",
            "atr60_coverage_pct",
            "atr20_to_atr60_coverage_pct",
            "atr20_change_20d_coverage_pct",
        ],
    )


def _build_atr_expansion_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in ("atr20_pct", "atr60_pct", "atr20_to_atr60", "atr20_change_20d_pct"):
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW atr_expansion_percentile_work AS
            SELECT
                *,
                percent_rank() OVER (
                    PARTITION BY market_scope, anchor_year
                    ORDER BY {feature}
                ) AS atr_feature_rank_pct
            FROM atr_expansion_scoped
            WHERE {feature} IS NOT NULL
            """
        )
        for bucket in _EXPANSION_BUCKET_ORDER:
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="atr_expansion_percentile_work",
                            condition=_percentile_condition(bucket),
                            condition_fields={
                                "condition_family": "annual_atr_percentile_bucket",
                                "atr_feature": feature,
                                "expansion_bucket": bucket,
                                "expansion_bucket_order": _EXPANSION_BUCKET_ORDER.index(
                                    bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_atr_expansion_response_columns())


def _build_return_regime_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for return_regime in _RETURN_REGIME_ORDER:
        for atr_state in _ATR_EXPANSION_STATE_ORDER:
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            condition=(
                                f"({_return_regime_condition(return_regime)}) "
                                f"AND ({_atr_expansion_state_condition(atr_state)})"
                            ),
                            condition_fields={
                                "condition_family": "return_regime_atr_expansion",
                                "return_regime": return_regime,
                                "return_regime_order": _RETURN_REGIME_ORDER.index(
                                    return_regime
                                ),
                                "atr_expansion_state": atr_state,
                                "atr_expansion_state_order": _ATR_EXPANSION_STATE_ORDER.index(
                                    atr_state
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_return_regime_interaction_columns())


def _build_atr_pair_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW atr_pair_percentile_work AS
        SELECT
            *,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY atr20_pct
            ) AS atr20_rank_pct,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY atr60_pct
            ) AS atr60_rank_pct
        FROM atr_expansion_scoped
        WHERE atr20_pct IS NOT NULL
          AND atr60_pct IS NOT NULL
        """
    )
    frames: list[pd.DataFrame] = []
    for atr20_bucket in _ATR_PAIR_BUCKET_ORDER:
        for atr60_bucket in _ATR_PAIR_BUCKET_ORDER:
            condition = (
                f"{_pair_bucket_condition('atr20_rank_pct', atr20_bucket)} "
                f"AND {_pair_bucket_condition('atr60_rank_pct', atr60_bucket)}"
            )
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="atr_pair_percentile_work",
                            condition=condition,
                            condition_fields={
                                "condition_family": "atr20_atr60_pair",
                                "atr20_bucket": atr20_bucket,
                                "atr20_bucket_order": _ATR_PAIR_BUCKET_ORDER.index(
                                    atr20_bucket
                                ),
                                "atr60_bucket": atr60_bucket,
                                "atr60_bucket_order": _ATR_PAIR_BUCKET_ORDER.index(
                                    atr60_bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_atr_pair_interaction_columns())


def _build_liquidity_color_atr_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for state in _LIQUIDITY_COLOR_ATR_STATE_ORDER:
        for entry_mode in _ENTRY_MODES:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="atr_liquidity_color_work",
                        condition=_liquidity_color_atr_state_condition(state),
                        condition_fields={
                            "condition_family": "liquidity_color_atr_expansion",
                            "atr_expansion_state": state,
                            "atr_expansion_state_order": _LIQUIDITY_COLOR_ATR_STATE_ORDER.index(
                                state
                            ),
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_return_column(entry_mode, horizon),
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                        group_columns=[
                            "market_scope",
                            "liquidity_regime",
                            "liquidity_regime_order",
                            "ui_color",
                            "ui_color_order",
                        ],
                    )
                )
    return _concat_sorted(frames, columns=_liquidity_color_atr_interaction_columns())


def _aggregate_condition(
    conn: Any,
    *,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
    source_name: str = "atr_expansion_scoped",
    group_columns: Sequence[str] = ("market_scope",),
) -> pd.DataFrame:
    group_select = ",\n            ".join(group_columns)
    group_by = ", ".join(group_columns)
    frame = conn.execute(
        f"""
        SELECT
            {group_select},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({return_column}) AS mean_forward_excess_return_pct,
            median({return_column}) AS median_forward_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({return_column}, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont({return_column}, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(atr20_pct) AS median_atr20_pct,
            median(atr60_pct) AS median_atr60_pct,
            median(atr20_to_atr60) AS median_atr20_to_atr60,
            median(atr20_change_20d_pct) AS median_atr20_change_20d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy
        FROM {source_name}
        WHERE {condition}
          AND {return_column} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), *group_columns]
    ordered.extend(
        column for column in _base_response_columns() if column not in group_columns
    )
    return frame.reindex(columns=ordered)


def _query_observation_sample_df(
    conn: Any,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    return_columns = ",\n            ".join(
        [
            *[
                f"forward_close_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"forward_next_open_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            scale_category,
            close,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            atr20_pct,
            atr60_pct,
            atr20_to_atr60,
            atr20_change_20d_pct,
            recent_return_20d_pct,
            recent_return_60d_pct,
            {return_columns}
        FROM atr_expansion_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _percentile_condition(bucket: str) -> str:
    if bucket == "top_10pct":
        return "atr_feature_rank_pct >= 0.9"
    if bucket == "top_20pct":
        return "atr_feature_rank_pct >= 0.8 AND atr_feature_rank_pct < 0.9"
    if bucket == "middle_60pct":
        return "atr_feature_rank_pct > 0.2 AND atr_feature_rank_pct < 0.8"
    if bucket == "bottom_20pct":
        return "atr_feature_rank_pct > 0.1 AND atr_feature_rank_pct <= 0.2"
    if bucket == "bottom_10pct":
        return "atr_feature_rank_pct <= 0.1"
    raise ValueError(f"unsupported expansion bucket: {bucket}")


def _return_regime_condition(regime: str) -> str:
    if regime == "persistent_runup":
        return "recent_return_20d_pct > 0 AND recent_return_60d_pct > 0"
    if regime == "short_pullback_in_uptrend":
        return "recent_return_20d_pct <= 0 AND recent_return_60d_pct > 0"
    if regime == "short_bounce":
        return "recent_return_20d_pct > 0 AND recent_return_60d_pct <= 0"
    if regime == "downtrend_decline":
        return "recent_return_20d_pct <= 0 AND recent_return_60d_pct <= 0"
    raise ValueError(f"unsupported return_regime: {regime}")


def _atr_expansion_state_condition(state: str) -> str:
    ratio_expansion = "atr20_to_atr60 >= 1.25"
    acceleration = "atr20_change_20d_pct >= 25.0"
    if state == "dual_expansion":
        return f"{ratio_expansion} AND {acceleration}"
    if state == "short_atr_expansion":
        return f"{ratio_expansion} AND NOT ({acceleration})"
    if state == "atr20_acceleration":
        return f"NOT ({ratio_expansion}) AND {acceleration}"
    if state == "no_expansion":
        return f"NOT ({ratio_expansion}) AND NOT ({acceleration})"
    raise ValueError(f"unsupported atr_expansion_state: {state}")


def _liquidity_color_atr_state_condition(state: str) -> str:
    if state == "all_atr":
        return "TRUE"
    if state == "overheat_excluded":
        return f"recent_return_20d_pct < {OVERHEAT_RETURN_20D_THRESHOLD_PCT}"
    if state == "overheat_only":
        return f"recent_return_20d_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT}"
    if state == "atr20_acceleration_ex_overheat":
        return (
            f"recent_return_20d_pct < {OVERHEAT_RETURN_20D_THRESHOLD_PCT} "
            f"AND ({_atr_expansion_state_condition('atr20_acceleration')})"
        )
    return _atr_expansion_state_condition(state)


def _liquidity_color_sql() -> dict[str, dict[str, str]]:
    strong_value = (
        "(r.pbr_percentile <= 0.2 AND r.forward_per_percentile <= 0.2) "
        "OR (r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 0.8)"
    )
    neutral_green = (
        "r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 0.8"
    )
    medium_value = (
        "r.pbr_percentile <= 0.2 "
        "OR (r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 1.0)"
    )
    return {
        "crowded_rerating": {
            "green": f"({strong_value})",
            "blue": f"({medium_value}) AND NOT ({strong_value})",
            "yellow": f"NOT ({medium_value})",
        },
        "neutral_rerating": {
            "green": f"({neutral_green})",
            "blue": f"NOT ({neutral_green})",
        },
    }


def _pair_bucket_condition(rank_column: str, bucket: str) -> str:
    if bucket == "high_20pct":
        return f"{rank_column} >= 0.8"
    if bucket == "middle_60pct":
        return f"{rank_column} > 0.2 AND {rank_column} < 0.8"
    if bucket == "low_20pct":
        return f"{rank_column} <= 0.2"
    raise ValueError(f"unsupported pair bucket: {bucket}")


def _return_column(entry_mode: str, horizon: int) -> str:
    if entry_mode == "close_to_close":
        return f"forward_close_excess_return_{horizon}d_pct"
    if entry_mode == "next_open_to_close":
        return f"forward_next_open_excess_return_{horizon}d_pct"
    raise ValueError(f"unsupported entry_mode: {entry_mode}")


def _offset_calendar_date(date: str | None, *, days: int) -> str | None:
    if date is None:
        return None
    return (pd.Timestamp(date) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _concat_sorted(
    frames: Sequence[pd.DataFrame],
    *,
    columns: Sequence[str],
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
        "market_scope",
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
        "median_atr20_pct",
        "median_atr60_pct",
        "median_atr20_to_atr60",
        "median_atr20_change_20d_pct",
        "median_med_adv60_mil_jpy",
    ]


def _atr_expansion_response_columns() -> list[str]:
    return [
        "condition_family",
        "atr_feature",
        "expansion_bucket",
        "expansion_bucket_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _return_regime_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "return_regime",
        "return_regime_order",
        "atr_expansion_state",
        "atr_expansion_state_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _atr_pair_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "atr20_bucket",
        "atr20_bucket_order",
        "atr60_bucket",
        "atr60_bucket_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _liquidity_color_atr_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "atr_expansion_state",
        "atr_expansion_state_order",
        "entry_mode",
        "horizon",
        "market_scope",
        "liquidity_regime",
        "liquidity_regime_order",
        "ui_color",
        "ui_color_order",
        *[
            column
            for column in _base_response_columns()
            if column
            not in {
                "market_scope",
                "liquidity_regime",
                "liquidity_regime_order",
                "ui_color",
                "ui_color_order",
            }
        ],
    ]
