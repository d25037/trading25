"""Fast research for Ranking valuation/liquidity color evidence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
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
from src.shared.utils.market_code_alias import MARKET_CODES_BY_SCOPE, normalize_market_scope

RANKING_COLOR_EVIDENCE_EXPERIMENT_ID = "market-behavior/ranking-color-evidence"
DEFAULT_HORIZONS: tuple[int, ...] = (20,)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
)
_VALUATION_FEATURES: tuple[str, ...] = (
    "per",
    "forward_per",
    "forward_p_op",
    "pbr",
)
_VALUATION_BUCKETS: tuple[str, ...] = (
    "cheapest_10pct",
    "cheapest_20pct",
    "middle_60pct",
    "expensive_20pct",
    "expensive_10pct",
)
_FORWARD_PER_POP_BUCKETS: tuple[str, ...] = (
    "low_forward_per_low_forward_p_op",
    "low_forward_per_high_forward_p_op",
    "low_forward_per_only",
    "low_forward_p_op_only",
    "neither_extreme",
)
_PER_RELATION_FEATURES: tuple[str, ...] = (
    "forward_per_to_per_ratio",
    "forward_p_op_to_per_ratio",
)
_LOW_PER_SCOPES: tuple[tuple[str, float], ...] = (
    ("low_per_10pct", 0.10),
    ("low_per_20pct", 0.20),
)
_RELATION_RATIO_BUCKETS: tuple[tuple[str, str], ...] = (
    ("ratio_lte_0_8", "{column} <= 0.8"),
    ("ratio_0_8_to_1_0", "{column} > 0.8 AND {column} <= 1.0"),
    ("ratio_1_0_to_1_25", "{column} > 1.0 AND {column} <= 1.25"),
    ("ratio_gt_1_25", "{column} > 1.25"),
)
_LIQUIDITY_REGIMES: tuple[str, ...] = (
    "neutral_rerating",
    "crowded_rerating",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
)
_TOPIX_REGIMES: tuple[tuple[str, str], ...] = (
    (
        "all_topix",
        "topix_recent_return_20d_pct IS NOT NULL AND topix_recent_return_60d_pct IS NOT NULL",
    ),
    (
        "topix_20d_ge_0_60d_ge_0",
        "topix_recent_return_20d_pct >= 0 AND topix_recent_return_60d_pct >= 0",
    ),
    (
        "topix_20d_lt_0_60d_gt_0",
        "topix_recent_return_20d_pct < 0 AND topix_recent_return_60d_pct > 0",
    ),
    (
        "topix_20d_lt_0",
        "topix_recent_return_20d_pct < 0",
    ),
    (
        "topix_60d_lt_0",
        "topix_recent_return_60d_pct < 0",
    ),
)
_RERATING_VALUE_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_value", "TRUE"),
    (
        "no_value_confirmation",
        "NOT (pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0))",
    ),
    ("low_pbr20", "pbr_percentile <= 0.2"),
    ("low_fwd_per20", "forward_per_percentile <= 0.2"),
    (
        "low_pbr20_low_fwd_per20",
        "pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2",
    ),
    (
        "low_per20_fwdper_per_lte_0_8",
        "per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8",
    ),
    (
        "medium_value_confirmation",
        "pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)",
    ),
    (
        "strong_value_confirmation",
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)",
    ),
)
_HIGH_VALUATION_CONDITIONS: tuple[tuple[str, str], ...] = (
    (
        "all_positive_per_pbr",
        "per_percentile IS NOT NULL AND pbr_percentile IS NOT NULL",
    ),
    (
        "high_per20_high_pbr20",
        "per_percentile >= 0.8 AND pbr_percentile >= 0.8",
    ),
    (
        "high_forward_per20_high_pbr20",
        "forward_per_percentile >= 0.8 AND pbr_percentile >= 0.8",
    ),
    (
        "high_per_or_pbr20",
        "per_percentile >= 0.8 OR pbr_percentile >= 0.8",
    ),
    (
        "not_high_per_pbr20",
        "per_percentile < 0.8 AND pbr_percentile < 0.8",
    ),
)
_MARKET_CAP_ABS_BUCKETS: tuple[tuple[str, str], ...] = (
    ("cap_lt_10bn", "market_cap_bil_jpy > 0 AND market_cap_bil_jpy < 10"),
    ("cap_10_50bn", "market_cap_bil_jpy >= 10 AND market_cap_bil_jpy < 50"),
    ("cap_50_200bn", "market_cap_bil_jpy >= 50 AND market_cap_bil_jpy < 200"),
    ("cap_200bn_1tn", "market_cap_bil_jpy >= 200 AND market_cap_bil_jpy < 1000"),
    ("cap_ge_1tn", "market_cap_bil_jpy >= 1000"),
)
_ADV60_ABS_BUCKETS: tuple[tuple[str, str], ...] = (
    (
        "adv_lt_10mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy > 0 AND med_adv60_jpy < 10000000",
    ),
    (
        "adv_10_50mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 10000000 "
        "AND med_adv60_jpy < 50000000",
    ),
    (
        "adv_50_300mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 50000000 "
        "AND med_adv60_jpy < 300000000",
    ),
    (
        "adv_300mn_1bn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 300000000 "
        "AND med_adv60_jpy < 1000000000",
    ),
    (
        "adv_ge_1bn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 1000000000",
    ),
)


@dataclass(frozen=True)
class RankingColorEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    ranking_color_evidence_df: pd.DataFrame
    per_relation_evidence_df: pd.DataFrame
    low_per_relation_evidence_df: pd.DataFrame
    low_per_relation_level_evidence_df: pd.DataFrame
    forward_per_pop_interaction_df: pd.DataFrame
    liquidity_regime_evidence_df: pd.DataFrame
    topix_regime_liquidity_value_evidence_df: pd.DataFrame
    rerating_good_valuation_chain_df: pd.DataFrame
    liquidity_color_long_trend_evidence_df: pd.DataFrame
    high_valuation_size_liquidity_interaction_df: pd.DataFrame


def run_ranking_color_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingColorEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = _offset_calendar_date(start_date, days=-150)
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-color-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        _create_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        observation_count = int(
            ctx.connection.execute("SELECT count(*) FROM ranking_color_panel").fetchone()[0]
        )
        result = RankingColorEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            ranking_color_evidence_df=_build_ranking_color_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            per_relation_evidence_df=_build_per_relation_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            low_per_relation_evidence_df=_build_low_per_relation_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            low_per_relation_level_evidence_df=(
                _build_low_per_relation_level_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            forward_per_pop_interaction_df=_build_forward_per_pop_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            liquidity_regime_evidence_df=_build_liquidity_regime_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            topix_regime_liquidity_value_evidence_df=(
                _build_topix_regime_liquidity_value_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            rerating_good_valuation_chain_df=(
                _build_rerating_good_valuation_chain_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            liquidity_color_long_trend_evidence_df=(
                _build_liquidity_color_long_trend_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            high_valuation_size_liquidity_interaction_df=(
                _build_high_valuation_size_liquidity_interaction_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_color_evidence_bundle(
    result: RankingColorEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_COLOR_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_color_evidence",
        function="run_ranking_color_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "ranking_color_evidence_df": result.ranking_color_evidence_df,
            "per_relation_evidence_df": result.per_relation_evidence_df,
            "low_per_relation_evidence_df": result.low_per_relation_evidence_df,
            "low_per_relation_level_evidence_df": (
                result.low_per_relation_level_evidence_df
            ),
            "forward_per_pop_interaction_df": result.forward_per_pop_interaction_df,
            "liquidity_regime_evidence_df": result.liquidity_regime_evidence_df,
            "topix_regime_liquidity_value_evidence_df": (
                result.topix_regime_liquidity_value_evidence_df
            ),
            "rerating_good_valuation_chain_df": result.rerating_good_valuation_chain_df,
            "liquidity_color_long_trend_evidence_df": (
                result.liquidity_color_long_trend_evidence_df
            ),
            "high_valuation_size_liquidity_interaction_df": (
                result.high_valuation_size_liquidity_interaction_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingColorEvidenceResult) -> str:
    parts = [
        "# Ranking Color Evidence",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Ranking Color Evidence",
        "",
        _top_rows_for_markdown(result.ranking_color_evidence_df, limit=60),
        "",
        "## Forward Valuation vs PER Relation Evidence",
        "",
        _top_rows_for_markdown(result.per_relation_evidence_df, limit=40),
        "",
        "## Low PER x Forward Valuation Relation Evidence",
        "",
        _top_rows_for_markdown(result.low_per_relation_evidence_df, limit=80),
        "",
        "## Low PER x Forward Valuation Relation Level Evidence",
        "",
        _top_rows_for_markdown(
            result.low_per_relation_level_evidence_df,
            limit=80,
        ),
        "",
        "## Forward PER x Forward P/OP Interaction",
        "",
        _top_rows_for_markdown(result.forward_per_pop_interaction_df, limit=40),
        "",
        "## Liquidity Regime Evidence",
        "",
        _top_rows_for_markdown(result.liquidity_regime_evidence_df, limit=40),
        "",
        "## TOPIX Regime x Liquidity x Value Evidence",
        "",
        _top_rows_for_markdown(
            result.topix_regime_liquidity_value_evidence_df,
            limit=120,
        ),
        "",
        "## Rerating Good x PER > Fwd PER > Fwd P/OP",
        "",
        _top_rows_for_markdown(
            result.rerating_good_valuation_chain_df,
            limit=80,
        ),
        "",
        "## Liquidity Color x Long Trend Evidence",
        "",
        _top_rows_for_markdown(
            result.liquidity_color_long_trend_evidence_df,
            limit=120,
        ),
        "",
        "## High Valuation x Size x Liquidity Interaction",
        "",
        _top_rows_for_markdown(
            result.high_valuation_size_liquidity_interaction_df,
            limit=160,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if not _table_exists(conn, "stock_master_daily"):
        missing.append("stock_master_daily")
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_observation_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
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
    forward_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (partition by code order by date) as future_close_{horizon}d"
        for horizon in horizons
    )
    return_exprs = ",\n            ".join(
        f"case when close > 0 and future_close_{horizon}d > 0 then "
        f"(future_close_{horizon}d / close - 1.0) * 100.0 end "
        f"as forward_close_return_{horizon}d_pct"
        for horizon in horizons
    )
    topix_forward_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (order by date) as topix_future_close_{horizon}d"
        for horizon in horizons
    )
    topix_lag_exprs = ",\n                ".join(
        f"lag(close, {lookback}) over (order by date) as topix_close_lag_{lookback}d"
        for lookback in (20, 60)
    )
    topix_return_exprs = ",\n            ".join(
        f"case when topix_close > 0 and topix_future_close_{horizon}d > 0 then "
        f"(topix_future_close_{horizon}d / topix_close - 1.0) * 100.0 end "
        f"as topix_close_return_{horizon}d_pct"
        for horizon in horizons
    )
    excess_exprs = ",\n            ".join(
        f"forward_close_return_{horizon}d_pct - topix_close_return_{horizon}d_pct "
        f"as forward_close_excess_return_{horizon}d_pct"
        for horizon in horizons
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
    final_where = "" if not final_conditions else "WHERE " + " AND ".join(final_conditions)
    market_filter = (
        "TRUE"
        if "all" in market_scopes
        else f"m.market IN ({_sql_string_list(market_scopes)})"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_color_panel AS
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
        {_market_master_cte(market_source=market_source, master_code=master_code)},
        scoped AS (
            SELECT
                p.*,
                m.company_name,
                m.market,
                m.market_code,
                m.scale_category,
                dv.per,
                dv.forward_per,
                dv.pbr,
                dv.p_op,
                dv.forward_p_op,
                dv.market_cap / 1000000000.0 AS market_cap_bil_jpy,
                coalesce(dv.free_float_market_cap, dv.market_cap) AS free_float_market_cap_jpy
            FROM prices p
            JOIN market_master m ON m.code = p.code AND m.date = p.date
            LEFT JOIN ranking_color_daily_valuation dv
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
                lag(close, 20) over (partition by code order by date) as close_lag_20d,
                lag(close, 60) over (partition by code order by date) as close_lag_60d,
                lag(close, 120) over (partition by code order by date) as close_lag_120d,
                lag(close, 150) over (partition by code order by date) as close_lag_150d,
                {forward_exprs}
            FROM scoped
        ),
        topix_featured AS (
            SELECT
                date,
                close AS topix_close,
                {topix_lag_exprs},
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
                case when close_lag_120d > 0 then (close / close_lag_120d - 1.0) * 100.0 end
                    as recent_return_120d_pct,
                case when close_lag_150d > 0 then (close / close_lag_150d - 1.0) * 100.0 end
                    as recent_return_150d_pct,
                case when topix_close_lag_20d > 0 then (topix_close / topix_close_lag_20d - 1.0) * 100.0 end
                    as topix_recent_return_20d_pct,
                case when topix_close_lag_60d > 0 then (topix_close / topix_close_lag_60d - 1.0) * 100.0 end
                    as topix_recent_return_60d_pct,
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
        residual_group_stats AS (
            SELECT
                date,
                market,
                count(*) AS residual_observations,
                avg(log_adv60) AS avg_log_adv60,
                avg(log_free_float_market_cap) AS avg_log_free_float_market_cap,
                var_samp(log_free_float_market_cap) AS var_log_free_float_market_cap,
                covar_samp(log_free_float_market_cap, log_adv60) AS covar_log_cap_adv
            FROM residual_source
            WHERE log_adv60 IS NOT NULL
              AND log_free_float_market_cap IS NOT NULL
            GROUP BY date, market
        ),
        residual_stats AS (
            SELECT
                rs.*,
                rgs.residual_observations,
                CASE
                    WHEN rgs.var_log_free_float_market_cap > 0
                        THEN rgs.covar_log_cap_adv / rgs.var_log_free_float_market_cap
                END AS residual_beta,
                CASE
                    WHEN rgs.var_log_free_float_market_cap > 0
                        THEN rgs.avg_log_adv60
                            - (rgs.covar_log_cap_adv / rgs.var_log_free_float_market_cap)
                            * rgs.avg_log_free_float_market_cap
                END AS residual_intercept
            FROM residual_source rs
            LEFT JOIN residual_group_stats rgs
              ON rgs.date = rs.date
             AND rgs.market = rs.market
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
        final_panel AS (
            SELECT
                *,
                CASE
                    WHEN liquidity_residual_std > 0
                        THEN liquidity_residual / liquidity_residual_std
                END AS liquidity_residual_z
            FROM residual_z_source
        )
        SELECT
            *,
            CASE
                WHEN liquidity_residual_std IS NULL OR liquidity_residual_std <= 0 THEN 'missing'
                WHEN liquidity_residual_z >= 1
                  AND recent_return_20d_pct >= 0
                  AND recent_return_60d_pct >= 0 THEN 'crowded_rerating'
                WHEN liquidity_residual_z >= 1 THEN 'distribution_stress'
                WHEN liquidity_residual_z <= -1 THEN 'stale_liquidity'
                WHEN liquidity_residual_z > -1
                  AND liquidity_residual_z < 1
                  AND recent_return_20d_pct >= 0
                  AND recent_return_60d_pct >= 0 THEN 'neutral_rerating'
                ELSE 'neutral'
            END AS liquidity_regime
        FROM final_panel
        {final_where}
        """,
        [*raw_params, *final_params],
    )
    _create_percentile_view(conn, include_all_scope="all" in market_scopes)


def _create_percentile_view(conn: Any, *, include_all_scope: bool) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_color_panel_relations AS
        SELECT
            *,
            CASE
                WHEN per > 0 AND forward_per > 0 THEN forward_per / per
            END AS forward_per_to_per_ratio,
            CASE
                WHEN per > 0 AND forward_p_op > 0 THEN forward_p_op / per
            END AS forward_p_op_to_per_ratio
        FROM ranking_color_panel
        """
    )
    all_scope_union = (
        """
        UNION ALL
        SELECT *, 'all' AS market_scope, 'all_liquidity' AS liquidity_scope
        FROM ranking_color_panel_relations
        """
        if include_all_scope
        else ""
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_color_scoped AS
        SELECT *, market AS market_scope, 'all_liquidity' AS liquidity_scope
        FROM ranking_color_panel_relations
        UNION ALL
        SELECT *, market AS market_scope, liquidity_regime AS liquidity_scope
        FROM ranking_color_panel_relations
        {all_scope_union}
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_color_ranked AS
        SELECT
            * EXCLUDE (
                per_valid_count,
                per_rank,
                forward_per_valid_count,
                forward_per_rank,
                forward_p_op_valid_count,
                forward_p_op_rank,
                pbr_valid_count,
                pbr_rank
            ),
            CASE
                WHEN per > 0 AND per_valid_count <= 1 THEN 0.0
                WHEN per > 0 THEN (per_rank - 1.0) / (per_valid_count - 1.0)
            END AS per_percentile,
            CASE
                WHEN forward_per > 0 AND forward_per_valid_count <= 1 THEN 0.0
                WHEN forward_per > 0 THEN (forward_per_rank - 1.0) / (forward_per_valid_count - 1.0)
            END AS forward_per_percentile,
            CASE
                WHEN forward_p_op > 0 AND forward_p_op_valid_count <= 1 THEN 0.0
                WHEN forward_p_op > 0 THEN (forward_p_op_rank - 1.0) / (forward_p_op_valid_count - 1.0)
            END AS forward_p_op_percentile,
            CASE
                WHEN pbr > 0 AND pbr_valid_count <= 1 THEN 0.0
                WHEN pbr > 0 THEN (pbr_rank - 1.0) / (pbr_valid_count - 1.0)
            END AS pbr_percentile
        FROM (
            SELECT
                *,
                count(*) FILTER (WHERE per > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS per_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN per > 0 THEN per END NULLS LAST
                ) AS per_rank,
                count(*) FILTER (WHERE forward_per > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_per_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN forward_per > 0 THEN forward_per END NULLS LAST
                ) AS forward_per_rank,
                count(*) FILTER (WHERE forward_p_op > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_p_op_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN forward_p_op > 0 THEN forward_p_op END NULLS LAST
                ) AS forward_p_op_rank,
                count(*) FILTER (WHERE pbr > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS pbr_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN pbr > 0 THEN pbr END NULLS LAST
                ) AS pbr_rank
            FROM ranking_color_scoped
            WHERE liquidity_scope = 'all_liquidity'
        )
        """
    )
    _add_per_relation_percentiles(conn, table_name="ranking_color_ranked")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_color_liquidity_ranked AS
        SELECT
            * EXCLUDE (
                per_valid_count,
                per_rank,
                forward_per_valid_count,
                forward_per_rank,
                forward_p_op_valid_count,
                forward_p_op_rank,
                pbr_valid_count,
                pbr_rank
            ),
            CASE
                WHEN per > 0 AND per_valid_count <= 1 THEN 0.0
                WHEN per > 0 THEN (per_rank - 1.0) / (per_valid_count - 1.0)
            END AS per_percentile,
            CASE
                WHEN forward_per > 0 AND forward_per_valid_count <= 1 THEN 0.0
                WHEN forward_per > 0 THEN (forward_per_rank - 1.0) / (forward_per_valid_count - 1.0)
            END AS forward_per_percentile,
            CASE
                WHEN forward_p_op > 0 AND forward_p_op_valid_count <= 1 THEN 0.0
                WHEN forward_p_op > 0 THEN (forward_p_op_rank - 1.0) / (forward_p_op_valid_count - 1.0)
            END AS forward_p_op_percentile,
            CASE
                WHEN pbr > 0 AND pbr_valid_count <= 1 THEN 0.0
                WHEN pbr > 0 THEN (pbr_rank - 1.0) / (pbr_valid_count - 1.0)
            END AS pbr_percentile
        FROM (
            SELECT
                *,
                count(*) FILTER (WHERE per > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS per_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN per > 0 THEN per END NULLS LAST
                ) AS per_rank,
                count(*) FILTER (WHERE forward_per > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_per_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN forward_per > 0 THEN forward_per END NULLS LAST
                ) AS forward_per_rank,
                count(*) FILTER (WHERE forward_p_op > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_p_op_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN forward_p_op > 0 THEN forward_p_op END NULLS LAST
                ) AS forward_p_op_rank,
                count(*) FILTER (WHERE pbr > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS pbr_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN pbr > 0 THEN pbr END NULLS LAST
                ) AS pbr_rank
            FROM ranking_color_scoped
            WHERE liquidity_scope != 'all_liquidity'
        )
        """
    )
    _add_per_relation_percentiles(conn, table_name="ranking_color_liquidity_ranked")


def _add_per_relation_percentiles(conn: Any, *, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT
            * EXCLUDE (
                forward_per_to_per_ratio_valid_count,
                forward_per_to_per_ratio_rank,
                forward_p_op_to_per_ratio_valid_count,
                forward_p_op_to_per_ratio_rank
            ),
            CASE
                WHEN forward_per_to_per_ratio IS NOT NULL
                  AND forward_per_to_per_ratio_valid_count <= 1 THEN 0.0
                WHEN forward_per_to_per_ratio IS NOT NULL
                    THEN (forward_per_to_per_ratio_rank - 1.0)
                        / (forward_per_to_per_ratio_valid_count - 1.0)
            END AS forward_per_to_per_ratio_percentile,
            CASE
                WHEN forward_p_op_to_per_ratio IS NOT NULL
                  AND forward_p_op_to_per_ratio_valid_count <= 1 THEN 0.0
                WHEN forward_p_op_to_per_ratio IS NOT NULL
                    THEN (forward_p_op_to_per_ratio_rank - 1.0)
                        / (forward_p_op_to_per_ratio_valid_count - 1.0)
            END AS forward_p_op_to_per_ratio_percentile
        FROM (
            SELECT
                *,
                count(*) FILTER (WHERE forward_per_to_per_ratio IS NOT NULL) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_per_to_per_ratio_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY forward_per_to_per_ratio NULLS LAST
                ) AS forward_per_to_per_ratio_rank,
                count(*) FILTER (WHERE forward_p_op_to_per_ratio IS NOT NULL) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_p_op_to_per_ratio_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY forward_p_op_to_per_ratio NULLS LAST
                ) AS forward_p_op_to_per_ratio_rank
            FROM {table_name}
        )
        """
    )


def _create_daily_valuation_view(conn: Any) -> None:
    valuation_code = normalize_code_sql("dv.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_color_daily_valuation AS
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
                {_optional_daily_valuation_double_expr(conn, "free_float_market_cap")}
                    AS free_float_market_cap,
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


def _build_ranking_color_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in _VALUATION_FEATURES:
        percentile_column = f"{feature}_percentile"
        for bucket in _VALUATION_BUCKETS:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_color_ranked",
                        condition=_valuation_bucket_condition(percentile_column, bucket),
                        condition_fields={
                            "condition_family": "ranking_color_percentile_evidence",
                            "valuation_feature": feature,
                            "ranking_color_bucket": bucket,
                            "ranking_color_bucket_order": _VALUATION_BUCKETS.index(bucket),
                            "evidence_tier": _evidence_tier(bucket),
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_ranking_color_evidence_columns())


def _build_forward_per_pop_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for bucket in _FORWARD_PER_POP_BUCKETS:
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_color_ranked",
                    condition=_forward_per_pop_condition(bucket),
                    condition_fields={
                        "condition_family": "forward_per_forward_p_op_relative",
                        "interaction_bucket": bucket,
                        "interaction_bucket_order": _FORWARD_PER_POP_BUCKETS.index(bucket),
                        "horizon": int(horizon),
                    },
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_forward_per_pop_columns())


def _build_per_relation_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in _PER_RELATION_FEATURES:
        percentile_column = f"{feature}_percentile"
        for bucket in _VALUATION_BUCKETS:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_color_ranked",
                        condition=_valuation_bucket_condition(percentile_column, bucket),
                        condition_fields={
                            "condition_family": "forward_valuation_per_relation",
                            "relation_feature": feature,
                            "relation_bucket": bucket,
                            "relation_bucket_order": _VALUATION_BUCKETS.index(bucket),
                            "evidence_tier": _evidence_tier(bucket),
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_per_relation_columns())


def _build_low_per_relation_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for per_scope, per_threshold in _LOW_PER_SCOPES:
        for feature in _PER_RELATION_FEATURES:
            percentile_column = f"{feature}_percentile"
            for bucket in _VALUATION_BUCKETS:
                relation_condition = _valuation_bucket_condition(
                    percentile_column,
                    bucket,
                )
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_color_ranked",
                            condition=(
                                f"per_percentile <= {per_threshold} "
                                f"AND {relation_condition}"
                            ),
                            condition_fields={
                                "condition_family": "low_per_forward_relation",
                                "per_scope": per_scope,
                                "relation_feature": feature,
                                "relation_bucket": bucket,
                                "relation_bucket_order": _VALUATION_BUCKETS.index(bucket),
                                "evidence_tier": _evidence_tier(bucket),
                                "horizon": int(horizon),
                            },
                            return_column=(
                                f"forward_close_excess_return_{int(horizon)}d_pct"
                            ),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_low_per_relation_columns())


def _build_low_per_relation_level_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for per_scope, per_threshold in _LOW_PER_SCOPES:
        for feature in _PER_RELATION_FEATURES:
            for bucket_order, (bucket, condition_template) in enumerate(
                _RELATION_RATIO_BUCKETS
            ):
                relation_condition = condition_template.format(column=feature)
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_color_ranked",
                            condition=(
                                f"per_percentile <= {per_threshold} "
                                f"AND {relation_condition}"
                            ),
                            condition_fields={
                                "condition_family": "low_per_forward_relation_level",
                                "per_scope": per_scope,
                                "relation_feature": feature,
                                "relation_level_bucket": bucket,
                                "relation_level_bucket_order": bucket_order,
                                "horizon": int(horizon),
                            },
                            return_column=(
                                f"forward_close_excess_return_{int(horizon)}d_pct"
                            ),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_low_per_relation_level_columns())


def _build_liquidity_regime_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for regime in _LIQUIDITY_REGIMES:
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_color_liquidity_ranked",
                    condition=f"liquidity_scope = '{regime}'",
                    condition_fields={
                        "condition_family": "liquidity_regime",
                        "liquidity_regime": regime,
                        "horizon": int(horizon),
                    },
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_liquidity_regime_columns())


def _build_topix_regime_liquidity_value_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for topix_order, (topix_regime, topix_condition) in enumerate(_TOPIX_REGIMES):
        for regime in _LIQUIDITY_REGIMES:
            for value_order, (value_condition, value_sql) in enumerate(
                _RERATING_VALUE_CONDITIONS
            ):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_color_liquidity_ranked",
                            condition=(
                                f"({topix_condition}) "
                                f"AND liquidity_scope = '{regime}' "
                                f"AND ({value_sql})"
                            ),
                            condition_fields={
                                "condition_family": "topix_regime_liquidity_value",
                                "topix_regime": topix_regime,
                                "topix_regime_order": topix_order,
                                "liquidity_regime": regime,
                                "value_condition": value_condition,
                                "value_condition_order": value_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_topix_regime_liquidity_value_columns())


def _build_liquidity_color_long_trend_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    trend_conditions: tuple[tuple[int, str, str], ...] = (
        (120, "trend_positive", "recent_return_120d_pct > 0"),
        (120, "trend_non_positive", "recent_return_120d_pct <= 0"),
        (150, "trend_positive", "recent_return_150d_pct > 0"),
        (150, "trend_non_positive", "recent_return_150d_pct <= 0"),
    )
    for regime_order, (regime, ui_colors) in enumerate(_liquidity_color_sql().items()):
        for color_order, (ui_color, color_sql) in enumerate(ui_colors.items()):
            for trend_order, (trend_window, trend_condition, trend_sql) in enumerate(
                trend_conditions
            ):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_color_liquidity_ranked",
                            condition=(
                                f"liquidity_scope = '{regime}' "
                                f"AND ({color_sql}) "
                                f"AND ({trend_sql})"
                            ),
                            condition_fields={
                                "condition_family": "liquidity_color_long_trend",
                                "liquidity_regime": regime,
                                "liquidity_regime_order": regime_order,
                                "ui_color": ui_color,
                                "ui_color_order": color_order,
                                "trend_window": int(trend_window),
                                "trend_condition": trend_condition,
                                "trend_condition_order": trend_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_liquidity_color_long_trend_columns())


def _build_rerating_good_valuation_chain_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    neutral_good = _neutral_rerating_good_condition()
    crowded_good = _crowded_rerating_good_condition()
    all_good = f"(({neutral_good}) OR ({crowded_good}))"
    chain = _per_forward_per_forward_p_op_chain_condition()
    scopes = (
        ("all_rerating_good", all_good, 0),
        ("neutral_rerating_good", neutral_good, 1),
        ("crowded_rerating_good", crowded_good, 2),
    )
    chain_conditions = (
        ("all_good", "TRUE", 0),
        ("per_gt_fwdper_gt_fwdpop", chain, 1),
        ("good_without_chain", f"NOT coalesce(({chain}), FALSE)", 2),
    )
    for good_scope, good_condition, good_order in scopes:
        for chain_condition, chain_sql, chain_order in chain_conditions:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_color_ranked",
                        condition=f"({good_condition}) AND ({chain_sql})",
                        condition_fields={
                            "condition_family": "rerating_good_forward_valuation_chain",
                            "good_scope": good_scope,
                            "good_scope_order": good_order,
                            "chain_condition": chain_condition,
                            "chain_condition_order": chain_order,
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_rerating_good_valuation_chain_columns())


def _build_high_valuation_size_liquidity_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for value_order, (valuation_condition, valuation_sql) in enumerate(
        _HIGH_VALUATION_CONDITIONS
    ):
        for cap_order, (market_cap_bucket, market_cap_sql) in enumerate(
            _MARKET_CAP_ABS_BUCKETS
        ):
            for adv_order, (adv60_bucket, adv60_sql) in enumerate(_ADV60_ABS_BUCKETS):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_color_ranked",
                            condition=(
                                f"({valuation_sql}) "
                                f"AND ({market_cap_sql}) "
                                f"AND ({adv60_sql})"
                            ),
                            condition_fields={
                                "condition_family": (
                                    "high_valuation_size_liquidity_interaction"
                                ),
                                "valuation_condition": valuation_condition,
                                "valuation_condition_order": value_order,
                                "market_cap_abs_bucket": market_cap_bucket,
                                "market_cap_abs_bucket_order": cap_order,
                                "adv60_abs_bucket": adv60_bucket,
                                "adv60_abs_bucket_order": adv_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(
        frames,
        columns=_high_valuation_size_liquidity_interaction_columns(),
    )


def _aggregate_condition(
    conn: Any,
    *,
    source_name: str,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = conn.execute(
        f"""
        SELECT
            market_scope,
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
            median(recent_return_120d_pct) AS median_recent_return_120d_pct,
            median(recent_return_150d_pct) AS median_recent_return_150d_pct,
            median(topix_recent_return_20d_pct) AS median_topix_recent_return_20d_pct,
            median(topix_recent_return_60d_pct) AS median_topix_recent_return_60d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy,
            median(market_cap_bil_jpy) AS median_market_cap_bil_jpy,
            median(free_float_market_cap_jpy) / 1000000000.0
                AS median_free_float_market_cap_bil_jpy,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per) AS median_per,
            median(forward_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op,
            median(forward_per_to_per_ratio) AS median_forward_per_to_per_ratio,
            median(forward_p_op_to_per_ratio) AS median_forward_p_op_to_per_ratio,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(forward_p_op_percentile) AS median_forward_p_op_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forward_per_to_per_ratio_percentile)
                AS median_forward_per_to_per_ratio_percentile,
            median(forward_p_op_to_per_ratio_percentile)
                AS median_forward_p_op_to_per_ratio_percentile
        FROM {source_name}
        WHERE {condition}
          AND {return_column} IS NOT NULL
        GROUP BY market_scope
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), "market_scope"]
    ordered.extend(_aggregate_metric_columns())
    return frame.reindex(columns=ordered)


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN per > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS per_coverage_pct,
            avg(CASE WHEN forward_per > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_per_coverage_pct,
            avg(CASE WHEN forward_p_op > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_p_op_coverage_pct,
            avg(CASE WHEN pbr > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS pbr_coverage_pct,
            avg(CASE WHEN liquidity_residual_z IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS liquidity_residual_z_coverage_pct
        FROM ranking_color_panel
        GROUP BY market
        ORDER BY market
        """
    ).fetchdf()


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
            topix_recent_return_20d_pct,
            topix_recent_return_60d_pct,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            free_float_market_cap_jpy / 1000000000.0 AS free_float_market_cap_bil_jpy,
            liquidity_residual_z,
            liquidity_regime,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            forward_per_to_per_ratio,
            forward_per_to_per_ratio_percentile,
            pbr,
            pbr_percentile,
            p_op,
            forward_p_op,
            forward_p_op_percentile,
            forward_p_op_to_per_ratio,
            forward_p_op_to_per_ratio_percentile,
            market_cap_bil_jpy,
            forward_close_excess_return_20d_pct
        FROM ranking_color_ranked
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _market_master_cte(*, market_source: str, master_code: str) -> str:
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    market_scope_case = _market_scope_case_sql("smd.market_code", "smd.market_name")
    return f"""
    raw_market_master AS (
        SELECT
            {master_code} AS code,
            smd.date,
            smd.company_name,
            {market_scope_case} AS market,
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


def _market_scope_case_sql(market_code_column: str, market_name_column: str) -> str:
    code_clauses = " ".join(
        f"WHEN lower(trim({market_code_column})) IN ({_sql_string_list(aliases)}) THEN '{scope}'"
        for scope, aliases in MARKET_CODES_BY_SCOPE.items()
    )
    name_clauses = " ".join(
        f"WHEN lower(trim({market_name_column})) IN ({_sql_string_list(aliases)}) THEN '{scope}'"
        for scope, aliases in MARKET_CODES_BY_SCOPE.items()
    )
    return f"""
            CASE
                {code_clauses}
                {name_clauses}
                ELSE 'unknown'
            END
            """


def _optional_daily_valuation_double_expr(conn: Any, column: str) -> str:
    if _daily_valuation_column_exists(conn, column):
        return f"CAST(dv.{column} AS DOUBLE)"
    return "CAST(NULL AS DOUBLE)"


def _daily_valuation_column_exists(conn: Any, column: str) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM pragma_table_info('daily_valuation') WHERE name = ?",
            [column],
        ).fetchone()[0]
    )


def _valuation_bucket_condition(percentile_column: str, bucket: str) -> str:
    if bucket == "cheapest_10pct":
        return f"{percentile_column} <= 0.1"
    if bucket == "cheapest_20pct":
        return f"{percentile_column} > 0.1 AND {percentile_column} <= 0.2"
    if bucket == "middle_60pct":
        return f"{percentile_column} > 0.2 AND {percentile_column} < 0.8"
    if bucket == "expensive_20pct":
        return f"{percentile_column} >= 0.8 AND {percentile_column} < 0.9"
    if bucket == "expensive_10pct":
        return f"{percentile_column} >= 0.9"
    raise ValueError(f"unsupported valuation bucket: {bucket}")


def _forward_per_pop_condition(bucket: str) -> str:
    low_forward_per = "forward_per_percentile <= 0.2"
    low_forward_p_op = "forward_p_op_percentile <= 0.2"
    high_forward_p_op = "forward_p_op_percentile >= 0.8"
    if bucket == "low_forward_per_low_forward_p_op":
        return f"{low_forward_per} AND {low_forward_p_op}"
    if bucket == "low_forward_per_high_forward_p_op":
        return f"{low_forward_per} AND {high_forward_p_op}"
    if bucket == "low_forward_per_only":
        return f"{low_forward_per} AND NOT ({low_forward_p_op}) AND NOT ({high_forward_p_op})"
    if bucket == "low_forward_p_op_only":
        return f"NOT ({low_forward_per}) AND {low_forward_p_op}"
    if bucket == "neither_extreme":
        return f"NOT ({low_forward_per}) AND NOT ({low_forward_p_op}) AND NOT ({high_forward_p_op})"
    raise ValueError(f"unsupported forward PER/P-OP bucket: {bucket}")


def _liquidity_color_sql() -> dict[str, dict[str, str]]:
    strong_value = (
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)"
    )
    neutral_green = "per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8"
    crowded_green = strong_value
    medium_value = (
        "pbr_percentile <= 0.2 "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)"
    )
    return {
        "crowded_rerating": {
            "green": f"({crowded_green})",
            "blue": f"({medium_value}) AND NOT ({crowded_green})",
        },
        "neutral_rerating": {
            "green": f"({neutral_green})",
            "blue": f"NOT ({neutral_green})",
        },
    }


def _neutral_rerating_good_condition() -> str:
    return (
        "liquidity_regime = 'neutral_rerating' "
        "AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)"
        ")"
    )


def _crowded_rerating_good_condition() -> str:
    return (
        "liquidity_regime = 'crowded_rerating' "
        "AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8) "
        "OR pbr_percentile <= 0.2 "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)"
        ")"
    )


def _per_forward_per_forward_p_op_chain_condition() -> str:
    return (
        "per > 0 "
        "AND forward_per > 0 "
        "AND forward_p_op > 0 "
        "AND per > forward_per "
        "AND forward_per > forward_p_op"
    )


def _evidence_tier(bucket: str) -> str:
    return {
        "cheapest_10pct": "excellent",
        "cheapest_20pct": "good",
        "middle_60pct": "neutral",
        "expensive_20pct": "bad",
        "expensive_10pct": "very_bad",
    }[bucket]


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _normalize_market_scopes(scopes: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(
            _normalize_market_scope_token(scope)
            for scope in scopes
        )
    )
    allowed = {"all", "prime", "standard", "growth", "unknown"}
    if not normalized or any(scope not in allowed for scope in normalized):
        raise ValueError("market_scopes must contain prime, standard, growth, unknown, or all")
    return normalized


def _normalize_market_scope_token(scope: str) -> str:
    fallback = scope.strip().lower()
    return normalize_market_scope(scope, default=fallback) or fallback


def _offset_calendar_date(date: str | None, *, days: int) -> str | None:
    if date is None:
        return None
    return (pd.Timestamp(date) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _sql_string_list(values: Sequence[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _aggregate_metric_columns() -> list[str]:
    return [
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
        "median_topix_recent_return_20d_pct",
        "median_topix_recent_return_60d_pct",
        "median_med_adv60_mil_jpy",
        "median_market_cap_bil_jpy",
        "median_free_float_market_cap_bil_jpy",
        "median_liquidity_residual_z",
        "median_per",
        "median_forward_per",
        "median_pbr",
        "median_p_op",
        "median_forward_p_op",
        "median_forward_per_to_per_ratio",
        "median_forward_p_op_to_per_ratio",
        "median_per_percentile",
        "median_forward_per_percentile",
        "median_forward_p_op_percentile",
        "median_pbr_percentile",
        "median_forward_per_to_per_ratio_percentile",
        "median_forward_p_op_to_per_ratio_percentile",
    ]


def _ranking_color_evidence_columns() -> list[str]:
    return [
        "condition_family",
        "valuation_feature",
        "ranking_color_bucket",
        "ranking_color_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _forward_per_pop_columns() -> list[str]:
    return [
        "condition_family",
        "interaction_bucket",
        "interaction_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _per_relation_columns() -> list[str]:
    return [
        "condition_family",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _low_per_relation_columns() -> list[str]:
    return [
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _low_per_relation_level_columns() -> list[str]:
    return [
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_level_bucket",
        "relation_level_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _liquidity_regime_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_regime",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _topix_regime_liquidity_value_columns() -> list[str]:
    return [
        "condition_family",
        "topix_regime",
        "topix_regime_order",
        "liquidity_regime",
        "value_condition",
        "value_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _liquidity_color_long_trend_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_regime",
        "liquidity_regime_order",
        "ui_color",
        "ui_color_order",
        "trend_window",
        "trend_condition",
        "trend_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _rerating_good_valuation_chain_columns() -> list[str]:
    return [
        "condition_family",
        "good_scope",
        "good_scope_order",
        "chain_condition",
        "chain_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _high_valuation_size_liquidity_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "valuation_condition",
        "valuation_condition_order",
        "market_cap_abs_bucket",
        "market_cap_abs_bucket_order",
        "adv60_abs_bucket",
        "adv60_abs_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]
