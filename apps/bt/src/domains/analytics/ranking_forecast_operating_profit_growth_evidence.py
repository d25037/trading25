"""Forecast operating-profit growth evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.ranking_color_evidence import (
    _crowded_rerating_good_condition,
    _neutral_rerating_good_condition,
)
from src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition import (
    _create_long_sector_leadership_tables,
    _create_long_signal_tables,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    _create_sector_strength_tables,
)
from src.domains.analytics.ranking_short_red_evidence import (
    _create_feature_panel as _create_short_red_feature_panel,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

RANKING_FORECAST_OP_GROWTH_EXPERIMENT_ID = (
    "market-behavior/ranking-forecast-operating-profit-growth-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_GROWTH_BUCKETS: tuple[tuple[str, str], ...] = (
    ("missing_or_non_positive", "forecast_operating_profit_growth_ratio IS NULL"),
    ("contraction_lt_1_0", "forecast_operating_profit_growth_ratio < 1.0"),
    (
        "low_growth_1_0_to_1_2",
        "forecast_operating_profit_growth_ratio >= 1.0 "
        "AND forecast_operating_profit_growth_ratio < 1.2",
    ),
    (
        "mid_growth_1_2_to_1_5",
        "forecast_operating_profit_growth_ratio >= 1.2 "
        "AND forecast_operating_profit_growth_ratio < 1.5",
    ),
    (
        "high_growth_1_5_to_2_0",
        "forecast_operating_profit_growth_ratio >= 1.5 "
        "AND forecast_operating_profit_growth_ratio < 2.0",
    ),
    ("exceptional_growth_ge_2_0", "forecast_operating_profit_growth_ratio >= 2.0"),
)
_VALUATION_GROWTH_RATIO_FEATURES: tuple[tuple[str, str], ...] = (
    ("per_to_fop_growth_ratio", "PER / forecast OP growth ratio"),
    ("forward_per_to_fop_growth_ratio", "Fwd PER / forecast OP growth ratio"),
)
_RATIO_BUCKETS: tuple[tuple[str, str], ...] = (
    ("lowest_20pct", "{feature}_percentile <= 0.2"),
    ("middle_60pct", "{feature}_percentile > 0.2 AND {feature}_percentile < 0.8"),
    ("highest_20pct", "{feature}_percentile >= 0.8"),
)
_DECISION_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("high_per20", "per_percentile >= 0.8"),
    ("high_forward_per20", "forward_per_percentile >= 0.8"),
    (
        "high_per_or_high_forward_per20",
        "per_percentile >= 0.8 OR forward_per_percentile >= 0.8",
    ),
    ("low_per20", "per_percentile <= 0.2"),
    ("low_forward_per20", "forward_per_percentile <= 0.2"),
    ("neutral_rerating_good", _neutral_rerating_good_condition()),
    ("crowded_rerating_good", _crowded_rerating_good_condition()),
    (
        "rally_overvalued",
        "recent_return_20d_pct >= 0 AND recent_return_60d_pct >= 0 "
        "AND (per_percentile >= 0.8 OR forward_per_percentile >= 0.8 "
        "OR pbr_percentile >= 0.8)",
    ),
    (
        "crowded_no_value",
        "liquidity_regime = 'crowded_rerating' "
        "AND NOT coalesce(("
        "pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)"
        "), FALSE)",
    ),
    (
        "stale_overvalued",
        "liquidity_regime = 'stale_liquidity' "
        "AND (per_percentile >= 0.8 OR forward_per_percentile >= 0.8 "
        "OR pbr_percentile >= 0.8)",
    ),
)
_GROWTH_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all", "TRUE"),
    ("growth_ge_1_2", "forecast_operating_profit_growth_ratio >= 1.2"),
    ("high_growth_ge_1_5", "forecast_operating_profit_growth_ratio >= 1.5"),
    ("contraction_lt_1_0", "forecast_operating_profit_growth_ratio < 1.0"),
    (
        "low_or_missing_growth",
        "forecast_operating_profit_growth_ratio IS NULL "
        "OR forecast_operating_profit_growth_ratio < 1.0",
    ),
)
_SHORT_GROWTH_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all", "TRUE"),
    (
        "low_or_missing_growth",
        "forecast_operating_profit_growth_ratio IS NULL "
        "OR forecast_operating_profit_growth_ratio < 1.0",
    ),
    ("contraction_lt_1_0", "forecast_operating_profit_growth_ratio < 1.0"),
    ("growth_ge_1_2", "forecast_operating_profit_growth_ratio >= 1.2"),
    ("high_growth_ge_1_5", "forecast_operating_profit_growth_ratio >= 1.5"),
)
_LONG_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    ("undervalued", "valuation_signal = 'medium_value_confirmation'"),
    (
        "long_hybrid_leadership_strong",
        "long_hybrid_leadership_score >= 0.799999",
    ),
    (
        "long_hybrid_leadership_strong_atr20_accel",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "long_hybrid_leadership_strong_momentum_20_60_top20",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND momentum_20_60_top20_flag",
    ),
    (
        "long_hybrid_current_not_weak_atr20_accel",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND coalesce(sector_strength_bucket, 'sector_unknown') <> 'sector_weak' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "current_sector_strong",
        "sector_strength_bucket = 'sector_strong'",
    ),
    (
        "current_sector_strong_long_hybrid_strong",
        "sector_strength_bucket = 'sector_strong' "
        "AND long_hybrid_leadership_score >= 0.799999",
    ),
    (
        "deep_value_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "overvalued_long_hybrid_atr20_accel",
        "overvalued_warning "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    ("per_growth_lowest_20pct", "per_to_fop_growth_ratio_percentile <= 0.2"),
    (
        "fwdper_growth_lowest_20pct",
        "forward_per_to_fop_growth_ratio_percentile <= 0.2",
    ),
)
_SHORT_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("sector_weak", "sector_strength_bucket = 'sector_weak'"),
    ("sector_strong", "sector_strength_bucket = 'sector_strong'"),
    (
        "overvalued",
        "overvalued_warning",
    ),
    ("very_overvalued", "very_overvalued_warning"),
    (
        "no_positive_earnings",
        "valuation_signal = 'no_positive_earnings_valuation'",
    ),
    (
        "overvalued_sector_weak",
        "overvalued_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "very_overvalued_sector_weak",
        "very_overvalued_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    ("crowded_no_value", "crowded_no_value_flag"),
    ("crowded_overvalued", "crowded_overvalued_flag"),
    (
        "crowded_no_value_sector_weak",
        "crowded_no_value_flag AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_overvalued_sector_weak",
        "crowded_overvalued_flag AND sector_strength_bucket = 'sector_weak'",
    ),
    ("stale_overvalued", "stale_overvalued_flag"),
    ("stale_rally_fade", "stale_rally_fade_flag"),
    (
        "stale_overvalued_sector_weak",
        "stale_overvalued_flag AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "stale_rally_fade_sector_weak",
        "stale_rally_fade_flag AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "distribution_stress_overvalued_sector_weak",
        "liquidity_regime = 'distribution_stress' "
        "AND overvalued_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_overvalued_overheat_sector_weak",
        "crowded_overvalued_flag "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_no_value_overheat_sector_weak",
        "crowded_no_value_flag "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
)


@dataclass(frozen=True)
class RankingForecastOperatingProfitGrowthEvidenceResult:
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
    growth_bucket_evidence_df: pd.DataFrame
    valuation_growth_ratio_evidence_df: pd.DataFrame
    decision_scope_growth_evidence_df: pd.DataFrame
    long_deep_dive_growth_evidence_df: pd.DataFrame
    short_deep_dive_growth_evidence_df: pd.DataFrame


def run_ranking_forecast_operating_profit_growth_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingForecastOperatingProfitGrowthEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=max(720, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-forecast-op-growth-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        create_daily_ranking_research_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            market_source=market_source,
            include_liquidity_ranked=True,
        )
        _create_atr_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            atr_windows=_REQUIRED_ATR_WINDOWS,
            return_windows=_REQUIRED_RETURN_WINDOWS,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_forecast_operating_profit_growth_panel(ctx.connection)
        _create_short_red_feature_panel(ctx.connection)
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
        )
        _create_long_signal_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
        )
        _create_deep_dive_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_forecast_op_growth_panel"
            ).fetchone()[0]
        )
        result = RankingForecastOperatingProfitGrowthEvidenceResult(
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
            growth_bucket_evidence_df=_build_growth_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            valuation_growth_ratio_evidence_df=(
                _build_valuation_growth_ratio_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            decision_scope_growth_evidence_df=_build_decision_scope_growth_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_deep_dive_growth_evidence_df=(
                _build_deep_dive_growth_evidence_df(
                    ctx.connection,
                    condition_family="long_hybrid_sector_atr_forecast_op_growth",
                    conditions=_LONG_DEEP_SCOPES,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            short_deep_dive_growth_evidence_df=(
                _build_deep_dive_growth_evidence_df(
                    ctx.connection,
                    condition_family="short_sector_crowded_stale_forecast_op_growth",
                    conditions=_SHORT_DEEP_SCOPES,
                    growth_conditions=_SHORT_GROWTH_CONDITIONS,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_forecast_operating_profit_growth_evidence_bundle(
    result: RankingForecastOperatingProfitGrowthEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_FORECAST_OP_GROWTH_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_forecast_operating_profit_growth_evidence",
        function="run_ranking_forecast_operating_profit_growth_evidence_research",
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
            "growth_bucket_evidence_df": result.growth_bucket_evidence_df,
            "valuation_growth_ratio_evidence_df": (
                result.valuation_growth_ratio_evidence_df
            ),
            "decision_scope_growth_evidence_df": (
                result.decision_scope_growth_evidence_df
            ),
            "long_deep_dive_growth_evidence_df": (
                result.long_deep_dive_growth_evidence_df
            ),
            "short_deep_dive_growth_evidence_df": (
                result.short_deep_dive_growth_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingForecastOperatingProfitGrowthEvidenceResult,
) -> str:
    parts = [
        "# Ranking Forecast Operating Profit Growth Evidence",
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
        "## Forecast OP Growth Bucket Evidence",
        "",
        _top_rows_for_markdown(result.growth_bucket_evidence_df, limit=80),
        "",
        "## PER/Fwd PER to Forecast OP Growth Ratio Evidence",
        "",
        _top_rows_for_markdown(result.valuation_growth_ratio_evidence_df, limit=80),
        "",
        "## Daily Ranking Decision Scope x Forecast OP Growth Evidence",
        "",
        _top_rows_for_markdown(result.decision_scope_growth_evidence_df, limit=160),
        "",
        "## Explicit Long Deep Dive: Long Hybrid Leadership x ATR x Forecast OP Growth",
        "",
        _top_rows_for_markdown(
            result.long_deep_dive_growth_evidence_df,
            limit=220,
        ),
        "",
        "## Explicit Short Deep Dive: Sector Score x Crowded/Stale x Forecast OP Growth",
        "",
        _top_rows_for_markdown(
            result.short_deep_dive_growth_evidence_df,
            limit=220,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_forecast_operating_profit_growth_panel(conn: Any) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_forecast_op_growth_panel AS
        SELECT
            *
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
        """
    )


def _create_deep_dive_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_forecast_op_growth_deep_panel AS
        SELECT
            g.*,
            l.sector_33_code,
            l.sector_33_name,
            l.sector_strength_bucket,
            l.sector_strength_score,
            l.sector_index_strength_score,
            l.sector_constituent_strength_score,
            l.long_index_leadership_score,
            l.long_constituent_breadth_leadership_score,
            l.long_hybrid_leadership_score,
            l.current_sector_bucket_label,
            l.long_hybrid_bucket_label,
            coalesce(l.momentum_20_60_top20_flag, FALSE)
                AS momentum_20_60_top20_flag,
            s.atr20_pct,
            s.atr60_pct,
            s.atr20_to_atr60,
            s.atr20_change_20d_pct,
            coalesce(s.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                s.atr20_acceleration
                AND coalesce(g.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(s.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(s.weak_trend, FALSE) AS weak_trend,
            (
                g.liquidity_regime = 'crowded_rerating'
                AND g.no_value_confirmation
            ) AS crowded_no_value_flag,
            (
                g.liquidity_regime = 'crowded_rerating'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
            ) AS crowded_overvalued_flag,
            (
                g.liquidity_regime = 'crowded_rerating'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
                AND coalesce(s.weak_trend, FALSE)
            ) AS crowded_overvalued_weak_trend_flag,
            (
                g.liquidity_regime = 'distribution_stress'
                AND coalesce(s.weak_trend, FALSE)
            ) AS distribution_stress_weak_trend_flag,
            (
                g.liquidity_regime = 'distribution_stress'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
            ) AS distribution_stress_overvalued_flag,
            (
                g.liquidity_regime = 'stale_liquidity'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
                AND coalesce(s.weak_trend, FALSE)
            ) AS stale_overvalued_weak_trend_flag,
            (
                g.liquidity_regime = 'stale_liquidity'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
            ) AS stale_overvalued_flag,
            (
                g.liquidity_regime = 'stale_liquidity'
                AND (
                    g.overvalued_warning
                    OR g.no_positive_earnings_valuation
                )
                AND g.recent_return_20d_pct > 0
                AND g.recent_return_60d_pct > 0
            ) AS stale_rally_fade_flag
        FROM ranking_forecast_op_growth_panel g
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = g.code
         AND l.date = g.date
         AND l.market_scope = g.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = g.code
         AND s.date = g.date
         AND s.market_scope = g.market_scope
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN p_op > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS p_op_coverage_pct,
            avg(CASE WHEN forward_p_op > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS forward_p_op_coverage_pct,
            avg(CASE WHEN forecast_operating_profit_growth_ratio IS NOT NULL
                THEN 1.0 ELSE 0.0 END) * 100.0 AS growth_ratio_coverage_pct,
            median(forecast_operating_profit_growth_ratio)
                AS median_forecast_operating_profit_growth_ratio,
            median(forecast_operating_profit_growth_pct)
                AS median_forecast_operating_profit_growth_pct,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op,
            median(per_to_fop_growth_ratio) AS median_per_to_fop_growth_ratio,
            median(forward_per_to_fop_growth_ratio)
                AS median_forward_per_to_fop_growth_ratio
        FROM ranking_forecast_op_growth_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_growth_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    growth_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_GROWTH_BUCKETS)}
        ) AS growth(growth_bucket, growth_bucket_order, condition_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                lateral_sql=growth_lateral_sql,
                match_condition="growth.condition_matches",
                group_select_sql=(
                    "'forecast_op_growth_bucket' AS condition_family,\n"
                    "            growth.growth_bucket,\n"
                    "            growth.growth_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "growth.growth_bucket, growth.growth_bucket_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return _concat_sorted(frames, columns=_growth_bucket_columns())


def _build_valuation_growth_ratio_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    feature_selects = "\n            UNION ALL\n            ".join(
        (
            "SELECT "
            f"{_sql_literal(feature)} AS ratio_feature, "
            f"{_sql_literal(label)} AS ratio_feature_label, "
            f"{feature}_percentile AS ratio_percentile"
        )
        for feature, label in _VALUATION_GROWTH_RATIO_FEATURES
    )
    bucket_lateral_sql = f"""
        CROSS JOIN LATERAL (
            {feature_selects}
        ) AS ratio_feature
        CROSS JOIN LATERAL (
            VALUES
            ('lowest_20pct', 0, ratio_feature.ratio_percentile <= 0.2),
            ('middle_60pct', 1, ratio_feature.ratio_percentile > 0.2
                AND ratio_feature.ratio_percentile < 0.8),
            ('highest_20pct', 2, ratio_feature.ratio_percentile >= 0.8)
        ) AS ratio_bucket(ratio_bucket, ratio_bucket_order, condition_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                lateral_sql=bucket_lateral_sql,
                match_condition="ratio_bucket.condition_matches",
                group_select_sql=(
                    "'valuation_to_forecast_op_growth_ratio' AS condition_family,\n"
                    "            ratio_feature.ratio_feature,\n"
                    "            ratio_feature.ratio_feature_label,\n"
                    "            ratio_bucket.ratio_bucket,\n"
                    "            ratio_bucket.ratio_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "ratio_feature.ratio_feature, ratio_feature.ratio_feature_label, "
                    "ratio_bucket.ratio_bucket, ratio_bucket.ratio_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return _concat_sorted(frames, columns=_valuation_growth_ratio_columns())


def _build_decision_scope_growth_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    decision_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_DECISION_SCOPES)}
        ) AS decision_scope(decision_scope, decision_scope_order, decision_matches)
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_GROWTH_CONDITIONS)}
        ) AS growth_condition(
            growth_condition,
            growth_condition_order,
            growth_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                lateral_sql=decision_lateral_sql,
                match_condition=(
                    "decision_scope.decision_matches "
                    "AND growth_condition.growth_matches"
                ),
                group_select_sql=(
                    "'decision_scope_forecast_op_growth' AS condition_family,\n"
                    "            decision_scope.decision_scope,\n"
                    "            decision_scope.decision_scope_order,\n"
                    "            growth_condition.growth_condition,\n"
                    "            growth_condition.growth_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "decision_scope.decision_scope, "
                    "decision_scope.decision_scope_order, "
                    "growth_condition.growth_condition, "
                    "growth_condition.growth_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return _concat_sorted(frames, columns=_decision_scope_growth_columns())


def _build_deep_dive_growth_evidence_df(
    conn: Any,
    *,
    condition_family: str,
    conditions: Sequence[tuple[str, str]],
    growth_conditions: Sequence[tuple[str, str]] = _GROWTH_CONDITIONS,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    deep_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(conditions)}
        ) AS deep_scope(deep_scope, deep_scope_order, deep_scope_matches)
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(growth_conditions)}
        ) AS growth_condition(
            growth_condition,
            growth_condition_order,
            growth_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_forecast_op_growth_deep_panel",
                lateral_sql=deep_lateral_sql,
                match_condition="deep_scope.deep_scope_matches "
                "AND growth_condition.growth_matches",
                group_select_sql=(
                    f"{_sql_literal(condition_family)} AS condition_family,\n"
                    "            deep_scope.deep_scope,\n"
                    "            deep_scope.deep_scope_order,\n"
                    "            growth_condition.growth_condition,\n"
                    "            growth_condition.growth_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "deep_scope.deep_scope, "
                    "deep_scope.deep_scope_order, "
                    "growth_condition.growth_condition, "
                    "growth_condition.growth_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_deep_dive_growth_columns())


def _aggregate_lateral_conditions(
    conn: Any,
    *,
    lateral_sql: str,
    match_condition: str,
    group_select_sql: str,
    group_by_sql: str,
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
    source_name: str = "ranking_forecast_op_growth_panel",
    extra_metric_sql: str = "",
) -> pd.DataFrame:
    horizon_prefix = return_column.replace("forward_close_excess_return_", "")
    raw_return_column = f"forward_close_return_{horizon_prefix}"
    topix_return_column = f"topix_close_return_{horizon_prefix}"
    frame = conn.execute(
        f"""
        SELECT
            {group_select_sql},
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({raw_return_column}) AS mean_forward_return_pct,
            median({raw_return_column}) AS median_forward_return_pct,
            avg({topix_return_column}) AS mean_topix_return_pct,
            median({topix_return_column}) AS median_topix_return_pct,
            avg({return_column}) AS mean_forward_excess_return_pct,
            median({return_column}) AS median_forward_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({return_column}, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont({return_column}, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS excess_win_rate_pct,
            avg(CASE WHEN {raw_return_column} < 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS negative_raw_return_rate_pct,
            avg(CASE WHEN {return_column} < 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS negative_excess_return_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per) AS median_per,
            median(forward_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forward_per_to_per_ratio) AS median_forward_per_to_per_ratio,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op,
            median(forecast_operating_profit_growth_ratio)
                AS median_forecast_operating_profit_growth_ratio,
            median(forecast_operating_profit_growth_pct)
                AS median_forecast_operating_profit_growth_pct,
            median(per_to_fop_growth_ratio) AS median_per_to_fop_growth_ratio,
            median(forward_per_to_fop_growth_ratio)
                AS median_forward_per_to_fop_growth_ratio
            {extra_metric_sql}
        FROM {source_name}
        {lateral_sql}
        WHERE {match_condition}
          AND {return_column} IS NOT NULL
        GROUP BY {group_by_sql}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    return frame


def _deep_dive_metric_sql() -> str:
    return """,
            median(sector_strength_score) AS median_sector_strength_score,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_strong' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_strong_rate_pct,
            median(long_hybrid_leadership_score)
                AS median_long_hybrid_leadership_score,
            avg(CASE WHEN long_hybrid_leadership_score >= 0.799999 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS long_hybrid_strong_rate_pct,
            median(atr20_change_20d_pct) AS median_atr20_change_20d_pct,
            median(atr20_to_atr60) AS median_atr20_to_atr60,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_acceleration_ex_overheat_rate_pct,
            avg(CASE WHEN atr20_to_atr60_overheat THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_to_atr60_overheat_rate_pct,
            avg(CASE WHEN momentum_20_60_top20_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS momentum_20_60_top20_rate_pct,
            avg(CASE WHEN valuation_signal = 'strong_value_confirmation' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS deep_value_rate_pct,
            avg(CASE WHEN valuation_signal = 'medium_value_confirmation' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS undervalued_rate_pct,
            avg(CASE WHEN overvalued_warning THEN 1.0 ELSE 0.0 END)
                * 100.0 AS overvalued_warning_rate_pct,
            avg(CASE WHEN very_overvalued_warning THEN 1.0 ELSE 0.0 END)
                * 100.0 AS very_overvalued_warning_rate_pct,
            avg(CASE WHEN no_positive_earnings_valuation THEN 1.0 ELSE 0.0 END)
                * 100.0 AS no_positive_earnings_valuation_rate_pct,
            avg(CASE WHEN no_value_confirmation THEN 1.0 ELSE 0.0 END)
                * 100.0 AS no_value_confirmation_rate_pct,
            avg(CASE WHEN weak_trend THEN 1.0 ELSE 0.0 END)
                * 100.0 AS weak_trend_rate_pct"""


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            liquidity_regime,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            pbr,
            pbr_percentile,
            forward_per_to_per_ratio,
            p_op,
            forward_p_op,
            forecast_operating_profit_growth_ratio,
            forecast_operating_profit_growth_pct,
            valuation_signal,
            strong_value_confirmation,
            medium_value_confirmation,
            overvalued_warning,
            very_overvalued_warning,
            no_positive_earnings_valuation,
            no_value_confirmation,
            per_to_fop_growth_ratio,
            per_to_fop_growth_ratio_percentile,
            forward_per_to_fop_growth_ratio,
            forward_per_to_fop_growth_ratio_percentile,
            forward_close_excess_return_20d_pct
        FROM ranking_forecast_op_growth_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _growth_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "growth_bucket",
        "growth_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _valuation_growth_ratio_columns() -> list[str]:
    return [
        "condition_family",
        "ratio_feature",
        "ratio_feature_label",
        "ratio_bucket",
        "ratio_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _decision_scope_growth_columns() -> list[str]:
    return [
        "condition_family",
        "decision_scope",
        "decision_scope_order",
        "growth_condition",
        "growth_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _deep_dive_growth_columns() -> list[str]:
    return [
        "condition_family",
        "deep_scope",
        "deep_scope_order",
        "growth_condition",
        "growth_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _aggregate_metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mean_topix_return_pct",
        "median_topix_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "excess_win_rate_pct",
        "negative_raw_return_rate_pct",
        "negative_excess_return_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_liquidity_residual_z",
        "median_per",
        "median_forward_per",
        "median_pbr",
        "median_per_percentile",
        "median_forward_per_percentile",
        "median_pbr_percentile",
        "median_forward_per_to_per_ratio",
        "median_p_op",
        "median_forward_p_op",
        "median_forecast_operating_profit_growth_ratio",
        "median_forecast_operating_profit_growth_pct",
        "median_per_to_fop_growth_ratio",
        "median_forward_per_to_fop_growth_ratio",
    ]


def _deep_dive_metric_columns() -> list[str]:
    return [
        "median_sector_strength_score",
        "sector_weak_rate_pct",
        "sector_strong_rate_pct",
        "median_long_hybrid_leadership_score",
        "long_hybrid_strong_rate_pct",
        "median_atr20_change_20d_pct",
        "median_atr20_to_atr60",
        "atr20_acceleration_ex_overheat_rate_pct",
        "atr20_to_atr60_overheat_rate_pct",
        "momentum_20_60_top20_rate_pct",
        "deep_value_rate_pct",
        "undervalued_rate_pct",
        "overvalued_warning_rate_pct",
        "very_overvalued_warning_rate_pct",
        "no_positive_earnings_valuation_rate_pct",
        "no_value_confirmation_rate_pct",
        "weak_trend_rate_pct",
    ]


def _condition_values_sql(conditions: Sequence[tuple[str, str]]) -> str:
    return ",\n            ".join(
        f"({_sql_literal(name)}, {index}, ({condition}))"
        for index, (name, condition) in enumerate(conditions)
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    order_columns = [
        column
        for column in (
            "condition_family",
            "market_scope",
            "horizon",
            "growth_bucket_order",
            "ratio_feature",
            "ratio_bucket_order",
            "decision_scope_order",
            "deep_scope_order",
            "growth_condition_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
