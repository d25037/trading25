"""Forecast operating-profit growth evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_sector_strength_features,
    build_short_scaffold_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
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
    (
        "neutral_rerating_good",
        "liquidity_regime = 'neutral_rerating' AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8))",
    ),
    (
        "crowded_rerating_good",
        "liquidity_regime = 'crowded_rerating' AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8) OR "
        "pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0))",
    ),
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
    ("deep_contraction_lt_0_8", "forecast_operating_profit_growth_ratio < 0.8"),
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
    ("deep_contraction_lt_0_8", "forecast_operating_profit_growth_ratio < 0.8"),
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

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-forecast-op-growth-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="forecast_op_growth",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(
                    tuple[MarketScope, ...],
                    resolved_market_scopes,
                ),
                include_liquidity=True,
                percentile_features=(
                    "per_to_fop_growth_ratio",
                    "forecast_per_to_fop_growth_ratio",
                ),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("forecast growth research requires liquidity-ranked signals")
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="forecast_op_growth_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="forecast_op_growth_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="forecast_op_growth_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="forecast_op_growth_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(leadership_features, short_features),
            namespace="forecast_op_growth",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="forecast_op_growth_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="forecast_op_growth_outcomes",
        )
        _create_forecast_operating_profit_growth_panel(
            ctx.connection,
            source_name=evaluated.name,
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
        "## Explicit Short Deep Dive: Balanced Sector Strength x Crowded/Stale x Forecast OP Growth",
        "",
        _top_rows_for_markdown(
            result.short_deep_dive_growth_evidence_df,
            limit=220,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_forecast_operating_profit_growth_panel(
    conn: Any,
    *,
    source_name: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_forecast_op_growth_panel AS
        SELECT
            source.*,
            source.forecast_per AS forward_per,
            source.forecast_per_percentile AS forward_per_percentile,
            source.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
            source.forecast_p_op AS forward_p_op,
            source.forecast_p_op_percentile AS forward_p_op_percentile,
            source.forecast_per_to_fop_growth_ratio
                AS forward_per_to_fop_growth_ratio,
            source.forecast_per_to_fop_growth_ratio_percentile
                AS forward_per_to_fop_growth_ratio_percentile
        FROM {source_name} source
        """
    )


def _create_deep_dive_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_forecast_op_growth_deep_panel AS
        SELECT
            g.*,
            coalesce(g.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                g.atr20_acceleration
                AND coalesce(g.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
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
                AND coalesce(g.weak_trend, FALSE)
            ) AS crowded_overvalued_weak_trend_flag,
            (
                g.liquidity_regime = 'distribution_stress'
                AND coalesce(g.weak_trend, FALSE)
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
                AND coalesce(g.weak_trend, FALSE)
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
    topix_return_expression = f"({raw_return_column} - {return_column})"
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
            avg({topix_return_expression}) AS mean_topix_return_pct,
            median({topix_return_expression}) AS median_topix_return_pct,
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


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


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
