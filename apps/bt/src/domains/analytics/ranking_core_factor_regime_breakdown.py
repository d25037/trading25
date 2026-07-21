"""Factor-regime breakdown for the momentum-value Ranking core."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    SectorStrengthFeaturesRequest,
    build_sector_strength_features,
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
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    DEFAULT_HORIZONS,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

DEFAULT_MIN_SECTOR_OBSERVATIONS = 5

RANKING_CORE_FACTOR_REGIME_BREAKDOWN_EXPERIMENT_ID = (
    "market-behavior/ranking-core-factor-regime-breakdown"
)


@dataclass(frozen=True)
class RankingCoreFactorRegimeBreakdownResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    min_sector_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    year_factor_spread_df: pd.DataFrame
    year_breadth_summary_df: pd.DataFrame
    annual_factor_breadth_df: pd.DataFrame
    nt_ratio_regime_summary_df: pd.DataFrame
    factor_nt_regime_df: pd.DataFrame
    bank_exclusion_df: pd.DataFrame
    factor_resilience_df: pd.DataFrame
    core_failure_decomposition_df: pd.DataFrame
    regime_comparison_df: pd.DataFrame
    sector_year_contribution_df: pd.DataFrame
    current_term_mapping_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_core_factor_regime_breakdown_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    min_sector_observations: int = DEFAULT_MIN_SECTOR_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingCoreFactorRegimeBreakdownResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        min_sector_observations=min_sector_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-core-factor-regime-breakdown-",
    ) as ctx:
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="core_factor_regime",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError(
                "factor regime research requires liquidity-ranked signals"
            )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="core_factor_regime_sector",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(sector_features,),
            namespace="core_factor_regime",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="core_factor_regime_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="core_factor_regime_outcomes",
        )
        _create_core_sector_relative_tables(
            ctx.connection,
            source_name=evaluated.name,
            min_sector_observations=min_sector_observations,
        )
        _create_factor_regime_tables(ctx.connection, source_name=evaluated.name)
        _create_nt_ratio_regime_tables(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_factor_regime_panel"
            ).fetchone()[0]
        )
        result = RankingCoreFactorRegimeBreakdownResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            min_sector_observations=int(min_sector_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            year_factor_spread_df=_build_year_factor_spread_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            year_breadth_summary_df=_build_year_breadth_summary_df(ctx.connection),
            annual_factor_breadth_df=_build_annual_factor_breadth_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            nt_ratio_regime_summary_df=_build_nt_ratio_regime_summary_df(
                ctx.connection
            ),
            factor_nt_regime_df=_build_factor_nt_regime_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            bank_exclusion_df=_build_bank_exclusion_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            factor_resilience_df=pd.DataFrame(),
            core_failure_decomposition_df=_build_core_failure_decomposition_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            regime_comparison_df=_build_regime_comparison_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sector_year_contribution_df=_build_sector_year_contribution_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            current_term_mapping_df=_build_current_term_mapping_df(ctx.connection),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
                horizons=resolved_horizons,
            ),
        )
        result = replace(
            result,
            factor_resilience_df=_build_factor_resilience_df(
                result.annual_factor_breadth_df
            ),
        )
    return result


def write_ranking_core_factor_regime_breakdown_bundle(
    result: RankingCoreFactorRegimeBreakdownResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_CORE_FACTOR_REGIME_BREAKDOWN_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_core_factor_regime_breakdown",
        function="run_ranking_core_factor_regime_breakdown_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "min_sector_observations": result.min_sector_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
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
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "year_factor_spread_df": result.year_factor_spread_df,
            "year_breadth_summary_df": result.year_breadth_summary_df,
            "annual_factor_breadth_df": result.annual_factor_breadth_df,
            "nt_ratio_regime_summary_df": result.nt_ratio_regime_summary_df,
            "factor_nt_regime_df": result.factor_nt_regime_df,
            "bank_exclusion_df": result.bank_exclusion_df,
            "factor_resilience_df": result.factor_resilience_df,
            "core_failure_decomposition_df": result.core_failure_decomposition_df,
            "regime_comparison_df": result.regime_comparison_df,
            "sector_year_contribution_df": result.sector_year_contribution_df,
            "current_term_mapping_df": result.current_term_mapping_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingCoreFactorRegimeBreakdownResult) -> str:
    parts = [
        "# Ranking Core Factor Regime Breakdown",
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
        "## Year Factor Spread",
        "",
        _top_rows_for_markdown(result.year_factor_spread_df, limit=240),
        "",
        "## Year Breadth Summary",
        "",
        _top_rows_for_markdown(result.year_breadth_summary_df, limit=160),
        "",
        "## Annual Factor x Breadth",
        "",
        _top_rows_for_markdown(result.annual_factor_breadth_df, limit=240),
        "",
        "## NT Ratio Regime Summary",
        "",
        _top_rows_for_markdown(result.nt_ratio_regime_summary_df, limit=160),
        "",
        "## Factor x NT 60D Regime",
        "",
        _top_rows_for_markdown(result.factor_nt_regime_df, limit=260),
        "",
        "## Bank Exclusion",
        "",
        _top_rows_for_markdown(result.bank_exclusion_df, limit=260),
        "",
        "## Factor Resilience",
        "",
        _top_rows_for_markdown(result.factor_resilience_df, limit=160),
        "",
        "## Core Failure Decomposition",
        "",
        _top_rows_for_markdown(result.core_failure_decomposition_df, limit=240),
        "",
        "## Regime Comparison",
        "",
        _top_rows_for_markdown(result.regime_comparison_df, limit=160),
        "",
        "## Sector Year Contribution",
        "",
        _top_rows_for_markdown(result.sector_year_contribution_df, limit=200),
        "",
        "## Current Daily Ranking Terms",
        "",
        _top_rows_for_markdown(result.current_term_mapping_df, limit=80),
        "",
        "## Observation Sample",
        "",
        _top_rows_for_markdown(result.observation_sample_df, limit=80),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _create_core_sector_relative_tables(
    conn: Any,
    *,
    source_name: str,
    min_sector_observations: int,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_core_sector_relative_universe AS
        WITH sector_universe AS (
            SELECT
                r.market_scope,
                r.date,
                r.code,
                r.sector_33_code,
                r.sector_33_name,
                r.pbr,
                r.forecast_per AS forward_per,
                r.pbr_percentile AS raw_pbr_percentile,
                r.forecast_per_percentile AS raw_forward_per_percentile,
                count(*) FILTER (WHERE r.pbr > 0) OVER (
                    PARTITION BY r.market_scope, r.date, r.sector_33_name
                ) AS sector_pbr_valid_count,
                rank() OVER (
                    PARTITION BY r.market_scope, r.date, r.sector_33_name
                    ORDER BY CASE WHEN r.pbr > 0 THEN r.pbr END NULLS LAST
                ) AS sector_pbr_rank,
                count(*) FILTER (WHERE r.forecast_per > 0) OVER (
                    PARTITION BY r.market_scope, r.date, r.sector_33_name
                ) AS sector_forward_per_valid_count,
                rank() OVER (
                    PARTITION BY r.market_scope, r.date, r.sector_33_name
                    ORDER BY CASE WHEN r.forecast_per > 0
                                  THEN r.forecast_per END NULLS LAST
                ) AS sector_forward_per_rank
            FROM {source_name} r
        )
        SELECT
            *,
            CASE
                WHEN pbr > 0 AND sector_pbr_valid_count >= ?
                    THEN (sector_pbr_rank - 1.0) / (sector_pbr_valid_count - 1.0)
            END AS sector_pbr_percentile,
            CASE
                WHEN forward_per > 0 AND sector_forward_per_valid_count >= ?
                    THEN (sector_forward_per_rank - 1.0)
                        / (sector_forward_per_valid_count - 1.0)
            END AS sector_forward_per_percentile
        FROM sector_universe
        """,
        [int(min_sector_observations), int(min_sector_observations)],
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_core_sector_relative_panel AS
        WITH joined AS (
            SELECT
                source.*,
                source.forecast_per AS forward_per,
                source.forecast_per_percentile AS forward_per_percentile,
                source.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
                u.sector_pbr_valid_count,
                u.sector_forward_per_valid_count,
                u.sector_pbr_percentile,
                u.sector_forward_per_percentile,
                CASE
                    WHEN source.pbr_percentile IS NOT NULL
                     AND source.forecast_per_percentile IS NOT NULL
                     AND u.sector_pbr_percentile IS NOT NULL
                     AND u.sector_forward_per_percentile IS NOT NULL
                        THEN (
                            source.pbr_percentile
                            + source.forecast_per_percentile
                            + u.sector_pbr_percentile
                            + u.sector_forward_per_percentile
                        ) / 4.0
                END AS hybrid_value_score
            FROM {source_name} source
            LEFT JOIN ranking_core_sector_relative_universe u
              ON u.market_scope = source.market_scope
             AND u.date = source.date
             AND u.code = source.code
             AND u.sector_33_name = source.sector_33_name
        ),
        ranked AS (
            SELECT
                *,
                CASE
                    WHEN hybrid_value_score IS NOT NULL
                        THEN percent_rank() OVER (
                            PARTITION BY market_scope, date
                            ORDER BY hybrid_value_score NULLS LAST
                        )
                END AS hybrid_value_percentile
            FROM joined
        )
        SELECT
            *,
            liquidity_scope = 'neutral_rerating'
                AND pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2
                AND sector_strength_bucket = 'sector_strong' AS raw_core_flag,
            liquidity_scope = 'neutral_rerating'
                AND sector_pbr_percentile <= 0.2
                AND sector_forward_per_percentile <= 0.2
                AND sector_strength_bucket = 'sector_strong' AS sector_relative_core_flag,
            liquidity_scope = 'neutral_rerating'
                AND hybrid_value_percentile <= 0.2
                AND sector_strength_bucket = 'sector_strong' AS hybrid_core_flag
        FROM ranked
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_core_rule_observations AS
        SELECT 'raw_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE raw_core_flag
        UNION ALL
        SELECT 'sector_relative_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE sector_relative_core_flag
        UNION ALL
        SELECT 'raw_and_sector_relative_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE raw_core_flag AND sector_relative_core_flag
        UNION ALL
        SELECT 'raw_only_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE raw_core_flag AND NOT sector_relative_core_flag
        UNION ALL
        SELECT 'sector_relative_only_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE sector_relative_core_flag AND NOT raw_core_flag
        UNION ALL
        SELECT 'hybrid_core' AS core_rule, *
        FROM ranking_core_sector_relative_panel
        WHERE hybrid_core_flag
        """
    )


def _create_factor_regime_tables(conn: Any, *, source_name: str) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE factor_signal_terms (
            factor_signal TEXT,
            factor_family TEXT,
            factor_display_name TEXT,
            display_order INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO factor_signal_terms VALUES (?, ?, ?, ?)",
        [
            ("low_value", "Valuation Signal", "Undervalued", 10),
            (
                "momentum_20_60_top20",
                "Individual Momentum",
                "20/60D Momentum",
                20,
            ),
            ("value_momentum", "Combined", "Momentum Value", 30),
            (
                "value_momentum_sector_strong",
                "Combined",
                "Momentum Value + Balanced Sector Strength: Strong",
                35,
            ),
            (
                "value_momentum_atr20_acceleration_sector_strong",
                "Combined",
                "Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong",
                36,
            ),
            (
                "overvalued_momentum",
                "Individual Momentum",
                "Overvalued + 20/60D Momentum",
                40,
            ),
            ("sector_strong", "Sector", "Balanced Sector Strength: Strong", 50),
            (
                "atr20_acceleration_ex_overheat",
                "Volatility / ATR",
                "ATR20 Accel",
                60,
            ),
            ("core_all", "Daily Ranking Core", "Momentum Value Core", 100),
            (
                "core_atr20_acceleration_ex_overheat",
                "Volatility / ATR",
                "Momentum Value Core + ATR20 Accel",
                110,
            ),
            (
                "core_without_atr20_acceleration_ex_overheat",
                "Daily Ranking Core",
                "Momentum Value Core without ATR20 Accel",
                111,
            ),
            (
                "core_momentum_20_60_top20",
                "Individual Momentum",
                "Momentum Value Core + 20/60D Momentum",
                120,
            ),
            (
                "core_without_momentum_20_60_top20",
                "Daily Ranking Core",
                "Momentum Value Core without 20/60D Momentum",
                121,
            ),
            (
                "core_sector_relative_confirmed",
                "Sector",
                "Momentum Value Core + Balanced Sector Strength confirmed",
                130,
            ),
            (
                "core_without_sector_relative_confirmed",
                "Daily Ranking Core",
                "Momentum Value Core without Balanced Sector Strength confirmed",
                131,
            ),
        ],
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_factor_regime_panel AS
        WITH base AS (
            SELECT
                r.market_scope,
                r.date,
                substr(CAST(r.date AS VARCHAR), 1, 4) AS year,
                CASE
                    WHEN substr(CAST(r.date AS VARCHAR), 1, 4) = '2026'
                        THEN '2026_partial'
                    WHEN substr(CAST(r.date AS VARCHAR), 1, 4) BETWEEN '2022' AND '2025'
                        THEN '2022_2025_history'
                    ELSE 'pre_2022'
                END AS year_group,
                r.code,
                r.company_name,
                r.sector_33_name,
                r.sector_strength_bucket,
                r.sector_strength_score,
                r.liquidity_regime,
                r.recent_return_20d_pct,
                r.recent_return_60d_pct,
                r.pbr_percentile,
                r.forecast_per_percentile AS forward_per_percentile,
                r.per_percentile,
                r.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
                r.forward_close_excess_return_5d_pct,
                r.forward_close_excess_return_10d_pct,
                r.forward_close_excess_return_20d_pct,
                r.forward_close_excess_return_60d_pct,
                r.atr20_change_20d_pct,
                r.atr20_to_atr60,
                r.recent_return_20d_pct AS atr_recent_return_20d_pct
            FROM {source_name} r
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY recent_return_20d_pct NULLS LAST
                ) AS momentum_20d_percentile,
                percent_rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY recent_return_60d_pct NULLS LAST
                ) AS momentum_60d_percentile
            FROM base
        )
        SELECT
            *,
            pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2 AS low_value_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS momentum_20_60_top20_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8
                AND (
                    pbr_percentile >= 0.8
                    OR forward_per_percentile >= 0.8
                ) AS overvalued_momentum_flag,
            pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2
                AND momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS value_momentum_flag,
            sector_strength_bucket = 'sector_strong' AS sector_strong_flag,
            atr_recent_return_20d_pct < 30.0
                AND atr20_change_20d_pct >= 25.0
                AND atr20_to_atr60 < 1.25 AS atr20_acceleration_ex_overheat_flag
        FROM ranked
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE daily_market_breadth_state AS
        WITH daily AS (
            SELECT
                market_scope,
                date,
                year,
                year_group,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                avg(CASE WHEN recent_return_20d_pct > 0 THEN 1.0 ELSE 0.0 END)
                    * 100.0 AS breadth_up_20d_pct,
                avg(CASE WHEN recent_return_60d_pct > 0 THEN 1.0 ELSE 0.0 END)
                    * 100.0 AS breadth_up_60d_pct,
                median(recent_return_20d_pct) AS median_recent_return_20d_pct,
                median(recent_return_60d_pct) AS median_recent_return_60d_pct
            FROM ranking_factor_regime_panel
            GROUP BY market_scope, date, year, year_group
        )
        SELECT
            *,
            CASE
                WHEN breadth_up_20d_pct < 30.0 THEN 'breadth_low_lt_30pct'
                WHEN breadth_up_20d_pct < 60.0 THEN 'breadth_mid_30_60pct'
                ELSE 'breadth_high_ge_60pct'
            END AS breadth_bucket_20d,
            CASE
                WHEN breadth_up_20d_pct < 30.0 THEN 'Low Breadth'
                WHEN breadth_up_20d_pct < 60.0 THEN 'Mid Breadth'
                ELSE 'High Breadth'
            END AS breadth_label_20d,
            CASE
                WHEN breadth_up_60d_pct < 30.0 THEN 'breadth_low_lt_30pct'
                WHEN breadth_up_60d_pct < 60.0 THEN 'breadth_mid_30_60pct'
                ELSE 'breadth_high_ge_60pct'
            END AS breadth_bucket_60d,
            CASE
                WHEN breadth_up_60d_pct < 30.0 THEN 'Low Breadth'
                WHEN breadth_up_60d_pct < 60.0 THEN 'Mid Breadth'
                ELSE 'High Breadth'
            END AS breadth_label_60d
        FROM daily
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE factor_signal_observations_raw AS
        SELECT 'low_value' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE low_value_flag
        UNION ALL
        SELECT 'momentum_20_60_top20' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE momentum_20_60_top20_flag
        UNION ALL
        SELECT 'value_momentum' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE value_momentum_flag
        UNION ALL
        SELECT 'value_momentum_sector_strong' AS factor_signal, *
        FROM ranking_factor_regime_panel
        WHERE value_momentum_flag AND sector_strong_flag
        UNION ALL
        SELECT 'value_momentum_atr20_acceleration_sector_strong' AS factor_signal, *
        FROM ranking_factor_regime_panel
        WHERE value_momentum_flag
          AND sector_strong_flag
          AND atr20_acceleration_ex_overheat_flag
        UNION ALL
        SELECT 'overvalued_momentum' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE overvalued_momentum_flag
        UNION ALL
        SELECT 'sector_strong' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE sector_strong_flag
        UNION ALL
        SELECT 'atr20_acceleration_ex_overheat' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE atr20_acceleration_ex_overheat_flag
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE factor_signal_observations AS
        SELECT
            raw.*,
            coalesce(terms.factor_family, 'Other') AS factor_family,
            coalesce(terms.factor_display_name, raw.factor_signal) AS factor_display_name,
            coalesce(terms.display_order, 999) AS factor_display_order
        FROM factor_signal_observations_raw raw
        LEFT JOIN factor_signal_terms terms USING (factor_signal)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE core_factor_panel AS
        SELECT
            c.core_rule,
            c.date,
            substr(CAST(c.date AS VARCHAR), 1, 4) AS year,
            CASE
                WHEN substr(CAST(c.date AS VARCHAR), 1, 4) = '2026'
                    THEN '2026_partial'
                WHEN substr(CAST(c.date AS VARCHAR), 1, 4) BETWEEN '2022' AND '2025'
                    THEN '2022_2025_history'
                ELSE 'pre_2022'
            END AS year_group,
            c.code,
            c.company_name,
            c.market_scope,
            c.sector_33_name,
            c.sector_strength_bucket,
            c.sector_strength_score,
            c.raw_core_flag AS core_flag,
            c.sector_relative_core_flag,
            c.hybrid_core_flag,
            c.forward_close_excess_return_5d_pct,
            c.forward_close_excess_return_10d_pct,
            c.forward_close_excess_return_20d_pct,
            c.forward_close_excess_return_60d_pct,
            f.momentum_20d_percentile,
            f.momentum_60d_percentile,
            f.low_value_flag,
            f.momentum_20_60_top20_flag,
            f.value_momentum_flag,
            f.overvalued_momentum_flag,
            f.atr20_acceleration_ex_overheat_flag,
            CASE
                WHEN f.atr20_acceleration_ex_overheat_flag
                    THEN 'atr20_acceleration_ex_overheat'
                ELSE 'not_atr20_acceleration_ex_overheat'
            END AS atr_state
        FROM ranking_core_rule_observations c
        JOIN ranking_factor_regime_panel f
          ON f.market_scope = c.market_scope
         AND f.date = c.date
         AND f.code = c.code
        WHERE c.core_rule = 'raw_core'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE core_failure_observations_raw AS
        SELECT 'core_all' AS core_slice, 'core_all' AS factor_signal, *
        FROM core_factor_panel
        UNION ALL
        SELECT 'core_atr20_acceleration_ex_overheat' AS core_slice,
               'core_atr20_acceleration_ex_overheat' AS factor_signal, *
        FROM core_factor_panel
        WHERE atr20_acceleration_ex_overheat_flag
        UNION ALL
        SELECT 'core_without_atr20_acceleration_ex_overheat' AS core_slice,
               'core_without_atr20_acceleration_ex_overheat' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT atr20_acceleration_ex_overheat_flag
        UNION ALL
        SELECT 'core_momentum_20_60_top20' AS core_slice,
               'core_momentum_20_60_top20' AS factor_signal, *
        FROM core_factor_panel
        WHERE momentum_20_60_top20_flag
        UNION ALL
        SELECT 'core_without_momentum_20_60_top20' AS core_slice,
               'core_without_momentum_20_60_top20' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT momentum_20_60_top20_flag
        UNION ALL
        SELECT 'core_sector_relative_confirmed' AS core_slice,
               'core_sector_relative_confirmed' AS factor_signal, *
        FROM core_factor_panel
        WHERE sector_relative_core_flag
        UNION ALL
        SELECT 'core_without_sector_relative_confirmed' AS core_slice,
               'core_without_sector_relative_confirmed' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT sector_relative_core_flag
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE core_failure_observations AS
        SELECT
            raw.*,
            coalesce(terms.factor_family, 'Other') AS factor_family,
            coalesce(terms.factor_display_name, raw.factor_signal) AS factor_display_name,
            coalesce(terms.display_order, 999) AS factor_display_order
        FROM core_failure_observations_raw raw
        LEFT JOIN factor_signal_terms terms USING (factor_signal)
        """
    )
    conn.execute(
        """
        INSERT INTO factor_signal_observations
        SELECT
            raw.*,
            coalesce(terms.factor_family, 'Other') AS factor_family,
            coalesce(terms.factor_display_name, raw.factor_signal) AS factor_display_name,
            coalesce(terms.display_order, 999) AS factor_display_order
        FROM (
            SELECT 'core_atr20_acceleration_ex_overheat' AS factor_signal, f.*
            FROM ranking_factor_regime_panel f
            JOIN core_factor_panel c
              ON c.market_scope = f.market_scope
             AND c.date = f.date
             AND c.code = f.code
            WHERE c.atr20_acceleration_ex_overheat_flag
        ) raw
        LEFT JOIN factor_signal_terms terms USING (factor_signal)
        """
    )


def _create_nt_ratio_regime_tables(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE nt_ratio_daily_state AS
        WITH nikkei AS (
            SELECT
                CAST(date AS VARCHAR) AS date,
                close AS n225_close
            FROM indices_data
            WHERE code = 'N225_UNDERPX'
              AND close > 0
        ),
        topix AS (
            SELECT
                CAST(date AS VARCHAR) AS date,
                close AS topix_close
            FROM topix_data
            WHERE close > 0
        ),
        base AS (
            SELECT
                nikkei.date,
                substr(nikkei.date, 1, 4) AS year,
                CASE
                    WHEN substr(nikkei.date, 1, 4) = '2026'
                        THEN '2026_partial'
                    WHEN substr(nikkei.date, 1, 4) BETWEEN '2022' AND '2025'
                        THEN '2022_2025_history'
                    ELSE 'pre_2022'
                END AS year_group,
                CASE
                    WHEN nikkei.date < '2022-01-01' THEN '2016-2021'
                    WHEN nikkei.date < '2026-01-01' THEN '2022-2025'
                    ELSE '2026'
                END AS nt_period,
                nikkei.n225_close,
                topix.topix_close,
                nikkei.n225_close / topix.topix_close AS nt_ratio,
                lag(nikkei.n225_close / topix.topix_close, 20)
                    OVER (ORDER BY nikkei.date) AS nt_ratio_lag_20d,
                lag(nikkei.n225_close / topix.topix_close, 60)
                    OVER (ORDER BY nikkei.date) AS nt_ratio_lag_60d
            FROM nikkei
            JOIN topix USING (date)
        )
        SELECT
            *,
            100.0 * (nt_ratio / nullif(nt_ratio_lag_20d, 0.0) - 1.0)
                AS nt_ratio_return_20d_pct,
            100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0)
                AS nt_ratio_return_60d_pct,
            CASE
                WHEN nt_ratio_lag_60d IS NULL THEN 'unknown'
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) >= 3.0
                    THEN 'nt_up_ge_3pct_60d'
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) <= -3.0
                    THEN 'nt_down_le_minus_3pct_60d'
                ELSE 'nt_flat_pm_3pct_60d'
            END AS nt_regime_60d,
            CASE
                WHEN nt_ratio_lag_60d IS NULL THEN 'NT Unknown'
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) >= 3.0
                    THEN 'NT Up >= +3% / 60D'
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) <= -3.0
                    THEN 'NT Down <= -3% / 60D'
                ELSE 'NT Flat +/-3% / 60D'
            END AS nt_regime_60d_label,
            CASE
                WHEN nt_ratio_lag_60d IS NULL THEN 99
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) <= -3.0
                    THEN 10
                WHEN 100.0 * (nt_ratio / nullif(nt_ratio_lag_60d, 0.0) - 1.0) < 3.0
                    THEN 20
                ELSE 30
            END AS nt_regime_60d_order
        FROM base
        """
    )


def _build_year_breadth_summary_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        WITH unpivoted AS (
            SELECT
                market_scope,
                year,
                20 AS breadth_lookback,
                breadth_bucket_20d AS breadth_bucket,
                breadth_label_20d AS breadth_label,
                breadth_up_20d_pct AS breadth_up_pct,
                median_recent_return_20d_pct AS median_recent_return_pct
            FROM daily_market_breadth_state
            UNION ALL
            SELECT
                market_scope,
                year,
                60 AS breadth_lookback,
                breadth_bucket_60d AS breadth_bucket,
                breadth_label_60d AS breadth_label,
                breadth_up_60d_pct AS breadth_up_pct,
                median_recent_return_60d_pct AS median_recent_return_pct
            FROM daily_market_breadth_state
        )
        SELECT
            market_scope,
            year,
            breadth_lookback,
            breadth_bucket,
            breadth_label,
            count(*) AS trading_day_count,
            avg(breadth_up_pct) AS avg_breadth_up_pct,
            median(breadth_up_pct) AS median_breadth_up_pct,
            min(breadth_up_pct) AS min_breadth_up_pct,
            max(breadth_up_pct) AS max_breadth_up_pct,
            median(median_recent_return_pct) AS median_cross_section_return_pct
        FROM unpivoted
        GROUP BY market_scope, year, breadth_lookback, breadth_bucket, breadth_label
        ORDER BY year, market_scope, breadth_lookback, breadth_bucket
        """
    ).fetchdf()


def _build_nt_ratio_regime_summary_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        WITH analysis_dates AS (
            SELECT DISTINCT date
            FROM ranking_factor_regime_panel
        ),
        filtered AS (
            SELECT n.*
            FROM nt_ratio_daily_state n
            JOIN analysis_dates d USING (date)
        )
        SELECT
            year,
            min(date) AS first_date,
            max(date) AS last_date,
            first(nt_ratio ORDER BY date) AS nt_ratio_start,
            last(nt_ratio ORDER BY date) AS nt_ratio_end,
            100.0 * (
                last(nt_ratio ORDER BY date) / first(nt_ratio ORDER BY date) - 1.0
            ) AS nt_ratio_change_pct,
            median(nt_ratio_return_60d_pct) AS median_nt_ratio_return_60d_pct,
            avg(nt_ratio_return_60d_pct) AS mean_nt_ratio_return_60d_pct,
            avg(CASE WHEN nt_regime_60d = 'nt_down_le_minus_3pct_60d'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS nt_down_day_share_pct,
            avg(CASE WHEN nt_regime_60d = 'nt_flat_pm_3pct_60d'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS nt_flat_day_share_pct,
            avg(CASE WHEN nt_regime_60d = 'nt_up_ge_3pct_60d'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS nt_up_day_share_pct,
            count(*) AS trading_day_count
        FROM filtered
        GROUP BY year
        ORDER BY year
        """
    ).fetchdf()


def _build_factor_nt_regime_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_nt_regime_table(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_factor_nt_regime_columns())


def _aggregate_factor_nt_regime_table(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    return conn.execute(
        f"""
        WITH time_buckets AS (
            SELECT
                p.market_scope,
                p.date,
                'year' AS time_bucket_type,
                p.year AS time_bucket,
                n.nt_regime_60d,
                n.nt_regime_60d_label,
                n.nt_regime_60d_order,
                n.nt_ratio_return_60d_pct
            FROM ranking_factor_regime_panel p
            JOIN nt_ratio_daily_state n
              ON n.date = p.date
            WHERE n.nt_regime_60d <> 'unknown'
            GROUP BY
                p.market_scope,
                p.date,
                p.year,
                n.nt_regime_60d,
                n.nt_regime_60d_label,
                n.nt_regime_60d_order,
                n.nt_ratio_return_60d_pct
            UNION ALL
            SELECT
                p.market_scope,
                p.date,
                'period' AS time_bucket_type,
                n.nt_period AS time_bucket,
                n.nt_regime_60d,
                n.nt_regime_60d_label,
                n.nt_regime_60d_order,
                n.nt_ratio_return_60d_pct
            FROM ranking_factor_regime_panel p
            JOIN nt_ratio_daily_state n
              ON n.date = p.date
            WHERE n.nt_regime_60d <> 'unknown'
            GROUP BY
                p.market_scope,
                p.date,
                n.nt_period,
                n.nt_regime_60d,
                n.nt_regime_60d_label,
                n.nt_regime_60d_order,
                n.nt_ratio_return_60d_pct
        ),
        baseline AS (
            SELECT
                {int(horizon)} AS horizon,
                b.time_bucket_type,
                b.time_bucket,
                b.nt_regime_60d,
                b.nt_regime_60d_label,
                any_value(b.nt_regime_60d_order) AS nt_regime_60d_order,
                count(*) AS baseline_observation_count,
                count(DISTINCT p.code) AS baseline_code_count,
                count(DISTINCT p.date) AS baseline_date_count,
                avg(b.nt_ratio_return_60d_pct)
                    AS baseline_mean_nt_ratio_return_60d_pct,
                median(p.{return_column})
                    AS baseline_median_forward_topix_excess_return_pct,
                avg(CASE WHEN p.{return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS baseline_win_rate_pct
            FROM ranking_factor_regime_panel p
            JOIN time_buckets b
              ON b.market_scope = p.market_scope
             AND b.date = p.date
            WHERE p.{return_column} IS NOT NULL
            GROUP BY
                b.time_bucket_type,
                b.time_bucket,
                b.nt_regime_60d,
                b.nt_regime_60d_label
        ),
        factor AS (
            SELECT
                {int(horizon)} AS horizon,
                b.time_bucket_type,
                b.time_bucket,
                b.nt_regime_60d,
                b.nt_regime_60d_label,
                f.factor_signal,
                f.factor_family,
                f.factor_display_name,
                count(*) AS factor_observation_count,
                count(DISTINCT f.code) AS factor_code_count,
                count(DISTINCT f.date) AS factor_date_count,
                count(DISTINCT f.sector_33_name) AS factor_sector_count,
                avg(b.nt_ratio_return_60d_pct)
                    AS factor_mean_nt_ratio_return_60d_pct,
                avg(f.{return_column})
                    AS factor_mean_forward_topix_excess_return_pct,
                median(f.{return_column})
                    AS factor_median_forward_topix_excess_return_pct,
                quantile_cont(f.{return_column}, 0.10)
                    AS factor_p10_forward_topix_excess_return_pct,
                quantile_cont(f.{return_column}, 0.90)
                    AS factor_p90_forward_topix_excess_return_pct,
                avg(CASE WHEN f.{return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS factor_win_rate_pct,
                avg(CASE WHEN f.{return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                    AS factor_severe_loss_rate_pct
            FROM factor_signal_observations f
            JOIN time_buckets b
              ON b.market_scope = f.market_scope
             AND b.date = f.date
            WHERE f.{return_column} IS NOT NULL
            GROUP BY
                b.time_bucket_type,
                b.time_bucket,
                b.nt_regime_60d,
                b.nt_regime_60d_label,
                f.factor_signal,
                f.factor_family,
                f.factor_display_name
            HAVING count(*) >= ?
        )
        SELECT
            factor.horizon,
            factor.time_bucket_type,
            factor.time_bucket,
            factor.nt_regime_60d,
            factor.nt_regime_60d_label,
            baseline.nt_regime_60d_order,
            factor.factor_signal,
            factor.factor_family,
            factor.factor_display_name,
            factor.factor_observation_count,
            factor.factor_code_count,
            factor.factor_date_count,
            factor.factor_sector_count,
            baseline.baseline_observation_count,
            baseline.baseline_code_count,
            baseline.baseline_date_count,
            factor.factor_mean_nt_ratio_return_60d_pct,
            baseline.baseline_mean_nt_ratio_return_60d_pct,
            factor.factor_mean_forward_topix_excess_return_pct,
            factor.factor_median_forward_topix_excess_return_pct,
            baseline.baseline_median_forward_topix_excess_return_pct,
            (
                factor.factor_median_forward_topix_excess_return_pct
                - baseline.baseline_median_forward_topix_excess_return_pct
            ) AS factor_minus_baseline_median_forward_topix_excess_return_pct,
            factor.factor_p10_forward_topix_excess_return_pct,
            factor.factor_p90_forward_topix_excess_return_pct,
            factor.factor_win_rate_pct,
            baseline.baseline_win_rate_pct,
            factor.factor_win_rate_pct - baseline.baseline_win_rate_pct
                AS factor_minus_baseline_win_rate_pct,
            factor.factor_severe_loss_rate_pct
        FROM factor
        JOIN baseline
          ON baseline.horizon = factor.horizon
         AND baseline.time_bucket_type = factor.time_bucket_type
         AND baseline.time_bucket = factor.time_bucket
         AND baseline.nt_regime_60d = factor.nt_regime_60d
        ORDER BY
            factor.horizon,
            factor.time_bucket_type,
            factor.time_bucket,
            baseline.nt_regime_60d_order,
            factor.factor_signal
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_bank_exclusion_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_bank_exclusion_table(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_bank_exclusion_columns())


def _aggregate_bank_exclusion_table(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    return conn.execute(
        f"""
        WITH target_factors AS (
            SELECT *
            FROM factor_signal_observations
            WHERE factor_signal IN (
                'low_value',
                'value_momentum',
                'value_momentum_sector_strong',
                'value_momentum_atr20_acceleration_sector_strong'
            )
        ),
        scoped_factor AS (
            SELECT
                'annual' AS analysis_scope,
                'All NT Regimes' AS analysis_scope_label,
                factor.*
            FROM target_factors factor
            UNION ALL
            SELECT
                'annual_nt_flat_60d' AS analysis_scope,
                'NT Flat +/-3% / 60D' AS analysis_scope_label,
                factor.*
            FROM target_factors factor
            JOIN nt_ratio_daily_state nt
              ON nt.date = factor.date
            WHERE nt.nt_regime_60d = 'nt_flat_pm_3pct_60d'
        ),
        factor_sector_scope AS (
            SELECT 'all_sectors' AS sector_scope,
                   'All sectors' AS sector_scope_label,
                   *
            FROM scoped_factor
            UNION ALL
            SELECT 'banks_only' AS sector_scope,
                   'Banks only' AS sector_scope_label,
                   *
            FROM scoped_factor
            WHERE sector_33_name = '銀行業'
            UNION ALL
            SELECT 'ex_banks' AS sector_scope,
                   'ex Banks' AS sector_scope_label,
                   *
            FROM scoped_factor
            WHERE sector_33_name <> '銀行業'
        ),
        baseline_scoped AS (
            SELECT
                'annual' AS analysis_scope,
                'All NT Regimes' AS analysis_scope_label,
                panel.*
            FROM ranking_factor_regime_panel panel
            UNION ALL
            SELECT
                'annual_nt_flat_60d' AS analysis_scope,
                'NT Flat +/-3% / 60D' AS analysis_scope_label,
                panel.*
            FROM ranking_factor_regime_panel panel
            JOIN nt_ratio_daily_state nt
              ON nt.date = panel.date
            WHERE nt.nt_regime_60d = 'nt_flat_pm_3pct_60d'
        ),
        baseline_sector_scope AS (
            SELECT 'all_sectors' AS sector_scope,
                   'All sectors' AS sector_scope_label,
                   *
            FROM baseline_scoped
            UNION ALL
            SELECT 'banks_only' AS sector_scope,
                   'Banks only' AS sector_scope_label,
                   *
            FROM baseline_scoped
            WHERE sector_33_name = '銀行業'
            UNION ALL
            SELECT 'ex_banks' AS sector_scope,
                   'ex Banks' AS sector_scope_label,
                   *
            FROM baseline_scoped
            WHERE sector_33_name <> '銀行業'
        ),
        baseline AS (
            SELECT
                {int(horizon)} AS horizon,
                analysis_scope,
                analysis_scope_label,
                year,
                sector_scope,
                sector_scope_label,
                count(*) AS baseline_observation_count,
                count(DISTINCT code) AS baseline_code_count,
                count(DISTINCT date) AS baseline_date_count,
                median({return_column})
                    AS baseline_median_forward_topix_excess_return_pct,
                avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS baseline_win_rate_pct
            FROM baseline_sector_scope
            WHERE {return_column} IS NOT NULL
            GROUP BY
                analysis_scope,
                analysis_scope_label,
                year,
                sector_scope,
                sector_scope_label
        )
        SELECT
            {int(horizon)} AS horizon,
            factor.analysis_scope,
            factor.analysis_scope_label,
            factor.year,
            factor.sector_scope,
            factor.sector_scope_label,
            factor.factor_signal,
            factor.factor_family,
            factor.factor_display_name,
            count(*) AS factor_observation_count,
            count(DISTINCT factor.code) AS factor_code_count,
            count(DISTINCT factor.date) AS factor_date_count,
            count(DISTINCT factor.sector_33_name) AS factor_sector_count,
            any_value(baseline.baseline_observation_count)
                AS baseline_observation_count,
            any_value(baseline.baseline_code_count) AS baseline_code_count,
            any_value(baseline.baseline_date_count) AS baseline_date_count,
            avg(factor.{return_column})
                AS factor_mean_forward_topix_excess_return_pct,
            median(factor.{return_column})
                AS factor_median_forward_topix_excess_return_pct,
            any_value(baseline.baseline_median_forward_topix_excess_return_pct)
                AS baseline_median_forward_topix_excess_return_pct,
            (
                median(factor.{return_column})
                - any_value(baseline.baseline_median_forward_topix_excess_return_pct)
            ) AS factor_minus_baseline_median_forward_topix_excess_return_pct,
            avg(CASE WHEN factor.{return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS factor_win_rate_pct,
            any_value(baseline.baseline_win_rate_pct) AS baseline_win_rate_pct,
            (
                avg(CASE WHEN factor.{return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                - any_value(baseline.baseline_win_rate_pct)
            ) AS factor_minus_baseline_win_rate_pct,
            avg(CASE WHEN factor.{return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS factor_severe_loss_rate_pct
        FROM factor_sector_scope factor
        JOIN baseline
          ON baseline.horizon = {int(horizon)}
         AND baseline.analysis_scope = factor.analysis_scope
         AND baseline.year = factor.year
         AND baseline.sector_scope = factor.sector_scope
        WHERE factor.{return_column} IS NOT NULL
        GROUP BY
            factor.analysis_scope,
            factor.analysis_scope_label,
            factor.year,
            factor.sector_scope,
            factor.sector_scope_label,
            factor.factor_signal,
            factor.factor_family,
            factor.factor_display_name
        HAVING count(*) >= ?
        ORDER BY
            horizon,
            factor.analysis_scope,
            factor.year,
            factor.factor_signal,
            factor.sector_scope
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_annual_factor_breadth_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_breadth_table(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_annual_factor_breadth_columns())


def _aggregate_factor_breadth_table(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    return conn.execute(
        f"""
        WITH joined AS (
            SELECT
                f.*,
                20 AS breadth_lookback,
                b.breadth_bucket_20d AS breadth_bucket,
                b.breadth_label_20d AS breadth_label,
                b.breadth_up_20d_pct AS breadth_up_pct
            FROM factor_signal_observations f
            JOIN daily_market_breadth_state b
              ON b.market_scope = f.market_scope
             AND b.date = f.date
            UNION ALL
            SELECT
                f.*,
                60 AS breadth_lookback,
                b.breadth_bucket_60d AS breadth_bucket,
                b.breadth_label_60d AS breadth_label,
                b.breadth_up_60d_pct AS breadth_up_pct
            FROM factor_signal_observations f
            JOIN daily_market_breadth_state b
              ON b.market_scope = f.market_scope
             AND b.date = f.date
        )
        SELECT
            {int(horizon)} AS horizon,
            year,
            breadth_lookback,
            breadth_bucket,
            breadth_label,
            factor_signal,
            factor_family,
            factor_display_name,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(breadth_up_pct) AS avg_breadth_up_pct,
            avg({return_column}) AS mean_forward_topix_excess_return_pct,
            median({return_column}) AS median_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_topix_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct
        FROM joined
        WHERE {return_column} IS NOT NULL
        GROUP BY
            year,
            breadth_lookback,
            breadth_bucket,
            breadth_label,
            factor_signal,
            factor_family,
            factor_display_name
        HAVING count(*) >= ?
        ORDER BY horizon, year, breadth_lookback, breadth_bucket, factor_signal
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_factor_resilience_df(annual_factor_breadth_df: pd.DataFrame) -> pd.DataFrame:
    columns = _factor_resilience_columns()
    if annual_factor_breadth_df.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, object]] = []
    group_columns = [
        "horizon",
        "year",
        "breadth_lookback",
        "factor_signal",
        "factor_family",
        "factor_display_name",
    ]
    for key, group in annual_factor_breadth_df.groupby(
        group_columns, dropna=False, sort=True
    ):
        by_bucket = group.set_index("breadth_bucket")
        if (
            "breadth_high_ge_60pct" not in by_bucket.index
            or "breadth_low_lt_30pct" not in by_bucket.index
        ):
            continue
        high = by_bucket.loc["breadth_high_ge_60pct"]
        low = by_bucket.loc["breadth_low_lt_30pct"]
        if isinstance(high, pd.DataFrame):
            high = high.iloc[0]
        if isinstance(low, pd.DataFrame):
            low = low.iloc[0]
        low_median = _coerce_float(low["median_forward_topix_excess_return_pct"])
        high_median = _coerce_float(high["median_forward_topix_excess_return_pct"])
        low_win = _coerce_float(low["win_rate_pct"])
        high_win = _coerce_float(high["win_rate_pct"])
        low_severe = _coerce_float(low["severe_loss_rate_pct"])
        high_severe = _coerce_float(high["severe_loss_rate_pct"])
        rows.append(
            {
                "horizon": key[0],
                "year": key[1],
                "breadth_lookback": key[2],
                "factor_signal": key[3],
                "factor_family": key[4],
                "factor_display_name": key[5],
                "high_breadth_observation_count": int(high["observation_count"]),
                "low_breadth_observation_count": int(low["observation_count"]),
                "high_breadth_median_forward_topix_excess_return_pct": high_median,
                "low_breadth_median_forward_topix_excess_return_pct": low_median,
                "low_minus_high_median_forward_topix_excess_return_pct": (
                    None
                    if low_median is None or high_median is None
                    else low_median - high_median
                ),
                "high_breadth_win_rate_pct": high_win,
                "low_breadth_win_rate_pct": low_win,
                "low_minus_high_win_rate_pct": (
                    None if low_win is None or high_win is None else low_win - high_win
                ),
                "high_breadth_severe_loss_rate_pct": high_severe,
                "low_breadth_severe_loss_rate_pct": low_severe,
                "low_minus_high_severe_loss_rate_pct": (
                    None
                    if low_severe is None or high_severe is None
                    else low_severe - high_severe
                ),
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows)
    return frame.reindex(columns=columns).sort_values(
        [
            "horizon",
            "year",
            "breadth_lookback",
            "low_minus_high_median_forward_topix_excess_return_pct",
        ],
        ascending=[True, True, True, False],
        na_position="last",
    )


def _build_current_term_mapping_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            factor_signal,
            factor_family,
            factor_display_name,
            display_order
        FROM factor_signal_terms
        ORDER BY display_order, factor_signal
        """
    ).fetchdf()


def _build_year_factor_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="factor_signal_observations",
            group_columns=[
                "horizon",
                "year",
                "factor_signal",
                "factor_family",
                "factor_display_name",
            ],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_year_factor_spread_columns())


def _build_core_failure_decomposition_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="core_failure_observations",
            group_columns=[
                "horizon",
                "year",
                "core_slice",
                "factor_signal",
                "factor_family",
                "factor_display_name",
            ],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    frame = _concat_sorted(
        frames,
        columns=[
            "horizon",
            "year",
            "core_slice",
            "factor_signal",
            "factor_family",
            "factor_display_name",
            *_metric_columns(),
        ],
    )
    if frame.empty:
        return pd.DataFrame(columns=_core_failure_decomposition_columns())
    frame["atr_state"] = (
        frame["core_slice"]
        .map(
            {
                "core_atr20_acceleration_ex_overheat": (
                    "atr20_acceleration_ex_overheat"
                ),
                "core_without_atr20_acceleration_ex_overheat": (
                    "not_atr20_acceleration_ex_overheat"
                ),
            }
        )
        .fillna("all_or_mixed_atr_states")
    )
    return frame.reindex(columns=_core_failure_decomposition_columns())


def _build_regime_comparison_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="factor_signal_observations",
            group_columns=[
                "horizon",
                "year_group",
                "factor_signal",
                "factor_family",
                "factor_display_name",
            ],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_regime_comparison_columns())


def _build_sector_year_contribution_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="core_failure_observations",
            group_columns=["horizon", "year", "core_slice", "sector_33_name"],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_year_contribution_columns())


def _aggregate_factor_table(
    conn: Any,
    *,
    table_name: str,
    group_columns: Sequence[str],
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    select_groups = ",\n            ".join(
        f"{horizon} AS horizon" if column == "horizon" else column
        for column in group_columns
    )
    group_by = ", ".join(str(index) for index in range(1, len(group_columns) + 1))
    frame = conn.execute(
        f"""
        SELECT
            {select_groups},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg({return_column}) AS mean_forward_topix_excess_return_pct,
            median({return_column}) AS median_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_topix_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct
        FROM {table_name}
        WHERE {return_column} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    return frame


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            min(date) AS min_date,
            max(date) AS max_date,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(CASE WHEN low_value_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS low_value_rate_pct,
            avg(CASE WHEN momentum_20_60_top20_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS momentum_20_60_top20_rate_pct,
            avg(CASE WHEN overvalued_momentum_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS overvalued_momentum_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_acceleration_ex_overheat_rate_pct
        FROM ranking_factor_regime_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    horizon_exprs = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    return conn.execute(
        f"""
        SELECT
            c.year,
            c.year_group,
            b.breadth_label_20d,
            b.breadth_up_20d_pct,
            n.nt_regime_60d_label,
            n.nt_ratio_return_60d_pct,
            c.core_slice,
            c.factor_signal,
            c.factor_family,
            c.factor_display_name,
            c.atr_state,
            c.date,
            c.code,
            c.company_name,
            c.sector_33_name,
            c.sector_strength_bucket,
            c.momentum_20d_percentile,
            c.momentum_60d_percentile,
            {horizon_exprs}
        FROM core_failure_observations c
        LEFT JOIN daily_market_breadth_state b
          ON b.market_scope = c.market_scope
         AND b.date = c.date
        LEFT JOIN nt_ratio_daily_state n
          ON n.date = c.date
        ORDER BY c.year, c.date, c.core_slice, c.code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    min_sector_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if not {5, 10, 20, 60}.issubset(set(horizons)):
        raise ValueError("horizons must include 5, 10, 20, and 60")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if min_sector_observations < 2:
        raise ValueError("min_sector_observations must be at least 2")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _parse_optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "p90_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    ]


def _year_factor_spread_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        *_metric_columns(),
    ]


def _annual_factor_breadth_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "breadth_lookback",
        "breadth_bucket",
        "breadth_label",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "avg_breadth_up_pct",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "p90_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    ]


def _factor_nt_regime_columns() -> list[str]:
    return [
        "horizon",
        "time_bucket_type",
        "time_bucket",
        "nt_regime_60d",
        "nt_regime_60d_label",
        "nt_regime_60d_order",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "factor_observation_count",
        "factor_code_count",
        "factor_date_count",
        "factor_sector_count",
        "baseline_observation_count",
        "baseline_code_count",
        "baseline_date_count",
        "factor_mean_nt_ratio_return_60d_pct",
        "baseline_mean_nt_ratio_return_60d_pct",
        "factor_mean_forward_topix_excess_return_pct",
        "factor_median_forward_topix_excess_return_pct",
        "baseline_median_forward_topix_excess_return_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
        "factor_p10_forward_topix_excess_return_pct",
        "factor_p90_forward_topix_excess_return_pct",
        "factor_win_rate_pct",
        "baseline_win_rate_pct",
        "factor_minus_baseline_win_rate_pct",
        "factor_severe_loss_rate_pct",
    ]


def _bank_exclusion_columns() -> list[str]:
    return [
        "horizon",
        "analysis_scope",
        "analysis_scope_label",
        "year",
        "sector_scope",
        "sector_scope_label",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "factor_observation_count",
        "factor_code_count",
        "factor_date_count",
        "factor_sector_count",
        "baseline_observation_count",
        "baseline_code_count",
        "baseline_date_count",
        "factor_mean_forward_topix_excess_return_pct",
        "factor_median_forward_topix_excess_return_pct",
        "baseline_median_forward_topix_excess_return_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
        "factor_win_rate_pct",
        "baseline_win_rate_pct",
        "factor_minus_baseline_win_rate_pct",
        "factor_severe_loss_rate_pct",
    ]


def _factor_resilience_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "breadth_lookback",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "high_breadth_observation_count",
        "low_breadth_observation_count",
        "high_breadth_median_forward_topix_excess_return_pct",
        "low_breadth_median_forward_topix_excess_return_pct",
        "low_minus_high_median_forward_topix_excess_return_pct",
        "high_breadth_win_rate_pct",
        "low_breadth_win_rate_pct",
        "low_minus_high_win_rate_pct",
        "high_breadth_severe_loss_rate_pct",
        "low_breadth_severe_loss_rate_pct",
        "low_minus_high_severe_loss_rate_pct",
    ]


def _core_failure_decomposition_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "core_slice",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "atr_state",
        *_metric_columns(),
    ]


def _regime_comparison_columns() -> list[str]:
    return [
        "horizon",
        "year_group",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        *_metric_columns(),
    ]


def _sector_year_contribution_columns() -> list[str]:
    return ["horizon", "year", "core_slice", "sector_33_name", *_metric_columns()]


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str | int | float):
        item = getattr(value, "item", None)
        if not callable(item):
            return None
        value = item()
    if not isinstance(value, str | int | float):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number
