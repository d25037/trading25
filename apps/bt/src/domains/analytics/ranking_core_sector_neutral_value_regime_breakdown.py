"""Annual regime breakdown for sector-neutral value Ranking factors."""

from __future__ import annotations

from dataclasses import dataclass
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
from src.domains.analytics.ranking_core_sector_relative_value_evidence import (
    DEFAULT_MIN_SECTOR_OBSERVATIONS,
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
from src.shared.utils.pandas_type_guards import required_int, required_str

RANKING_CORE_SECTOR_NEUTRAL_VALUE_REGIME_BREAKDOWN_EXPERIMENT_ID = (
    "market-behavior/ranking-core-sector-neutral-value-regime-breakdown"
)


@dataclass(frozen=True)
class RankingCoreSectorNeutralValueRegimeBreakdownResult:
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
    annual_strategy_summary_df: pd.DataFrame
    bank_displacement_df: pd.DataFrame
    sector_breadth_df: pd.DataFrame
    sector_year_contribution_df: pd.DataFrame
    strategy_breadth_regime_df: pd.DataFrame
    nt_regime_strategy_df: pd.DataFrame
    strategy_comparison_df: pd.DataFrame
    current_term_mapping_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_core_sector_neutral_value_regime_breakdown_research(
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
) -> RankingCoreSectorNeutralValueRegimeBreakdownResult:
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
        snapshot_prefix="ranking-core-sector-neutral-value-regime-breakdown-",
    ) as ctx:
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="core_sector_neutral_value",
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
                "sector-neutral value research requires liquidity-ranked signals"
            )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="core_sector_neutral_value_sector",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(sector_features,),
            namespace="core_sector_neutral_value",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="core_sector_neutral_value_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="core_sector_neutral_value_outcomes",
        )
        _create_nt_ratio_regime_tables(ctx.connection)
        _create_sector_neutral_value_tables(
            ctx.connection,
            source_name=evaluated.name,
            horizons=resolved_horizons,
            min_sector_observations=min_sector_observations,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM sector_neutral_value_signal_observations"
            ).fetchone()[0]
        )
        annual_strategy_summary_df = _build_annual_strategy_summary_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        result = RankingCoreSectorNeutralValueRegimeBreakdownResult(
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
            annual_strategy_summary_df=annual_strategy_summary_df,
            bank_displacement_df=_build_bank_displacement_df(
                annual_strategy_summary_df
            ),
            sector_breadth_df=_build_sector_breadth_df(
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
            strategy_breadth_regime_df=_build_strategy_breadth_regime_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            nt_regime_strategy_df=_build_nt_regime_strategy_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            strategy_comparison_df=_build_strategy_comparison_df(
                annual_strategy_summary_df
            ),
            current_term_mapping_df=_build_current_term_mapping_df(ctx.connection),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
                horizons=resolved_horizons,
            ),
        )
    return result


def write_ranking_core_sector_neutral_value_regime_breakdown_bundle(
    result: RankingCoreSectorNeutralValueRegimeBreakdownResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_CORE_SECTOR_NEUTRAL_VALUE_REGIME_BREAKDOWN_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_core_sector_neutral_value_regime_breakdown",
        function="run_ranking_core_sector_neutral_value_regime_breakdown_research",
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
            "annual_strategy_summary_df": result.annual_strategy_summary_df,
            "bank_displacement_df": result.bank_displacement_df,
            "sector_breadth_df": result.sector_breadth_df,
            "sector_year_contribution_df": result.sector_year_contribution_df,
            "strategy_breadth_regime_df": result.strategy_breadth_regime_df,
            "nt_regime_strategy_df": result.nt_regime_strategy_df,
            "strategy_comparison_df": result.strategy_comparison_df,
            "current_term_mapping_df": result.current_term_mapping_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingCoreSectorNeutralValueRegimeBreakdownResult,
) -> str:
    parts = [
        "# Ranking Core Sector-Neutral Value Regime Breakdown",
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
        f"- min_sector_observations: `{result.min_sector_observations}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=40),
        "",
        "## Annual Strategy Summary",
        "",
        _top_rows_for_markdown(result.annual_strategy_summary_df, limit=320),
        "",
        "## Bank Displacement",
        "",
        _top_rows_for_markdown(result.bank_displacement_df, limit=260),
        "",
        "## Sector Breadth",
        "",
        _top_rows_for_markdown(result.sector_breadth_df, limit=260),
        "",
        "## Sector Year Contribution",
        "",
        _top_rows_for_markdown(result.sector_year_contribution_df, limit=260),
        "",
        "## Strategy x Breadth Regime",
        "",
        _top_rows_for_markdown(result.strategy_breadth_regime_df, limit=260),
        "",
        "## Strategy x NT 60D Regime",
        "",
        _top_rows_for_markdown(result.nt_regime_strategy_df, limit=260),
        "",
        "## Strategy Comparison",
        "",
        _top_rows_for_markdown(result.strategy_comparison_df, limit=220),
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


def _create_sector_neutral_value_tables(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_sector_observations: int,
) -> None:
    return_columns = ",\n                ".join(
        f"r.forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE sector_neutral_value_terms (
            factor_signal TEXT,
            factor_family TEXT,
            factor_display_name TEXT,
            display_order INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO sector_neutral_value_terms VALUES (?, ?, ?, ?)",
        [
            ("raw_undervalued", "Valuation Signal", "Undervalued", 10),
            (
                "sector_neutral_undervalued",
                "Valuation Signal",
                "Sector-Neutral Undervalued",
                20,
            ),
            (
                "raw_undervalued_sector_strong",
                "Valuation Signal",
                "Undervalued + Balanced Sector Strength: Strong",
                30,
            ),
            (
                "sector_neutral_undervalued_sector_strong",
                "Valuation Signal",
                "Sector-Neutral Undervalued + Balanced Sector Strength: Strong",
                40,
            ),
            ("raw_momentum_value", "Combined", "Momentum Value", 50),
            (
                "sector_neutral_momentum_value",
                "Combined",
                "Sector-Neutral Momentum Value",
                60,
            ),
            (
                "raw_momentum_value_sector_strong",
                "Combined",
                "Momentum Value + Balanced Sector Strength: Strong",
                70,
            ),
            (
                "sector_neutral_momentum_value_sector_strong",
                "Combined",
                "Sector-Neutral Momentum Value + Balanced Sector Strength: Strong",
                80,
            ),
            (
                "hybrid_momentum_value_sector_strong",
                "Combined",
                "Hybrid Momentum Value + Balanced Sector Strength: Strong",
                90,
            ),
            (
                "raw_and_sector_neutral_momentum_value_sector_strong",
                "Combined",
                "Raw + Sector-Neutral Momentum Value + Balanced Sector Strength: Strong",
                100,
            ),
            (
                "sector_neutral_only_momentum_value_sector_strong",
                "Combined",
                "Sector-Neutral Only Momentum Value + Balanced Sector Strength: Strong",
                110,
            ),
        ],
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE sector_neutral_value_base_panel AS
        WITH sector_universe AS (
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
                r.sector_33_code,
                r.sector_33_name,
                coalesce(r.sector_strength_bucket, 'sector_unknown')
                    AS sector_strength_bucket,
                r.sector_strength_score,
                r.liquidity_regime,
                r.recent_return_20d_pct,
                r.recent_return_60d_pct,
                r.pbr,
                r.forecast_per AS forward_per,
                r.pbr_percentile AS raw_pbr_percentile,
                r.forecast_per_percentile AS raw_forward_per_percentile,
                r.per_percentile,
                r.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
                {return_columns},
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
        ),
        sector_scored AS (
            SELECT
                *,
                CASE
                    WHEN pbr > 0
                     AND sector_pbr_valid_count >= ?
                        THEN (sector_pbr_rank - 1.0) / (sector_pbr_valid_count - 1.0)
                END AS sector_pbr_percentile,
                CASE
                    WHEN forward_per > 0
                     AND sector_forward_per_valid_count >= ?
                        THEN (sector_forward_per_rank - 1.0)
                            / (sector_forward_per_valid_count - 1.0)
                END AS sector_forward_per_percentile
            FROM sector_universe
        ),
        hybrid_scored AS (
            SELECT
                *,
                CASE
                    WHEN raw_pbr_percentile IS NOT NULL
                     AND raw_forward_per_percentile IS NOT NULL
                     AND sector_pbr_percentile IS NOT NULL
                     AND sector_forward_per_percentile IS NOT NULL
                        THEN (
                            raw_pbr_percentile
                            + raw_forward_per_percentile
                            + sector_pbr_percentile
                            + sector_forward_per_percentile
                        ) / 4.0
                END AS hybrid_value_score
            FROM sector_scored
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
                ) AS momentum_60d_percentile,
                CASE
                    WHEN hybrid_value_score IS NOT NULL
                        THEN percent_rank() OVER (
                            PARTITION BY market_scope, date
                            ORDER BY hybrid_value_score NULLS LAST
                        )
                END AS hybrid_value_percentile
            FROM hybrid_scored
        )
        SELECT
            *,
            raw_pbr_percentile <= 0.2
                AND raw_forward_per_percentile <= 0.2 AS raw_undervalued_flag,
            sector_pbr_percentile <= 0.2
                AND sector_forward_per_percentile <= 0.2
                AS sector_neutral_undervalued_flag,
            hybrid_value_percentile <= 0.2 AS hybrid_undervalued_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS momentum_20_60_top20_flag,
            sector_strength_bucket = 'sector_strong' AS sector_strong_flag,
            sector_33_name = '銀行業' AS bank_sector_flag
        FROM ranked
        """,
        [int(min_sector_observations), int(min_sector_observations)],
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE sector_neutral_value_signal_observations_raw AS
        SELECT 'raw_undervalued' AS factor_signal, * FROM sector_neutral_value_base_panel
        WHERE raw_undervalued_flag
        UNION ALL
        SELECT 'sector_neutral_undervalued' AS factor_signal, * FROM sector_neutral_value_base_panel
        WHERE sector_neutral_undervalued_flag
        UNION ALL
        SELECT 'raw_undervalued_sector_strong' AS factor_signal, * FROM sector_neutral_value_base_panel
        WHERE raw_undervalued_flag AND sector_strong_flag
        UNION ALL
        SELECT 'sector_neutral_undervalued_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE sector_neutral_undervalued_flag AND sector_strong_flag
        UNION ALL
        SELECT 'raw_momentum_value' AS factor_signal, * FROM sector_neutral_value_base_panel
        WHERE raw_undervalued_flag AND momentum_20_60_top20_flag
        UNION ALL
        SELECT 'sector_neutral_momentum_value' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE sector_neutral_undervalued_flag AND momentum_20_60_top20_flag
        UNION ALL
        SELECT 'raw_momentum_value_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE raw_undervalued_flag AND momentum_20_60_top20_flag AND sector_strong_flag
        UNION ALL
        SELECT 'sector_neutral_momentum_value_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE sector_neutral_undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strong_flag
        UNION ALL
        SELECT 'hybrid_momentum_value_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE hybrid_undervalued_flag AND momentum_20_60_top20_flag AND sector_strong_flag
        UNION ALL
        SELECT 'raw_and_sector_neutral_momentum_value_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE raw_undervalued_flag
          AND sector_neutral_undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strong_flag
        UNION ALL
        SELECT 'sector_neutral_only_momentum_value_sector_strong' AS factor_signal, *
        FROM sector_neutral_value_base_panel
        WHERE NOT raw_undervalued_flag
          AND sector_neutral_undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strong_flag
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE sector_neutral_value_signal_observations AS
        SELECT
            raw.*,
            coalesce(terms.factor_family, 'Other') AS factor_family,
            coalesce(terms.factor_display_name, raw.factor_signal) AS factor_display_name,
            coalesce(terms.display_order, 999) AS factor_display_order
        FROM sector_neutral_value_signal_observations_raw raw
        LEFT JOIN sector_neutral_value_terms terms USING (factor_signal)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE sector_neutral_value_daily_breadth_state AS
        WITH daily AS (
            SELECT
                market_scope,
                date,
                year,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                avg(CASE WHEN recent_return_20d_pct > 0 THEN 1.0 ELSE 0.0 END)
                    * 100.0 AS breadth_up_20d_pct,
                avg(CASE WHEN recent_return_60d_pct > 0 THEN 1.0 ELSE 0.0 END)
                    * 100.0 AS breadth_up_60d_pct
            FROM sector_neutral_value_base_panel
            GROUP BY market_scope, date, year
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


def _build_annual_strategy_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_annual_strategy_summary(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_annual_strategy_summary_columns())


def _aggregate_annual_strategy_summary(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    return conn.execute(
        f"""
        WITH base_scoped AS (
            SELECT 'all' AS sector_scope, 'All sectors' AS sector_scope_label, *
            FROM sector_neutral_value_base_panel
            UNION ALL
            SELECT 'ex_banks' AS sector_scope, 'ex Banks' AS sector_scope_label, *
            FROM sector_neutral_value_base_panel
            WHERE NOT bank_sector_flag
            UNION ALL
            SELECT 'banks_only' AS sector_scope, 'Banks only' AS sector_scope_label, *
            FROM sector_neutral_value_base_panel
            WHERE bank_sector_flag
        ),
        factor_scoped AS (
            SELECT 'all' AS sector_scope, 'All sectors' AS sector_scope_label, *
            FROM sector_neutral_value_signal_observations
            UNION ALL
            SELECT 'ex_banks' AS sector_scope, 'ex Banks' AS sector_scope_label, *
            FROM sector_neutral_value_signal_observations
            WHERE NOT bank_sector_flag
            UNION ALL
            SELECT 'banks_only' AS sector_scope, 'Banks only' AS sector_scope_label, *
            FROM sector_neutral_value_signal_observations
            WHERE bank_sector_flag
        ),
        baseline AS (
            SELECT
                {int(horizon)} AS horizon,
                market_scope,
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
            FROM base_scoped
            WHERE {return_column} IS NOT NULL
            GROUP BY market_scope, year, sector_scope, sector_scope_label
        ),
        factor AS (
            SELECT
                {int(horizon)} AS horizon,
                market_scope,
                year,
                sector_scope,
                sector_scope_label,
                factor_signal,
                factor_family,
                factor_display_name,
                any_value(factor_display_order) AS factor_display_order,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                count(DISTINCT date) AS date_count,
                count(DISTINCT sector_33_name) AS sector_count,
                avg({return_column}) AS mean_forward_topix_excess_return_pct,
                median({return_column}) AS median_forward_topix_excess_return_pct,
                quantile_cont({return_column}, 0.10)
                    AS p10_forward_topix_excess_return_pct,
                quantile_cont({return_column}, 0.90)
                    AS p90_forward_topix_excess_return_pct,
                avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS win_rate_pct,
                avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                    AS severe_loss_rate_pct,
                avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                    AS bank_observation_share_pct,
                median(raw_pbr_percentile) AS median_raw_pbr_percentile,
                median(raw_forward_per_percentile) AS median_raw_forward_per_percentile,
                median(sector_pbr_percentile) AS median_sector_pbr_percentile,
                median(sector_forward_per_percentile)
                    AS median_sector_forward_per_percentile,
                median(hybrid_value_percentile) AS median_hybrid_value_percentile,
                median(sector_strength_score) AS median_sector_strength_score
            FROM factor_scoped
            WHERE {return_column} IS NOT NULL
            GROUP BY
                market_scope,
                year,
                sector_scope,
                sector_scope_label,
                factor_signal,
                factor_family,
                factor_display_name
            HAVING count(*) >= ?
        )
        SELECT
            factor.horizon,
            factor.market_scope,
            factor.year,
            factor.sector_scope,
            factor.sector_scope_label,
            factor.factor_signal,
            factor.factor_family,
            factor.factor_display_name,
            factor.observation_count,
            factor.code_count,
            factor.date_count,
            factor.sector_count,
            baseline.baseline_observation_count,
            baseline.baseline_code_count,
            factor.mean_forward_topix_excess_return_pct,
            factor.median_forward_topix_excess_return_pct,
            factor.p10_forward_topix_excess_return_pct,
            factor.p90_forward_topix_excess_return_pct,
            factor.win_rate_pct,
            factor.severe_loss_rate_pct,
            factor.bank_observation_share_pct,
            baseline.baseline_median_forward_topix_excess_return_pct,
            baseline.baseline_win_rate_pct,
            factor.median_forward_topix_excess_return_pct
                - baseline.baseline_median_forward_topix_excess_return_pct
                AS factor_minus_baseline_median_forward_topix_excess_return_pct,
            factor.median_raw_pbr_percentile,
            factor.median_raw_forward_per_percentile,
            factor.median_sector_pbr_percentile,
            factor.median_sector_forward_per_percentile,
            factor.median_hybrid_value_percentile,
            factor.median_sector_strength_score,
            factor.factor_display_order
        FROM factor
        JOIN baseline
          ON baseline.horizon = factor.horizon
         AND baseline.market_scope = factor.market_scope
         AND baseline.year = factor.year
         AND baseline.sector_scope = factor.sector_scope
        ORDER BY
            factor.horizon,
            factor.market_scope,
            factor.year,
            factor.sector_scope,
            factor.factor_display_order
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_bank_displacement_df(annual_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "horizon",
        "market_scope",
        "year",
        "factor_signal",
        "factor_display_name",
        "all_observation_count",
        "bank_observation_share_pct",
        "all_median_forward_topix_excess_return_pct",
        "ex_banks_median_forward_topix_excess_return_pct",
        "banks_only_median_forward_topix_excess_return_pct",
        "ex_banks_minus_all_median_forward_topix_excess_return_pct",
        "banks_only_minus_all_median_forward_topix_excess_return_pct",
        "all_factor_minus_baseline_median_forward_topix_excess_return_pct",
        "ex_banks_factor_minus_baseline_median_forward_topix_excess_return_pct",
        "banks_only_factor_minus_baseline_median_forward_topix_excess_return_pct",
    ]
    if annual_df.empty:
        return pd.DataFrame(columns=columns)
    key_cols = ["horizon", "market_scope", "year", "factor_signal"]
    value_cols = [
        "factor_display_name",
        "observation_count",
        "bank_observation_share_pct",
        "median_forward_topix_excess_return_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
    ]
    scoped = annual_df[key_cols + ["sector_scope", *value_cols]].copy()
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(key_cols, sort=False):
        by_scope = {
            str(row["sector_scope"]): row for row in group.to_dict(orient="records")
        }
        all_row = by_scope.get("all")
        ex_row = by_scope.get("ex_banks")
        bank_row = by_scope.get("banks_only")
        if all_row is None:
            continue
        horizon, market_scope, year, factor_signal = keys
        all_median = _to_float(all_row.get("median_forward_topix_excess_return_pct"))
        ex_median = _to_float(
            ex_row.get("median_forward_topix_excess_return_pct")
            if ex_row is not None
            else None
        )
        bank_median = _to_float(
            bank_row.get("median_forward_topix_excess_return_pct")
            if bank_row is not None
            else None
        )
        records.append(
            {
                "horizon": required_int(horizon, field="horizon"),
                "market_scope": required_str(market_scope, field="market_scope"),
                "year": required_str(year, field="year"),
                "factor_signal": required_str(factor_signal, field="factor_signal"),
                "factor_display_name": str(all_row["factor_display_name"]),
                "all_observation_count": int(all_row["observation_count"]),
                "bank_observation_share_pct": _to_float(
                    all_row.get("bank_observation_share_pct")
                ),
                "all_median_forward_topix_excess_return_pct": all_median,
                "ex_banks_median_forward_topix_excess_return_pct": ex_median,
                "banks_only_median_forward_topix_excess_return_pct": bank_median,
                "ex_banks_minus_all_median_forward_topix_excess_return_pct": (
                    ex_median - all_median
                    if ex_median is not None and all_median is not None
                    else None
                ),
                "banks_only_minus_all_median_forward_topix_excess_return_pct": (
                    bank_median - all_median
                    if bank_median is not None and all_median is not None
                    else None
                ),
                "all_factor_minus_baseline_median_forward_topix_excess_return_pct": _to_float(
                    all_row.get(
                        "factor_minus_baseline_median_forward_topix_excess_return_pct"
                    )
                ),
                "ex_banks_factor_minus_baseline_median_forward_topix_excess_return_pct": _to_float(
                    ex_row.get(
                        "factor_minus_baseline_median_forward_topix_excess_return_pct"
                    )
                    if ex_row is not None
                    else None
                ),
                "banks_only_factor_minus_baseline_median_forward_topix_excess_return_pct": _to_float(
                    bank_row.get(
                        "factor_minus_baseline_median_forward_topix_excess_return_pct"
                    )
                    if bank_row is not None
                    else None
                ),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _build_sector_breadth_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            WITH sector_rows AS (
                SELECT
                    {int(horizon)} AS horizon,
                    market_scope,
                    year,
                    factor_signal,
                    factor_family,
                    factor_display_name,
                    any_value(factor_display_order) AS factor_display_order,
                    sector_33_name,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct
                FROM sector_neutral_value_signal_observations
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY
                    market_scope,
                    year,
                    factor_signal,
                    factor_family,
                    factor_display_name,
                    sector_33_name
                HAVING count(*) >= ?
            ),
            totals AS (
                SELECT
                    horizon,
                    market_scope,
                    year,
                    factor_signal,
                    sum(observation_count) AS total_observation_count
                FROM sector_rows
                GROUP BY horizon, market_scope, year, factor_signal
            )
            SELECT
                s.horizon,
                s.market_scope,
                s.year,
                s.factor_signal,
                s.factor_family,
                s.factor_display_name,
                sum(s.observation_count) AS observation_count,
                count(*) AS sector_count_with_min_obs,
                sum(CASE WHEN s.median_forward_topix_excess_return_pct > 0
                    THEN 1 ELSE 0 END) AS positive_median_sector_count,
                100.0 * sum(CASE WHEN s.median_forward_topix_excess_return_pct > 0
                    THEN 1 ELSE 0 END) / nullif(count(*), 0)
                    AS positive_median_sector_share_pct,
                max(100.0 * s.observation_count / nullif(t.total_observation_count, 0))
                    AS max_sector_observation_share_pct,
                sum(CASE WHEN s.sector_33_name = '銀行業'
                    THEN s.observation_count ELSE 0 END)
                    AS bank_observation_count,
                100.0 * sum(CASE WHEN s.sector_33_name = '銀行業'
                    THEN s.observation_count ELSE 0 END)
                    / nullif(sum(s.observation_count), 0)
                    AS bank_observation_share_pct,
                median(s.median_forward_topix_excess_return_pct)
                    AS median_of_sector_medians_forward_topix_excess_return_pct,
                avg(s.mean_forward_topix_excess_return_pct)
                    AS mean_of_sector_means_forward_topix_excess_return_pct,
                avg(s.severe_loss_rate_pct) AS mean_sector_severe_loss_rate_pct,
                any_value(s.factor_display_order) AS factor_display_order
            FROM sector_rows s
            JOIN totals t
              ON t.horizon = s.horizon
             AND t.market_scope = s.market_scope
             AND t.year = s.year
             AND t.factor_signal = s.factor_signal
            GROUP BY
                s.horizon,
                s.market_scope,
                s.year,
                s.factor_signal,
                s.factor_family,
                s.factor_display_name
            ORDER BY
                s.horizon,
                s.market_scope,
                s.year,
                factor_display_order
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_breadth_columns())


def _build_sector_year_contribution_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            WITH sector_rows AS (
                SELECT
                    {int(horizon)} AS horizon,
                    market_scope,
                    year,
                    factor_signal,
                    factor_family,
                    factor_display_name,
                    any_value(factor_display_order) AS factor_display_order,
                    sector_33_name,
                    sector_33_name = '銀行業' AS bank_sector_flag,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct
                FROM sector_neutral_value_signal_observations
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY
                    market_scope,
                    year,
                    factor_signal,
                    factor_family,
                    factor_display_name,
                    sector_33_name
                HAVING count(*) >= ?
            ),
            totals AS (
                SELECT
                    horizon,
                    market_scope,
                    year,
                    factor_signal,
                    sum(observation_count) AS total_observation_count
                FROM sector_rows
                GROUP BY horizon, market_scope, year, factor_signal
            )
            SELECT
                s.*,
                100.0 * s.observation_count / nullif(t.total_observation_count, 0)
                    AS sector_observation_share_pct,
                (
                    s.observation_count / nullif(t.total_observation_count, 0)
                ) * s.mean_forward_topix_excess_return_pct
                    AS weighted_mean_contribution_pct
            FROM sector_rows s
            JOIN totals t
              ON t.horizon = s.horizon
             AND t.market_scope = s.market_scope
             AND t.year = s.year
             AND t.factor_signal = s.factor_signal
            ORDER BY
                s.horizon,
                s.market_scope,
                s.year,
                s.factor_display_order,
                sector_observation_share_pct DESC,
                s.sector_33_name
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_year_contribution_columns())


def _build_strategy_breadth_regime_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = []
    for horizon in horizons:
        horizon_int = int(horizon)
        breadth_bucket = (
            "b.breadth_bucket_60d" if horizon_int >= 60 else "b.breadth_bucket_20d"
        )
        breadth_label = (
            "b.breadth_label_60d" if horizon_int >= 60 else "b.breadth_label_20d"
        )
        breadth_up = (
            "b.breadth_up_60d_pct" if horizon_int >= 60 else "b.breadth_up_20d_pct"
        )
        frames.append(
            conn.execute(
                f"""
                SELECT
                    {horizon_int} AS horizon,
                    f.market_scope,
                    f.year,
                    {breadth_bucket} AS breadth_bucket,
                    {breadth_label} AS breadth_label,
                    f.factor_signal,
                    f.factor_family,
                    f.factor_display_name,
                    count(*) AS observation_count,
                    count(DISTINCT f.code) AS code_count,
                    count(DISTINCT f.date) AS date_count,
                    count(DISTINCT f.sector_33_name) AS sector_count,
                    median({breadth_up}) AS median_breadth_up_pct,
                    avg(CASE WHEN f.bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS bank_observation_share_pct,
                    avg(f.forward_close_excess_return_{horizon_int}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(f.forward_close_excess_return_{horizon_int}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN f.forward_close_excess_return_{horizon_int}d_pct > 0
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS win_rate_pct,
                    avg(CASE
                        WHEN f.forward_close_excess_return_{horizon_int}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct,
                    any_value(f.factor_display_order) AS factor_display_order
                FROM sector_neutral_value_signal_observations f
                JOIN sector_neutral_value_daily_breadth_state b
                  ON b.market_scope = f.market_scope
                 AND b.date = f.date
                WHERE f.forward_close_excess_return_{horizon_int}d_pct IS NOT NULL
                GROUP BY
                    f.market_scope,
                    f.year,
                    {breadth_bucket},
                    {breadth_label},
                    f.factor_signal,
                    f.factor_family,
                    f.factor_display_name
                HAVING count(*) >= ?
                ORDER BY
                    horizon,
                    market_scope,
                    year,
                    breadth_bucket,
                    factor_display_order
                """,
                [float(severe_loss_threshold_pct), int(min_observations)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_strategy_breadth_regime_columns())


def _build_nt_regime_strategy_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = []
    for horizon in horizons:
        horizon_int = int(horizon)
        frames.append(
            conn.execute(
                f"""
                WITH time_buckets AS (
                    SELECT
                        f.market_scope,
                        f.date,
                        'year' AS time_bucket_type,
                        f.year AS time_bucket,
                        n.nt_regime_60d,
                        n.nt_regime_60d_label,
                        n.nt_regime_60d_order,
                        n.nt_ratio_return_60d_pct
                    FROM sector_neutral_value_signal_observations f
                    JOIN nt_ratio_daily_state n
                      ON n.date = f.date
                    WHERE n.nt_regime_60d <> 'unknown'
                    GROUP BY
                        f.market_scope,
                        f.date,
                        f.year,
                        n.nt_regime_60d,
                        n.nt_regime_60d_label,
                        n.nt_regime_60d_order,
                        n.nt_ratio_return_60d_pct
                    UNION ALL
                    SELECT
                        f.market_scope,
                        f.date,
                        'period' AS time_bucket_type,
                        n.nt_period AS time_bucket,
                        n.nt_regime_60d,
                        n.nt_regime_60d_label,
                        n.nt_regime_60d_order,
                        n.nt_ratio_return_60d_pct
                    FROM sector_neutral_value_signal_observations f
                    JOIN nt_ratio_daily_state n
                      ON n.date = f.date
                    WHERE n.nt_regime_60d <> 'unknown'
                    GROUP BY
                        f.market_scope,
                        f.date,
                        n.nt_period,
                        n.nt_regime_60d,
                        n.nt_regime_60d_label,
                        n.nt_regime_60d_order,
                        n.nt_ratio_return_60d_pct
                )
                SELECT
                    {horizon_int} AS horizon,
                    f.market_scope,
                    b.time_bucket_type,
                    b.time_bucket,
                    b.nt_regime_60d,
                    b.nt_regime_60d_label,
                    any_value(b.nt_regime_60d_order) AS nt_regime_60d_order,
                    f.factor_signal,
                    f.factor_family,
                    f.factor_display_name,
                    count(*) AS observation_count,
                    count(DISTINCT f.code) AS code_count,
                    count(DISTINCT f.date) AS date_count,
                    count(DISTINCT f.sector_33_name) AS sector_count,
                    avg(b.nt_ratio_return_60d_pct)
                        AS mean_nt_ratio_return_60d_pct,
                    avg(CASE WHEN f.bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS bank_observation_share_pct,
                    avg(f.forward_close_excess_return_{horizon_int}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(f.forward_close_excess_return_{horizon_int}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN f.forward_close_excess_return_{horizon_int}d_pct > 0
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS win_rate_pct,
                    avg(CASE
                        WHEN f.forward_close_excess_return_{horizon_int}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct,
                    any_value(f.factor_display_order) AS factor_display_order
                FROM sector_neutral_value_signal_observations f
                JOIN time_buckets b
                  ON b.market_scope = f.market_scope
                 AND b.date = f.date
                WHERE f.forward_close_excess_return_{horizon_int}d_pct IS NOT NULL
                GROUP BY
                    f.market_scope,
                    b.time_bucket_type,
                    b.time_bucket,
                    b.nt_regime_60d,
                    b.nt_regime_60d_label,
                    f.factor_signal,
                    f.factor_family,
                    f.factor_display_name
                HAVING count(*) >= ?
                ORDER BY
                    horizon,
                    market_scope,
                    time_bucket_type,
                    time_bucket,
                    nt_regime_60d_order,
                    factor_display_order
                """,
                [float(severe_loss_threshold_pct), int(min_observations)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_nt_regime_strategy_columns())


def _build_strategy_comparison_df(annual_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "horizon",
        "market_scope",
        "year",
        "sector_scope",
        "sector_scope_label",
        "comparison",
        "left_factor_signal",
        "left_factor_display_name",
        "right_factor_signal",
        "right_factor_display_name",
        "left_observation_count",
        "right_observation_count",
        "left_median_forward_topix_excess_return_pct",
        "right_median_forward_topix_excess_return_pct",
        "left_minus_right_median_forward_topix_excess_return_pct",
        "left_bank_observation_share_pct",
        "right_bank_observation_share_pct",
    ]
    if annual_df.empty:
        return pd.DataFrame(columns=columns)
    pairs = [
        (
            "sector_neutral_vs_raw_momentum_value_sector_strong",
            "sector_neutral_momentum_value_sector_strong",
            "raw_momentum_value_sector_strong",
        ),
        (
            "sector_neutral_vs_raw_momentum_value",
            "sector_neutral_momentum_value",
            "raw_momentum_value",
        ),
        (
            "sector_neutral_vs_raw_undervalued",
            "sector_neutral_undervalued",
            "raw_undervalued",
        ),
        (
            "hybrid_vs_raw_momentum_value_sector_strong",
            "hybrid_momentum_value_sector_strong",
            "raw_momentum_value_sector_strong",
        ),
    ]
    index_cols = ["horizon", "market_scope", "year", "sector_scope"]
    by_key = {
        (
            int(row["horizon"]),
            str(row["market_scope"]),
            str(row["year"]),
            str(row["sector_scope"]),
            str(row["factor_signal"]),
        ): row
        for row in annual_df.to_dict(orient="records")
    }
    scope_labels = annual_df.set_index(index_cols)["sector_scope_label"].to_dict()
    records: list[dict[str, Any]] = []
    base_keys = annual_df[index_cols].drop_duplicates().to_dict(orient="records")
    for key in base_keys:
        key_tuple = (
            int(key["horizon"]),
            str(key["market_scope"]),
            str(key["year"]),
            str(key["sector_scope"]),
        )
        for comparison, left_signal, right_signal in pairs:
            left = by_key.get((*key_tuple, left_signal))
            right = by_key.get((*key_tuple, right_signal))
            if left is None or right is None:
                continue
            left_median = _to_float(left.get("median_forward_topix_excess_return_pct"))
            right_median = _to_float(
                right.get("median_forward_topix_excess_return_pct")
            )
            records.append(
                {
                    "horizon": key_tuple[0],
                    "market_scope": key_tuple[1],
                    "year": key_tuple[2],
                    "sector_scope": key_tuple[3],
                    "sector_scope_label": str(scope_labels.get(key_tuple, "")),
                    "comparison": comparison,
                    "left_factor_signal": left_signal,
                    "left_factor_display_name": str(left["factor_display_name"]),
                    "right_factor_signal": right_signal,
                    "right_factor_display_name": str(right["factor_display_name"]),
                    "left_observation_count": int(left["observation_count"]),
                    "right_observation_count": int(right["observation_count"]),
                    "left_median_forward_topix_excess_return_pct": left_median,
                    "right_median_forward_topix_excess_return_pct": right_median,
                    "left_minus_right_median_forward_topix_excess_return_pct": (
                        left_median - right_median
                        if left_median is not None and right_median is not None
                        else None
                    ),
                    "left_bank_observation_share_pct": _to_float(
                        left.get("bank_observation_share_pct")
                    ),
                    "right_bank_observation_share_pct": _to_float(
                        right.get("bank_observation_share_pct")
                    ),
                }
            )
    return pd.DataFrame(records, columns=columns)


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(CASE WHEN raw_pbr_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS raw_pbr_coverage_pct,
            avg(CASE WHEN raw_forward_per_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS raw_forward_per_coverage_pct,
            avg(CASE WHEN sector_pbr_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS sector_pbr_coverage_pct,
            avg(CASE WHEN sector_forward_per_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS sector_forward_per_coverage_pct,
            avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS bank_observation_share_pct,
            min(date) AS first_date,
            max(date) AS last_date
        FROM sector_neutral_value_base_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_current_term_mapping_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            factor_signal,
            factor_family,
            factor_display_name,
            display_order,
            CASE
                WHEN factor_signal LIKE 'sector_neutral_%'
                    THEN 'Same-date, same-market, same-sector PBR and Forward PER percentiles <= 20%.'
                WHEN factor_signal LIKE 'hybrid_%'
                    THEN 'Average raw and same-sector valuation percentiles, re-ranked by date.'
                WHEN factor_signal LIKE 'raw_%'
                    THEN 'Same-date, same-market raw PBR and Forward PER percentiles <= 20%.'
                ELSE 'Ranking factor variant.'
            END AS definition
        FROM sector_neutral_value_terms
        ORDER BY display_order
        """
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    return_columns = ", ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    return conn.execute(
        f"""
        SELECT
            market_scope,
            year,
            date,
            code,
            company_name,
            sector_33_name,
            factor_signal,
            factor_display_name,
            raw_pbr_percentile,
            raw_forward_per_percentile,
            sector_pbr_percentile,
            sector_forward_per_percentile,
            hybrid_value_percentile,
            momentum_20d_percentile,
            momentum_60d_percentile,
            sector_strength_bucket,
            sector_strength_score,
            bank_sector_flag,
            {return_columns}
        FROM sector_neutral_value_signal_observations
        ORDER BY date, factor_display_order, code
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
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if int(min_observations) < 1:
        raise ValueError("min_observations must be >= 1")
    if int(min_sector_observations) < 2:
        raise ValueError("min_sector_observations must be >= 2")
    if float(severe_loss_threshold_pct) >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if int(observation_sample_limit) < 0:
        raise ValueError("observation_sample_limit must be >= 0")


def _parse_optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    result = pd.concat(non_empty, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[list(columns)]


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _annual_strategy_summary_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "sector_scope",
        "sector_scope_label",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "baseline_observation_count",
        "baseline_code_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "p90_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "bank_observation_share_pct",
        "baseline_median_forward_topix_excess_return_pct",
        "baseline_win_rate_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
        "median_raw_pbr_percentile",
        "median_raw_forward_per_percentile",
        "median_sector_pbr_percentile",
        "median_sector_forward_per_percentile",
        "median_hybrid_value_percentile",
        "median_sector_strength_score",
        "factor_display_order",
    )


def _sector_breadth_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "observation_count",
        "sector_count_with_min_obs",
        "positive_median_sector_count",
        "positive_median_sector_share_pct",
        "max_sector_observation_share_pct",
        "bank_observation_count",
        "bank_observation_share_pct",
        "median_of_sector_medians_forward_topix_excess_return_pct",
        "mean_of_sector_means_forward_topix_excess_return_pct",
        "mean_sector_severe_loss_rate_pct",
        "factor_display_order",
    )


def _sector_year_contribution_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "factor_display_order",
        "sector_33_name",
        "bank_sector_flag",
        "observation_count",
        "code_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "sector_observation_share_pct",
        "weighted_mean_contribution_pct",
    )


def _strategy_breadth_regime_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "breadth_bucket",
        "breadth_label",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "median_breadth_up_pct",
        "bank_observation_share_pct",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "factor_display_order",
    )


def _nt_regime_strategy_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "time_bucket_type",
        "time_bucket",
        "nt_regime_60d",
        "nt_regime_60d_label",
        "nt_regime_60d_order",
        "factor_signal",
        "factor_family",
        "factor_display_name",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_nt_ratio_return_60d_pct",
        "bank_observation_share_pct",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "factor_display_order",
    )
