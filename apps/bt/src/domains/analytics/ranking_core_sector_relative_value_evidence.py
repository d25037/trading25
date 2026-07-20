"""Sector-relative valuation evidence for the neutral-rerating core sleeve."""

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

RANKING_CORE_SECTOR_RELATIVE_VALUE_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-core-sector-relative-value-evidence"
)
DEFAULT_MIN_SECTOR_OBSERVATIONS = 5
_RETURN_LENSES: tuple[tuple[str, str], ...] = (
    ("raw", "forward_close_return"),
    ("topix_excess", "forward_topix_excess"),
    ("sector_excess", "forward_sector_excess"),
)
_CORE_RULE_ORDER: tuple[str, ...] = (
    "raw_core",
    "sector_relative_core",
    "raw_and_sector_relative_core",
    "raw_only_core",
    "sector_relative_only_core",
    "hybrid_core",
)


@dataclass(frozen=True)
class RankingCoreSectorRelativeValueEvidenceResult:
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
    core_rule_summary_df: pd.DataFrame
    yearly_core_rule_summary_df: pd.DataFrame
    raw_sector_relative_matrix_df: pd.DataFrame
    sector_concentration_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_core_sector_relative_value_evidence_research(
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
) -> RankingCoreSectorRelativeValueEvidenceResult:
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
        snapshot_prefix="ranking-core-sector-relative-value-evidence-",
    ) as ctx:
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="core_sector_relative_value",
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
                "sector-relative value requires liquidity-ranked signals"
            )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="core_sector_relative_value_sector",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(sector_features,),
            namespace="core_sector_relative_value",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="core_sector_relative_value_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="core_sector_relative_value_outcomes",
        )
        _create_core_sector_relative_tables(
            ctx.connection,
            source_name=evaluated.name,
            horizons=resolved_horizons,
            min_sector_observations=min_sector_observations,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_core_sector_relative_panel"
            ).fetchone()[0]
        )
        result = RankingCoreSectorRelativeValueEvidenceResult(
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
            core_rule_summary_df=_build_core_rule_summary_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            yearly_core_rule_summary_df=_build_yearly_core_rule_summary_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            raw_sector_relative_matrix_df=_build_raw_sector_relative_matrix_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sector_concentration_df=_build_sector_concentration_df(ctx.connection),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
                horizons=resolved_horizons,
            ),
        )
    return result


def write_ranking_core_sector_relative_value_evidence_bundle(
    result: RankingCoreSectorRelativeValueEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_CORE_SECTOR_RELATIVE_VALUE_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_core_sector_relative_value_evidence",
        function="run_ranking_core_sector_relative_value_evidence_research",
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
            "core_rule_summary_df": result.core_rule_summary_df,
            "yearly_core_rule_summary_df": result.yearly_core_rule_summary_df,
            "raw_sector_relative_matrix_df": result.raw_sector_relative_matrix_df,
            "sector_concentration_df": result.sector_concentration_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingCoreSectorRelativeValueEvidenceResult,
) -> str:
    parts = [
        "# Ranking Core Sector-Relative Value Evidence",
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
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Core Rule Summary",
        "",
        _top_rows_for_markdown(result.core_rule_summary_df, limit=180),
        "",
        "## Yearly Core Rule Summary",
        "",
        _top_rows_for_markdown(result.yearly_core_rule_summary_df, limit=220),
        "",
        "## Raw x Sector-Relative Matrix",
        "",
        _top_rows_for_markdown(result.raw_sector_relative_matrix_df, limit=160),
        "",
        "## Sector Concentration",
        "",
        _top_rows_for_markdown(result.sector_concentration_df, limit=120),
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
    horizons: Sequence[int],
    min_sector_observations: int,
) -> None:
    sector_forward = ",\n                ".join(
        f"avg(forward_close_return_{int(horizon)}d_pct) OVER ("
        "PARTITION BY market_scope, date, sector_33_name) "
        f"AS sector_forward_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    sector_excess = ",\n                ".join(
        f"forward_close_return_{int(horizon)}d_pct "
        f"- sector_forward_return_{int(horizon)}d_pct "
        f"AS forward_sector_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_core_sector_relative_universe AS
        WITH sector_outcomes AS (
            SELECT
                source.*,
                {sector_forward}
            FROM {source_name} source
        ),
        adapted AS (
            SELECT
                r.*,
                r.forecast_per AS forward_per,
                r.forecast_per_percentile AS forward_per_percentile,
                {sector_excess},
                CASE
                    WHEN r.liquidity_scope = 'crowded_rerating'
                     AND ((r.pbr_percentile <= 0.2
                           AND r.forecast_per_percentile <= 0.2)
                          OR (r.per_percentile <= 0.2
                              AND r.forecast_per_to_per_ratio <= 0.8))
                        THEN 'green'
                    WHEN r.liquidity_scope = 'crowded_rerating'
                     AND (r.pbr_percentile <= 0.2
                          OR (r.per_percentile <= 0.2
                              AND r.forecast_per_to_per_ratio <= 1.0))
                        THEN 'blue'
                    WHEN r.liquidity_scope = 'neutral_rerating'
                     AND r.per_percentile <= 0.2
                     AND r.forecast_per_to_per_ratio <= 0.8
                        THEN 'green'
                    WHEN r.liquidity_scope = 'neutral_rerating'
                        THEN 'blue'
                END AS ui_color,
                CASE
                    WHEN r.per_percentile <= 0.2
                     AND r.forecast_per_to_per_ratio <= 0.8
                        THEN 'low_per20_fwdper_per_lte_0_8'
                    WHEN r.pbr_percentile <= 0.2
                     AND r.forecast_per_percentile <= 0.2
                        THEN 'low_pbr20_low_fwd_per20'
                    WHEN r.pbr_percentile <= 0.2
                        THEN 'low_pbr20_only'
                    WHEN r.per_percentile <= 0.2
                     AND r.forecast_per_to_per_ratio <= 1.0
                        THEN 'low_per20_fwdper_per_lte_1_0'
                    ELSE 'no_value_confirmation'
                END AS value_condition
            FROM sector_outcomes r
        ),
        sector_universe AS (
            SELECT
                adapted.*,
                adapted.pbr_percentile AS raw_pbr_percentile,
                adapted.forward_per_percentile AS raw_forward_per_percentile,
                count(*) FILTER (WHERE adapted.pbr > 0) OVER (
                    PARTITION BY adapted.market_scope, adapted.date,
                                 adapted.sector_33_name
                ) AS sector_pbr_valid_count,
                rank() OVER (
                    PARTITION BY adapted.market_scope, adapted.date,
                                 adapted.sector_33_name
                    ORDER BY CASE WHEN adapted.pbr > 0 THEN adapted.pbr END NULLS LAST
                ) AS sector_pbr_rank,
                count(*) FILTER (WHERE adapted.forward_per > 0) OVER (
                    PARTITION BY adapted.market_scope, adapted.date,
                                 adapted.sector_33_name
                ) AS sector_forward_per_valid_count,
                rank() OVER (
                    PARTITION BY adapted.market_scope, adapted.date,
                                 adapted.sector_33_name
                    ORDER BY CASE WHEN adapted.forward_per > 0
                                  THEN adapted.forward_per END NULLS LAST
                ) AS sector_forward_per_rank
            FROM adapted
            WHERE adapted.sector_33_name IS NOT NULL
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
        """
        CREATE OR REPLACE TEMP TABLE ranking_core_sector_relative_panel AS
        WITH joined AS (
            SELECT
                u.*,
                CASE
                    WHEN u.pbr_percentile IS NOT NULL
                     AND u.forward_per_percentile IS NOT NULL
                     AND u.sector_pbr_percentile IS NOT NULL
                     AND u.sector_forward_per_percentile IS NOT NULL
                        THEN (
                            u.pbr_percentile
                            + u.forward_per_percentile
                            + u.sector_pbr_percentile
                            + u.sector_forward_per_percentile
                        ) / 4.0
                END AS hybrid_value_score
            FROM ranking_core_sector_relative_universe u
            WHERE u.liquidity_scope IN ('crowded_rerating', 'neutral_rerating')
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
                AND ui_color = 'blue'
                AND value_condition = 'low_pbr20_low_fwd_per20'
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


def _build_core_rule_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        horizon_int = int(horizon)
        frames.extend(
            _aggregate_core_rules(
                conn,
                horizon=horizon_int,
                return_lens=return_lens,
                return_column=_return_column(return_key, horizon_int),
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            for return_lens, return_key in _RETURN_LENSES
        )
    return _concat_sorted(frames, columns=_core_rule_summary_columns())


def _aggregate_core_rules(
    conn: Any,
    *,
    horizon: int,
    return_lens: str,
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = conn.execute(
        f"""
        SELECT
            core_rule,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg({return_column}) AS mean_return_pct,
            median({return_column}) AS median_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(pbr_percentile) AS median_raw_pbr_percentile,
            median(forward_per_percentile) AS median_raw_forward_per_percentile,
            median(sector_pbr_percentile) AS median_sector_pbr_percentile,
            median(sector_forward_per_percentile) AS median_sector_forward_per_percentile,
            median(hybrid_value_percentile) AS median_hybrid_value_percentile,
            median(sector_strength_score) AS median_sector_strength_score,
            median(sector_20d_topix_excess_pct) AS median_sector_20d_topix_excess_pct,
            median(sector_60d_topix_excess_pct) AS median_sector_60d_topix_excess_pct
        FROM ranking_core_rule_observations
        WHERE {return_column} IS NOT NULL
        GROUP BY core_rule
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    frame.insert(0, "return_lens", return_lens)
    frame.insert(0, "horizon", int(horizon))
    frame["core_rule_order"] = (
        frame["core_rule"].map(_core_rule_order_map()).fillna(999)
    )
    return frame.sort_values(["horizon", "return_lens", "core_rule_order", "core_rule"])


def _build_yearly_core_rule_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        horizon_int = int(horizon)
        frames.append(
            conn.execute(
                f"""
                SELECT
                    ? AS horizon,
                    substr(CAST(date AS VARCHAR), 1, 4) AS year,
                    core_rule,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    count(DISTINCT sector_33_name) AS sector_count,
                    avg(forward_close_excess_return_{horizon_int}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(forward_close_excess_return_{horizon_int}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{horizon_int}d_pct > 0
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{horizon_int}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct,
                    median(forward_sector_excess_return_{horizon_int}d_pct)
                        AS median_forward_sector_excess_return_pct
                FROM ranking_core_rule_observations
                WHERE forward_close_excess_return_{horizon_int}d_pct IS NOT NULL
                GROUP BY 1, 2, 3
                HAVING count(*) >= ?
                ORDER BY horizon, year, core_rule
                """,
                [
                    int(horizon_int),
                    float(severe_loss_threshold_pct),
                    int(min_observations),
                ],
            ).fetchdf()
        )
    frame = _concat_sorted(frames, columns=_yearly_core_rule_summary_columns())
    if frame.empty:
        return frame
    frame["core_rule_order"] = (
        frame["core_rule"].map(_core_rule_order_map()).fillna(999)
    )
    return frame.sort_values(["horizon", "year", "core_rule_order", "core_rule"])


def _build_raw_sector_relative_matrix_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        horizon_int = int(horizon)
        frames.append(
            conn.execute(
                f"""
                SELECT
                    ? AS horizon,
                    {_percentile_bucket_case("pbr_percentile")} AS raw_pbr_bucket,
                    {_percentile_bucket_case("forward_per_percentile")}
                        AS raw_forward_per_bucket,
                    {_percentile_bucket_case("sector_pbr_percentile")}
                        AS sector_relative_pbr_bucket,
                    {_percentile_bucket_case("sector_forward_per_percentile")}
                        AS sector_relative_forward_per_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    count(DISTINCT sector_33_name) AS sector_count,
                    avg(forward_close_excess_return_{horizon_int}d_pct)
                        AS mean_forward_topix_excess_return_pct,
                    median(forward_close_excess_return_{horizon_int}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{horizon_int}d_pct > 0
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{horizon_int}d_pct <= ?
                            THEN 1.0
                        ELSE 0.0
                    END) * 100.0 AS severe_loss_rate_pct,
                    median(forward_sector_excess_return_{horizon_int}d_pct)
                        AS median_forward_sector_excess_return_pct,
                    median(hybrid_value_percentile) AS median_hybrid_value_percentile
                FROM ranking_core_sector_relative_panel
                WHERE liquidity_scope = 'neutral_rerating'
                  AND sector_strength_bucket = 'sector_strong'
                  AND forward_close_excess_return_{horizon_int}d_pct IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5
                HAVING count(*) >= ?
                ORDER BY
                    horizon,
                    raw_pbr_bucket,
                    raw_forward_per_bucket,
                    sector_relative_pbr_bucket,
                    sector_relative_forward_per_bucket
                """,
                [
                    int(horizon_int),
                    float(severe_loss_threshold_pct),
                    int(min_observations),
                ],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_raw_sector_relative_matrix_columns())


def _build_sector_concentration_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            core_rule,
            sector_33_name,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            median(pbr_percentile) AS median_raw_pbr_percentile,
            median(forward_per_percentile) AS median_raw_forward_per_percentile,
            median(sector_pbr_percentile) AS median_sector_pbr_percentile,
            median(sector_forward_per_percentile) AS median_sector_forward_per_percentile,
            median(sector_strength_score) AS median_sector_strength_score
        FROM ranking_core_rule_observations
        GROUP BY core_rule, sector_33_name
        ORDER BY observation_count DESC, core_rule, sector_33_name
        """
    ).fetchdf()


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(CASE WHEN sector_pbr_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_pbr_coverage_pct,
            avg(CASE
                WHEN sector_forward_per_percentile IS NOT NULL THEN 1.0 ELSE 0.0
            END) * 100.0 AS sector_forward_per_coverage_pct,
            avg(CASE WHEN raw_core_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS raw_core_rate_pct,
            avg(CASE WHEN sector_relative_core_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sector_relative_core_rate_pct,
            avg(CASE WHEN hybrid_core_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS hybrid_core_rate_pct
        FROM ranking_core_sector_relative_panel
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
        f"forward_close_return_{int(horizon)}d_pct,\n"
        f"            forward_close_excess_return_{int(horizon)}d_pct,\n"
        f"            forward_sector_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    return conn.execute(
        f"""
        SELECT
            core_rule,
            date,
            code,
            company_name,
            market_scope,
            sector_33_name,
            sector_strength_bucket,
            liquidity_scope AS liquidity_regime,
            ui_color,
            value_condition,
            pbr_percentile AS raw_pbr_percentile,
            forward_per_percentile AS raw_forward_per_percentile,
            sector_pbr_percentile,
            sector_forward_per_percentile,
            hybrid_value_score,
            hybrid_value_percentile,
            {horizon_exprs}
        FROM ranking_core_rule_observations
        ORDER BY date, core_rule, code
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
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if min_sector_observations < 2:
        raise ValueError("min_sector_observations must be at least 2")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _return_column(return_key: str, horizon: int) -> str:
    if return_key == "forward_close_return":
        return f"forward_close_return_{horizon}d_pct"
    if return_key == "forward_topix_excess":
        return f"forward_close_excess_return_{horizon}d_pct"
    if return_key == "forward_sector_excess":
        return f"forward_sector_excess_return_{horizon}d_pct"
    raise ValueError(f"unknown return key: {return_key}")


def _percentile_bucket_case(column: str) -> str:
    return f"""
        CASE
            WHEN {column} <= 0.2 THEN 'q1_low'
            WHEN {column} <= 0.4 THEN 'q2'
            WHEN {column} <= 0.6 THEN 'q3'
            WHEN {column} <= 0.8 THEN 'q4'
            WHEN {column} IS NOT NULL THEN 'q5_high'
            ELSE 'missing'
        END
    """


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _core_rule_order_map() -> dict[str, int]:
    return {value: index for index, value in enumerate(_CORE_RULE_ORDER)}


def _core_rule_summary_columns() -> list[str]:
    return [
        "horizon",
        "return_lens",
        "core_rule",
        "core_rule_order",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_return_pct",
        "median_return_pct",
        "p10_return_pct",
        "p90_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_raw_pbr_percentile",
        "median_raw_forward_per_percentile",
        "median_sector_pbr_percentile",
        "median_sector_forward_per_percentile",
        "median_hybrid_value_percentile",
        "median_sector_strength_score",
        "median_sector_20d_topix_excess_pct",
        "median_sector_60d_topix_excess_pct",
    ]


def _raw_sector_relative_matrix_columns() -> list[str]:
    return [
        "horizon",
        "raw_pbr_bucket",
        "raw_forward_per_bucket",
        "sector_relative_pbr_bucket",
        "sector_relative_forward_per_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_forward_sector_excess_return_pct",
        "median_hybrid_value_percentile",
    ]


def _yearly_core_rule_summary_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "core_rule",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_forward_sector_excess_return_pct",
    ]
