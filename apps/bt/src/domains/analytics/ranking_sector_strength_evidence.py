"""PIT sector-strength evidence for Daily Ranking green/blue rerating signals."""

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
    SectorStrengthFeaturesRequest,
    build_sector_strength_features,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_sector_strength_features
RANKING_SECTOR_STRENGTH_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sector-strength-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "indices_data",
    "index_master",
    "daily_valuation",
    "stock_master_daily",
)
_SECTOR_BUCKET_ORDER: tuple[str, ...] = (
    "sector_weak",
    "sector_neutral",
    "sector_strong",
)
_SECTOR_CONSISTENCY_ORDER: tuple[str, ...] = (
    "sector_weak_consistent",
    "sector_mixed",
    "sector_strong_consistent",
)
_VALUE_CONDITION_ORDER: tuple[str, ...] = (
    "low_per20_fwdper_per_lte_0_8",
    "low_pbr20_low_fwd_per20",
    "low_pbr20_only",
    "low_per20_fwdper_per_lte_1_0",
    "no_value_confirmation",
)
_VALUE_TIER_ORDER: tuple[str, ...] = (
    "strong_value_confirmation",
    "medium_value_confirmation",
    "no_value_confirmation",
)


@dataclass(frozen=True)
class RankingSectorStrengthEvidenceResult:
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
    sector_daily_state_df: pd.DataFrame
    sector_strength_evidence_df: pd.DataFrame
    color_sector_interaction_df: pd.DataFrame
    sector_excess_interaction_df: pd.DataFrame
    sector_concentration_df: pd.DataFrame


def run_ranking_sector_strength_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSectorStrengthEvidenceResult:
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

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sector-strength-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sector_strength",
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
            raise RuntimeError("sector strength requires liquidity-ranked signals")
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="sector_strength_features",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(sector_features,),
            namespace="sector_strength",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="sector_strength_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="sector_strength_outcomes",
        )
        _create_sector_evidence_tables(
            ctx.connection,
            source_name=evaluated.name,
            horizons=resolved_horizons,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sector_signal_panel"
            ).fetchone()[0]
        )
        result = RankingSectorStrengthEvidenceResult(
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
                horizons=resolved_horizons,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            sector_daily_state_df=_query_sector_daily_state_df(ctx.connection),
            sector_strength_evidence_df=_build_sector_strength_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            color_sector_interaction_df=_build_color_sector_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sector_excess_interaction_df=_build_sector_excess_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sector_concentration_df=_build_sector_concentration_df(ctx.connection),
        )
    return result


def write_ranking_sector_strength_evidence_bundle(
    result: RankingSectorStrengthEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SECTOR_STRENGTH_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sector_strength_evidence",
        function="run_ranking_sector_strength_evidence_research",
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
            "sector_strength_score_definition": (
                "average of official sector-index score "
                "(0.20*5d + 0.45*20d + 0.25*60d TOPIX-excess ranks + "
                "0.10*constituent breadth rank) and constituent score "
                "(20d TOPIX-excess rank + 60d TOPIX-excess rank + breadth rank)/3"
            ),
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "sector_daily_state_df": result.sector_daily_state_df,
            "sector_strength_evidence_df": result.sector_strength_evidence_df,
            "color_sector_interaction_df": result.color_sector_interaction_df,
            "sector_excess_interaction_df": result.sector_excess_interaction_df,
            "sector_concentration_df": result.sector_concentration_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSectorStrengthEvidenceResult) -> str:
    parts = [
        "# Ranking Sector Strength Evidence",
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
        "## Sector Daily State",
        "",
        _top_rows_for_markdown(result.sector_daily_state_df, limit=80),
        "",
        "## Sector Strength Baseline",
        "",
        _top_rows_for_markdown(result.sector_strength_evidence_df, limit=80),
        "",
        "## Color x Sector Strength",
        "",
        _top_rows_for_markdown(result.color_sector_interaction_df, limit=160),
        "",
        "## Color x Sector Excess",
        "",
        _top_rows_for_markdown(result.sector_excess_interaction_df, limit=160),
        "",
        "## Sector Concentration",
        "",
        _top_rows_for_markdown(result.sector_concentration_df, limit=120),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_sector_evidence_tables(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
) -> None:
    """Publish the legacy result tables from a frozen, evaluated cohort."""

    sector_state_columns = (
        "market_scope",
        "date",
        "sector_33_code",
        "sector_33_name",
        "sector_observation_count",
        "sector_code_count",
        "sector_index_code",
        "sector_index_return_5d_pct",
        "sector_index_return_20d_pct",
        "sector_index_return_60d_pct",
        "sector_index_5d_topix_excess_pct",
        "sector_index_20d_topix_excess_pct",
        "sector_index_60d_topix_excess_pct",
        "sector_constituent_20d_topix_excess_pct",
        "sector_constituent_60d_topix_excess_pct",
        "sector_20d_topix_excess_pct",
        "sector_60d_topix_excess_pct",
        "sector_breadth_20d_pct",
        "sector_index_5d_strength_rank",
        "sector_20d_strength_rank",
        "sector_60d_strength_rank",
        "sector_constituent_20d_strength_rank",
        "sector_constituent_60d_strength_rank",
        "sector_breadth_strength_rank",
        "sector_index_strength_score",
        "sector_constituent_strength_score",
        "sector_strength_score",
        "sector_strength_bucket",
        "sector_consistency_bucket",
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE ranking_sector_daily_state AS "
        f"SELECT DISTINCT {', '.join(sector_state_columns)} FROM {source_name} "
        "WHERE sector_33_code IS NOT NULL"
    )
    sector_forward = ",\n                ".join(
        f"avg(forward_close_return_{int(horizon)}d_pct) OVER ("
        "PARTITION BY market_scope, date, sector_33_code) "
        f"AS sector_forward_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    sector_excess = ",\n            ".join(
        f"r.forward_close_return_{int(horizon)}d_pct "
        f"- r.sector_forward_return_{int(horizon)}d_pct "
        f"AS forward_sector_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sector_signal_panel AS
        WITH sector_outcomes AS (
            SELECT
                source.*,
                {sector_forward}
            FROM {source_name} source
        )
        SELECT
            r.*,
            r.forecast_per_percentile AS forward_per_percentile,
            r.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
            {sector_excess},
            {_ui_color_case_sql()} AS ui_color,
            {_value_condition_case_sql()} AS value_condition,
            {_value_tier_case_sql()} AS value_confirmation_tier
        FROM sector_outcomes r
        WHERE r.liquidity_scope IN ('crowded_rerating', 'neutral_rerating')
        """
    )


def _ui_color_case_sql() -> str:
    crowded_green = (
        "(r.pbr_percentile <= 0.2 AND r.forecast_per_percentile <= 0.2) "
        "OR (r.per_percentile <= 0.2 AND r.forecast_per_to_per_ratio <= 0.8)"
    )
    crowded_medium = (
        "r.pbr_percentile <= 0.2 "
        "OR (r.per_percentile <= 0.2 AND r.forecast_per_to_per_ratio <= 1.0)"
    )
    crowded_blue = f"({crowded_medium}) AND NOT ({crowded_green})"
    neutral_green = "r.per_percentile <= 0.2 AND r.forecast_per_to_per_ratio <= 0.8"
    neutral_blue = f"NOT ({neutral_green})"
    return f"""
        CASE
            WHEN r.liquidity_scope = 'crowded_rerating' AND ({crowded_green}) THEN 'green'
            WHEN r.liquidity_scope = 'crowded_rerating' AND ({crowded_blue}) THEN 'blue'
            WHEN r.liquidity_scope = 'neutral_rerating' AND ({neutral_green}) THEN 'green'
            WHEN r.liquidity_scope = 'neutral_rerating' AND ({neutral_blue}) THEN 'blue'
        END
    """


def _value_condition_case_sql() -> str:
    return """
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
        END
    """


def _value_tier_case_sql() -> str:
    strong_value = (
        "(r.pbr_percentile <= 0.2 AND r.forecast_per_percentile <= 0.2) "
        "OR (r.per_percentile <= 0.2 AND r.forecast_per_to_per_ratio <= 0.8)"
    )
    medium_value = (
        "r.pbr_percentile <= 0.2 "
        "OR (r.per_percentile <= 0.2 AND r.forecast_per_to_per_ratio <= 1.0)"
    )
    return f"""
        CASE
            WHEN {strong_value} THEN 'strong_value_confirmation'
            WHEN {medium_value} THEN 'medium_value_confirmation'
            ELSE 'no_value_confirmation'
        END
    """


def _build_color_sector_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_grouped(
            conn,
            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
            return_prefix="forward_topix_excess",
            group_columns=[
                "market_scope",
                "liquidity_scope",
                "ui_color",
                "value_confirmation_tier",
                "value_condition",
                "sector_strength_bucket",
                "sector_consistency_bucket",
            ],
            extra_fields={"horizon": int(horizon)},
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_color_sector_interaction_columns())


def _build_sector_excess_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_grouped(
            conn,
            return_column=f"forward_sector_excess_return_{int(horizon)}d_pct",
            return_prefix="forward_sector_excess",
            group_columns=[
                "market_scope",
                "liquidity_scope",
                "ui_color",
                "value_confirmation_tier",
                "value_condition",
                "sector_strength_bucket",
                "sector_consistency_bucket",
            ],
            extra_fields={"horizon": int(horizon)},
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_excess_interaction_columns())


def _build_sector_strength_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_grouped(
            conn,
            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
            return_prefix="forward_topix_excess",
            group_columns=[
                "market_scope",
                "sector_strength_bucket",
                "sector_consistency_bucket",
            ],
            extra_fields={"horizon": int(horizon)},
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            include_all_ui_colors=True,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_strength_evidence_columns())


def _aggregate_grouped(
    conn: Any,
    *,
    return_column: str,
    return_prefix: str,
    group_columns: Sequence[str],
    extra_fields: dict[str, Any],
    min_observations: int,
    severe_loss_threshold_pct: float,
    include_all_ui_colors: bool = False,
) -> pd.DataFrame:
    select_groups = ",\n            ".join(group_columns)
    group_by = ", ".join(str(index) for index in range(1, len(group_columns) + 1))
    ui_color_filter = (
        "TRUE" if include_all_ui_colors else "ui_color IN ('green', 'blue')"
    )
    frame = conn.execute(
        f"""
        SELECT
            {select_groups},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({return_column}) AS mean_{return_prefix}_return_pct,
            median({return_column}) AS median_{return_prefix}_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_{return_prefix}_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_{return_prefix}_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(sector_strength_score) AS median_sector_strength_score,
            median(sector_index_5d_topix_excess_pct)
                AS median_sector_index_5d_topix_excess_pct,
            median(sector_index_20d_topix_excess_pct)
                AS median_sector_index_20d_topix_excess_pct,
            median(sector_index_60d_topix_excess_pct)
                AS median_sector_index_60d_topix_excess_pct,
            median(sector_constituent_20d_topix_excess_pct)
                AS median_sector_constituent_20d_topix_excess_pct,
            median(sector_constituent_60d_topix_excess_pct)
                AS median_sector_constituent_60d_topix_excess_pct,
            median(sector_20d_topix_excess_pct) AS median_sector_20d_topix_excess_pct,
            median(sector_60d_topix_excess_pct) AS median_sector_60d_topix_excess_pct,
            median(sector_breadth_20d_pct) AS median_sector_breadth_20d_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(pbr_percentile) AS median_pbr_percentile
        FROM ranking_sector_signal_panel
        WHERE {ui_color_filter}
          AND {return_column} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in extra_fields.items():
        frame[column] = value
    if "liquidity_scope" in frame.columns:
        frame = frame.rename(columns={"liquidity_scope": "liquidity_regime"})
    return frame.reindex(
        columns=[*extra_fields.keys(), *frame.columns.drop(extra_fields.keys())]
    )


def _build_sector_concentration_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            liquidity_scope AS liquidity_regime,
            ui_color,
            value_confirmation_tier,
            value_condition,
            sector_33_name,
            sector_strength_bucket,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            median(sector_strength_score) AS median_sector_strength_score,
            median(sector_20d_topix_excess_pct) AS median_sector_20d_topix_excess_pct,
            median(sector_60d_topix_excess_pct) AS median_sector_60d_topix_excess_pct
        FROM ranking_sector_signal_panel
        WHERE ui_color IN ('green', 'blue')
        GROUP BY
            market_scope,
            liquidity_scope,
            ui_color,
            value_confirmation_tier,
            value_condition,
            sector_33_name,
            sector_strength_bucket
        ORDER BY observation_count DESC, market_scope, liquidity_regime, ui_color
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
            avg(CASE WHEN sector_strength_score IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_strength_coverage_pct,
            avg(CASE WHEN ui_color IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS green_blue_color_coverage_pct
        FROM ranking_sector_signal_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _query_sector_daily_state_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            date,
            sector_33_code,
            sector_33_name,
            sector_observation_count,
            sector_code_count,
            sector_index_code,
            sector_index_return_5d_pct,
            sector_index_return_20d_pct,
            sector_index_return_60d_pct,
            sector_index_5d_topix_excess_pct,
            sector_index_20d_topix_excess_pct,
            sector_index_60d_topix_excess_pct,
            sector_constituent_20d_topix_excess_pct,
            sector_constituent_60d_topix_excess_pct,
            sector_20d_topix_excess_pct,
            sector_60d_topix_excess_pct,
            sector_breadth_20d_pct,
            sector_index_5d_strength_rank,
            sector_20d_strength_rank,
            sector_60d_strength_rank,
            sector_constituent_20d_strength_rank,
            sector_constituent_60d_strength_rank,
            sector_breadth_strength_rank,
            sector_index_strength_score,
            sector_constituent_strength_score,
            sector_strength_score,
            sector_strength_bucket,
            sector_consistency_bucket
        FROM ranking_sector_daily_state
        ORDER BY date, market_scope, sector_strength_score DESC, sector_33_name
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
            date,
            code,
            company_name,
            market_scope,
            liquidity_scope AS liquidity_regime,
            ui_color,
            value_confirmation_tier,
            value_condition,
            sector_33_name,
            sector_index_code,
            sector_strength_score,
            sector_strength_bucket,
            sector_consistency_bucket,
            sector_index_5d_topix_excess_pct,
            sector_index_20d_topix_excess_pct,
            sector_index_60d_topix_excess_pct,
            sector_constituent_20d_topix_excess_pct,
            sector_constituent_60d_topix_excess_pct,
            recent_return_20d_pct,
            recent_return_60d_pct,
            sector_20d_topix_excess_pct,
            sector_60d_topix_excess_pct,
            sector_breadth_20d_pct,
            per_percentile,
            forward_per_percentile,
            pbr_percentile,
            {horizon_exprs}
        FROM ranking_sector_signal_panel
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
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _common_metric_columns(return_prefix: str) -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        f"mean_{return_prefix}_return_pct",
        f"median_{return_prefix}_return_pct",
        f"p10_{return_prefix}_return_pct",
        f"p90_{return_prefix}_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_sector_strength_score",
        "median_sector_index_5d_topix_excess_pct",
        "median_sector_index_20d_topix_excess_pct",
        "median_sector_index_60d_topix_excess_pct",
        "median_sector_constituent_20d_topix_excess_pct",
        "median_sector_constituent_60d_topix_excess_pct",
        "median_sector_20d_topix_excess_pct",
        "median_sector_60d_topix_excess_pct",
        "median_sector_breadth_20d_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_per_percentile",
        "median_forward_per_percentile",
        "median_pbr_percentile",
    ]


def _sector_strength_evidence_columns() -> list[str]:
    return [
        "horizon",
        "market_scope",
        "sector_strength_bucket",
        "sector_consistency_bucket",
        *_common_metric_columns("forward_topix_excess"),
    ]


def _color_sector_interaction_columns() -> list[str]:
    return [
        "horizon",
        "market_scope",
        "liquidity_regime",
        "ui_color",
        "value_confirmation_tier",
        "value_condition",
        "sector_strength_bucket",
        "sector_consistency_bucket",
        *_common_metric_columns("forward_topix_excess"),
    ]


def _sector_excess_interaction_columns() -> list[str]:
    return [
        "horizon",
        "market_scope",
        "liquidity_regime",
        "ui_color",
        "value_confirmation_tier",
        "value_condition",
        "sector_strength_bucket",
        "sector_consistency_bucket",
        *_common_metric_columns("forward_sector_excess"),
    ]
