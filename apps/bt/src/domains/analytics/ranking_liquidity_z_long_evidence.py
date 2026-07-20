"""Liquidity-z threshold evidence for Daily Ranking long-side candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    aggregate_lateral_conditions,
    aggregate_metric_columns,
    compose_daily_ranking_signal_features,
    condition_values_sql,
    deep_dive_metric_columns,
    deep_dive_metric_sql,
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
    SignalExpression,
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
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_LIQUIDITY_Z_LONG_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-liquidity-z-long-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 200
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_LIQUIDITY_Z_BUCKETS: tuple[tuple[str, str], ...] = (
    ("z_lt_minus3", "liquidity_residual_z < -3.0"),
    ("z_minus3_to_minus2", "liquidity_residual_z >= -3.0 AND liquidity_residual_z < -2.0"),
    ("z_minus2_to_minus1", "liquidity_residual_z >= -2.0 AND liquidity_residual_z < -1.0"),
    ("z_minus1_to_0", "liquidity_residual_z >= -1.0 AND liquidity_residual_z < 0.0"),
    ("z_0_to_1", "liquidity_residual_z >= 0.0 AND liquidity_residual_z < 1.0"),
    ("z_1_to_2", "liquidity_residual_z >= 1.0 AND liquidity_residual_z < 2.0"),
    ("z_2_to_3", "liquidity_residual_z >= 2.0 AND liquidity_residual_z < 3.0"),
    ("z_ge_3", "liquidity_residual_z >= 3.0"),
)
_LIQUIDITY_Z_CAPS: tuple[tuple[str, str], ...] = (
    (
        "z_cap_minus1_to_1",
        "liquidity_residual_z > -1.0 AND liquidity_residual_z < 1.0",
    ),
    (
        "z_cap_minus1_to_1_5",
        "liquidity_residual_z > -1.0 AND liquidity_residual_z < 1.5",
    ),
    (
        "z_cap_minus1_to_2",
        "liquidity_residual_z > -1.0 AND liquidity_residual_z < 2.0",
    ),
    (
        "z_cap_minus1_to_2_5",
        "liquidity_residual_z > -1.0 AND liquidity_residual_z < 2.5",
    ),
    (
        "z_cap_minus1_to_3",
        "liquidity_residual_z > -1.0 AND liquidity_residual_z < 3.0",
    ),
    ("z_cap_ge_3_tail", "liquidity_residual_z >= 3.0"),
)
_LONG_SCAFFOLDS: tuple[tuple[str, str], ...] = (
    ("all_rerating_price_action", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    (
        "long_hybrid_atr20_accel",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_sector_strong_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND sector_strength_bucket = 'sector_strong' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_long_hybrid_sector_strong_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND sector_strength_bucket = 'sector_strong' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)


@dataclass(frozen=True)
class RankingLiquidityZLongEvidenceResult:
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
    z_bucket_evidence_df: pd.DataFrame
    long_scaffold_z_bucket_evidence_df: pd.DataFrame
    long_scaffold_z_cap_evidence_df: pd.DataFrame


def run_ranking_liquidity_z_long_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingLiquidityZLongEvidenceResult:
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
        snapshot_prefix="ranking-liquidity-z-long-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="liquidity_z_long",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(
                    tuple[MarketScope, ...],
                    resolved_market_scopes,
                ),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("liquidity-z research requires liquidity-ranked signals")
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="liquidity_z_long_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="liquidity_z_long_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="liquidity_z_long_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="liquidity_z_long_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(leadership_features, short_features),
            namespace="liquidity_z_long",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="liquidity_z_long_signals",
            predicate=SignalExpression(
                sql=(
                    "liquidity_residual_z IS NOT NULL "
                    "AND recent_return_20d_pct >= 0 "
                    "AND recent_return_60d_pct >= 0"
                ),
                referenced_columns=(
                    "liquidity_residual_z",
                    "recent_return_20d_pct",
                    "recent_return_60d_pct",
                ),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="liquidity_z_long_outcomes",
        )
        _create_liquidity_z_long_panel(ctx.connection, source_name=evaluated.name)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_liquidity_z_long_panel"
            ).fetchone()[0]
        )
        result = RankingLiquidityZLongEvidenceResult(
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
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            z_bucket_evidence_df=_build_z_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_scaffold_z_bucket_evidence_df=_build_long_scaffold_z_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_scaffold_z_cap_evidence_df=_build_long_scaffold_z_cap_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
        )
    return result


def write_ranking_liquidity_z_long_evidence_bundle(
    result: RankingLiquidityZLongEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_LIQUIDITY_Z_LONG_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_liquidity_z_long_evidence",
        function="run_ranking_liquidity_z_long_evidence_research",
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
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
            "liquidity_feature": "liquidity_residual_z",
            "price_action_scope": "recent_return_20d_pct >= 0 and recent_return_60d_pct >= 0",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "z_bucket_evidence_df": result.z_bucket_evidence_df,
            "long_scaffold_z_bucket_evidence_df": result.long_scaffold_z_bucket_evidence_df,
            "long_scaffold_z_cap_evidence_df": result.long_scaffold_z_cap_evidence_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingLiquidityZLongEvidenceResult) -> str:
    parts = [
        "# Ranking Liquidity Z Long Evidence",
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
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Liquidity Z Bucket Evidence",
        "",
        _top_rows_for_markdown(result.z_bucket_evidence_df, limit=120),
        "",
        "## Long Scaffold x Liquidity Z Bucket Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_z_bucket_evidence_df, limit=260),
        "",
        "## Long Scaffold x Liquidity Z Cap Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_z_cap_evidence_df, limit=260),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [
        table
        for table in _REQUIRED_TABLES
        if not bool(
            conn.execute(
                """
                SELECT count(*)
                FROM duckdb_tables()
                WHERE table_name = ?
                """,
                [table],
            ).fetchone()[0]
        )
    ]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_liquidity_z_long_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_liquidity_z_long_panel AS
        SELECT
            g.*,
            CASE
                WHEN g.liquidity_residual_z < -3.0 THEN 'z_lt_minus3'
                WHEN g.liquidity_residual_z < -2.0 THEN 'z_minus3_to_minus2'
                WHEN g.liquidity_residual_z < -1.0 THEN 'z_minus2_to_minus1'
                WHEN g.liquidity_residual_z < 0.0 THEN 'z_minus1_to_0'
                WHEN g.liquidity_residual_z < 1.0 THEN 'z_0_to_1'
                WHEN g.liquidity_residual_z < 2.0 THEN 'z_1_to_2'
                WHEN g.liquidity_residual_z < 3.0 THEN 'z_2_to_3'
                ELSE 'z_ge_3'
            END AS liquidity_z_bucket,
            CASE
                WHEN g.liquidity_residual_z > -1.0
                 AND g.liquidity_residual_z < 1.0 THEN 'z_cap_minus1_to_1'
                WHEN g.liquidity_residual_z >= 3.0 THEN 'z_cap_ge_3_tail'
            END AS liquidity_z_cap,
            g.sector_33_code,
            g.sector_33_name,
            g.sector_strength_bucket,
            g.sector_strength_score,
            g.sector_index_strength_score,
            g.sector_constituent_strength_score,
            g.long_index_leadership_score,
            g.long_constituent_breadth_leadership_score,
            g.long_hybrid_leadership_score,
            g.balanced_sector_strength_bucket_label,
            g.long_hybrid_bucket_label,
            coalesce(g.momentum_20_60_top20_flag, FALSE)
                AS momentum_20_60_top20_flag,
            g.atr20_pct,
            g.atr60_pct,
            g.atr20_to_atr60,
            g.atr20_change_20d_pct,
            coalesce(g.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                g.atr20_acceleration
                AND coalesce(g.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(g.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(g.weak_trend, FALSE) AS weak_trend
        FROM {source_name} g
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            liquidity_z_bucket,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN liquidity_regime = 'neutral_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS neutral_rerating_rate_pct,
            avg(CASE WHEN liquidity_regime = 'crowded_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS crowded_rerating_rate_pct,
            avg(CASE WHEN valuation_signal = 'strong_value_confirmation'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS deep_value_rate_pct,
            avg(CASE WHEN long_hybrid_leadership_score >= 0.799999
                THEN 1.0 ELSE 0.0 END) * 100.0 AS long_hybrid_strong_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_acceleration_ex_overheat_rate_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forecast_per_percentile) AS median_forward_per_percentile
        FROM ranking_liquidity_z_long_panel
        GROUP BY market_scope, liquidity_z_bucket
        ORDER BY market_scope, liquidity_z_bucket
        """
    ).fetchdf()


def _build_z_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    z_bucket_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LIQUIDITY_Z_BUCKETS)}
        ) AS liquidity_z_bucket(
            liquidity_z_bucket,
            liquidity_z_bucket_order,
            liquidity_z_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_liquidity_z_long_panel",
                lateral_sql=z_bucket_lateral_sql,
                match_condition="liquidity_z_bucket.liquidity_z_bucket_matches",
                group_select_sql=(
                    "'liquidity_z_bucket' AS condition_family,\n"
                    "            liquidity_z_bucket.liquidity_z_bucket,\n"
                    "            liquidity_z_bucket.liquidity_z_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_z_bucket.liquidity_z_bucket, "
                    "liquidity_z_bucket.liquidity_z_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_z_bucket_columns())


def _build_long_scaffold_z_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LIQUIDITY_Z_BUCKETS)}
        ) AS liquidity_z_bucket(
            liquidity_z_bucket,
            liquidity_z_bucket_order,
            liquidity_z_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_liquidity_z_long_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND liquidity_z_bucket.liquidity_z_bucket_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_liquidity_z_bucket' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            liquidity_z_bucket.liquidity_z_bucket,\n"
                    "            liquidity_z_bucket.liquidity_z_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "liquidity_z_bucket.liquidity_z_bucket, "
                    "liquidity_z_bucket.liquidity_z_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_z_bucket_columns())


def _build_long_scaffold_z_cap_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LIQUIDITY_Z_CAPS)}
        ) AS liquidity_z_cap(
            liquidity_z_cap,
            liquidity_z_cap_order,
            liquidity_z_cap_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_liquidity_z_long_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND liquidity_z_cap.liquidity_z_cap_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_liquidity_z_cap' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            liquidity_z_cap.liquidity_z_cap,\n"
                    "            liquidity_z_cap.liquidity_z_cap_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "liquidity_z_cap.liquidity_z_cap, "
                    "liquidity_z_cap.liquidity_z_cap_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_z_cap_columns())


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            liquidity_regime,
            liquidity_residual_z,
            liquidity_z_bucket,
            liquidity_z_cap,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            valuation_signal,
            pbr,
            pbr_percentile,
            forecast_per AS forward_per,
            forecast_per_percentile AS forward_per_percentile,
            sector_strength_bucket,
            sector_strength_score,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_acceleration_ex_overheat_flag,
            {horizon_columns}
        FROM ranking_liquidity_z_long_panel
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


def _z_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_z_bucket",
        "liquidity_z_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
    ]


def _long_scaffold_z_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "liquidity_z_bucket",
        "liquidity_z_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
    ]


def _long_scaffold_z_cap_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "liquidity_z_cap",
        "liquidity_z_cap_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
    ]


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
            "long_scaffold_order",
            "liquidity_z_bucket_order",
            "liquidity_z_cap_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
