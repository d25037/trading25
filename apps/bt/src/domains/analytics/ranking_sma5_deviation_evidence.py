"""SMA5 deviation evidence for Daily Ranking technical-state diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast, Iterable, Sequence

import pandas as pd

from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    LongScaffoldFeaturesRequest,
    PsrFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_psr_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
)
from src.domains.analytics.daily_ranking_consumer_support import (
    PRICE_ACTION_BUCKETS,
    SHORT_OVERLAYS,
    aggregate_lateral_conditions,
    aggregate_metric_columns,
    compose_daily_ranking_signal_features,
    condition_values_sql,
    deep_dive_metric_columns,
    deep_dive_metric_sql,
    liquidity_band_labels,
    normalize_liquidity_bands,
    psr_metric_columns,
    recomposition_metric_columns,
    recomposition_metric_sql,
    sql_string_list,
    table_exists,
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
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_sma_features
RANKING_SMA5_DEVIATION_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-deviation-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_LIQUIDITY_BANDS: tuple[str, ...] = ("high", "mid", "low")
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_WARMUP_CALENDAR_DAYS = 820
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "stock_data",
    "stock_provider_windows",
    "stock_adjustment_events",
    "current_basis_recompute_pending",
    "current_basis_fundamentals_state",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "statements",
    "statement_metrics_adjusted",
    "indices_data",
    "index_master",
)
_LONG_SCAFFOLDS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    (
        "neutral_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "crowded_long_hybrid",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999",
    ),
)
_SMA5_DEVIATION_BUCKETS: tuple[tuple[str, str], ...] = (
    ("below_sma5_le_neg2", "sma5_deviation_pct <= -2.0"),
    (
        "below_sma5_neg2_to_0",
        "sma5_deviation_pct > -2.0 AND sma5_deviation_pct <= 0.0",
    ),
    (
        "above_sma5_0_to_2",
        "sma5_deviation_pct > 0.0 AND sma5_deviation_pct <= 2.0",
    ),
    (
        "above_sma5_2_to_5",
        "sma5_deviation_pct > 2.0 AND sma5_deviation_pct <= 5.0",
    ),
    ("above_sma5_gt_5", "sma5_deviation_pct > 5.0"),
)


@dataclass(frozen=True)
class RankingSma5DeviationEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    liquidity_bands: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    sma5_deviation_bucket_evidence_df: pd.DataFrame
    long_scaffold_sma5_deviation_evidence_df: pd.DataFrame
    short_overlay_sma5_deviation_evidence_df: pd.DataFrame


def run_ranking_sma5_deviation_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    liquidity_bands: Sequence[str] = DEFAULT_LIQUIDITY_BANDS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5DeviationEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_liquidity_bands = normalize_liquidity_bands(liquidity_bands)
    _validate_params(
        horizons=resolved_horizons,
        liquidity_bands=resolved_liquidity_bands,
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
        snapshot_prefix="ranking-sma5-deviation-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        request = DailyRankingPanelRequest(
            namespace="sma5_deviation",
            analysis_start_date=_parse_optional_date(start_date),
            analysis_end_date=_parse_optional_date(end_date),
            horizons=resolved_horizons,
            market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
            include_liquidity=True,
            percentile_features=(),
            required_valid_sessions=505,
        )
        relations = build_daily_ranking_research_base(ctx.connection, request)
        source = relations.ranked_signals
        atr = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=source, namespace="sma5_deviation"),
        )
        short = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=source,
                atr_features=atr,
                namespace="sma5_deviation",
            ),
        )
        sector = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=source,
                population_source=source,
                namespace="sma5_deviation",
            ),
        )
        leadership = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=source,
                sector_features=sector,
                namespace="sma5_deviation",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=source,
                leadership_features=leadership,
                short_scaffold_features=short,
                namespace="sma5_deviation",
            ),
        )
        psr = build_psr_features(
            ctx.connection,
            PsrFeaturesRequest(source=source, namespace="sma5_deviation"),
        )
        sma = build_sma_features(
            ctx.connection,
            SmaFeaturesRequest(
                source=source,
                price_history=relations.price_history,
                namespace="sma5_deviation",
            ),
        )
        featured = compose_daily_ranking_signal_features(
            ctx.connection,
            source=source,
            features=(long_features, psr, sma),
            namespace="sma5_deviation",
        )
        liquidity_labels = sql_string_list(
            liquidity_band_labels(resolved_liquidity_bands)
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=featured,
            name="sma5_deviation_signals",
            predicate=SignalExpression(
                sql=(
                    "liquidity_residual_z IS NOT NULL "
                    "AND recent_return_20d_pct IS NOT NULL "
                    "AND recent_return_60d_pct IS NOT NULL "
                    "AND sma5_deviation_pct IS NOT NULL "
                    "AND CASE "
                    "WHEN liquidity_residual_z >= 1 THEN 'high_liquidity_z_ge_1' "
                    "WHEN liquidity_residual_z > -1 AND liquidity_residual_z < 1 "
                    "THEN 'mid_liquidity_z_minus1_to_1' "
                    "WHEN liquidity_residual_z < -1 THEN 'low_liquidity_z_lt_minus1' "
                    f"END IN ({liquidity_labels}) "
                    "AND ((recent_return_20d_pct > 0 AND recent_return_60d_pct > 0) "
                    "OR (recent_return_20d_pct > 0 AND recent_return_60d_pct < 0) "
                    "OR (recent_return_20d_pct < 0 AND recent_return_60d_pct > 0) "
                    "OR (recent_return_20d_pct < 0 AND recent_return_60d_pct < 0))"
                ),
                referenced_columns=(
                    "liquidity_residual_z",
                    "recent_return_20d_pct",
                    "recent_return_60d_pct",
                    "sma5_deviation_pct",
                ),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="sma5_deviation_outcomes",
        )
        _create_sma5_deviation_panel(
            ctx.connection,
            evaluated_name=evaluated.name,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sma5_deviation_panel"
            ).fetchone()[0]
        )
        result = RankingSma5DeviationEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            liquidity_bands=resolved_liquidity_bands,
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
            sma5_deviation_bucket_evidence_df=(
                _build_sma5_deviation_bucket_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            long_scaffold_sma5_deviation_evidence_df=(
                _build_long_scaffold_sma5_deviation_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            short_overlay_sma5_deviation_evidence_df=(
                _build_short_overlay_sma5_deviation_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_sma5_deviation_evidence_bundle(
    result: RankingSma5DeviationEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_DEVIATION_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_deviation_evidence",
        function="run_ranking_sma5_deviation_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "liquidity_bands": list(result.liquidity_bands),
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
            "sma5_parameter": "sma5_deviation_pct = (close / sma5 - 1) * 100",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "sma5_deviation_bucket_evidence_df": (
                result.sma5_deviation_bucket_evidence_df
            ),
            "long_scaffold_sma5_deviation_evidence_df": (
                result.long_scaffold_sma5_deviation_evidence_df
            ),
            "short_overlay_sma5_deviation_evidence_df": (
                result.short_overlay_sma5_deviation_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5DeviationEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 Deviation Evidence",
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
        f"- liquidity_bands: `{', '.join(result.liquidity_bands)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## SMA5 Deviation Bucket Evidence",
        "",
        _top_rows_for_markdown(result.sma5_deviation_bucket_evidence_df, limit=160),
        "",
        "## Long Scaffold x SMA5 Deviation Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_sma5_deviation_evidence_df,
            limit=260,
        ),
        "",
        "## Short Overlay x SMA5 Deviation Evidence",
        "",
        _top_rows_for_markdown(
            result.short_overlay_sma5_deviation_evidence_df,
            limit=260,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_sma5_deviation_panel(
    conn: Any,
    *,
    evaluated_name: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_deviation_panel AS
        SELECT
            evaluated.*,
            CASE
                WHEN liquidity_residual_z >= 1 THEN 'high_liquidity_z_ge_1'
                WHEN liquidity_residual_z > -1 AND liquidity_residual_z < 1
                    THEN 'mid_liquidity_z_minus1_to_1'
                WHEN liquidity_residual_z < -1 THEN 'low_liquidity_z_lt_minus1'
            END AS liquidity_band,
            CASE
                WHEN recent_return_20d_pct > 0 AND recent_return_60d_pct > 0
                    THEN 'dual_positive_crowded'
                WHEN recent_return_20d_pct > 0 AND recent_return_60d_pct < 0
                    THEN 'recent20_positive_60d_negative'
                WHEN recent_return_20d_pct < 0 AND recent_return_60d_pct > 0
                    THEN 'recent20_negative_60d_positive'
                WHEN recent_return_20d_pct < 0 AND recent_return_60d_pct < 0
                    THEN 'dual_negative_stress'
            END AS price_action_bucket
        FROM {evaluated_name} evaluated
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            sma5_deviation_bucket,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            median(sma5_deviation_pct) AS median_sma5_deviation_pct,
            quantile_cont(sma5_deviation_pct, 0.1) AS p10_sma5_deviation_pct,
            quantile_cont(sma5_deviation_pct, 0.9) AS p90_sma5_deviation_pct,
            avg(CASE WHEN liquidity_regime = 'neutral_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS neutral_rerating_rate_pct,
            avg(CASE WHEN liquidity_regime = 'crowded_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS crowded_rerating_rate_pct,
            avg(CASE WHEN valuation_signal = 'strong_value_confirmation'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS deep_value_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_acceleration_ex_overheat_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(psr_percentile) AS median_psr_percentile
        FROM ranking_sma5_deviation_panel
        GROUP BY market_scope, sma5_deviation_bucket
        ORDER BY market_scope, min(sma5_deviation_pct)
        """
    ).fetchdf()


def _build_sma5_deviation_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_SMA5_DEVIATION_BUCKETS)}
        ) AS sma5_deviation_bucket(
            sma5_deviation_bucket,
            sma5_deviation_bucket_order,
            sma5_deviation_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_deviation_panel",
                lateral_sql=lateral_sql,
                match_condition="sma5_deviation_bucket.sma5_deviation_bucket_matches",
                group_select_sql=(
                    "'sma5_deviation_bucket' AS condition_family,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "sma5_deviation_bucket.sma5_deviation_bucket, "
                    "sma5_deviation_bucket.sma5_deviation_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_deviation_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_sma5_deviation_bucket_columns())


def _build_long_scaffold_sma5_deviation_evidence_df(
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
            VALUES {condition_values_sql(_SMA5_DEVIATION_BUCKETS)}
        ) AS sma5_deviation_bucket(
            sma5_deviation_bucket,
            sma5_deviation_bucket_order,
            sma5_deviation_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_deviation_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND sma5_deviation_bucket.sma5_deviation_bucket_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_sma5_deviation' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "sma5_deviation_bucket.sma5_deviation_bucket, "
                    "sma5_deviation_bucket.sma5_deviation_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=deep_dive_metric_sql() + _sma5_deviation_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_scaffold_sma5_deviation_columns(),
    )


def _build_short_overlay_sma5_deviation_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(PRICE_ACTION_BUCKETS)}
        ) AS price_action_bucket(
            price_action_bucket,
            price_action_bucket_order,
            price_action_bucket_matches
        )
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(SHORT_OVERLAYS)}
        ) AS short_overlay(short_overlay, short_overlay_order, short_overlay_matches)
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_SMA5_DEVIATION_BUCKETS)}
        ) AS sma5_deviation_bucket(
            sma5_deviation_bucket,
            sma5_deviation_bucket_order,
            sma5_deviation_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_deviation_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "price_action_bucket.price_action_bucket_matches "
                    "AND short_overlay.short_overlay_matches "
                    "AND sma5_deviation_bucket.sma5_deviation_bucket_matches"
                ),
                group_select_sql=(
                    "'short_overlay_sma5_deviation' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    "            short_overlay.short_overlay,\n"
                    "            short_overlay.short_overlay_order,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket,\n"
                    "            sma5_deviation_bucket.sma5_deviation_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "short_overlay.short_overlay, "
                    "short_overlay.short_overlay_order, "
                    "sma5_deviation_bucket.sma5_deviation_bucket, "
                    "sma5_deviation_bucket.sma5_deviation_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=recomposition_metric_sql()
                + _sma5_deviation_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_short_overlay_sma5_deviation_columns(),
    )


def _sma5_deviation_metric_sql() -> str:
    return """,
            median(sma5_deviation_pct) AS median_sma5_deviation_pct,
            avg(CASE WHEN sma5_deviation_pct <= 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS below_or_equal_sma5_rate_pct,
            avg(CASE WHEN sma5_deviation_pct > 2.0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_deviation_gt_2_rate_pct,
            avg(CASE WHEN sma5_deviation_pct > 5.0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_deviation_gt_5_rate_pct"""


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
            liquidity_band,
            price_action_bucket,
            close,
            sma5,
            sma5_deviation_pct,
            sma5_deviation_bucket,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            valuation_signal,
            psr_percentile,
            sector_strength_bucket,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_acceleration_ex_overheat_flag,
            {horizon_columns}
        FROM ranking_sma5_deviation_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    liquidity_bands: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not liquidity_bands:
        raise ValueError("liquidity_bands must contain at least one item")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _top_rows_for_markdown(frame: pd.DataFrame, *, limit: int) -> str:
    if frame.empty:
        return "_No rows._"
    return "```text\n" + frame.head(int(limit)).to_string(index=False) + "\n```"


def _sma5_deviation_metric_columns() -> list[str]:
    return [
        "median_sma5_deviation_pct",
        "below_or_equal_sma5_rate_pct",
        "sma5_deviation_gt_2_rate_pct",
        "sma5_deviation_gt_5_rate_pct",
    ]


def _sma5_deviation_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "sma5_deviation_bucket",
        "sma5_deviation_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *_sma5_deviation_metric_columns(),
    ]


def _long_scaffold_sma5_deviation_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "sma5_deviation_bucket",
        "sma5_deviation_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
        *_sma5_deviation_metric_columns(),
    ]


def _short_overlay_sma5_deviation_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "sma5_deviation_bucket",
        "sma5_deviation_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *psr_metric_columns(),
        *recomposition_metric_columns(),
        *_sma5_deviation_metric_columns(),
    ]


def _concat_sorted(
    frames: Sequence[pd.DataFrame],
    *,
    columns: Sequence[str],
) -> pd.DataFrame:
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
            "liquidity_band",
            "price_action_bucket_order",
            "short_overlay_order",
            "long_scaffold_order",
            "sma5_deviation_bucket_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
