"""Liquidity x price-action recomposition evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast, Iterable, Sequence

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    aggregate_lateral_conditions as _aggregate_lateral_conditions,
    aggregate_metric_columns as _aggregate_metric_columns,
    condition_values_sql as _condition_values_sql,
    psr_metric_columns as _psr_metric_columns,
    psr_metric_sql as _psr_metric_sql,
    table_exists as _table_exists,
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    PsrFeaturesRequest,
    SectorStrengthFeaturesRequest,
    build_psr_features,
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
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

RANKING_LIQUIDITY_PRICE_ACTION_RECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/ranking-liquidity-price-action-recomposition"
)
DEFAULT_HORIZONS: tuple[int, ...] = (20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_LIQUIDITY_BANDS: tuple[str, ...] = ("high",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "statements",
    "indices_data",
    "index_master",
)
_PRICE_ACTION_BUCKETS: tuple[tuple[str, str], ...] = (
    (
        "dual_positive_crowded",
        "recent_return_20d_pct > 0 AND recent_return_60d_pct > 0",
    ),
    (
        "recent20_positive_60d_negative",
        "recent_return_20d_pct > 0 AND recent_return_60d_pct < 0",
    ),
    (
        "recent20_negative_60d_positive",
        "recent_return_20d_pct < 0 AND recent_return_60d_pct > 0",
    ),
    (
        "dual_negative_stress",
        "recent_return_20d_pct < 0 AND recent_return_60d_pct < 0",
    ),
)
_SHORT_OVERLAYS: tuple[tuple[str, str], ...] = (
    ("all_high_liquidity", "TRUE"),
    ("high_psr", "psr_percentile >= 0.8"),
    ("sector_weak", "sector_strength_bucket = 'sector_weak'"),
    (
        "high_psr_sector_weak",
        "psr_percentile >= 0.8 AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "overvalued_sector_weak",
        "overvalued_warning AND sector_strength_bucket = 'sector_weak'",
    ),
)


@dataclass(frozen=True)
class RankingLiquidityPriceActionRecompositionResult:
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
    coverage_diagnostics_df: pd.DataFrame
    price_action_bucket_evidence_df: pd.DataFrame
    short_overlay_evidence_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_liquidity_price_action_recomposition_research(
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
) -> RankingLiquidityPriceActionRecompositionResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_liquidity_bands = _normalize_liquidity_bands(liquidity_bands)
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
        snapshot_prefix="ranking-liquidity-price-action-recomposition-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="liquidity_price_action",
                analysis_start_date=None
                if start_date is None
                else date.fromisoformat(start_date),
                analysis_end_date=None
                if end_date is None
                else date.fromisoformat(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError(
                "liquidity price-action research requires liquidity signals"
            )
        psr_features = build_psr_features(
            ctx.connection,
            PsrFeaturesRequest(source=signal_source, namespace="liquidity_price_psr"),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="liquidity_price_sector",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(psr_features, sector_features),
            namespace="liquidity_price_action",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="liquidity_price_action_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="liquidity_price_action_outcomes",
        )
        _create_recomposition_panel(
            ctx.connection,
            source_name=evaluated.name,
            liquidity_bands=resolved_liquidity_bands,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_liquidity_price_action_recomposition_panel"
            ).fetchone()[0]
        )
        result = RankingLiquidityPriceActionRecompositionResult(
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
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            price_action_bucket_evidence_df=_build_price_action_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            short_overlay_evidence_df=_build_short_overlay_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_liquidity_price_action_recomposition_bundle(
    result: RankingLiquidityPriceActionRecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_LIQUIDITY_PRICE_ACTION_RECOMPOSITION_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_liquidity_price_action_recomposition",
        function="run_ranking_liquidity_price_action_recomposition_research",
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
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "price_action_bucket_evidence_df": result.price_action_bucket_evidence_df,
            "short_overlay_evidence_df": result.short_overlay_evidence_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingLiquidityPriceActionRecompositionResult,
) -> str:
    parts = [
        "# Ranking Liquidity Price Action Recomposition",
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
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=40),
        "",
        "## Price Action Bucket Evidence",
        "",
        _top_rows_for_markdown(result.price_action_bucket_evidence_df, limit=120),
        "",
        "## Short Overlay Evidence",
        "",
        _top_rows_for_markdown(result.short_overlay_evidence_df, limit=180),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_recomposition_panel(
    conn: Any,
    *,
    source_name: str,
    liquidity_bands: Sequence[str],
) -> None:
    liquidity_band_list = _sql_string_list(_liquidity_band_labels(liquidity_bands))
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_liquidity_price_action_recomposition_panel AS
        WITH classified AS (
            SELECT
                p.*,
                CASE
                    WHEN p.liquidity_residual_z >= 1 THEN 'high_liquidity_z_ge_1'
                    WHEN p.liquidity_residual_z > -1
                     AND p.liquidity_residual_z < 1
                        THEN 'mid_liquidity_z_minus1_to_1'
                    WHEN p.liquidity_residual_z < -1 THEN 'low_liquidity_z_lt_minus1'
                    ELSE 'liquidity_boundary_unclassified'
                END AS liquidity_band,
                CASE
                    WHEN p.recent_return_20d_pct > 0
                     AND p.recent_return_60d_pct > 0
                        THEN 'dual_positive_crowded'
                    WHEN p.recent_return_20d_pct > 0
                     AND p.recent_return_60d_pct < 0
                        THEN 'recent20_positive_60d_negative'
                    WHEN p.recent_return_20d_pct < 0
                     AND p.recent_return_60d_pct > 0
                        THEN 'recent20_negative_60d_positive'
                    WHEN p.recent_return_20d_pct < 0
                     AND p.recent_return_60d_pct < 0
                        THEN 'dual_negative_stress'
                    ELSE 'price_action_unclassified'
                END AS price_action_bucket
            FROM {source_name} p
            WHERE p.liquidity_residual_z IS NOT NULL
              AND p.recent_return_20d_pct IS NOT NULL
              AND p.recent_return_60d_pct IS NOT NULL
        )
        SELECT *
        FROM classified
        WHERE liquidity_band IN ({liquidity_band_list})
          AND price_action_bucket <> 'price_action_unclassified'
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            liquidity_band,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN liquidity_regime = 'crowded_rerating' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS crowded_rerating_rate_pct,
            avg(CASE WHEN liquidity_regime = 'distribution_stress' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS distribution_stress_rate_pct,
            avg(CASE WHEN price_action_bucket = 'dual_positive_crowded' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS dual_positive_rate_pct,
            avg(CASE WHEN price_action_bucket = 'recent20_positive_60d_negative' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS recent20_positive_60d_negative_rate_pct,
            avg(CASE WHEN price_action_bucket = 'recent20_negative_60d_positive' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS recent20_negative_60d_positive_rate_pct,
            avg(CASE WHEN price_action_bucket = 'dual_negative_stress' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS dual_negative_rate_pct,
            avg(CASE WHEN psr_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_psr_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(psr_percentile) AS median_psr_percentile
        FROM ranking_liquidity_price_action_recomposition_panel
        GROUP BY market_scope, liquidity_band
        ORDER BY market_scope, liquidity_band
        """
    ).fetchdf()


def _build_price_action_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    bucket_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PRICE_ACTION_BUCKETS)}
        ) AS price_action_bucket(
            price_action_bucket,
            price_action_bucket_order,
            price_action_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_liquidity_price_action_recomposition_panel",
                lateral_sql=bucket_lateral_sql,
                match_condition="price_action_bucket.price_action_bucket_matches",
                group_select_sql=(
                    "'price_action_bucket' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_recomposition_metric_sql(),
            )
        )
    return _concat_recomposition_sorted(
        frames,
        columns=_price_action_bucket_columns(),
    )


def _build_short_overlay_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    overlay_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PRICE_ACTION_BUCKETS)}
        ) AS price_action_bucket(
            price_action_bucket,
            price_action_bucket_order,
            price_action_bucket_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SHORT_OVERLAYS)}
        ) AS short_overlay(short_overlay, short_overlay_order, short_overlay_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_liquidity_price_action_recomposition_panel",
                lateral_sql=overlay_lateral_sql,
                match_condition=(
                    "price_action_bucket.price_action_bucket_matches "
                    "AND short_overlay.short_overlay_matches"
                ),
                group_select_sql=(
                    "'price_action_short_overlay' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    "            short_overlay.short_overlay,\n"
                    "            short_overlay.short_overlay_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "short_overlay.short_overlay, "
                    "short_overlay.short_overlay_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_recomposition_metric_sql(),
            )
        )
    return _concat_recomposition_sorted(frames, columns=_short_overlay_columns())


def _recomposition_metric_sql() -> str:
    return (
        _psr_metric_sql()
        + """,
            median(sector_strength_score) AS median_sector_strength_score,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN overvalued_warning THEN 1.0 ELSE 0.0 END)
                * 100.0 AS overvalued_warning_rate_pct,
            avg(CASE WHEN very_overvalued_warning THEN 1.0 ELSE 0.0 END)
                * 100.0 AS very_overvalued_warning_rate_pct"""
    )


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
            liquidity_band,
            price_action_bucket,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            sector_strength_score,
            sector_strength_bucket,
            psr,
            psr_percentile,
            psr_signal,
            overvalued_warning,
            very_overvalued_warning,
            forward_close_excess_return_20d_pct
        FROM ranking_liquidity_price_action_recomposition_panel
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


def _price_action_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_recomposition_metric_columns(),
    ]


def _short_overlay_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_recomposition_metric_columns(),
    ]


def _recomposition_metric_columns() -> list[str]:
    return [
        "median_sector_strength_score",
        "sector_weak_rate_pct",
        "overvalued_warning_rate_pct",
        "very_overvalued_warning_rate_pct",
    ]


def _concat_recomposition_sorted(
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
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )


def _normalize_liquidity_bands(liquidity_bands: Sequence[str]) -> tuple[str, ...]:
    aliases = {
        "high": "high",
        "high_liquidity": "high",
        "z_ge_1": "high",
        "mid": "mid",
        "middle": "mid",
        "neutral": "mid",
        "minus1_to_1": "mid",
        "low": "low",
        "stale": "low",
        "low_liquidity": "low",
        "z_lt_minus1": "low",
    }
    values: list[str] = []
    for raw_value in liquidity_bands:
        value = str(raw_value).strip()
        if not value:
            continue
        normalized = aliases.get(value)
        if normalized is None:
            raise ValueError(
                f"liquidity_bands must contain only high, mid, or low (got {value!r})"
            )
        if normalized not in values:
            values.append(normalized)
    return tuple(values)


def _liquidity_band_labels(liquidity_bands: Sequence[str]) -> tuple[str, ...]:
    labels = {
        "high": "high_liquidity_z_ge_1",
        "mid": "mid_liquidity_z_minus1_to_1",
        "low": "low_liquidity_z_lt_minus1",
    }
    return tuple(labels[value] for value in liquidity_bands)


def _sql_string_list(values: Sequence[str]) -> str:
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)
