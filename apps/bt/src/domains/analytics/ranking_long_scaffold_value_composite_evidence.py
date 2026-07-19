"""PIT value-composite evidence for Daily Ranking long-side scaffolds."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    build_long_scaffold_features,
    publish_legacy_long_scaffold_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_forecast_operating_profit_growth_evidence import (
    _aggregate_lateral_conditions,
    _aggregate_metric_columns,
    _condition_values_sql,
    _concat_sorted,
    _deep_dive_metric_columns,
    _deep_dive_metric_sql,
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
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_LONG_SCAFFOLD_VALUE_COMPOSITE_EXPERIMENT_ID = (
    "market-behavior/ranking-long-scaffold-value-composite-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 100
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_LONG_WARMUP_CALENDAR_DAYS = 820
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_LONG_SCAFFOLDS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    (
        "long_hybrid_atr20_accel",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "neutral_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "neutral_deep_value_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "value_composite_long_hybrid_atr20_accel",
        "value_composite_equal_score >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)
_VALUE_COMPOSITE_BUCKETS: tuple[tuple[str, str], ...] = (
    ("missing", "value_composite_equal_score IS NULL"),
    ("score_ge_0_90", "value_composite_equal_score >= 0.90"),
    (
        "score_0_80_to_0_90",
        "value_composite_equal_score >= 0.80 AND value_composite_equal_score < 0.90",
    ),
    (
        "score_0_70_to_0_80",
        "value_composite_equal_score >= 0.70 AND value_composite_equal_score < 0.80",
    ),
    (
        "score_0_60_to_0_70",
        "value_composite_equal_score >= 0.60 AND value_composite_equal_score < 0.70",
    ),
    (
        "score_0_50_to_0_60",
        "value_composite_equal_score >= 0.50 AND value_composite_equal_score < 0.60",
    ),
    (
        "score_0_40_to_0_50",
        "value_composite_equal_score >= 0.40 AND value_composite_equal_score < 0.50",
    ),
    (
        "score_0_30_to_0_40",
        "value_composite_equal_score >= 0.30 AND value_composite_equal_score < 0.40",
    ),
    (
        "score_0_20_to_0_30",
        "value_composite_equal_score >= 0.20 AND value_composite_equal_score < 0.30",
    ),
    ("score_lt_0_20", "value_composite_equal_score < 0.20"),
)


@dataclass(frozen=True)
class RankingLongScaffoldValueCompositeEvidenceResult:
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
    long_scaffold_evidence_df: pd.DataFrame
    value_composite_bucket_evidence_df: pd.DataFrame
    long_scaffold_value_composite_bucket_evidence_df: pd.DataFrame
    value_composite_bucket_correlation_df: pd.DataFrame
    date_basket_evidence_df: pd.DataFrame


def run_ranking_long_scaffold_value_composite_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingLongScaffoldValueCompositeEvidenceResult:
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
        warmup_calendar_days=_LONG_WARMUP_CALENDAR_DAYS,
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-long-scaffold-value-composite-",
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
        _create_value_composite_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_long_scaffold_value_composite_panel"
            ).fetchone()[0]
        )
        value_bucket_df = _build_value_composite_bucket_evidence_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        long_bucket_df = _build_long_scaffold_value_composite_bucket_evidence_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        result = RankingLongScaffoldValueCompositeEvidenceResult(
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
            long_scaffold_evidence_df=_build_long_scaffold_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            value_composite_bucket_evidence_df=value_bucket_df,
            long_scaffold_value_composite_bucket_evidence_df=long_bucket_df,
            value_composite_bucket_correlation_df=(
                _build_value_composite_bucket_correlation_df(long_bucket_df)
            ),
            date_basket_evidence_df=_build_date_basket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
        )
    return result


def write_ranking_long_scaffold_value_composite_evidence_bundle(
    result: RankingLongScaffoldValueCompositeEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_LONG_SCAFFOLD_VALUE_COMPOSITE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_long_scaffold_value_composite_evidence",
        function="run_ranking_long_scaffold_value_composite_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
            "value_composite": "equal-weight low forward PER percentile + low PBR percentile",
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
            "long_scaffold_evidence_df": result.long_scaffold_evidence_df,
            "value_composite_bucket_evidence_df": (
                result.value_composite_bucket_evidence_df
            ),
            "long_scaffold_value_composite_bucket_evidence_df": (
                result.long_scaffold_value_composite_bucket_evidence_df
            ),
            "value_composite_bucket_correlation_df": (
                result.value_composite_bucket_correlation_df
            ),
            "date_basket_evidence_df": result.date_basket_evidence_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingLongScaffoldValueCompositeEvidenceResult,
) -> str:
    parts = [
        "# Ranking Long Scaffold Value Composite Evidence",
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
        "## Long Scaffold Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_evidence_df, limit=80),
        "",
        "## Value Composite Bucket Evidence",
        "",
        _top_rows_for_markdown(result.value_composite_bucket_evidence_df, limit=120),
        "",
        "## Long Scaffold x Value Composite Bucket Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_value_composite_bucket_evidence_df,
            limit=260,
        ),
        "",
        "## Value Composite Bucket Correlation",
        "",
        _top_rows_for_markdown(result.value_composite_bucket_correlation_df, limit=120),
        "",
        "## Date Basket Evidence",
        "",
        _top_rows_for_markdown(result.date_basket_evidence_df, limit=180),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_value_composite_panel(conn: Any) -> None:
    publish_legacy_long_scaffold_features(conn)


PUBLIC_FEATURE_BUILDER = build_long_scaffold_features


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN forward_per_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS forward_per_percentile_coverage_pct,
            avg(CASE WHEN pbr_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS pbr_percentile_coverage_pct,
            avg(CASE WHEN value_composite_equal_score IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS value_composite_coverage_pct,
            avg(CASE WHEN value_composite_equal_score >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS value_composite_ge_0_8_rate_pct,
            avg(CASE WHEN value_composite_equal_score >= 0.9 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS value_composite_ge_0_9_rate_pct,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(value_composite_equal_score) AS median_value_composite_equal_score
        FROM ranking_long_scaffold_value_composite_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_value_composite_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    bucket_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_VALUE_COMPOSITE_BUCKETS)}
        ) AS value_bucket(
            value_bucket,
            value_bucket_order,
            value_bucket_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_value_composite_panel",
                lateral_sql=bucket_lateral_sql,
                match_condition="value_bucket.value_bucket_matches",
                group_select_sql=(
                    "'value_composite_bucket' AS condition_family,\n"
                    "            value_bucket.value_bucket,\n"
                    "            value_bucket.value_bucket_order,\n"
                    "            median(value_composite_equal_score) "
                    "AS median_value_composite_equal_score,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "value_bucket.value_bucket, value_bucket.value_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_value_composite_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_value_composite_bucket_columns())


def _build_long_scaffold_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    scaffold_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_value_composite_panel",
                lateral_sql=scaffold_lateral_sql,
                match_condition="long_scaffold.long_scaffold_matches",
                group_select_sql=(
                    "'long_scaffold' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_value_composite_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_columns())


def _build_long_scaffold_value_composite_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_VALUE_COMPOSITE_BUCKETS)}
        ) AS value_bucket(
            value_bucket,
            value_bucket_order,
            value_bucket_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_value_composite_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND value_bucket.value_bucket_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_value_composite_bucket' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            value_bucket.value_bucket,\n"
                    "            value_bucket.value_bucket_order,\n"
                    "            median(value_composite_equal_score) "
                    "AS median_value_composite_equal_score,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "value_bucket.value_bucket, value_bucket.value_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_value_composite_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_value_composite_bucket_columns())


def _build_value_composite_bucket_correlation_df(bucket_df: pd.DataFrame) -> pd.DataFrame:
    if bucket_df.empty:
        return pd.DataFrame(columns=_value_composite_bucket_correlation_columns())
    rows: list[dict[str, Any]] = []
    non_missing = bucket_df[
        bucket_df["value_bucket"].astype(str).ne("missing")
        & bucket_df["median_value_composite_equal_score"].notna()
        & bucket_df["median_forward_excess_return_pct"].notna()
    ].copy()
    group_columns = ["market_scope", "long_scaffold", "horizon"]
    for group_key, group in non_missing.groupby(group_columns, dropna=False, sort=False):
        market_scope, long_scaffold, horizon = tuple(group_key)
        ordered = group.sort_values("median_value_composite_equal_score")
        if len(ordered) < 3:
            continue
        score = ordered["median_value_composite_equal_score"]
        median_excess = ordered["median_forward_excess_return_pct"]
        top = ordered.nlargest(1, "median_value_composite_equal_score").iloc[0]
        bottom = ordered.nsmallest(1, "median_value_composite_equal_score").iloc[0]
        rows.append(
            {
                "market_scope": market_scope,
                "long_scaffold": long_scaffold,
                "horizon": int(str(horizon)),
                "bucket_count": int(len(ordered)),
                "score_to_median_excess_pearson": float(score.corr(median_excess)),
                "score_to_median_excess_spearman": float(
                    score.corr(median_excess, method="spearman")
                ),
                "top_bucket": top["value_bucket"],
                "top_bucket_median_score": float(top["median_value_composite_equal_score"]),
                "top_bucket_median_excess_return_pct": float(
                    top["median_forward_excess_return_pct"]
                ),
                "bottom_bucket": bottom["value_bucket"],
                "bottom_bucket_median_score": float(
                    bottom["median_value_composite_equal_score"]
                ),
                "bottom_bucket_median_excess_return_pct": float(
                    bottom["median_forward_excess_return_pct"]
                ),
                "top_minus_bottom_median_excess_return_pct": float(
                    top["median_forward_excess_return_pct"]
                    - bottom["median_forward_excess_return_pct"]
                ),
            }
        )
    return pd.DataFrame(rows, columns=_value_composite_bucket_correlation_columns())


def _build_date_basket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        return_column = f"forward_close_excess_return_{int(horizon)}d_pct"
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE value_composite_date_baskets AS
            SELECT
                long_scaffold.long_scaffold,
                long_scaffold.long_scaffold_order,
                p.market_scope,
                p.date,
                count(*) AS basket_observation_count,
                avg({return_column}) AS basket_mean_excess_return_pct
            FROM ranking_long_scaffold_value_composite_panel p
            CROSS JOIN LATERAL (
                VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
            ) AS long_scaffold(
                long_scaffold,
                long_scaffold_order,
                long_scaffold_matches
            )
            WHERE long_scaffold.long_scaffold_matches
              AND p.value_composite_equal_score >= 0.8
              AND {return_column} IS NOT NULL
            GROUP BY
                long_scaffold.long_scaffold,
                long_scaffold.long_scaffold_order,
                p.market_scope,
                p.date
            """
        )
        frames.append(
            conn.execute(
                """
                SELECT
                    'date_basket_value_composite_ge_0_8' AS condition_family,
                    long_scaffold,
                    long_scaffold_order,
                    market_scope,
                    ? AS horizon,
                    count(*) AS date_count,
                    sum(basket_observation_count) AS observation_count,
                    median(basket_observation_count) AS median_daily_observation_count,
                    avg(basket_mean_excess_return_pct) AS mean_date_basket_excess_return_pct,
                    median(basket_mean_excess_return_pct)
                        AS median_date_basket_excess_return_pct,
                    quantile_cont(basket_mean_excess_return_pct, 0.10)
                        AS p10_date_basket_excess_return_pct,
                    quantile_cont(basket_mean_excess_return_pct, 0.90)
                        AS p90_date_basket_excess_return_pct,
                    avg(CASE WHEN basket_mean_excess_return_pct > 0 THEN 1.0 ELSE 0.0 END)
                        * 100.0 AS positive_date_rate_pct,
                    avg(CASE WHEN basket_mean_excess_return_pct <= ? THEN 1.0 ELSE 0.0 END)
                        * 100.0 AS severe_date_rate_pct,
                    CASE
                        WHEN stddev_samp(basket_mean_excess_return_pct) > 0
                            THEN avg(basket_mean_excess_return_pct)
                                / stddev_samp(basket_mean_excess_return_pct)
                    END AS date_level_ir
                FROM value_composite_date_baskets
                GROUP BY long_scaffold, long_scaffold_order, market_scope
                HAVING sum(basket_observation_count) >= ?
                ORDER BY market_scope, long_scaffold_order
                """,
                [
                    int(horizon),
                    float(severe_loss_threshold_pct),
                    int(min_observations),
                ],
            ).fetchdf()
        )
    columns = _date_basket_evidence_columns()
    if not any(not frame.empty for frame in frames):
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True).reindex(columns=columns)


def _value_composite_metric_sql() -> str:
    return (
        _deep_dive_metric_sql()
        + """,
            median(low_forward_per_score) AS median_low_forward_per_score,
            median(low_pbr_score) AS median_low_pbr_score"""
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
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            low_forward_per_score,
            pbr,
            pbr_percentile,
            low_pbr_score,
            value_composite_equal_score,
            valuation_signal,
            strong_value_confirmation,
            medium_value_confirmation,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_to_atr60,
            atr20_acceleration_ex_overheat_flag,
            sector_strength_bucket,
            forward_close_excess_return_5d_pct,
            forward_close_excess_return_20d_pct
        FROM ranking_long_scaffold_value_composite_panel
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
    if not pd.notna(severe_loss_threshold_pct):
        raise ValueError("severe_loss_threshold_pct must be finite")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _value_composite_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "value_bucket",
        "value_bucket_order",
        "market_scope",
        "horizon",
        "median_value_composite_equal_score",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        "median_low_forward_per_score",
        "median_low_pbr_score",
    ]


def _long_scaffold_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "market_scope",
        "horizon",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        "median_low_forward_per_score",
        "median_low_pbr_score",
    ]


def _long_scaffold_value_composite_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "value_bucket",
        "value_bucket_order",
        "market_scope",
        "horizon",
        "median_value_composite_equal_score",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        "median_low_forward_per_score",
        "median_low_pbr_score",
    ]


def _value_composite_bucket_correlation_columns() -> list[str]:
    return [
        "market_scope",
        "long_scaffold",
        "horizon",
        "bucket_count",
        "score_to_median_excess_pearson",
        "score_to_median_excess_spearman",
        "top_bucket",
        "top_bucket_median_score",
        "top_bucket_median_excess_return_pct",
        "bottom_bucket",
        "bottom_bucket_median_score",
        "bottom_bucket_median_excess_return_pct",
        "top_minus_bottom_median_excess_return_pct",
    ]


def _date_basket_evidence_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "market_scope",
        "horizon",
        "date_count",
        "observation_count",
        "median_daily_observation_count",
        "mean_date_basket_excess_return_pct",
        "median_date_basket_excess_return_pct",
        "p10_date_basket_excess_return_pct",
        "p90_date_basket_excess_return_pct",
        "positive_date_rate_pct",
        "severe_date_rate_pct",
        "date_level_ir",
    ]
