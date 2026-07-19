"""Public, generation-bound feature overlays for Daily Ranking research."""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
import re
from typing import Any
from uuid import uuid4

from src.domains.analytics.daily_ranking_research_base import (
    RelationRef,
    RelationSchema,
    _relation_ref,
    publish_daily_ranking_signal_features,
    validate_daily_ranking_signal_relation,
)
from src.domains.analytics.readonly_duckdb_support import normalize_code_sql

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class AtrFeaturesRequest:
    source: RelationRef
    namespace: str


@dataclass(frozen=True)
class SectorStrengthFeaturesRequest:
    source: RelationRef
    population_source: RelationRef
    namespace: str


@dataclass(frozen=True)
class PsrFeaturesRequest:
    source: RelationRef
    namespace: str


@dataclass(frozen=True)
class SmaFeaturesRequest:
    source: RelationRef
    namespace: str


@dataclass(frozen=True)
class RoeFeaturesRequest:
    source: RelationRef
    namespace: str


@dataclass(frozen=True)
class LongScaffoldFeaturesRequest:
    source: RelationRef
    leadership_features: RelationRef
    short_scaffold_features: RelationRef
    namespace: str


@dataclass(frozen=True)
class ShortScaffoldFeaturesRequest:
    source: RelationRef
    atr_features: RelationRef
    namespace: str


_ATR_SCHEMA: RelationSchema = (
    ("atr20_pct", "DOUBLE"),
    ("atr60_pct", "DOUBLE"),
    ("atr20_to_atr60", "DOUBLE"),
    ("atr20_change_20d_pct", "DOUBLE"),
)
_SHORT_SCAFFOLD_SCHEMA: RelationSchema = (
    ("liquidity_regime", "VARCHAR"),
    *_ATR_SCHEMA,
    ("strong_value_confirmation", "BOOLEAN"),
    ("medium_value_confirmation", "BOOLEAN"),
    ("overvalued_percentile", "BOOLEAN"),
    ("missing_earnings_warning", "BOOLEAN"),
    ("weak_trend", "BOOLEAN"),
    ("overvalued_or_no_earnings_warning", "BOOLEAN"),
    ("no_value_confirmation", "BOOLEAN"),
    ("atr20_acceleration", "BOOLEAN"),
    ("atr20_to_atr60_overheat", "BOOLEAN"),
)
_PSR_SCHEMA: RelationSchema = (
    ("actual_sales", "DOUBLE"),
    ("actual_sales_disclosed_date", "DATE"),
    ("psr", "DOUBLE"),
    ("psr_percentile", "DOUBLE"),
    ("psr_signal", "VARCHAR"),
)
_ROE_SCHEMA: RelationSchema = (
    ("quality_disclosed_date", "DATE"),
    ("quality_period_end", "DATE"),
    ("adjusted_eps", "DOUBLE"),
    ("adjusted_bps", "DOUBLE"),
    ("adjusted_forecast_eps", "DOUBLE"),
    ("roe", "DOUBLE"),
    ("forecast_roe", "DOUBLE"),
    ("roe_percentile", "DOUBLE"),
    ("forecast_roe_percentile", "DOUBLE"),
    ("roe_signal", "VARCHAR"),
    ("forecast_roe_signal", "VARCHAR"),
)
_SMA_SCHEMA: RelationSchema = (
    ("sma5", "DOUBLE"),
    ("sma5_deviation_pct", "DOUBLE"),
    ("close_below_sma5_flag", "INTEGER"),
    ("close_below_sma5_count_3d", "INTEGER"),
    ("sma5_above_count_5d", "INTEGER"),
    ("below_sma5_streak_ge3_flag", "BOOLEAN"),
    ("sma5_below_streak_bucket", "VARCHAR"),
    ("sma5_count_group", "VARCHAR"),
    ("sma5_deviation_bucket", "VARCHAR"),
)
_SECTOR_SCHEMA: RelationSchema = (
    ("sector_33_code", "VARCHAR"),
    ("sector_33_name", "VARCHAR"),
    ("sector_observation_count", "BIGINT"),
    ("sector_code_count", "BIGINT"),
    ("sector_index_code", "VARCHAR"),
    ("sector_index_return_5d_pct", "DOUBLE"),
    ("sector_index_return_20d_pct", "DOUBLE"),
    ("sector_index_return_60d_pct", "DOUBLE"),
    ("sector_index_5d_topix_excess_pct", "DOUBLE"),
    ("sector_index_20d_topix_excess_pct", "DOUBLE"),
    ("sector_index_60d_topix_excess_pct", "DOUBLE"),
    ("sector_constituent_20d_topix_excess_pct", "DOUBLE"),
    ("sector_constituent_60d_topix_excess_pct", "DOUBLE"),
    ("sector_20d_topix_excess_pct", "DOUBLE"),
    ("sector_60d_topix_excess_pct", "DOUBLE"),
    ("sector_breadth_20d_pct", "DOUBLE"),
    ("sector_index_5d_strength_rank", "DOUBLE"),
    ("sector_20d_strength_rank", "DOUBLE"),
    ("sector_60d_strength_rank", "DOUBLE"),
    ("sector_constituent_20d_strength_rank", "DOUBLE"),
    ("sector_constituent_60d_strength_rank", "DOUBLE"),
    ("sector_breadth_strength_rank", "DOUBLE"),
    ("sector_index_strength_score", "DOUBLE"),
    ("sector_constituent_strength_score", "DOUBLE"),
    ("sector_strength_score", "DOUBLE"),
    ("sector_strength_bucket", "VARCHAR"),
    ("sector_consistency_bucket", "VARCHAR"),
)
_LONG_SCAFFOLD_SCHEMA: RelationSchema = (
    ("sector_33_code", "VARCHAR"),
    ("sector_33_name", "VARCHAR"),
    ("sector_strength_bucket", "VARCHAR"),
    ("sector_strength_score", "DOUBLE"),
    ("sector_index_strength_score", "DOUBLE"),
    ("sector_constituent_strength_score", "DOUBLE"),
    ("long_index_leadership_score", "DOUBLE"),
    ("long_constituent_breadth_leadership_score", "DOUBLE"),
    ("long_hybrid_leadership_score", "DOUBLE"),
    ("balanced_sector_strength_bucket_label", "VARCHAR"),
    ("long_hybrid_bucket_label", "VARCHAR"),
    ("momentum_20_60_top20_flag", "BOOLEAN"),
    *_ATR_SCHEMA,
    ("atr20_acceleration_flag", "BOOLEAN"),
    ("atr20_acceleration_ex_overheat_flag", "BOOLEAN"),
    ("atr20_to_atr60_overheat", "BOOLEAN"),
    ("weak_trend", "BOOLEAN"),
    ("low_forecast_per_score", "DOUBLE"),
    ("low_pbr_score", "DOUBLE"),
    ("value_composite_equal_score", "DOUBLE"),
)


def build_atr_features(conn: Any, request: AtrFeaturesRequest) -> RelationRef:
    """Project the canonical ATR family from one trusted signal relation."""

    source = request.source
    validate_daily_ranking_signal_relation(
        conn,
        source,
        required_columns=tuple(column for column, _ in _ATR_SCHEMA),
    )
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="atr",
        feature_schema=_ATR_SCHEMA,
        select_sql=",\n                ".join(
            f"CAST(source.{column} AS {sql_type}) AS {column}"
            for column, sql_type in _ATR_SCHEMA
        ),
    )


def build_short_scaffold_features(
    conn: Any,
    request: ShortScaffoldFeaturesRequest,
) -> RelationRef:
    """Build the existing short/red value, trend, and ATR flags."""

    source = request.source
    atr = request.atr_features
    validate_daily_ranking_signal_relation(
        conn,
        source,
        required_columns=(
            "liquidity_scope",
            "pbr_percentile",
            "forecast_per_percentile",
            "per_percentile",
            "forecast_per_to_per_ratio",
            "forecast_p_op_percentile",
            "recent_return_20d_pct",
            "recent_return_60d_pct",
        ),
    )
    _validate_dependency(conn, source, atr, _ATR_SCHEMA)
    strong = (
        "(source.pbr_percentile <= 0.2 AND source.forecast_per_percentile <= 0.2) "
        "OR (source.per_percentile <= 0.2 "
        "AND source.forecast_per_to_per_ratio <= 0.8)"
    )
    medium = (
        "source.pbr_percentile <= 0.2 OR (source.per_percentile <= 0.2 "
        "AND source.forecast_per_to_per_ratio <= 1.0)"
    )
    overvalued = (
        "source.per_percentile >= 0.8 OR source.forecast_per_percentile >= 0.8 "
        "OR source.forecast_p_op_percentile >= 0.8 OR source.pbr_percentile >= 0.8"
    )
    missing = (
        "source.per_percentile IS NULL AND source.forecast_per_percentile IS NULL"
    )
    weak = "source.recent_return_20d_pct <= 0 OR source.recent_return_60d_pct <= 0"
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="short_scaffold",
        feature_schema=_SHORT_SCAFFOLD_SCHEMA,
        joins_sql=f"LEFT JOIN {atr.name} atr USING ({', '.join(source.key_columns)})",
        select_sql=f"""
                CAST(source.liquidity_scope AS VARCHAR) AS liquidity_regime,
                CAST(atr.atr20_pct AS DOUBLE) AS atr20_pct,
                CAST(atr.atr60_pct AS DOUBLE) AS atr60_pct,
                CAST(atr.atr20_to_atr60 AS DOUBLE) AS atr20_to_atr60,
                CAST(atr.atr20_change_20d_pct AS DOUBLE) AS atr20_change_20d_pct,
                CAST(CASE WHEN {strong} THEN TRUE ELSE FALSE END AS BOOLEAN)
                    AS strong_value_confirmation,
                CAST(CASE WHEN {medium} THEN TRUE ELSE FALSE END AS BOOLEAN)
                    AS medium_value_confirmation,
                CAST(CASE WHEN {overvalued} THEN TRUE ELSE FALSE END AS BOOLEAN)
                    AS overvalued_percentile,
                CAST(CASE WHEN {missing} THEN TRUE ELSE FALSE END AS BOOLEAN)
                    AS missing_earnings_warning,
                CAST(CASE WHEN {weak} THEN TRUE ELSE FALSE END AS BOOLEAN) AS weak_trend,
                CAST(({overvalued}) OR ({missing}) AS BOOLEAN)
                    AS overvalued_or_no_earnings_warning,
                CAST(NOT (CASE WHEN {medium} THEN TRUE ELSE FALSE END) AS BOOLEAN)
                    AS no_value_confirmation,
                CAST(atr.atr20_change_20d_pct >= 25.0
                     AND atr.atr20_to_atr60 < 1.25 AS BOOLEAN) AS atr20_acceleration,
                CAST(atr.atr20_change_20d_pct >= 25.0
                     AND atr.atr20_to_atr60 >= 1.25 AS BOOLEAN)
                    AS atr20_to_atr60_overheat
        """,
    )


def build_psr_features(conn: Any, request: PsrFeaturesRequest) -> RelationRef:
    """Build the existing event-time FY-sales PSR features."""

    source = request.source
    validate_daily_ranking_signal_relation(
        conn,
        source,
        required_columns=(
            "market_cap_bil_jpy",
            "market_scope",
            "valuation_basis_id",
        ),
    )
    has_daily_psr = _column_exists(conn, "daily_valuation", "psr")
    if has_daily_psr:
        _assert_normalized_alias_consistency(
            conn,
            relation="daily_valuation",
            natural_key_columns=("date", "basis_version"),
            payload_columns=("psr",),
        )
    _assert_normalized_alias_consistency(
        conn,
        relation="statements",
        natural_key_columns=("disclosed_date",),
        payload_columns=("sales", "type_of_current_period", "type_of_document"),
    )
    statement_code = normalize_code_sql("statement.code")
    valuation_code = normalize_code_sql("valuation.code")
    daily_psr = "CAST(valuation.psr AS DOUBLE)"
    daily_psr_projection = (
        "CAST(valuation.psr AS DOUBLE)" if has_daily_psr else "CAST(NULL AS DOUBLE)"
    )
    ctes = f"""
        actual_fy_sales_raw AS (
            SELECT {statement_code} AS code,
                   CAST(statement.disclosed_date AS DATE) AS disclosed_date,
                   CAST(statement.sales AS DOUBLE) AS actual_sales,
                   row_number() OVER (
                       PARTITION BY {statement_code},
                                    CAST(statement.disclosed_date AS DATE)
                       ORDER BY CASE WHEN length(statement.code) = 4 THEN 0 ELSE 1 END,
                                statement.code,
                                statement.type_of_document
                   ) AS alias_rank
            FROM statements statement
            WHERE statement.sales > 0
              AND upper(statement.type_of_current_period) = 'FY'
              AND (statement.type_of_document LIKE '%FinancialStatements%'
                   OR coalesce(statement.type_of_document, '') = '')
        ),
        actual_fy_sales AS (
            SELECT code, disclosed_date, actual_sales,
                   lead(CAST(statement.disclosed_date AS DATE)) OVER (
                       PARTITION BY code ORDER BY disclosed_date
                   ) AS valid_to
            FROM actual_fy_sales_raw statement
            WHERE alias_rank = 1
        ),
        daily_valuation_normalized AS (
            SELECT {valuation_code} AS code,
                   CAST(valuation.date AS DATE) AS date,
                   CAST(valuation.basis_version AS VARCHAR) AS basis_version,
                   {daily_psr_projection} AS psr,
                   row_number() OVER (
                       PARTITION BY {valuation_code}, CAST(valuation.date AS DATE),
                                    CAST(valuation.basis_version AS VARCHAR)
                       ORDER BY CASE WHEN length(valuation.code) = 4 THEN 0 ELSE 1 END,
                                valuation.code
                   ) AS alias_rank
            FROM daily_valuation valuation
        ),
        daily_valuation_exact AS (
            SELECT * EXCLUDE (alias_rank)
            FROM daily_valuation_normalized
            WHERE alias_rank = 1
        ),
        joined AS (
            SELECT source.*,
                   sales.actual_sales,
                   sales.disclosed_date AS actual_sales_disclosed_date,
                   coalesce(
                       {daily_psr},
                       CASE WHEN source.market_cap_bil_jpy > 0 AND sales.actual_sales > 0
                            THEN source.market_cap_bil_jpy * 1000000000.0
                                 / sales.actual_sales END
                   ) AS psr
            FROM {source.name} source
            LEFT JOIN daily_valuation_exact valuation
              ON valuation.code = source.code
             AND CAST(valuation.date AS DATE) = source.date
             AND valuation.basis_version = source.valuation_basis_id
            LEFT JOIN actual_fy_sales sales
              ON sales.code = source.code
             AND sales.disclosed_date <= source.date
             AND (sales.valid_to IS NULL OR source.date < sales.valid_to)
        ),
        ranked AS (
            SELECT *,
                   count(*) FILTER (WHERE psr > 0) OVER (
                       PARTITION BY market_scope, date
                   ) AS psr_valid_count,
                   rank() OVER (
                       PARTITION BY market_scope, date
                       ORDER BY CASE WHEN psr > 0 THEN psr END NULLS LAST
                   ) AS psr_rank
            FROM joined
        )
    """
    percentile = (
        "CASE WHEN ranked.psr > 0 AND ranked.psr_valid_count <= 1 THEN 0.0 "
        "WHEN ranked.psr > 0 THEN (ranked.psr_rank - 1.0) "
        "/ (ranked.psr_valid_count - 1.0) END"
    )
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="psr",
        feature_schema=_PSR_SCHEMA,
        source_sql="ranked",
        ctes_sql=ctes,
        select_sql=f"""
                CAST(ranked.actual_sales AS DOUBLE) AS actual_sales,
                CAST(ranked.actual_sales_disclosed_date AS DATE)
                    AS actual_sales_disclosed_date,
                CAST(ranked.psr AS DOUBLE) AS psr,
                CAST(({percentile}) AS DOUBLE) AS psr_percentile,
                CAST(CASE
                    WHEN ranked.psr IS NULL THEN 'missing_psr'
                    WHEN ranked.psr_valid_count <= 1
                      OR (ranked.psr_rank - 1.0) / (ranked.psr_valid_count - 1.0) <= 0.2
                        THEN 'psr_undervalued'
                    WHEN (ranked.psr_rank - 1.0) / (ranked.psr_valid_count - 1.0) >= 0.9
                        THEN 'psr_very_overvalued'
                    WHEN (ranked.psr_rank - 1.0) / (ranked.psr_valid_count - 1.0) >= 0.8
                        THEN 'psr_overvalued'
                END AS VARCHAR) AS psr_signal
        """,
    )


def build_roe_features(conn: Any, request: RoeFeaturesRequest) -> RelationRef:
    """Build the existing event-time adjusted ROE and forward-ROE features."""

    source = request.source
    validate_daily_ranking_signal_relation(
        conn,
        source,
        required_columns=("market_scope", "valuation_basis_id"),
    )
    _assert_normalized_alias_consistency(
        conn,
        relation="statement_metrics_adjusted",
        natural_key_columns=("basis_version", "disclosed_date", "period_end"),
        payload_columns=(
            "period_type",
            "adjusted_eps",
            "adjusted_bps",
            "adjusted_forecast_eps",
        ),
    )
    metrics_code = normalize_code_sql("metrics.code")
    ctes = f"""
        quality_metrics_raw AS (
            SELECT {metrics_code} AS code,
                   CAST(metrics.basis_version AS VARCHAR) AS basis_version,
                   CAST(metrics.disclosed_date AS DATE) AS disclosed_date,
                   CAST(metrics.period_end AS DATE) AS period_end,
                   CAST(metrics.adjusted_eps AS DOUBLE) AS adjusted_eps,
                   CAST(metrics.adjusted_bps AS DOUBLE) AS adjusted_bps,
                   CAST(metrics.adjusted_forecast_eps AS DOUBLE) AS adjusted_forecast_eps,
                   CASE WHEN metrics.adjusted_bps > 0
                              AND metrics.adjusted_eps IS NOT NULL
                        THEN metrics.adjusted_eps / metrics.adjusted_bps * 100.0 END AS roe,
                   CASE WHEN metrics.adjusted_bps > 0
                              AND metrics.adjusted_forecast_eps IS NOT NULL
                        THEN metrics.adjusted_forecast_eps / metrics.adjusted_bps * 100.0
                   END AS forward_roe,
                   row_number() OVER (
                       PARTITION BY {metrics_code}, metrics.basis_version,
                                    CAST(metrics.disclosed_date AS DATE)
                       ORDER BY CASE WHEN metrics.adjusted_forecast_eps IS NOT NULL
                                     THEN 0 ELSE 1 END,
                                CAST(metrics.period_end AS DATE) DESC,
                                CASE WHEN length(metrics.code) = 4 THEN 0 ELSE 1 END,
                                metrics.code
                   ) AS same_disclosure_rank
            FROM statement_metrics_adjusted metrics
            WHERE upper(coalesce(metrics.period_type, '')) = 'FY'
              AND metrics.adjusted_bps > 0
        ),
        quality_metrics AS (
            SELECT * EXCLUDE (same_disclosure_rank),
                   lead(disclosed_date) OVER (
                       PARTITION BY code, basis_version ORDER BY disclosed_date
                   ) AS valid_to
            FROM quality_metrics_raw
            WHERE same_disclosure_rank = 1
        ),
        joined AS (
            SELECT source.*,
                   quality.disclosed_date AS quality_disclosed_date,
                   quality.period_end AS quality_period_end,
                   quality.adjusted_eps, quality.adjusted_bps,
                   quality.adjusted_forecast_eps, quality.roe, quality.forward_roe
            FROM {source.name} source
            LEFT JOIN quality_metrics quality
              ON quality.code = source.code
             AND quality.basis_version = source.valuation_basis_id
             AND quality.disclosed_date <= source.date
             AND (quality.valid_to IS NULL OR source.date < quality.valid_to)
        ),
        ranked AS (
            SELECT *,
                   count(*) FILTER (WHERE roe IS NOT NULL) OVER (
                       PARTITION BY market_scope, date
                   ) AS roe_valid_count,
                   rank() OVER (PARTITION BY market_scope, date
                                ORDER BY roe NULLS LAST) AS roe_rank,
                   count(*) FILTER (WHERE forward_roe IS NOT NULL) OVER (
                       PARTITION BY market_scope, date
                   ) AS forward_roe_valid_count,
                   rank() OVER (PARTITION BY market_scope, date
                                ORDER BY forward_roe NULLS LAST) AS forward_roe_rank
            FROM joined
        )
    """
    roe_percentile = (
        "CASE WHEN ranked.roe IS NOT NULL AND ranked.roe_valid_count <= 1 THEN 0.0 "
        "WHEN ranked.roe IS NOT NULL THEN (ranked.roe_rank - 1.0) "
        "/ (ranked.roe_valid_count - 1.0) END"
    )
    forward_percentile = (
        "CASE WHEN ranked.forward_roe IS NOT NULL "
        "AND ranked.forward_roe_valid_count <= 1 THEN 0.0 "
        "WHEN ranked.forward_roe IS NOT NULL THEN (ranked.forward_roe_rank - 1.0) "
        "/ (ranked.forward_roe_valid_count - 1.0) END"
    )
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="roe",
        feature_schema=_ROE_SCHEMA,
        source_sql="ranked",
        ctes_sql=ctes,
        select_sql=f"""
                CAST(ranked.quality_disclosed_date AS DATE) AS quality_disclosed_date,
                CAST(ranked.quality_period_end AS DATE) AS quality_period_end,
                CAST(ranked.adjusted_eps AS DOUBLE) AS adjusted_eps,
                CAST(ranked.adjusted_bps AS DOUBLE) AS adjusted_bps,
                CAST(ranked.adjusted_forecast_eps AS DOUBLE) AS adjusted_forecast_eps,
                CAST(ranked.roe AS DOUBLE) AS roe,
                CAST(ranked.forward_roe AS DOUBLE) AS forecast_roe,
                CAST(({roe_percentile}) AS DOUBLE) AS roe_percentile,
                CAST(({forward_percentile}) AS DOUBLE) AS forecast_roe_percentile,
                CAST(CASE
                    WHEN ranked.roe IS NULL THEN 'missing_roe'
                    WHEN ranked.roe_valid_count <= 1
                      OR (ranked.roe_rank - 1.0) / (ranked.roe_valid_count - 1.0) <= 0.2
                        THEN 'roe_low'
                    WHEN (ranked.roe_rank - 1.0) / (ranked.roe_valid_count - 1.0) >= 0.9
                        THEN 'roe_very_high'
                    WHEN (ranked.roe_rank - 1.0) / (ranked.roe_valid_count - 1.0) >= 0.8
                        THEN 'roe_high'
                END AS VARCHAR) AS roe_signal,
                CAST(CASE
                    WHEN ranked.forward_roe IS NULL THEN 'missing_forward_roe'
                    WHEN ranked.forward_roe_valid_count <= 1
                      OR (ranked.forward_roe_rank - 1.0)
                         / (ranked.forward_roe_valid_count - 1.0) <= 0.2
                        THEN 'forward_roe_low'
                    WHEN (ranked.forward_roe_rank - 1.0)
                         / (ranked.forward_roe_valid_count - 1.0) >= 0.9
                        THEN 'forward_roe_very_high'
                    WHEN (ranked.forward_roe_rank - 1.0)
                         / (ranked.forward_roe_valid_count - 1.0) >= 0.8
                        THEN 'forward_roe_high'
                END AS VARCHAR) AS forecast_roe_signal
        """,
    )


def build_sma_features(conn: Any, request: SmaFeaturesRequest) -> RelationRef:
    """Build the shared SMA5 count, deviation, and below-streak primitives."""

    source = request.source
    validate_daily_ranking_signal_relation(
        conn, source, required_columns=("close",)
    )
    partition = ", ".join(
        f"source.{column}" for column in source.key_columns if column != "date"
    )
    ctes = f"""
        valid_prices AS (
            SELECT source.*
            FROM {source.name} source
            WHERE source.close > 0 AND isfinite(source.close)
        ),
        sma_base AS (
            SELECT source.*,
                   avg(source.close) OVER (
                       PARTITION BY {partition} ORDER BY source.date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS sma5,
                   count(source.close) OVER (
                       PARTITION BY {partition} ORDER BY source.date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS sma5_sessions
            FROM valid_prices source
        ),
        flags AS (
            SELECT *,
                   CASE WHEN sma5_sessions = 5 AND close < sma5 THEN 1
                        WHEN sma5_sessions = 5 THEN 0 END AS below_flag,
                   CASE WHEN sma5_sessions = 5 AND close > sma5 THEN 1
                        WHEN sma5_sessions = 5 THEN 0 END AS above_flag
            FROM sma_base
        ),
        counted AS (
            SELECT *,
                   sum(below_flag) OVER (
                       PARTITION BY {partition.replace('source.', '')} ORDER BY date
                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                   ) AS below_count_3d,
                   count(below_flag) OVER (
                       PARTITION BY {partition.replace('source.', '')} ORDER BY date
                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                   ) AS below_sessions_3d,
                   sum(above_flag) OVER (
                       PARTITION BY {partition.replace('source.', '')} ORDER BY date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS above_count_5d,
                   count(above_flag) OVER (
                       PARTITION BY {partition.replace('source.', '')} ORDER BY date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS above_sessions_5d
            FROM flags
        ),
        featured AS (
            SELECT source.*,
                   counted.sma5, counted.sma5_sessions,
                   counted.below_flag, counted.above_flag,
                   counted.below_count_3d, counted.below_sessions_3d,
                   counted.above_count_5d, counted.above_sessions_5d
            FROM {source.name} source
            LEFT JOIN counted USING ({', '.join(source.key_columns)})
        )
    """
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="sma",
        feature_schema=_SMA_SCHEMA,
        source_sql="featured",
        ctes_sql=ctes,
        select_sql="""
                CAST(CASE WHEN featured.sma5_sessions = 5 THEN featured.sma5 END
                     AS DOUBLE) AS sma5,
                CAST(CASE WHEN featured.sma5_sessions = 5
                          THEN (featured.close / NULLIF(featured.sma5, 0.0) - 1.0)
                               * 100.0 END
                     AS DOUBLE) AS sma5_deviation_pct,
                CAST(featured.below_flag AS INTEGER) AS close_below_sma5_flag,
                CAST(CASE WHEN featured.below_sessions_3d = 3
                          THEN featured.below_count_3d END AS INTEGER)
                    AS close_below_sma5_count_3d,
                CAST(CASE WHEN featured.above_sessions_5d = 5
                          THEN featured.above_count_5d END AS INTEGER)
                    AS sma5_above_count_5d,
                CAST(CASE WHEN featured.below_sessions_3d = 3
                          THEN featured.below_count_3d = 3 END
                     AS BOOLEAN) AS below_sma5_streak_ge3_flag,
                CAST(CASE WHEN featured.below_sessions_3d < 3
                               OR featured.below_sessions_3d IS NULL THEN NULL
                          WHEN featured.below_count_3d = 3
                          THEN 'below_sma5_streak_ge3'
                          ELSE 'below_sma5_streak_other' END AS VARCHAR)
                    AS sma5_below_streak_bucket,
                CAST(CASE WHEN featured.above_sessions_5d = 5
                                AND featured.above_count_5d IN (0, 1)
                          THEN 'sma5_above_count_0_1'
                          WHEN featured.above_sessions_5d = 5
                                AND featured.above_count_5d IN (2, 3)
                          THEN 'sma5_above_count_2_3'
                          WHEN featured.above_sessions_5d = 5
                                AND featured.above_count_5d IN (4, 5)
                          THEN 'sma5_above_count_4_5' END AS VARCHAR) AS sma5_count_group,
                CAST(CASE
                    WHEN featured.sma5_sessions != 5
                      OR featured.sma5_sessions IS NULL THEN NULL
                    WHEN (featured.close / NULLIF(featured.sma5, 0.0) - 1.0) * 100.0 <= -2
                        THEN 'below_sma5_le_neg2'
                    WHEN (featured.close / NULLIF(featured.sma5, 0.0) - 1.0) * 100.0 <= 0
                        THEN 'below_sma5_neg2_to_0'
                    WHEN (featured.close / NULLIF(featured.sma5, 0.0) - 1.0) * 100.0 <= 2
                        THEN 'above_sma5_0_to_2'
                    WHEN (featured.close / NULLIF(featured.sma5, 0.0) - 1.0) * 100.0 <= 5
                        THEN 'above_sma5_2_to_5'
                    ELSE 'above_sma5_gt_5' END AS VARCHAR) AS sma5_deviation_bucket
        """,
    )


def build_sector_strength_features(
    conn: Any,
    request: SectorStrengthFeaturesRequest,
) -> RelationRef:
    """Build signal-time 33-sector strength without carrying forward outcomes."""

    source = request.source
    population = request.population_source
    required = (
        "market_scope",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "topix_recent_return_20d_pct",
        "topix_recent_return_60d_pct",
    )
    validate_daily_ranking_signal_relation(conn, source, required_columns=required)
    _validate_dependency(conn, source, population, ())
    validate_daily_ranking_signal_relation(
        conn, population, authority=source, required_columns=required
    )
    master_code = normalize_code_sql("master.code")
    ctes = f"""
        sector_map(sector_33_code, sector_index_code) AS (
            VALUES
                ('0050', '0040'), ('1050', '0041'), ('2050', '0042'),
                ('3050', '0043'), ('3100', '0044'), ('3150', '0045'),
                ('3200', '0046'), ('3250', '0047'), ('3300', '0048'),
                ('3350', '0049'), ('3400', '004A'), ('3450', '004B'),
                ('3500', '004C'), ('3550', '004D'), ('3600', '004E'),
                ('3650', '004F'), ('3700', '0050'), ('3750', '0051'),
                ('3800', '0052'), ('4050', '0053'), ('5050', '0054'),
                ('5100', '0055'), ('5150', '0056'), ('5200', '0057'),
                ('5250', '0058'), ('6050', '0059'), ('6100', '005A'),
                ('7050', '005B'), ('7100', '005C'), ('7150', '005D'),
                ('7200', '005E'), ('8050', '005F'), ('9050', '0060')
        ),
        sector_master AS (
            SELECT code, date, sector_33_code, sector_33_name
            FROM (
                SELECT {master_code} AS code, CAST(master.date AS DATE) AS date,
                       nullif(trim(master.sector_33_code), '') AS sector_33_code,
                       nullif(trim(master.sector_33_name), '') AS sector_33_name,
                       row_number() OVER (
                           PARTITION BY {master_code}, CAST(master.date AS DATE)
                           ORDER BY CASE WHEN length(master.code) = 4 THEN 0 ELSE 1 END,
                                    master.code
                       ) AS alias_rank
                FROM stock_master_daily master
            )
            WHERE alias_rank = 1 AND sector_33_name IS NOT NULL
        ),
        index_lagged AS (
            SELECT map.sector_33_code, map.sector_index_code,
                   CAST(indexes.date AS DATE) AS date,
                   CAST(indexes.close AS DOUBLE) AS close,
                   lag(CAST(indexes.close AS DOUBLE), 5) OVER (
                       PARTITION BY indexes.code ORDER BY CAST(indexes.date AS DATE)
                   ) AS close_5d_ago,
                   lag(CAST(indexes.close AS DOUBLE), 20) OVER (
                       PARTITION BY indexes.code ORDER BY CAST(indexes.date AS DATE)
                   ) AS close_20d_ago,
                   lag(CAST(indexes.close AS DOUBLE), 60) OVER (
                       PARTITION BY indexes.code ORDER BY CAST(indexes.date AS DATE)
                   ) AS close_60d_ago
            FROM indices_data indexes
            JOIN sector_map map ON map.sector_index_code = indexes.code
            JOIN index_master catalog
              ON catalog.code = indexes.code AND catalog.category = 'sector33'
        ),
        topix_lagged AS (
            SELECT CAST(date AS DATE) AS date, CAST(close AS DOUBLE) AS close,
                   lag(CAST(close AS DOUBLE), 5) OVER (ORDER BY CAST(date AS DATE))
                       AS close_5d_ago,
                   lag(CAST(close AS DOUBLE), 20) OVER (ORDER BY CAST(date AS DATE))
                       AS close_20d_ago,
                   lag(CAST(close AS DOUBLE), 60) OVER (ORDER BY CAST(date AS DATE))
                       AS close_60d_ago
            FROM topix_data
        ),
        index_daily AS (
            SELECT indexes.sector_33_code, indexes.sector_index_code, indexes.date,
                   100.0 * (indexes.close / NULLIF(indexes.close_5d_ago, 0) - 1.0)
                       AS sector_index_return_5d_pct,
                   100.0 * (indexes.close / NULLIF(indexes.close_20d_ago, 0) - 1.0)
                       AS sector_index_return_20d_pct,
                   100.0 * (indexes.close / NULLIF(indexes.close_60d_ago, 0) - 1.0)
                       AS sector_index_return_60d_pct,
                   100.0 * ((indexes.close / NULLIF(indexes.close_5d_ago, 0))
                            - (topix.close / NULLIF(topix.close_5d_ago, 0)))
                       AS sector_index_5d_topix_excess_pct,
                   100.0 * ((indexes.close / NULLIF(indexes.close_20d_ago, 0))
                            - (topix.close / NULLIF(topix.close_20d_ago, 0)))
                       AS sector_index_20d_topix_excess_pct,
                   100.0 * ((indexes.close / NULLIF(indexes.close_60d_ago, 0))
                            - (topix.close / NULLIF(topix.close_60d_ago, 0)))
                       AS sector_index_60d_topix_excess_pct
            FROM index_lagged indexes
            JOIN topix_lagged topix USING (date)
        ),
        sector_raw AS (
            SELECT population.market_scope, population.date,
                   master.sector_33_code, master.sector_33_name,
                   count(*) AS sector_observation_count,
                   count(DISTINCT population.code) AS sector_code_count,
                   avg(population.recent_return_20d_pct
                       - population.topix_recent_return_20d_pct)
                       AS sector_constituent_20d_topix_excess_pct,
                   avg(population.recent_return_60d_pct
                       - population.topix_recent_return_60d_pct)
                       AS sector_constituent_60d_topix_excess_pct,
                   avg(CASE WHEN population.recent_return_20d_pct
                                      IS NOT NULL
                                  AND population.topix_recent_return_20d_pct IS NOT NULL
                            THEN CASE WHEN population.recent_return_20d_pct
                                              - population.topix_recent_return_20d_pct > 0
                                      THEN 1.0 ELSE 0.0 END END) * 100.0
                       AS sector_breadth_20d_pct
            FROM {population.name} population
            JOIN sector_master master
              ON master.code = population.code AND master.date = population.date
            GROUP BY population.market_scope, population.date,
                     master.sector_33_code, master.sector_33_name
        ),
        sector_complete AS (
            SELECT raw.*, indexes.* EXCLUDE (sector_33_code, date)
            FROM sector_raw raw
            JOIN index_daily indexes
              ON indexes.sector_33_code = raw.sector_33_code
             AND indexes.date = raw.date
            WHERE raw.sector_constituent_20d_topix_excess_pct IS NOT NULL
              AND raw.sector_constituent_60d_topix_excess_pct IS NOT NULL
              AND raw.sector_breadth_20d_pct IS NOT NULL
              AND indexes.sector_index_return_5d_pct IS NOT NULL
              AND indexes.sector_index_return_20d_pct IS NOT NULL
              AND indexes.sector_index_return_60d_pct IS NOT NULL
              AND indexes.sector_index_5d_topix_excess_pct IS NOT NULL
              AND indexes.sector_index_20d_topix_excess_pct IS NOT NULL
              AND indexes.sector_index_60d_topix_excess_pct IS NOT NULL
        ),
        sector_ranked AS (
            SELECT complete.*,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_index_5d_topix_excess_pct
                   ) AS sector_index_5d_strength_rank,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_index_20d_topix_excess_pct
                   ) AS sector_20d_strength_rank,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_index_60d_topix_excess_pct
                   ) AS sector_60d_strength_rank,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_constituent_20d_topix_excess_pct
                   ) AS sector_constituent_20d_strength_rank,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_constituent_60d_topix_excess_pct
                   ) AS sector_constituent_60d_strength_rank,
                   percent_rank() OVER (
                       PARTITION BY complete.market_scope, complete.date
                       ORDER BY complete.sector_breadth_20d_pct
                   ) AS sector_breadth_strength_rank
            FROM sector_complete complete
        ),
        sector_scored AS (
            SELECT *,
                   sector_index_5d_strength_rank * 0.20
                     + sector_20d_strength_rank * 0.45
                     + sector_60d_strength_rank * 0.25
                     + sector_breadth_strength_rank * 0.10
                       AS sector_index_strength_score,
                   (sector_constituent_20d_strength_rank
                     + sector_constituent_60d_strength_rank
                     + sector_breadth_strength_rank) / 3.0
                       AS sector_constituent_strength_score
            FROM sector_ranked
        ),
        sector_state AS (
            SELECT *,
                   (sector_index_strength_score + sector_constituent_strength_score)
                     / 2.0 AS sector_strength_score
            FROM sector_scored
        ),
        featured AS (
            SELECT source.*,
                   state.sector_33_code, state.sector_33_name,
                   state.* EXCLUDE (market_scope, date, sector_33_code, sector_33_name)
            FROM {source.name} source
            LEFT JOIN sector_master master
              ON master.code = source.code AND master.date = source.date
            LEFT JOIN sector_state state
              ON state.market_scope = source.market_scope AND state.date = source.date
             AND state.sector_33_code = master.sector_33_code
             AND state.sector_33_name = master.sector_33_name
        )
    """
    feature_select = ",\n                ".join(
        _sector_feature_expression(column) for column, _ in _SECTOR_SCHEMA
    )
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="sector_strength",
        feature_schema=_SECTOR_SCHEMA,
        source_sql="featured",
        ctes_sql=ctes,
        select_sql=feature_select,
    )


def build_long_scaffold_features(
    conn: Any,
    request: LongScaffoldFeaturesRequest,
) -> RelationRef:
    """Build the existing long leadership/value-composite scaffold overlay."""

    source = request.source
    leadership = request.leadership_features
    short = request.short_scaffold_features
    validate_daily_ranking_signal_relation(
        conn,
        source,
        required_columns=(
            "forecast_per_percentile",
            "pbr_percentile",
            "recent_return_20d_pct",
        ),
    )
    leadership_required = tuple(column for column, _ in _LONG_SCAFFOLD_SCHEMA[:12])
    _validate_dependency(
        conn,
        source,
        leadership,
        tuple((name, "") for name in leadership_required),
    )
    _validate_dependency(conn, source, short, _SHORT_SCAFFOLD_SCHEMA)
    key_join = ", ".join(source.key_columns)
    low_forward = (
        "CASE WHEN source.forecast_per_percentile IS NOT NULL "
        "THEN 1.0 - source.forecast_per_percentile END"
    )
    low_pbr = (
        "CASE WHEN source.pbr_percentile IS NOT NULL "
        "THEN 1.0 - source.pbr_percentile END"
    )
    return _materialize(
        conn,
        source=source,
        namespace=request.namespace,
        family="long_scaffold",
        feature_schema=_LONG_SCAFFOLD_SCHEMA,
        joins_sql=(
            f"LEFT JOIN {leadership.name} leadership USING ({key_join})\n"
            f"LEFT JOIN {short.name} short USING ({key_join})"
        ),
        select_sql=f"""
                CAST(leadership.sector_33_code AS VARCHAR) AS sector_33_code,
                CAST(leadership.sector_33_name AS VARCHAR) AS sector_33_name,
                CAST(leadership.sector_strength_bucket AS VARCHAR)
                    AS sector_strength_bucket,
                CAST(leadership.sector_strength_score AS DOUBLE) AS sector_strength_score,
                CAST(leadership.sector_index_strength_score AS DOUBLE)
                    AS sector_index_strength_score,
                CAST(leadership.sector_constituent_strength_score AS DOUBLE)
                    AS sector_constituent_strength_score,
                CAST(leadership.long_index_leadership_score AS DOUBLE)
                    AS long_index_leadership_score,
                CAST(leadership.long_constituent_breadth_leadership_score AS DOUBLE)
                    AS long_constituent_breadth_leadership_score,
                CAST(leadership.long_hybrid_leadership_score AS DOUBLE)
                    AS long_hybrid_leadership_score,
                CAST(leadership.balanced_sector_strength_bucket_label AS VARCHAR)
                    AS balanced_sector_strength_bucket_label,
                CAST(leadership.long_hybrid_bucket_label AS VARCHAR)
                    AS long_hybrid_bucket_label,
                CAST(coalesce(leadership.momentum_20_60_top20_flag, FALSE) AS BOOLEAN)
                    AS momentum_20_60_top20_flag,
                CAST(short.atr20_pct AS DOUBLE) AS atr20_pct,
                CAST(short.atr60_pct AS DOUBLE) AS atr60_pct,
                CAST(short.atr20_to_atr60 AS DOUBLE) AS atr20_to_atr60,
                CAST(short.atr20_change_20d_pct AS DOUBLE) AS atr20_change_20d_pct,
                CAST(coalesce(short.atr20_acceleration, FALSE) AS BOOLEAN)
                    AS atr20_acceleration_flag,
                CAST(coalesce(short.atr20_acceleration
                         AND coalesce(source.recent_return_20d_pct, 0.0) < 30.0,
                         FALSE) AS BOOLEAN) AS atr20_acceleration_ex_overheat_flag,
                CAST(coalesce(short.atr20_to_atr60_overheat, FALSE) AS BOOLEAN)
                    AS atr20_to_atr60_overheat,
                CAST(coalesce(short.weak_trend, FALSE) AS BOOLEAN) AS weak_trend,
                CAST(({low_forward}) AS DOUBLE) AS low_forecast_per_score,
                CAST(({low_pbr}) AS DOUBLE) AS low_pbr_score,
                CAST(CASE WHEN ({low_forward}) IS NOT NULL AND ({low_pbr}) IS NOT NULL
                          THEN (({low_forward}) + ({low_pbr})) / 2.0 END AS DOUBLE)
                    AS value_composite_equal_score
        """,
    )


def _cleans_legacy_intermediates(publisher: Any) -> Any:
    """Keep fixed legacy overlays while removing every UUID work table."""

    @wraps(publisher)
    def wrapped(conn: Any) -> None:
        before = _legacy_intermediate_names(conn)
        try:
            publisher(conn)
        finally:
            for name in sorted(_legacy_intermediate_names(conn) - before):
                if not _NAMESPACE_RE.fullmatch(name):
                    raise RuntimeError(f"invalid legacy intermediate name: {name!r}")
                conn.execute(f"DROP TABLE IF EXISTS {name}")

    return wrapped


@_cleans_legacy_intermediates
def publish_legacy_psr_features(conn: Any) -> None:
    """Task 8 bridge: publish the legacy PSR panel through the public builder."""

    source = _legacy_source_ref(
        conn,
        relation="daily_ranking_research_ranked",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            ("valuation_basis_id", "valuation_basis_id"),
            ("market_cap_bil_jpy", "market_cap_bil_jpy"),
        ),
    )
    features = build_psr_features(
        conn, PsrFeaturesRequest(source=source, namespace="legacy_psr")
    )
    _publish_legacy_overlay(
        conn,
        source_relation="daily_ranking_research_ranked",
        features=features,
        output_relation="ranking_psr_valuation_panel",
        feature_columns=tuple(column for column, _ in _PSR_SCHEMA),
    )


@_cleans_legacy_intermediates
def publish_legacy_roe_features(conn: Any) -> None:
    """Task 8 bridge: publish the legacy ROE panel through the public builder."""

    source = _legacy_source_ref(
        conn,
        relation="daily_ranking_research_ranked",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            ("valuation_basis_id", "valuation_basis_id"),
        ),
    )
    features = build_roe_features(
        conn, RoeFeaturesRequest(source=source, namespace="legacy_roe")
    )
    aliases = {
        "forecast_roe": "forward_roe",
        "forecast_roe_percentile": "forward_roe_percentile",
        "forecast_roe_signal": "forward_roe_signal",
    }
    _publish_legacy_overlay(
        conn,
        source_relation="daily_ranking_research_ranked",
        features=features,
        output_relation="ranking_roe_quality_panel",
        feature_columns=tuple(column for column, _ in _ROE_SCHEMA),
        aliases=aliases,
    )


@_cleans_legacy_intermediates
def publish_legacy_short_scaffold_features(conn: Any) -> None:
    """Task 8 bridge: publish the legacy short/red panel through the public builder."""

    source_columns = (
        ("code", "code"),
        ("date", "date"),
        ("market_scope", "market_scope"),
        ("liquidity_scope", "liquidity_scope"),
        ("pbr_percentile", "pbr_percentile"),
        ("forecast_per_percentile", "forward_per_percentile"),
        ("per_percentile", "per_percentile"),
        ("forecast_per_to_per_ratio", "forward_per_to_per_ratio"),
        ("forecast_p_op_percentile", "forward_p_op_percentile"),
        ("recent_return_20d_pct", "recent_return_20d_pct"),
        ("recent_return_60d_pct", "recent_return_60d_pct"),
    )
    source = _legacy_source_ref(
        conn,
        relation="daily_ranking_research_liquidity_ranked",
        columns=source_columns,
        where=(
            "liquidity_scope IN ('crowded_rerating', 'distribution_stress', "
            "'stale_liquidity', 'neutral_rerating', 'neutral')"
        ),
    )
    atr = _legacy_source_ref(
        conn,
        relation="atr_expansion_scoped",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            *((column, column) for column, _ in _ATR_SCHEMA),
        ),
        authority=source,
    )
    features = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=source,
            atr_features=atr,
            namespace="legacy_short_scaffold",
        ),
    )
    _publish_legacy_overlay(
        conn,
        source_relation="daily_ranking_research_liquidity_ranked",
        features=features,
        output_relation="ranking_short_red_feature_panel",
        feature_columns=tuple(column for column, _ in _SHORT_SCAFFOLD_SCHEMA),
        where=(
            "source.liquidity_scope IN ('crowded_rerating', 'distribution_stress', "
            "'stale_liquidity', 'neutral_rerating', 'neutral')"
        ),
    )


@_cleans_legacy_intermediates
def publish_legacy_long_scaffold_features(conn: Any) -> None:
    """Task 8 bridge: publish the legacy value-composite panel via the public builder."""

    source = _legacy_source_ref(
        conn,
        relation="daily_ranking_research_ranked",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            ("forecast_per_percentile", "forward_per_percentile"),
            ("pbr_percentile", "pbr_percentile"),
            ("recent_return_20d_pct", "recent_return_20d_pct"),
        ),
    )
    leadership = _legacy_source_ref(
        conn,
        relation="long_sector_leadership_base_panel",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            *((column, column) for column, _ in _LONG_SCAFFOLD_SCHEMA[:12]),
        ),
        authority=source,
    )
    short = _legacy_source_ref(
        conn,
        relation="ranking_short_red_feature_panel",
        columns=(
            ("code", "code"),
            ("date", "date"),
            ("market_scope", "market_scope"),
            *((column, column) for column, _ in _SHORT_SCAFFOLD_SCHEMA),
        ),
        authority=source,
    )
    features = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=source,
            leadership_features=leadership,
            short_scaffold_features=short,
            namespace="legacy_long_scaffold",
        ),
    )
    _publish_legacy_overlay(
        conn,
        source_relation="daily_ranking_research_ranked",
        features=features,
        output_relation="ranking_long_scaffold_value_composite_panel",
        feature_columns=tuple(column for column, _ in _LONG_SCAFFOLD_SCHEMA),
        aliases={"low_forecast_per_score": "low_forward_per_score"},
    )


def _materialize(
    conn: Any,
    *,
    source: RelationRef,
    namespace: str,
    family: str,
    feature_schema: RelationSchema,
    select_sql: str,
    source_sql: str = "source",
    ctes_sql: str = "",
    joins_sql: str = "",
) -> RelationRef:
    _validate_namespace(namespace)
    relation_name = f"{source.generation}_{namespace}_{family}_g_{uuid4().hex}"
    source_types = dict(zip(source.columns, source.column_types, strict=True))
    key_schema: RelationSchema = tuple(
        (column, source_types[column]) for column in source.key_columns
    )
    expected_schema = key_schema + feature_schema
    key_select = ", ".join(
        f"CAST({source_sql}.{column} AS {source_types[column]}) AS {column}"
        for column in source.key_columns
    )
    with_sql = "" if not ctes_sql else f"WITH {ctes_sql}"
    from_sql = f"{source.name} source" if source_sql == "source" else source_sql
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE {relation_name} AS
            {with_sql}
            SELECT {key_select},
                   {select_sql}
            FROM {from_sql}
            {joins_sql}
            """
        )
        return publish_daily_ranking_signal_features(
            conn,
            source=source,
            relation_name=relation_name,
            expected_schema=expected_schema,
        )
    except Exception:
        conn.execute(f"DROP TABLE IF EXISTS {relation_name}")
        raise


def _validate_dependency(
    conn: Any,
    source: RelationRef,
    dependency: RelationRef,
    schema: RelationSchema,
) -> None:
    if dependency.key_columns != source.key_columns:
        raise ValueError("signal feature dependency keys do not match source keys")
    validate_daily_ranking_signal_relation(
        conn,
        dependency,
        authority=source,
        required_columns=tuple(column for column, _ in schema),
    )


def _legacy_source_ref(
    conn: Any,
    *,
    relation: str,
    columns: tuple[tuple[str, str], ...],
    where: str = "TRUE",
    authority: RelationRef | None = None,
) -> RelationRef:
    generation = (
        f"legacy_feature_g_{uuid4().hex}"
        if authority is None
        else authority.generation
    )
    capability = object() if authority is None else authority._capability
    name = f"{generation}_source_g_{uuid4().hex}"
    projection = ", ".join(
        (
            f"CAST({source_column} AS DATE) AS {canonical_column}"
            if canonical_column == "date"
            else f"{source_column} AS {canonical_column}"
        )
        for canonical_column, source_column in columns
    )
    try:
        conn.execute(
            f"CREATE TEMP TABLE {name} AS "
            f"SELECT {projection} FROM {relation} WHERE {where}"
        )
        schema = tuple(
            (str(row[1]), str(row[2]).upper())
            for row in conn.execute(f"PRAGMA table_info('{name}')").fetchall()
        )
        return _relation_ref(
            conn,
            name,
            key_columns=("code", "date", "market_scope"),
            expected_schema=schema,
            generation=generation,
            kind="signal_features",
            capability=capability,
            forbid_outcomes=True,
        )
    except Exception:
        conn.execute(f"DROP TABLE IF EXISTS {name}")
        raise


def _publish_legacy_overlay(
    conn: Any,
    *,
    source_relation: str,
    features: RelationRef,
    output_relation: str,
    feature_columns: tuple[str, ...],
    aliases: dict[str, str] | None = None,
    where: str = "TRUE",
) -> None:
    aliases = {} if aliases is None else aliases
    key_join = " AND ".join(
        f"features.{column} = source.{column}" for column in features.key_columns
    )
    projection = ", ".join(
        f"features.{column} AS {aliases.get(column, column)}"
        for column in feature_columns
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output_relation} AS
        SELECT source.*, {projection}
        FROM {source_relation} source
        JOIN {features.name} features ON {key_join}
        WHERE {where}
        """
    )


def _validate_namespace(namespace: str) -> None:
    if not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError("feature namespace must use lowercase ASCII identifiers")


def _column_exists(conn: Any, relation: str, column: str) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM pragma_table_info(?) WHERE name = ?",
        [relation, column],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _assert_normalized_alias_consistency(
    conn: Any,
    *,
    relation: str,
    natural_key_columns: tuple[str, ...],
    payload_columns: tuple[str, ...],
) -> None:
    """Reject conflicting 4/5-digit aliases before deterministic de-duplication."""

    for identifier in (relation, "code", *natural_key_columns, *payload_columns):
        if not _NAMESPACE_RE.fullmatch(identifier):
            raise ValueError(f"invalid SQL identifier: {identifier!r}")
    normalized_code = normalize_code_sql("source.code")
    natural_keys = ", ".join(f"source.{column}" for column in natural_key_columns)
    payload = ", ".join(
        f"{column} := source.{column}" for column in payload_columns
    )
    conflict = conn.execute(
        f"""
        SELECT {normalized_code} AS code, {natural_keys}
        FROM {relation} source
        GROUP BY {normalized_code}, {natural_keys}
        HAVING count(DISTINCT source.code) > 1
           AND count(DISTINCT struct_pack({payload})) > 1
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        raise RuntimeError(
            f"{relation} normalized code aliases contain conflicting payloads: "
            f"code={conflict[0]}"
        )


def _legacy_intermediate_names(conn: Any) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE temporary AND table_name LIKE 'legacy_feature_g_%'"
        ).fetchall()
    }


def _sector_feature_expression(column: str) -> str:
    if column == "sector_20d_topix_excess_pct":
        return "CAST(featured.sector_index_20d_topix_excess_pct AS DOUBLE) AS sector_20d_topix_excess_pct"
    if column == "sector_60d_topix_excess_pct":
        return "CAST(featured.sector_index_60d_topix_excess_pct AS DOUBLE) AS sector_60d_topix_excess_pct"
    if column == "sector_strength_bucket":
        return """CAST(CASE
            WHEN featured.sector_strength_score IS NULL THEN NULL
            WHEN featured.sector_strength_score >= 0.799999 THEN 'sector_strong'
            WHEN featured.sector_strength_score <= 0.200001 THEN 'sector_weak'
            ELSE 'sector_neutral' END AS VARCHAR) AS sector_strength_bucket"""
    if column == "sector_consistency_bucket":
        return """CAST(CASE
            WHEN featured.sector_strength_score IS NULL THEN NULL
            WHEN featured.sector_index_20d_topix_excess_pct > 0
             AND featured.sector_index_60d_topix_excess_pct > 0
             AND featured.sector_strength_score >= 0.7 THEN 'sector_strong_consistent'
            WHEN featured.sector_index_20d_topix_excess_pct < 0
             AND featured.sector_index_60d_topix_excess_pct < 0
             AND featured.sector_strength_score <= 0.3 THEN 'sector_weak_consistent'
            ELSE 'sector_mixed' END AS VARCHAR) AS sector_consistency_bucket"""
    sql_type = dict(_SECTOR_SCHEMA)[column]
    return f"CAST(featured.{column} AS {sql_type}) AS {column}"
