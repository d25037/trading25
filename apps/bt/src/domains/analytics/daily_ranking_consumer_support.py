"""Public support for typed Daily Ranking research consumers."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Sequence
from uuid import uuid4

import pandas as pd

from src.domains.analytics.daily_ranking_core import percent_rank_sql
from src.domains.analytics.daily_ranking_research_base import (
    RelationRef,
    RelationSchema,
    publish_daily_ranking_signal_features,
    validate_daily_ranking_signal_relation,
)
from src.domains.analytics.readonly_duckdb_support import normalize_code_sql

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class DailyValuationPsrPercentileFeaturesRequest:
    source: RelationRef
    authority: RelationRef
    namespace: str


def build_daily_valuation_psr_percentile_features(
    conn: Any,
    request: DailyValuationPsrPercentileFeaturesRequest,
) -> RelationRef:
    """Publish actual/forward PSR percentiles from exact valuation-basis rows."""

    if not _NAMESPACE_RE.fullmatch(request.namespace):
        raise ValueError("PSR feature namespace must be a SQL identifier")
    source = request.source
    authority = request.authority
    validate_daily_ranking_signal_relation(conn, authority)
    validate_daily_ranking_signal_relation(
        conn,
        source,
        authority=authority,
        required_columns=("code", "date", "market_scope", "valuation_basis_id"),
    )
    if source.key_columns != ("code", "date", "market_scope"):
        raise ValueError("PSR features require the exact Daily Ranking signal key")

    normalized_code = normalize_code_sql("valuation.code")
    psr_sql = _finite_valuation_column_sql(conn, "psr")
    forward_psr_sql = _finite_valuation_column_sql(conn, "forward_psr")
    valuation_cte = f"""
        valuation_rows AS (
            SELECT
                {normalized_code} AS code,
                CAST(valuation.date AS DATE) AS date,
                CAST(valuation.basis_version AS VARCHAR) AS valuation_basis_id,
                {psr_sql} AS psr,
                {forward_psr_sql} AS forward_psr
            FROM daily_valuation valuation
        )
    """
    key_drift = conn.execute(
        f"""
        WITH {valuation_cte}
        SELECT source.code, source.date, source.market_scope, count(valuation.code)
        FROM {source.name} source
        LEFT JOIN valuation_rows valuation
          ON valuation.code = source.code
         AND valuation.date = source.date
         AND valuation.valuation_basis_id = source.valuation_basis_id
        GROUP BY source.code, source.date, source.market_scope
        HAVING count(valuation.code) <> 1
        LIMIT 1
        """
    ).fetchone()
    if key_drift is not None:
        raise RuntimeError(
            "daily_valuation must provide one exact valuation key per signal row"
        )

    psr_percentile = percent_rank_sql(
        value_sql="psr",
        partition_by_sql="market_scope, date",
    )
    forward_psr_percentile = percent_rank_sql(
        value_sql="forward_psr",
        partition_by_sql="market_scope, date",
    )
    relation_name = (
        f"{source.generation}_{request.namespace}_psr_percentiles_g_{uuid4().hex}"
    )
    source_types = dict(zip(source.columns, source.column_types, strict=True))
    expected_schema: RelationSchema = tuple(
        (column, source_types[column]) for column in source.key_columns
    ) + (
        ("psr", "DOUBLE"),
        ("psr_percentile", "DOUBLE"),
        ("forecast_psr", "DOUBLE"),
        ("forecast_psr_percentile", "DOUBLE"),
    )
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE {relation_name} AS
            WITH {valuation_cte},
            joined AS (
                SELECT
                    source.code,
                    source.date,
                    source.market_scope,
                    valuation.psr,
                    valuation.forward_psr
                FROM {source.name} source
                JOIN valuation_rows valuation
                  ON valuation.code = source.code
                 AND valuation.date = source.date
                 AND valuation.valuation_basis_id = source.valuation_basis_id
            ),
            psr_ranked AS (
                SELECT code, date, market_scope, psr,
                       {psr_percentile} AS psr_percentile
                FROM joined
                WHERE psr > 0
            ),
            forward_psr_ranked AS (
                SELECT code, date, market_scope, forward_psr,
                       {forward_psr_percentile} AS forward_psr_percentile
                FROM joined
                WHERE forward_psr > 0
            )
            SELECT
                joined.code,
                joined.date,
                joined.market_scope,
                joined.psr,
                psr_ranked.psr_percentile,
                joined.forward_psr AS forecast_psr,
                forward_psr_ranked.forward_psr_percentile
                    AS forecast_psr_percentile
            FROM joined
            LEFT JOIN psr_ranked USING (code, date, market_scope)
            LEFT JOIN forward_psr_ranked USING (code, date, market_scope)
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


def _finite_valuation_column_sql(conn: Any, column: str) -> str:
    exists = conn.execute(
        "SELECT count(*) FROM pragma_table_info('daily_valuation') WHERE name = ?",
        [column],
    ).fetchone()
    if not exists or int(exists[0]) == 0:
        return "CAST(NULL AS DOUBLE)"
    return (
        f"CASE WHEN isfinite(CAST(valuation.{column} AS DOUBLE)) "
        f"THEN CAST(valuation.{column} AS DOUBLE) END"
    )


def compose_daily_ranking_signal_features(
    conn: Any,
    *,
    source: RelationRef,
    features: Sequence[RelationRef],
    namespace: str,
) -> RelationRef:
    """Compose exact issued feature overlays into one explicit signal relation."""

    if not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError("feature composition namespace must be a SQL identifier")
    if not features:
        raise ValueError("feature composition requires at least one feature relation")
    feature_relations = tuple(features)
    validate_daily_ranking_signal_relation(conn, source)
    feature_alias_by_column: dict[str, str] = {}
    source_types = dict(zip(source.columns, source.column_types, strict=True))
    appended_schema: list[tuple[str, str]] = []
    for index, feature in enumerate(feature_relations):
        if feature.key_columns != source.key_columns:
            raise ValueError("feature composition keys do not match the source")
        validate_daily_ranking_signal_relation(conn, feature, authority=source)
        feature_types = dict(zip(feature.columns, feature.column_types, strict=True))
        for column in feature.columns:
            if column in feature.key_columns:
                continue
            if column in feature_alias_by_column:
                raise ValueError(f"duplicate composed feature column: {column}")
            if column in source_types and source_types[column] != feature_types[column]:
                raise ValueError(f"composed feature type mismatch: {column}")
            feature_alias_by_column[column] = f"feature_{index}"
            if column not in source_types:
                appended_schema.append((column, feature_types[column]))

    expected_schema: RelationSchema = tuple(
        (column, sql_type) for column, sql_type in zip(
            source.columns,
            source.column_types,
            strict=True,
        )
    ) + tuple(appended_schema)
    relation_name = f"{source.generation}_{namespace}_composite_g_{uuid4().hex}"
    projections = [f"source.{column} AS {column}" for column in source.columns]
    projections.extend(
        f"{feature_alias_by_column[column]}.{column} AS {column}"
        for column, _ in appended_schema
    )
    joins: list[str] = []
    for index, feature in enumerate(feature_relations):
        alias = f"feature_{index}"
        key_join = " AND ".join(
            f"{alias}.{column} = source.{column}" for column in source.key_columns
        )
        joins.append(f"JOIN {feature.name} {alias} ON {key_join}")
    try:
        conn.execute(
            f"CREATE TEMP TABLE {relation_name} AS "
            f"SELECT {', '.join(projections)} FROM {source.name} source "
            + " ".join(joins)
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


def table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE lower(table_name) = lower(?)",
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def aggregate_lateral_conditions(
    conn: Any,
    *,
    lateral_sql: str,
    match_condition: str,
    group_select_sql: str,
    group_by_sql: str,
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
    source_name: str,
    extra_metric_sql: str = "",
) -> pd.DataFrame:
    horizon_prefix = return_column.replace("forward_close_excess_return_", "")
    raw_return_column = f"forward_close_return_{horizon_prefix}"
    topix_return_expression = f"({raw_return_column} - {return_column})"
    return conn.execute(
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
            median(forecast_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(per_percentile) AS median_per_percentile,
            median(forecast_per_percentile) AS median_forward_per_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forecast_per_to_per_ratio) AS median_forward_per_to_per_ratio,
            median(p_op) AS median_p_op,
            median(forecast_p_op) AS median_forward_p_op,
            median(forecast_operating_profit_growth_ratio)
                AS median_forecast_operating_profit_growth_ratio,
            median(forecast_operating_profit_growth_pct)
                AS median_forecast_operating_profit_growth_pct,
            median(per_to_fop_growth_ratio) AS median_per_to_fop_growth_ratio,
            median(forecast_per_to_fop_growth_ratio)
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


def deep_dive_metric_sql() -> str:
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


def aggregate_metric_columns() -> list[str]:
    return [
        "observation_count", "code_count", "date_count",
        "mean_forward_return_pct", "median_forward_return_pct",
        "mean_topix_return_pct", "median_topix_return_pct",
        "mean_forward_excess_return_pct", "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct", "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct", "p90_forward_excess_return_pct",
        "excess_win_rate_pct", "negative_raw_return_rate_pct",
        "negative_excess_return_rate_pct", "severe_loss_rate_pct",
        "median_recent_return_20d_pct", "median_recent_return_60d_pct",
        "median_liquidity_residual_z", "median_per", "median_forward_per",
        "median_pbr", "median_per_percentile", "median_forward_per_percentile",
        "median_pbr_percentile", "median_forward_per_to_per_ratio",
        "median_p_op", "median_forward_p_op",
        "median_forecast_operating_profit_growth_ratio",
        "median_forecast_operating_profit_growth_pct",
        "median_per_to_fop_growth_ratio",
        "median_forward_per_to_fop_growth_ratio",
    ]


def deep_dive_metric_columns() -> list[str]:
    return [
        "median_sector_strength_score", "sector_weak_rate_pct",
        "sector_strong_rate_pct", "median_long_hybrid_leadership_score",
        "long_hybrid_strong_rate_pct", "median_atr20_change_20d_pct",
        "median_atr20_to_atr60", "atr20_acceleration_ex_overheat_rate_pct",
        "atr20_to_atr60_overheat_rate_pct", "momentum_20_60_top20_rate_pct",
        "deep_value_rate_pct", "undervalued_rate_pct",
        "overvalued_warning_rate_pct", "very_overvalued_warning_rate_pct",
        "no_positive_earnings_valuation_rate_pct",
        "no_value_confirmation_rate_pct", "weak_trend_rate_pct",
    ]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def condition_values_sql(conditions: Sequence[tuple[str, str]]) -> str:
    return ",\n            ".join(
        f"({sql_literal(name)}, {index}, ({condition}))"
        for index, (name, condition) in enumerate(conditions)
    )


def concat_sorted(
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
            "condition_family", "market_scope", "horizon", "growth_bucket_order",
            "ratio_feature", "ratio_bucket_order", "decision_scope_order",
            "deep_scope_order", "growth_condition_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(order_columns, kind="stable")
