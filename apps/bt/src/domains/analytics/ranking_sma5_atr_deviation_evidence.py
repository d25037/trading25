"""ATR-normalized SMA5 deviation evidence for Daily Ranking diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_forecast_operating_profit_growth_evidence import (
    _aggregate_lateral_conditions,
    _aggregate_metric_columns,
    _condition_values_sql,
    _deep_dive_metric_columns,
    _deep_dive_metric_sql,
)
from src.domains.analytics.ranking_liquidity_price_action_recomposition import (
    _PRICE_ACTION_BUCKETS,
    _SHORT_OVERLAYS,
    _normalize_liquidity_bands,
    _recomposition_metric_columns,
    _recomposition_metric_sql,
)
from src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition import (
    _create_long_sector_leadership_tables,
    _create_long_signal_tables,
)
from src.domains.analytics.ranking_psr_valuation_evidence import (
    _create_psr_valuation_panel,
    _psr_metric_columns,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    _create_sector_strength_tables,
)
from src.domains.analytics.ranking_short_red_evidence import (
    _create_feature_panel as _create_short_red_feature_panel,
)
from src.domains.analytics.ranking_sma5_count_long_evidence import _LONG_SCAFFOLDS
from src.domains.analytics.ranking_sma5_deviation_evidence import (
    DEFAULT_HORIZONS,
    DEFAULT_LIQUIDITY_BANDS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    _LEADERSHIP_WINDOWS,
    _REQUIRED_RETURN_WINDOWS,
    _REQUIRED_TABLES,
    _WARMUP_CALENDAR_DAYS,
    _assert_required_tables,
    _concat_sorted,
    _create_sma5_deviation_panel,
    _validate_params as _validate_base_params,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

RANKING_SMA5_ATR_DEVIATION_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-atr-deviation-evidence"
)
DEFAULT_ATR_WINDOWS: tuple[int, ...] = (5, 20)
DEFAULT_THRESHOLD_ABS_ATR: tuple[float, ...] = (0.5, 1.0, 1.5, 2.0)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_SMA5_ATR_DEVIATION_BUCKETS: tuple[tuple[str, str], ...] = (
    ("below_le_neg2_atr", "{column} <= -2.0"),
    ("below_neg2_to_neg1_atr", "{column} > -2.0 AND {column} <= -1.0"),
    ("below_neg1_to_neg05_atr", "{column} > -1.0 AND {column} <= -0.5"),
    ("below_neg05_to_0_atr", "{column} > -0.5 AND {column} <= 0.0"),
    ("above_0_to_05_atr", "{column} > 0.0 AND {column} <= 0.5"),
    ("above_05_to_1_atr", "{column} > 0.5 AND {column} <= 1.0"),
    ("above_1_to_2_atr", "{column} > 1.0 AND {column} <= 2.0"),
    ("above_gt_2_atr", "{column} > 2.0"),
)


@dataclass(frozen=True)
class RankingSma5AtrDeviationEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    atr_windows: tuple[int, ...]
    threshold_abs_atr: tuple[float, ...]
    market_scopes: tuple[str, ...]
    liquidity_bands: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    sma5_atr_deviation_bucket_evidence_df: pd.DataFrame
    long_scaffold_sma5_atr_threshold_evidence_df: pd.DataFrame
    short_overlay_sma5_atr_threshold_evidence_df: pd.DataFrame


def run_ranking_sma5_atr_deviation_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    atr_windows: Iterable[int] = DEFAULT_ATR_WINDOWS,
    threshold_abs_atr: Iterable[float] = DEFAULT_THRESHOLD_ABS_ATR,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    liquidity_bands: Sequence[str] = DEFAULT_LIQUIDITY_BANDS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5AtrDeviationEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_atr_windows = tuple(sorted({int(window) for window in atr_windows}))
    resolved_thresholds = tuple(sorted({float(value) for value in threshold_abs_atr}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_liquidity_bands = _normalize_liquidity_bands(liquidity_bands)
    _validate_params(
        horizons=resolved_horizons,
        atr_windows=resolved_atr_windows,
        threshold_abs_atr=resolved_thresholds,
        liquidity_bands=resolved_liquidity_bands,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=max(_WARMUP_CALENDAR_DAYS, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-atr-deviation-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
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
            include_relation_percentiles=False,
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
        _create_psr_valuation_panel(ctx.connection)
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
        _create_sma5_deviation_panel(
            ctx.connection,
            liquidity_bands=resolved_liquidity_bands,
        )
        _create_sma5_atr_deviation_panel(
            ctx.connection,
            atr_windows=resolved_atr_windows,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sma5_atr_deviation_panel"
            ).fetchone()[0]
        )
        result = RankingSma5AtrDeviationEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            atr_windows=resolved_atr_windows,
            threshold_abs_atr=resolved_thresholds,
            market_scopes=resolved_market_scopes,
            liquidity_bands=resolved_liquidity_bands,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                atr_windows=resolved_atr_windows,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(
                ctx.connection,
                atr_windows=resolved_atr_windows,
            ),
            sma5_atr_deviation_bucket_evidence_df=(
                _build_sma5_atr_deviation_bucket_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    atr_windows=resolved_atr_windows,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            long_scaffold_sma5_atr_threshold_evidence_df=(
                _build_long_scaffold_sma5_atr_threshold_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    atr_windows=resolved_atr_windows,
                    threshold_abs_atr=resolved_thresholds,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            short_overlay_sma5_atr_threshold_evidence_df=(
                _build_short_overlay_sma5_atr_threshold_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    atr_windows=resolved_atr_windows,
                    threshold_abs_atr=resolved_thresholds,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_sma5_atr_deviation_evidence_bundle(
    result: RankingSma5AtrDeviationEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_ATR_DEVIATION_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_atr_deviation_evidence",
        function="run_ranking_sma5_atr_deviation_evidence_research",
        params={
            "horizons": list(result.horizons),
            "atr_windows": list(result.atr_windows),
            "threshold_abs_atr": list(result.threshold_abs_atr),
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
            "sma5_parameter": "sma5_atrN_deviation = (close - sma5) / atrN",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "sma5_atr_deviation_bucket_evidence_df": (
                result.sma5_atr_deviation_bucket_evidence_df
            ),
            "long_scaffold_sma5_atr_threshold_evidence_df": (
                result.long_scaffold_sma5_atr_threshold_evidence_df
            ),
            "short_overlay_sma5_atr_threshold_evidence_df": (
                result.short_overlay_sma5_atr_threshold_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5AtrDeviationEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 ATR Deviation Evidence",
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
        f"- atr_windows: `{', '.join(str(item) for item in result.atr_windows)}`",
        f"- threshold_abs_atr: `{', '.join(str(item) for item in result.threshold_abs_atr)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- liquidity_bands: `{', '.join(result.liquidity_bands)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=120),
        "",
        "## SMA5 ATR Deviation Bucket Evidence",
        "",
        _top_rows_for_markdown(result.sma5_atr_deviation_bucket_evidence_df, limit=220),
        "",
        "## Long Scaffold x SMA5 ATR Threshold Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_sma5_atr_threshold_evidence_df,
            limit=320,
        ),
        "",
        "## Short Overlay x SMA5 ATR Threshold Evidence",
        "",
        _top_rows_for_markdown(
            result.short_overlay_sma5_atr_threshold_evidence_df,
            limit=320,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _create_sma5_atr_deviation_panel(
    conn: Any,
    *,
    atr_windows: Sequence[int],
) -> None:
    stock_code = normalize_code_sql("sd.code")
    atr_exprs = ",\n                ".join(
        (
            "avg(true_range) OVER ("
            f"PARTITION BY code ORDER BY date ROWS BETWEEN {int(window) - 1} "
            f"PRECEDING AND CURRENT ROW) AS atr{int(window)}"
        )
        for window in atr_windows
    )
    atr_count_exprs = ",\n                ".join(
        (
            "count(true_range) OVER ("
            f"PARTITION BY code ORDER BY date ROWS BETWEEN {int(window) - 1} "
            f"PRECEDING AND CURRENT ROW) AS atr{int(window)}_sessions"
        )
        for window in atr_windows
    )
    select_atr_columns = ",\n            ".join(
        f"a.atr{int(window)}, a.atr{int(window)}_sessions" for window in atr_windows
    )
    deviation_columns = ",\n            ".join(
        (
            f"CASE WHEN a.atr{int(window)}_sessions = {int(window)} "
            f"AND a.atr{int(window)} > 0 "
            f"THEN (p.close - p.sma5) / a.atr{int(window)} "
            f"END AS sma5_atr{int(window)}_deviation"
        )
        for window in atr_windows
    )
    bucket_columns = ",\n            ".join(
        _bucket_case_sql(
            f"sma5_atr{int(window)}_deviation",
            f"sma5_atr{int(window)}_deviation_bucket",
        )
        for window in atr_windows
    )
    non_null_filter = " OR ".join(
        f"sma5_atr{int(window)}_deviation IS NOT NULL" for window in atr_windows
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_atr_values AS
        WITH normalized_prices AS (
            SELECT
                {stock_code} AS code,
                sd.date,
                arg_min(sd.open, CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code) AS open,
                arg_min(sd.high, CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code) AS high,
                arg_min(sd.low, CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code) AS low,
                arg_min(sd.close, CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code) AS close
            FROM stock_data sd
            WHERE EXISTS (
                SELECT 1
                FROM ranking_sma5_deviation_panel p
                WHERE p.code = {stock_code}
            )
            GROUP BY {stock_code}, sd.date
        ),
        true_range_base AS (
            SELECT
                *,
                lag(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
            FROM normalized_prices
            WHERE open > 0
              AND high > 0
              AND low > 0
              AND close > 0
        ),
        true_range AS (
            SELECT
                *,
                greatest(
                    high - low,
                    coalesce(abs(high - prev_close), 0.0),
                    coalesce(abs(low - prev_close), 0.0)
                ) AS true_range
            FROM true_range_base
        )
        SELECT
            code,
            date,
            {atr_exprs},
            {atr_count_exprs}
        FROM true_range
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_atr_deviation_panel AS
        WITH joined AS (
            SELECT
                p.*,
                {select_atr_columns},
                {deviation_columns}
            FROM ranking_sma5_deviation_panel p
            LEFT JOIN ranking_sma5_atr_values a
              ON a.code = p.code
             AND a.date = p.date
        )
        SELECT
            *,
            {bucket_columns}
        FROM joined
        WHERE {non_null_filter}
        """
    )


def _bucket_case_sql(column: str, alias: str) -> str:
    return f"""CASE
                WHEN {column} <= -2.0 THEN 'below_le_neg2_atr'
                WHEN {column} <= -1.0 THEN 'below_neg2_to_neg1_atr'
                WHEN {column} <= -0.5 THEN 'below_neg1_to_neg05_atr'
                WHEN {column} <= 0.0 THEN 'below_neg05_to_0_atr'
                WHEN {column} <= 0.5 THEN 'above_0_to_05_atr'
                WHEN {column} <= 1.0 THEN 'above_05_to_1_atr'
                WHEN {column} <= 2.0 THEN 'above_1_to_2_atr'
                ELSE 'above_gt_2_atr'
            END AS {alias}"""


def _build_coverage_diagnostics_df(
    conn: Any,
    *,
    atr_windows: Sequence[int],
) -> pd.DataFrame:
    frames = []
    for window in atr_windows:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(window)} AS atr_window,
                    sma5_atr{int(window)}_deviation_bucket AS sma5_atr_deviation_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    median(sma5_atr{int(window)}_deviation)
                        AS median_sma5_atr_deviation,
                    quantile_cont(sma5_atr{int(window)}_deviation, 0.1)
                        AS p10_sma5_atr_deviation,
                    quantile_cont(sma5_atr{int(window)}_deviation, 0.9)
                        AS p90_sma5_atr_deviation,
                    median(sma5_deviation_pct) AS median_sma5_deviation_pct,
                    median(atr{int(window)}) AS median_atr
                FROM ranking_sma5_atr_deviation_panel
                WHERE sma5_atr{int(window)}_deviation IS NOT NULL
                GROUP BY market_scope, sma5_atr{int(window)}_deviation_bucket
                ORDER BY market_scope, min(sma5_atr{int(window)}_deviation)
                """
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_coverage_diagnostics_columns())


def _build_sma5_atr_deviation_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    atr_windows: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in atr_windows:
        column = f"sma5_atr{int(window)}_deviation"
        bucket_conditions = tuple(
            (label, condition.format(column=column))
            for label, condition in _SMA5_ATR_DEVIATION_BUCKETS
        )
        lateral_sql = f"""
            CROSS JOIN LATERAL (
                VALUES {_condition_values_sql(bucket_conditions)}
            ) AS sma5_atr_deviation_bucket(
                sma5_atr_deviation_bucket,
                sma5_atr_deviation_bucket_order,
                sma5_atr_deviation_bucket_matches
            )
        """
        for horizon in horizons:
            frames.append(
                _aggregate_lateral_conditions(
                    conn,
                    source_name="ranking_sma5_atr_deviation_panel",
                    lateral_sql=lateral_sql,
                    match_condition=(
                        "sma5_atr_deviation_bucket."
                        "sma5_atr_deviation_bucket_matches"
                    ),
                    group_select_sql=(
                        "'sma5_atr_deviation_bucket' AS condition_family,\n"
                        f"            {int(window)} AS atr_window,\n"
                        "            sma5_atr_deviation_bucket.sma5_atr_deviation_bucket,\n"
                        "            sma5_atr_deviation_bucket.sma5_atr_deviation_bucket_order,\n"
                        f"            {int(horizon)} AS horizon"
                    ),
                    group_by_sql=(
                        "sma5_atr_deviation_bucket.sma5_atr_deviation_bucket, "
                        "sma5_atr_deviation_bucket.sma5_atr_deviation_bucket_order, "
                        "market_scope"
                    ),
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    extra_metric_sql=_sma5_atr_deviation_metric_sql(window),
                )
            )
    return _concat_sorted(frames, columns=_sma5_atr_deviation_bucket_columns())


def _build_long_scaffold_sma5_atr_threshold_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    atr_windows: Sequence[int],
    threshold_abs_atr: Sequence[float],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in atr_windows:
        lateral_sql = f"""
            CROSS JOIN LATERAL (
                VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
            ) AS long_scaffold(
                long_scaffold,
                long_scaffold_order,
                long_scaffold_matches
            )
            CROSS JOIN LATERAL (
                VALUES {_threshold_values_sql(window, threshold_abs_atr)}
            ) AS atr_threshold(
                threshold_label,
                threshold_order,
                direction,
                threshold_abs_atr,
                threshold_matches
            )
        """
        for horizon in horizons:
            frames.append(
                _aggregate_lateral_conditions(
                    conn,
                    source_name="ranking_sma5_atr_deviation_panel",
                    lateral_sql=lateral_sql,
                    match_condition=(
                        "long_scaffold.long_scaffold_matches "
                        "AND atr_threshold.threshold_matches"
                    ),
                    group_select_sql=(
                        "'long_scaffold_sma5_atr_threshold' AS condition_family,\n"
                        "            long_scaffold.long_scaffold,\n"
                        "            long_scaffold.long_scaffold_order,\n"
                        f"            {int(window)} AS atr_window,\n"
                        "            atr_threshold.direction,\n"
                        "            atr_threshold.threshold_abs_atr,\n"
                        "            atr_threshold.threshold_label,\n"
                        "            atr_threshold.threshold_order,\n"
                        f"            {int(horizon)} AS horizon"
                    ),
                    group_by_sql=(
                        "long_scaffold.long_scaffold, "
                        "long_scaffold.long_scaffold_order, "
                        "atr_threshold.direction, "
                        "atr_threshold.threshold_abs_atr, "
                        "atr_threshold.threshold_label, "
                        "atr_threshold.threshold_order, "
                        "market_scope"
                    ),
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    extra_metric_sql=(
                        _deep_dive_metric_sql()
                        + _sma5_atr_deviation_metric_sql(window)
                    ),
                )
            )
    return _concat_sorted(frames, columns=_long_scaffold_sma5_atr_threshold_columns())


def _build_short_overlay_sma5_atr_threshold_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    atr_windows: Sequence[int],
    threshold_abs_atr: Sequence[float],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in atr_windows:
        lateral_sql = f"""
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
            CROSS JOIN LATERAL (
                VALUES {_threshold_values_sql(window, threshold_abs_atr)}
            ) AS atr_threshold(
                threshold_label,
                threshold_order,
                direction,
                threshold_abs_atr,
                threshold_matches
            )
        """
        for horizon in horizons:
            frames.append(
                _aggregate_lateral_conditions(
                    conn,
                    source_name="ranking_sma5_atr_deviation_panel",
                    lateral_sql=lateral_sql,
                    match_condition=(
                        "price_action_bucket.price_action_bucket_matches "
                        "AND short_overlay.short_overlay_matches "
                        "AND atr_threshold.threshold_matches"
                    ),
                    group_select_sql=(
                        "'short_overlay_sma5_atr_threshold' AS condition_family,\n"
                        "            liquidity_band,\n"
                        "            price_action_bucket.price_action_bucket,\n"
                        "            price_action_bucket.price_action_bucket_order,\n"
                        "            short_overlay.short_overlay,\n"
                        "            short_overlay.short_overlay_order,\n"
                        f"            {int(window)} AS atr_window,\n"
                        "            atr_threshold.direction,\n"
                        "            atr_threshold.threshold_abs_atr,\n"
                        "            atr_threshold.threshold_label,\n"
                        "            atr_threshold.threshold_order,\n"
                        f"            {int(horizon)} AS horizon"
                    ),
                    group_by_sql=(
                        "liquidity_band, "
                        "price_action_bucket.price_action_bucket, "
                        "price_action_bucket.price_action_bucket_order, "
                        "short_overlay.short_overlay, "
                        "short_overlay.short_overlay_order, "
                        "atr_threshold.direction, "
                        "atr_threshold.threshold_abs_atr, "
                        "atr_threshold.threshold_label, "
                        "atr_threshold.threshold_order, "
                        "market_scope"
                    ),
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    extra_metric_sql=(
                        _recomposition_metric_sql()
                        + _sma5_atr_deviation_metric_sql(window)
                    ),
                )
            )
    return _concat_sorted(frames, columns=_short_overlay_sma5_atr_threshold_columns())


def _threshold_values_sql(
    atr_window: int,
    threshold_abs_atr: Sequence[float],
) -> str:
    column = f"sma5_atr{int(atr_window)}_deviation"
    rows: list[str] = []
    order = 0
    for direction in ("below", "above"):
        for threshold in threshold_abs_atr:
            order += 1
            threshold_label = (
                f"below_le_neg{_threshold_label(threshold)}_atr"
                if direction == "below"
                else f"above_ge_{_threshold_label(threshold)}_atr"
            )
            condition = (
                f"{column} <= -{float(threshold)}"
                if direction == "below"
                else f"{column} >= {float(threshold)}"
            )
            rows.append(
                "("
                f"'{threshold_label}', "
                f"{order}, "
                f"'{direction}', "
                f"{float(threshold)}, "
                f"{condition}"
                ")"
            )
    return ", ".join(rows)


def _threshold_label(value: float) -> str:
    return str(float(value)).replace(".", "_").rstrip("0").rstrip("_")


def _sma5_atr_deviation_metric_sql(atr_window: int) -> str:
    column = f"sma5_atr{int(atr_window)}_deviation"
    atr_column = f"atr{int(atr_window)}"
    return f""",
            median({column}) AS median_sma5_atr_deviation,
            quantile_cont({column}, 0.1) AS p10_sma5_atr_deviation,
            quantile_cont({column}, 0.9) AS p90_sma5_atr_deviation,
            median(abs({column})) AS median_abs_sma5_atr_deviation,
            avg(CASE WHEN {column} <= -1.0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS below_le_neg1_atr_rate_pct,
            avg(CASE WHEN {column} >= 1.0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS above_ge_1_atr_rate_pct,
            median({atr_column}) AS median_atr,
            median(sma5_deviation_pct) AS median_sma5_deviation_pct"""


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    atr_windows: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    atr_columns = ",\n            ".join(
        (
            f"atr{int(window)},\n"
            f"            sma5_atr{int(window)}_deviation,\n"
            f"            sma5_atr{int(window)}_deviation_bucket"
        )
        for window in atr_windows
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
            {atr_columns},
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
        FROM ranking_sma5_atr_deviation_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    atr_windows: Sequence[int],
    threshold_abs_atr: Sequence[float],
    liquidity_bands: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    _validate_base_params(
        horizons=horizons,
        liquidity_bands=liquidity_bands,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    if not atr_windows or any(int(window) <= 1 for window in atr_windows):
        raise ValueError("atr_windows must contain integers greater than 1")
    if not threshold_abs_atr or any(float(value) <= 0 for value in threshold_abs_atr):
        raise ValueError("threshold_abs_atr must contain positive values")


def _coverage_diagnostics_columns() -> list[str]:
    return [
        "market_scope",
        "atr_window",
        "sma5_atr_deviation_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "median_sma5_atr_deviation",
        "p10_sma5_atr_deviation",
        "p90_sma5_atr_deviation",
        "median_sma5_deviation_pct",
        "median_atr",
    ]


def _sma5_atr_deviation_metric_columns() -> list[str]:
    return [
        "median_sma5_atr_deviation",
        "p10_sma5_atr_deviation",
        "p90_sma5_atr_deviation",
        "median_abs_sma5_atr_deviation",
        "below_le_neg1_atr_rate_pct",
        "above_ge_1_atr_rate_pct",
        "median_atr",
        "median_sma5_deviation_pct",
    ]


def _sma5_atr_deviation_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "atr_window",
        "sma5_atr_deviation_bucket",
        "sma5_atr_deviation_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_sma5_atr_deviation_metric_columns(),
    ]


def _long_scaffold_sma5_atr_threshold_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "atr_window",
        "direction",
        "threshold_abs_atr",
        "threshold_label",
        "threshold_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_sma5_atr_deviation_metric_columns(),
    ]


def _short_overlay_sma5_atr_threshold_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "atr_window",
        "direction",
        "threshold_abs_atr",
        "threshold_label",
        "threshold_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_recomposition_metric_columns(),
        *_sma5_atr_deviation_metric_columns(),
    ]
