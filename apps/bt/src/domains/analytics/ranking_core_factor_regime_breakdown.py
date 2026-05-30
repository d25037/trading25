"""Factor-regime breakdown for the momentum-value Ranking core."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    DEFAULT_ATR_WINDOWS,
    DEFAULT_RETURN_WINDOWS,
    _assert_required_tables as _assert_atr_required_tables,
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    _assert_required_tables as _assert_ranking_required_tables,
    _create_observation_panel as _create_ranking_observation_panel,
    _normalize_market_scopes,
    _offset_calendar_date,
)
from src.domains.analytics.ranking_core_sector_relative_value_evidence import (
    DEFAULT_MIN_SECTOR_OBSERVATIONS,
    _create_core_sector_relative_tables,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    DEFAULT_HORIZONS,
    _create_sector_strength_tables,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_CORE_FACTOR_REGIME_BREAKDOWN_EXPERIMENT_ID = (
    "market-behavior/ranking-core-factor-regime-breakdown"
)


@dataclass(frozen=True)
class RankingCoreFactorRegimeBreakdownResult:
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
    year_factor_spread_df: pd.DataFrame
    core_failure_decomposition_df: pd.DataFrame
    regime_comparison_df: pd.DataFrame
    sector_year_contribution_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_core_factor_regime_breakdown_research(
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
) -> RankingCoreFactorRegimeBreakdownResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
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

    query_start = _offset_calendar_date(start_date, days=-220)
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-core-factor-regime-breakdown-",
    ) as ctx:
        _assert_ranking_required_tables(ctx.connection)
        _assert_atr_required_tables(ctx.connection)
        _create_ranking_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_core_sector_relative_tables(
            ctx.connection,
            min_sector_observations=min_sector_observations,
        )
        _create_atr_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            atr_windows=DEFAULT_ATR_WINDOWS,
            return_windows=DEFAULT_RETURN_WINDOWS,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_factor_regime_tables(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_factor_regime_panel"
            ).fetchone()[0]
        )
        result = RankingCoreFactorRegimeBreakdownResult(
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
            year_factor_spread_df=_build_year_factor_spread_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            core_failure_decomposition_df=_build_core_failure_decomposition_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            regime_comparison_df=_build_regime_comparison_df(
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
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
                horizons=resolved_horizons,
            ),
        )
    return result


def write_ranking_core_factor_regime_breakdown_bundle(
    result: RankingCoreFactorRegimeBreakdownResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_CORE_FACTOR_REGIME_BREAKDOWN_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_core_factor_regime_breakdown",
        function="run_ranking_core_factor_regime_breakdown_research",
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
            "year_factor_spread_df": result.year_factor_spread_df,
            "core_failure_decomposition_df": result.core_failure_decomposition_df,
            "regime_comparison_df": result.regime_comparison_df,
            "sector_year_contribution_df": result.sector_year_contribution_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingCoreFactorRegimeBreakdownResult) -> str:
    parts = [
        "# Ranking Core Factor Regime Breakdown",
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
        "## Year Factor Spread",
        "",
        _top_rows_for_markdown(result.year_factor_spread_df, limit=240),
        "",
        "## Core Failure Decomposition",
        "",
        _top_rows_for_markdown(result.core_failure_decomposition_df, limit=240),
        "",
        "## Regime Comparison",
        "",
        _top_rows_for_markdown(result.regime_comparison_df, limit=160),
        "",
        "## Sector Year Contribution",
        "",
        _top_rows_for_markdown(result.sector_year_contribution_df, limit=200),
        "",
        "## Observation Sample",
        "",
        _top_rows_for_markdown(result.observation_sample_df, limit=80),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _create_factor_regime_tables(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_factor_regime_panel AS
        WITH base AS (
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
                sm.sector_33_name,
                s.sector_strength_bucket,
                s.sector_strength_score,
                r.liquidity_regime,
                r.recent_return_20d_pct,
                r.recent_return_60d_pct,
                r.pbr_percentile,
                r.forward_per_percentile,
                r.per_percentile,
                r.forward_per_to_per_ratio,
                r.forward_close_excess_return_5d_pct,
                r.forward_close_excess_return_10d_pct,
                r.forward_close_excess_return_20d_pct,
                r.forward_close_excess_return_60d_pct,
                a.atr20_change_20d_pct,
                a.atr20_to_atr60,
                a.recent_return_20d_pct AS atr_recent_return_20d_pct
            FROM ranking_color_ranked r
            JOIN ranking_sector_master sm
              ON sm.code = r.code
             AND sm.date = r.date
            LEFT JOIN ranking_sector_daily_state s
              ON s.market_scope = r.market_scope
             AND s.date = r.date
             AND s.sector_33_name = sm.sector_33_name
            LEFT JOIN atr_expansion_panel a
              ON a.market = r.market_scope
             AND a.date = r.date
             AND a.code = r.code
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
                ) AS momentum_60d_percentile
            FROM base
        )
        SELECT
            *,
            pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2 AS low_value_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS momentum_20_60_top20_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8
                AND (
                    pbr_percentile >= 0.8
                    OR forward_per_percentile >= 0.8
                ) AS high_valuation_momentum_flag,
            pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2
                AND momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS value_momentum_flag,
            sector_strength_bucket = 'sector_strong' AS sector_strong_flag,
            atr_recent_return_20d_pct < 30.0
                AND atr20_change_20d_pct >= 25.0
                AND atr20_to_atr60 < 1.25 AS atr20_acceleration_ex_overheat_flag
        FROM ranked
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE factor_signal_observations AS
        SELECT 'low_value' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE low_value_flag
        UNION ALL
        SELECT 'momentum_20_60_top20' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE momentum_20_60_top20_flag
        UNION ALL
        SELECT 'value_momentum' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE value_momentum_flag
        UNION ALL
        SELECT 'high_valuation_momentum' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE high_valuation_momentum_flag
        UNION ALL
        SELECT 'sector_strong' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE sector_strong_flag
        UNION ALL
        SELECT 'atr20_acceleration_ex_overheat' AS factor_signal, * FROM ranking_factor_regime_panel
        WHERE atr20_acceleration_ex_overheat_flag
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE core_factor_panel AS
        SELECT
            c.core_rule,
            c.date,
            substr(CAST(c.date AS VARCHAR), 1, 4) AS year,
            CASE
                WHEN substr(CAST(c.date AS VARCHAR), 1, 4) = '2026'
                    THEN '2026_partial'
                WHEN substr(CAST(c.date AS VARCHAR), 1, 4) BETWEEN '2022' AND '2025'
                    THEN '2022_2025_history'
                ELSE 'pre_2022'
            END AS year_group,
            c.code,
            c.company_name,
            c.market_scope,
            c.sector_33_name,
            c.sector_strength_bucket,
            c.sector_strength_score,
            c.raw_core_flag AS core_flag,
            c.sector_relative_core_flag,
            c.hybrid_core_flag,
            c.forward_close_excess_return_5d_pct,
            c.forward_close_excess_return_10d_pct,
            c.forward_close_excess_return_20d_pct,
            c.forward_close_excess_return_60d_pct,
            f.momentum_20d_percentile,
            f.momentum_60d_percentile,
            f.low_value_flag,
            f.momentum_20_60_top20_flag,
            f.value_momentum_flag,
            f.high_valuation_momentum_flag,
            f.atr20_acceleration_ex_overheat_flag,
            CASE
                WHEN f.atr20_acceleration_ex_overheat_flag
                    THEN 'atr20_acceleration_ex_overheat'
                ELSE 'not_atr20_acceleration_ex_overheat'
            END AS atr_state
        FROM ranking_core_rule_observations c
        JOIN ranking_factor_regime_panel f
          ON f.market_scope = c.market_scope
         AND f.date = c.date
         AND f.code = c.code
        WHERE c.core_rule = 'raw_core'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE core_failure_observations AS
        SELECT 'core_all' AS core_slice, 'core_all' AS factor_signal, *
        FROM core_factor_panel
        UNION ALL
        SELECT 'core_atr20_acceleration_ex_overheat' AS core_slice,
               'core_atr20_acceleration_ex_overheat' AS factor_signal, *
        FROM core_factor_panel
        WHERE atr20_acceleration_ex_overheat_flag
        UNION ALL
        SELECT 'core_without_atr20_acceleration_ex_overheat' AS core_slice,
               'core_without_atr20_acceleration_ex_overheat' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT atr20_acceleration_ex_overheat_flag
        UNION ALL
        SELECT 'core_momentum_20_60_top20' AS core_slice,
               'core_momentum_20_60_top20' AS factor_signal, *
        FROM core_factor_panel
        WHERE momentum_20_60_top20_flag
        UNION ALL
        SELECT 'core_without_momentum_20_60_top20' AS core_slice,
               'core_without_momentum_20_60_top20' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT momentum_20_60_top20_flag
        UNION ALL
        SELECT 'core_sector_relative_confirmed' AS core_slice,
               'core_sector_relative_confirmed' AS factor_signal, *
        FROM core_factor_panel
        WHERE sector_relative_core_flag
        UNION ALL
        SELECT 'core_without_sector_relative_confirmed' AS core_slice,
               'core_without_sector_relative_confirmed' AS factor_signal, *
        FROM core_factor_panel
        WHERE NOT sector_relative_core_flag
        """
    )
    conn.execute(
        """
        INSERT INTO factor_signal_observations
        SELECT 'core_atr20_acceleration_ex_overheat' AS factor_signal, f.*
        FROM ranking_factor_regime_panel f
        JOIN core_factor_panel c
          ON c.market_scope = f.market_scope
         AND c.date = f.date
         AND c.code = f.code
        WHERE c.atr20_acceleration_ex_overheat_flag
        """
    )


def _build_year_factor_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="factor_signal_observations",
            group_columns=["horizon", "year", "factor_signal"],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_year_factor_spread_columns())


def _build_core_failure_decomposition_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="core_failure_observations",
            group_columns=["horizon", "year", "core_slice", "factor_signal"],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    frame = _concat_sorted(
        frames,
        columns=[
            "horizon",
            "year",
            "core_slice",
            "factor_signal",
            *_metric_columns(),
        ],
    )
    if frame.empty:
        return pd.DataFrame(columns=_core_failure_decomposition_columns())
    frame["atr_state"] = frame["core_slice"].map(
        {
            "core_atr20_acceleration_ex_overheat": (
                "atr20_acceleration_ex_overheat"
            ),
            "core_without_atr20_acceleration_ex_overheat": (
                "not_atr20_acceleration_ex_overheat"
            ),
        }
    ).fillna("all_or_mixed_atr_states")
    return frame.reindex(columns=_core_failure_decomposition_columns())


def _build_regime_comparison_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="factor_signal_observations",
            group_columns=["horizon", "year_group", "factor_signal"],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_regime_comparison_columns())


def _build_sector_year_contribution_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_factor_table(
            conn,
            table_name="core_failure_observations",
            group_columns=["horizon", "year", "core_slice", "sector_33_name"],
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_year_contribution_columns())


def _aggregate_factor_table(
    conn: Any,
    *,
    table_name: str,
    group_columns: Sequence[str],
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return_column = f"forward_close_excess_return_{horizon}d_pct"
    select_groups = ",\n            ".join(
        f"{horizon} AS horizon" if column == "horizon" else column
        for column in group_columns
    )
    group_by = ", ".join(str(index) for index in range(1, len(group_columns) + 1))
    frame = conn.execute(
        f"""
        SELECT
            {select_groups},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg({return_column}) AS mean_forward_topix_excess_return_pct,
            median({return_column}) AS median_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_topix_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_topix_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct
        FROM {table_name}
        WHERE {return_column} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    return frame


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            min(date) AS min_date,
            max(date) AS max_date,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(CASE WHEN low_value_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS low_value_rate_pct,
            avg(CASE WHEN momentum_20_60_top20_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS momentum_20_60_top20_rate_pct,
            avg(CASE WHEN high_valuation_momentum_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS high_valuation_momentum_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_acceleration_ex_overheat_rate_pct
        FROM ranking_factor_regime_panel
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
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
    )
    return conn.execute(
        f"""
        SELECT
            year,
            year_group,
            core_slice,
            factor_signal,
            atr_state,
            date,
            code,
            company_name,
            sector_33_name,
            sector_strength_bucket,
            momentum_20d_percentile,
            momentum_60d_percentile,
            {horizon_exprs}
        FROM core_failure_observations
        ORDER BY year, date, core_slice, code
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
    if not {5, 10, 20, 60}.issubset(set(horizons)):
        raise ValueError("horizons must include 5, 10, 20, and 60")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if min_sector_observations < 2:
        raise ValueError("min_sector_observations must be at least 2")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "p90_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    ]


def _year_factor_spread_columns() -> list[str]:
    return ["horizon", "year", "factor_signal", *_metric_columns()]


def _core_failure_decomposition_columns() -> list[str]:
    return [
        "horizon",
        "year",
        "core_slice",
        "factor_signal",
        "atr_state",
        *_metric_columns(),
    ]


def _regime_comparison_columns() -> list[str]:
    return ["horizon", "year_group", "factor_signal", *_metric_columns()]


def _sector_year_contribution_columns() -> list[str]:
    return ["horizon", "year", "core_slice", "sector_33_name", *_metric_columns()]
