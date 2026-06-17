"""Nikkei 225 benchmark readout for neutral-rerating Daily Ranking candidates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
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

RANKING_N225_NEUTRAL_RERATING_BENCHMARK_EXPERIMENT_ID = (
    "market-behavior/ranking-n225-neutral-rerating-benchmark"
)
DEFAULT_LIQUIDITY_REGIMES: tuple[str, ...] = ("neutral_rerating",)
DEFAULT_MIN_OBSERVATIONS_N225 = 100

_SIGNALS: tuple[tuple[str, str, int, str], ...] = (
    ("neutral_all", "Neutral Rerating: all", 0, "TRUE"),
    ("deep_value", "Deep Value", 10, "deep_value_flag"),
    ("sector_strong", "Balanced Sector Strength: Strong", 20, "sector_strong_flag"),
    (
        "atr20_acceleration_ex_overheat",
        "ATR20 Accel ex-overheat",
        30,
        "atr20_acceleration_ex_overheat_flag",
    ),
    ("momentum_20_60_top20", "Momentum 20D/60D top20", 40, "momentum_20_60_top20_flag"),
    (
        "deep_value_sector_strong",
        "Deep Value + Sector Strong",
        50,
        "deep_value_flag AND sector_strong_flag",
    ),
    (
        "deep_value_atr20_acceleration",
        "Deep Value + ATR20 Accel",
        60,
        "deep_value_flag AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_momentum",
        "Deep Value + Momentum",
        70,
        "deep_value_flag AND momentum_20_60_top20_flag",
    ),
    (
        "sector_strong_atr20_acceleration",
        "Sector Strong + ATR20 Accel",
        80,
        "sector_strong_flag AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "sector_strong_momentum",
        "Sector Strong + Momentum",
        90,
        "sector_strong_flag AND momentum_20_60_top20_flag",
    ),
    (
        "deep_value_sector_strong_atr20_acceleration",
        "Deep Value + Sector Strong + ATR20 Accel",
        100,
        "deep_value_flag AND sector_strong_flag AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_sector_strong_momentum",
        "Deep Value + Sector Strong + Momentum",
        110,
        "deep_value_flag AND sector_strong_flag AND momentum_20_60_top20_flag",
    ),
    (
        "deep_value_atr20_acceleration_momentum",
        "Deep Value + ATR20 Accel + Momentum",
        120,
        "deep_value_flag AND atr20_acceleration_ex_overheat_flag AND momentum_20_60_top20_flag",
    ),
    (
        "deep_value_sector_strong_atr20_acceleration_momentum",
        "Deep Value + Sector Strong + ATR20 Accel + Momentum",
        130,
        (
            "deep_value_flag AND sector_strong_flag "
            "AND atr20_acceleration_ex_overheat_flag AND momentum_20_60_top20_flag"
        ),
    ),
)


@dataclass(frozen=True)
class RankingN225NeutralReratingBenchmarkResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    liquidity_regimes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    signal_summary_df: pd.DataFrame
    signal_benchmark_comparison_df: pd.DataFrame
    yearly_signal_summary_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_n225_neutral_rerating_benchmark_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    liquidity_regimes: Sequence[str] = DEFAULT_LIQUIDITY_REGIMES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS_N225,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingN225NeutralReratingBenchmarkResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_liquidity_regimes = tuple(
        dict.fromkeys(str(value).strip() for value in liquidity_regimes if str(value).strip())
    )
    _validate_params(
        horizons=resolved_horizons,
        liquidity_regimes=resolved_liquidity_regimes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(start_date, warmup_calendar_days=720)
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-n225-neutral-rerating-benchmark-",
    ) as ctx:
        assert_daily_ranking_research_tables(ctx.connection)
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
            include_relation_percentiles=True,
        )
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_atr_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            atr_windows=(20, 60),
            return_windows=(20, 60),
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_analysis_panel(
            ctx.connection,
            liquidity_regimes=resolved_liquidity_regimes,
            horizons=resolved_horizons,
        )
        _create_signal_observations(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_n225_neutral_rerating_panel"
            ).fetchone()[0]
        )
        result = RankingN225NeutralReratingBenchmarkResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            liquidity_regimes=resolved_liquidity_regimes,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            signal_summary_df=_build_signal_summary_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            signal_benchmark_comparison_df=_build_signal_benchmark_comparison_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            yearly_signal_summary_df=_build_yearly_signal_summary_df(
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


def write_ranking_n225_neutral_rerating_benchmark_bundle(
    result: RankingN225NeutralReratingBenchmarkResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_N225_NEUTRAL_RERATING_BENCHMARK_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_n225_neutral_rerating_benchmark",
        function="run_ranking_n225_neutral_rerating_benchmark_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "liquidity_regimes": list(result.liquidity_regimes),
            "min_observations": result.min_observations,
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
            "primary_outcome": "forward_close_n225_excess_return_{horizon}d_pct",
            "comparison_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "signal_summary_df": result.signal_summary_df,
            "signal_benchmark_comparison_df": result.signal_benchmark_comparison_df,
            "yearly_signal_summary_df": result.yearly_signal_summary_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingN225NeutralReratingBenchmarkResult) -> str:
    parts = [
        "# Ranking N225 Neutral Rerating Benchmark",
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
        f"- liquidity_regimes: `{', '.join(result.liquidity_regimes)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Signal Summary",
        "",
        _top_rows_for_markdown(result.signal_summary_df, limit=120),
        "",
        "## Signal Benchmark Comparison",
        "",
        _top_rows_for_markdown(result.signal_benchmark_comparison_df, limit=160),
        "",
        "## Yearly Signal Summary",
        "",
        _top_rows_for_markdown(result.yearly_signal_summary_df, limit=160),
    ]
    return "\n".join(parts)


def _validate_params(
    *,
    horizons: Sequence[int],
    liquidity_regimes: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not liquidity_regimes:
        raise ValueError("liquidity_regimes must not be empty")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _create_analysis_panel(
    conn: Any,
    *,
    liquidity_regimes: Sequence[str],
    horizons: Sequence[int],
) -> None:
    return_columns = ",\n                ".join(
        [
            *[
                f"r.forward_close_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"r.topix_close_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"r.forward_close_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"r.n225_close_return_{horizon}d_pct"
                for horizon in horizons
            ],
            *[
                f"r.forward_close_n225_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_n225_neutral_rerating_panel AS
        WITH base AS (
            SELECT
                r.market_scope,
                r.date,
                substr(CAST(r.date AS VARCHAR), 1, 4) AS year,
                r.code,
                r.company_name,
                sm.sector_33_code,
                sm.sector_33_name,
                r.liquidity_regime,
                r.valuation_signal,
                r.strong_value_confirmation AS deep_value_flag,
                r.medium_value_confirmation AS undervalued_flag,
                r.overvalued_warning,
                r.very_overvalued_warning,
                r.per_percentile,
                r.forward_per_percentile,
                r.pbr_percentile,
                r.forward_per_to_per_ratio,
                r.recent_return_20d_pct,
                r.recent_return_60d_pct,
                s.sector_strength_bucket,
                s.sector_strength_score,
                a.atr20_pct,
                a.atr60_pct,
                a.atr20_to_atr60,
                a.atr20_change_20d_pct,
                {return_columns},
                percent_rank() OVER (
                    PARTITION BY r.market_scope, r.date
                    ORDER BY r.recent_return_20d_pct NULLS LAST
                ) AS momentum_20d_percentile,
                percent_rank() OVER (
                    PARTITION BY r.market_scope, r.date
                    ORDER BY r.recent_return_60d_pct NULLS LAST
                ) AS momentum_60d_percentile
            FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
            JOIN ranking_sector_master sm
              ON sm.code = r.code
             AND sm.date = r.date
            LEFT JOIN ranking_sector_daily_state s
              ON s.market_scope = r.market_scope
             AND s.date = r.date
             AND s.sector_33_code = sm.sector_33_code
             AND s.sector_33_name = sm.sector_33_name
            LEFT JOIN atr_expansion_panel a
              ON a.code = r.code
             AND a.date = r.date
             AND a.market = r.market_scope
            WHERE r.liquidity_regime IN ({_sql_string_list(liquidity_regimes)})
        )
        SELECT
            *,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS momentum_20_60_top20_flag,
            sector_strength_bucket = 'sector_strong' AS sector_strong_flag,
            coalesce(
                atr20_change_20d_pct >= 25.0
                AND atr20_to_atr60 < 1.25
                AND coalesce(recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag
        FROM base
        """
    )


def _create_signal_observations(conn: Any) -> None:
    selects = []
    for signal, label, order, condition in _SIGNALS:
        selects.append(
            f"""
            SELECT
                *,
                '{signal}' AS signal,
                '{label}' AS signal_label,
                {order} AS signal_order
            FROM ranking_n225_neutral_rerating_panel
            WHERE {condition}
            """
        )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE ranking_n225_signal_observations AS\n"
        + "\nUNION ALL\n".join(selects)
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            liquidity_regime,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            min(date) AS min_date,
            max(date) AS max_date,
            avg(CASE WHEN forward_close_n225_excess_return_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS n225_20d_coverage_pct,
            avg(CASE WHEN sector_strength_bucket IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS sector_strength_coverage_pct,
            avg(CASE WHEN atr20_change_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr_coverage_pct,
            avg(CASE WHEN deep_value_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS deep_value_rate_pct,
            avg(CASE WHEN sector_strong_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sector_strong_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_acceleration_ex_overheat_rate_pct,
            avg(CASE WHEN momentum_20_60_top20_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS momentum_20_60_top20_rate_pct
        FROM ranking_n225_neutral_rerating_panel
        GROUP BY market_scope, liquidity_regime
        ORDER BY market_scope, liquidity_regime
        """
    ).fetchdf()


def _build_signal_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_signal_summary(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat(frames)


def _aggregate_signal_summary(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            signal,
            signal_label,
            signal_order,
            {horizon} AS horizon,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(forward_close_n225_excess_return_{horizon}d_pct) AS mean_n225_excess_return_pct,
            median(forward_close_n225_excess_return_{horizon}d_pct) AS median_n225_excess_return_pct,
            quantile_cont(forward_close_n225_excess_return_{horizon}d_pct, 0.10) AS p10_n225_excess_return_pct,
            quantile_cont(forward_close_n225_excess_return_{horizon}d_pct, 0.90) AS p90_n225_excess_return_pct,
            avg(CASE WHEN forward_close_n225_excess_return_{horizon}d_pct > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS n225_excess_win_rate_pct,
            avg(CASE WHEN forward_close_n225_excess_return_{horizon}d_pct <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS n225_excess_severe_loss_rate_pct,
            avg(forward_close_excess_return_{horizon}d_pct) AS mean_topix_excess_return_pct,
            median(forward_close_excess_return_{horizon}d_pct) AS median_topix_excess_return_pct,
            median(forward_close_n225_excess_return_{horizon}d_pct)
                - median(forward_close_excess_return_{horizon}d_pct)
                AS median_n225_minus_topix_excess_pct,
            median(forward_close_return_{horizon}d_pct) AS median_raw_return_pct,
            median(n225_close_return_{horizon}d_pct) AS median_n225_return_pct,
            avg(CASE WHEN sector_33_name = '銀行業' THEN 1.0 ELSE 0.0 END) * 100.0
                AS bank_share_pct
        FROM ranking_n225_signal_observations
        WHERE forward_close_n225_excess_return_{horizon}d_pct IS NOT NULL
        GROUP BY signal, signal_label, signal_order
        HAVING count(*) >= ?
        ORDER BY horizon, signal_order
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_signal_benchmark_comparison_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = []
    for horizon in horizons:
        for benchmark, column in (
            ("n225", f"forward_close_n225_excess_return_{int(horizon)}d_pct"),
            ("topix", f"forward_close_excess_return_{int(horizon)}d_pct"),
            ("raw", f"forward_close_return_{int(horizon)}d_pct"),
        ):
            frames.append(
                conn.execute(
                    f"""
                    SELECT
                        signal,
                        signal_label,
                        signal_order,
                        {int(horizon)} AS horizon,
                        '{benchmark}' AS benchmark,
                        count(*) AS observation_count,
                        avg({column}) AS mean_return_pct,
                        median({column}) AS median_return_pct,
                        quantile_cont({column}, 0.10) AS p10_return_pct,
                        quantile_cont({column}, 0.90) AS p90_return_pct,
                        avg(CASE WHEN {column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                            AS win_rate_pct,
                        avg(CASE WHEN {column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                            AS severe_loss_rate_pct
                    FROM ranking_n225_signal_observations
                    WHERE {column} IS NOT NULL
                    GROUP BY signal, signal_label, signal_order
                    HAVING count(*) >= ?
                    ORDER BY horizon, signal_order, benchmark
                    """,
                    [float(severe_loss_threshold_pct), int(min_observations)],
                ).fetchdf()
            )
    return _concat(frames)


def _build_yearly_signal_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = []
    for horizon in horizons:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    signal,
                    signal_label,
                    signal_order,
                    {int(horizon)} AS horizon,
                    year,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    median(forward_close_n225_excess_return_{int(horizon)}d_pct)
                        AS median_n225_excess_return_pct,
                    avg(CASE WHEN forward_close_n225_excess_return_{int(horizon)}d_pct > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                        AS n225_excess_win_rate_pct,
                    avg(CASE WHEN forward_close_n225_excess_return_{int(horizon)}d_pct <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                        AS n225_excess_severe_loss_rate_pct
                FROM ranking_n225_signal_observations
                WHERE forward_close_n225_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY signal, signal_label, signal_order, year
                HAVING count(*) >= ?
                ORDER BY horizon, signal_order, year
                """,
                [float(severe_loss_threshold_pct), int(min_observations)],
            ).fetchdf()
        )
    return _concat(frames)


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market_scope,
            liquidity_regime,
            valuation_signal,
            sector_33_name,
            sector_strength_bucket,
            deep_value_flag,
            sector_strong_flag,
            atr20_acceleration_ex_overheat_flag,
            momentum_20_60_top20_flag,
            recent_return_20d_pct,
            recent_return_60d_pct,
            atr20_change_20d_pct,
            atr20_to_atr60,
            forward_close_return_20d_pct,
            topix_close_return_20d_pct,
            n225_close_return_20d_pct,
            forward_close_excess_return_20d_pct,
            forward_close_n225_excess_return_20d_pct
        FROM ranking_n225_neutral_rerating_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _concat(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _sql_string_list(values: Sequence[str]) -> str:
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)
