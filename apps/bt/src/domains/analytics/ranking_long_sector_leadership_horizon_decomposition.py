"""Long-side sector leadership horizon decomposition for Momentum Value."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

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
from src.domains.analytics.ranking_sector_strength_evidence import (
    DEFAULT_HORIZONS,
    _create_sector_strength_tables,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_LONG_SECTOR_LEADERSHIP_HORIZON_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/ranking-long-sector-leadership-horizon-decomposition"
)
DEFAULT_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_FUTURE_TOP5_SECTORS: tuple[str, ...] = (
    "非鉄金属",
    "海運業",
    "卸売業",
    "電気機器",
    "保険業",
)
_FUTURE_BOTTOM5_SECTORS: tuple[str, ...] = (
    "空運業",
    "陸運業",
    "パルプ・紙",
    "繊維製品",
    "医薬品",
)


@dataclass(frozen=True)
class RankingLongSectorLeadershipHorizonDecompositionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    leadership_windows: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    annual_overlay_summary_df: pd.DataFrame
    bank_concentration_df: pd.DataFrame
    sector_contribution_df: pd.DataFrame
    leadership_horizon_df: pd.DataFrame
    current_vs_long_matrix_df: pd.DataFrame
    future_top5_diagnostic_df: pd.DataFrame
    overlay_comparison_df: pd.DataFrame
    current_term_mapping_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_long_sector_leadership_horizon_decomposition_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    leadership_windows: Iterable[int] = DEFAULT_LEADERSHIP_WINDOWS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingLongSectorLeadershipHorizonDecompositionResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_leadership_windows = tuple(
        sorted({int(window) for window in leadership_windows})
    )
    resolved_market_scopes = _normalize_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        leadership_windows=resolved_leadership_windows,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    max_warmup_days = max(max(resolved_leadership_windows) * 3, 252)
    query_start = _offset_calendar_date(start_date, days=-max_warmup_days)
    query_end = _offset_calendar_date(end_date, days=max(resolved_horizons) * 4 + 30)
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-long-sector-leadership-horizon-decomposition-",
    ) as ctx:
        _assert_ranking_required_tables(ctx.connection)
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
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=resolved_leadership_windows,
        )
        _create_long_signal_tables(
            ctx.connection,
            leadership_windows=resolved_leadership_windows,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM long_sector_leadership_signal_observations"
            ).fetchone()[0]
        )
        annual_overlay_summary_df = _build_annual_overlay_summary_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        result = RankingLongSectorLeadershipHorizonDecompositionResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            leadership_windows=resolved_leadership_windows,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            annual_overlay_summary_df=annual_overlay_summary_df,
            bank_concentration_df=_build_bank_concentration_df(
                annual_overlay_summary_df
            ),
            sector_contribution_df=_build_sector_contribution_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            leadership_horizon_df=_build_leadership_horizon_df(
                ctx.connection,
                horizons=resolved_horizons,
                leadership_windows=resolved_leadership_windows,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            current_vs_long_matrix_df=_build_current_vs_long_matrix_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            future_top5_diagnostic_df=_build_future_top5_diagnostic_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            overlay_comparison_df=_build_overlay_comparison_df(
                annual_overlay_summary_df
            ),
            current_term_mapping_df=_build_current_term_mapping_df(ctx.connection),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
                horizons=resolved_horizons,
            ),
        )
    return result


def write_ranking_long_sector_leadership_horizon_decomposition_bundle(
    result: RankingLongSectorLeadershipHorizonDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_LONG_SECTOR_LEADERSHIP_HORIZON_DECOMPOSITION_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition",
        function="run_ranking_long_sector_leadership_horizon_decomposition_research",
        params={
            "horizons": list(result.horizons),
            "leadership_windows": list(result.leadership_windows),
            "market_scopes": list(result.market_scopes),
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
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "annual_overlay_summary_df": result.annual_overlay_summary_df,
            "bank_concentration_df": result.bank_concentration_df,
            "sector_contribution_df": result.sector_contribution_df,
            "leadership_horizon_df": result.leadership_horizon_df,
            "current_vs_long_matrix_df": result.current_vs_long_matrix_df,
            "future_top5_diagnostic_df": result.future_top5_diagnostic_df,
            "overlay_comparison_df": result.overlay_comparison_df,
            "current_term_mapping_df": result.current_term_mapping_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingLongSectorLeadershipHorizonDecompositionResult,
) -> str:
    parts = [
        "# Ranking Long Sector Leadership Horizon Decomposition",
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
        f"- leadership_windows: `{', '.join(str(item) for item in result.leadership_windows)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Annual Overlay Summary",
        "",
        _top_rows_for_markdown(result.annual_overlay_summary_df, limit=320),
        "",
        "## Bank Concentration",
        "",
        _top_rows_for_markdown(result.bank_concentration_df, limit=240),
        "",
        "## Sector Contribution",
        "",
        _top_rows_for_markdown(result.sector_contribution_df, limit=260),
        "",
        "## Leadership Horizon",
        "",
        _top_rows_for_markdown(result.leadership_horizon_df, limit=260),
        "",
        "## Current x Long Matrix",
        "",
        _top_rows_for_markdown(result.current_vs_long_matrix_df, limit=260),
        "",
        "## Future Top 5 Diagnostic",
        "",
        _top_rows_for_markdown(result.future_top5_diagnostic_df, limit=260),
        "",
        "## Overlay Comparison",
        "",
        _top_rows_for_markdown(result.overlay_comparison_df, limit=260),
        "",
        "## Current Daily Ranking Terms",
        "",
        _top_rows_for_markdown(result.current_term_mapping_df, limit=80),
        "",
        "## Observation Sample",
        "",
        _top_rows_for_markdown(result.observation_sample_df, limit=80),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _create_long_sector_leadership_tables(
    conn: Any,
    *,
    leadership_windows: Sequence[int],
) -> None:
    _create_sector_index_map(conn)
    window_selects = ",\n                ".join(
        f"lag(close, {int(window)}) OVER (PARTITION BY code ORDER BY date) AS close_lag_{int(window)}d"
        for window in leadership_windows
    )
    stock_return_selects = ",\n                ".join(
        f"100.0 * (close / NULLIF(close_lag_{int(window)}d, 0.0) - 1.0) AS stock_return_{int(window)}d_pct"
        for window in leadership_windows
    )
    topix_lag_selects = ",\n                ".join(
        f"lag(close, {int(window)}) OVER (ORDER BY date) AS topix_close_lag_{int(window)}d"
        for window in leadership_windows
    )
    topix_return_selects = ",\n                ".join(
        f"100.0 * (close / NULLIF(topix_close_lag_{int(window)}d, 0.0) - 1.0) AS topix_return_{int(window)}d_pct"
        for window in leadership_windows
    )
    sector_aggregate_selects = ",\n                ".join(
        [
            f"avg(sr.stock_return_{int(window)}d_pct - tr.topix_return_{int(window)}d_pct) AS sector_constituent_{int(window)}d_topix_excess_pct,\n"
            f"                avg(CASE WHEN sr.stock_return_{int(window)}d_pct - tr.topix_return_{int(window)}d_pct > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS sector_breadth_{int(window)}d_pct"
            for window in leadership_windows
        ]
    )
    index_lag_selects = ",\n                ".join(
        f"lag(close, {int(window)}) OVER (PARTITION BY sector_33_code ORDER BY date) AS close_lag_{int(window)}d"
        for window in leadership_windows
    )
    index_return_selects = ",\n                ".join(
        f"100.0 * (close / NULLIF(close_lag_{int(window)}d, 0.0) - 1.0) AS sector_index_return_{int(window)}d_pct"
        for window in leadership_windows
    )
    index_excess_selects = ",\n                ".join(
        f"i.sector_index_return_{int(window)}d_pct - tr.topix_return_{int(window)}d_pct AS sector_index_{int(window)}d_topix_excess_pct"
        for window in leadership_windows
    )
    rank_selects: list[str] = []
    for window in leadership_windows:
        window_int = int(window)
        rank_selects.extend(
            [
                f"percent_rank() OVER (PARTITION BY sr.market_scope, sr.date ORDER BY si.sector_index_{window_int}d_topix_excess_pct) AS sector_index_{window_int}d_rank",
                f"percent_rank() OVER (PARTITION BY sr.market_scope, sr.date ORDER BY sr.sector_constituent_{window_int}d_topix_excess_pct) AS sector_constituent_{window_int}d_rank",
                f"percent_rank() OVER (PARTITION BY sr.market_scope, sr.date ORDER BY sr.sector_breadth_{window_int}d_pct) AS sector_breadth_{window_int}d_rank",
            ]
        )
    rank_select_sql = ",\n                ".join(rank_selects)
    index_rank_columns = [
        f"sector_index_{int(window)}d_rank" for window in leadership_windows
    ]
    constituent_rank_columns = [
        f"sector_constituent_{int(window)}d_rank" for window in leadership_windows
    ]
    breadth_rank_columns = [
        f"sector_breadth_{int(window)}d_rank" for window in leadership_windows
    ]
    index_score_expr = (
        f"(({ ' + '.join(index_rank_columns) }) / {len(index_rank_columns)})"
    )
    constituent_score_expr = (
        f"(({ ' + '.join([*constituent_rank_columns, *breadth_rank_columns]) }) / "
        f"{len([*constituent_rank_columns, *breadth_rank_columns])})"
    )
    hybrid_score_expr = f"(({index_score_expr} + {constituent_score_expr}) / 2.0)"

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_stock_returns AS
        WITH prices AS (
            SELECT
                code,
                CAST(date AS VARCHAR) AS date,
                close,
                {window_selects}
            FROM stock_data
            WHERE close > 0
        )
        SELECT
            code,
            date,
            {stock_return_selects}
        FROM prices
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_topix_returns AS
        WITH prices AS (
            SELECT
                CAST(date AS VARCHAR) AS date,
                close,
                {topix_lag_selects}
            FROM topix_data
            WHERE close > 0
        )
        SELECT
            date,
            {topix_return_selects}
        FROM prices
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_sector_constituent_state AS
        SELECT
            r.market_scope,
            r.date,
            sm.sector_33_code,
            sm.sector_33_name,
            count(*) AS sector_observation_count,
            count(DISTINCT r.code) AS sector_code_count,
            {sector_aggregate_selects}
        FROM ranking_color_ranked r
        JOIN ranking_sector_master sm
          ON sm.code = r.code
         AND sm.date = r.date
        JOIN long_stock_returns sr
          ON sr.code = r.code
         AND sr.date = r.date
        JOIN long_topix_returns tr
          ON tr.date = r.date
        GROUP BY
            r.market_scope,
            r.date,
            sm.sector_33_code,
            sm.sector_33_name
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_sector_index_returns AS
        WITH index_prices AS (
            SELECT
                m.sector_33_code,
                m.sector_index_code,
                CAST(i.date AS VARCHAR) AS date,
                i.close,
                {index_lag_selects}
            FROM ranking_long_sector_index_map m
            JOIN indices_data i
              ON i.code = m.sector_index_code
            WHERE i.close > 0
        ),
        index_returns AS (
            SELECT
                sector_33_code,
                sector_index_code,
                date,
                {index_return_selects}
            FROM index_prices
        )
        SELECT
            i.sector_33_code,
            i.sector_index_code,
            i.date,
            {index_excess_selects}
        FROM index_returns i
        JOIN long_topix_returns tr
          ON tr.date = i.date
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_state AS
        WITH ranked AS (
            SELECT
                sr.*,
                si.sector_index_code,
                {", ".join(f"si.sector_index_{int(window)}d_topix_excess_pct" for window in leadership_windows)},
                {rank_select_sql}
            FROM long_sector_constituent_state sr
            JOIN long_sector_index_returns si
              ON si.sector_33_code = sr.sector_33_code
             AND si.date = sr.date
        )
        SELECT
            *,
            {index_score_expr} AS long_index_leadership_score,
            {constituent_score_expr} AS long_constituent_breadth_leadership_score,
            {hybrid_score_expr} AS long_hybrid_leadership_score
        FROM ranked
        """
    )


def _create_sector_index_map(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_long_sector_index_map AS
        SELECT * FROM (
            VALUES
                ('0050', '0040'),
                ('1050', '0041'),
                ('2050', '0042'),
                ('3050', '0043'),
                ('3100', '0044'),
                ('3150', '0045'),
                ('3200', '0046'),
                ('3250', '0047'),
                ('3300', '0048'),
                ('3350', '0049'),
                ('3400', '004A'),
                ('3450', '004B'),
                ('3500', '004C'),
                ('3550', '004D'),
                ('3600', '004E'),
                ('3650', '004F'),
                ('3700', '0050'),
                ('3750', '0051'),
                ('3800', '0052'),
                ('4050', '0053'),
                ('5050', '0054'),
                ('5100', '0055'),
                ('5150', '0056'),
                ('5200', '0057'),
                ('5250', '0058'),
                ('6050', '0059'),
                ('6100', '005A'),
                ('7050', '005B'),
                ('7100', '005C'),
                ('7150', '005D'),
                ('7200', '005E'),
                ('8050', '005F'),
                ('9050', '0060')
        ) AS t(sector_33_code, sector_index_code)
        """
    )


def _create_long_signal_tables(
    conn: Any,
    *,
    leadership_windows: Sequence[int],
) -> None:
    leadership_rank_selects = ",\n                ".join(
        [
            f"l.sector_index_{int(window)}d_rank,"
            f"\n                l.sector_constituent_{int(window)}d_rank,"
            f"\n                l.sector_breadth_{int(window)}d_rank"
            for window in leadership_windows
        ]
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE long_sector_overlay_terms (
            overlay_signal TEXT,
            overlay_family TEXT,
            overlay_display_name TEXT,
            display_order INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO long_sector_overlay_terms VALUES (?, ?, ?, ?)",
        [
            ("no_sector_overlay", "Baseline", "Momentum Value", 10),
            (
                "current_sector_strong",
                "Current Sector Score",
                "Momentum Value + Current Sector Score: Strong",
                20,
            ),
            (
                "long_index_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Long Index Leadership",
                30,
            ),
            (
                "long_constituent_breadth_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Long Constituent/Breadth Leadership",
                40,
            ),
            (
                "long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Long Hybrid Leadership",
                50,
            ),
            (
                "current_not_weak_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Current not Weak + Long Hybrid Leadership",
                60,
            ),
            (
                "current_strong_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Current Strong + Long Hybrid Leadership",
                70,
            ),
            (
                "current_weak_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Current Weak + Long Hybrid Leadership",
                80,
            ),
        ],
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_base_panel AS
        WITH ranked AS (
            SELECT
                r.*,
                sm.sector_33_code,
                sm.sector_33_name,
                s.sector_strength_bucket,
                s.sector_strength_score,
                s.sector_index_strength_score,
                s.sector_constituent_strength_score,
                l.long_index_leadership_score,
                l.long_constituent_breadth_leadership_score,
                l.long_hybrid_leadership_score,
                {leadership_rank_selects},
                l.sector_observation_count AS long_sector_observation_count,
                l.sector_code_count AS long_sector_code_count,
                percent_rank() OVER (
                    PARTITION BY r.market_scope, r.date
                    ORDER BY r.recent_return_20d_pct NULLS LAST
                ) AS momentum_20d_percentile,
                percent_rank() OVER (
                    PARTITION BY r.market_scope, r.date
                    ORDER BY r.recent_return_60d_pct NULLS LAST
                ) AS momentum_60d_percentile
            FROM ranking_color_ranked r
            JOIN ranking_sector_master sm
              ON sm.code = r.code
             AND sm.date = r.date
            LEFT JOIN ranking_sector_daily_state s
              ON s.market_scope = r.market_scope
             AND s.date = r.date
             AND s.sector_33_code = sm.sector_33_code
             AND s.sector_33_name = sm.sector_33_name
            LEFT JOIN long_sector_leadership_state l
              ON l.market_scope = r.market_scope
             AND l.date = r.date
             AND l.sector_33_code = sm.sector_33_code
             AND l.sector_33_name = sm.sector_33_name
        )
        SELECT
            *,
            substr(CAST(date AS VARCHAR), 1, 4) AS year,
            pbr_percentile <= 0.2
                AND forward_per_percentile <= 0.2 AS undervalued_flag,
            momentum_20d_percentile >= 0.8
                AND momentum_60d_percentile >= 0.8 AS momentum_20_60_top20_flag,
            sector_33_name = '銀行業' AS bank_sector_flag,
            sector_33_name IN ('非鉄金属', '海運業', '卸売業', '電気機器', '保険業')
                AS future_top5_sector_flag,
            sector_33_name IN ('空運業', '陸運業', 'パルプ・紙', '繊維製品', '医薬品')
                AS future_bottom5_sector_flag,
            CASE
                WHEN sector_strength_bucket = 'sector_strong' THEN 'Current Strong'
                WHEN sector_strength_bucket = 'sector_weak' THEN 'Current Weak'
                WHEN sector_strength_bucket IS NULL THEN 'Current Unknown'
                ELSE 'Current Neutral'
            END AS current_sector_bucket_label,
            CASE
                WHEN long_hybrid_leadership_score >= 0.799999 THEN 'Long Strong'
                WHEN long_hybrid_leadership_score <= 0.200001 THEN 'Long Weak'
                WHEN long_hybrid_leadership_score IS NULL THEN 'Long Unknown'
                ELSE 'Long Neutral'
            END AS long_hybrid_bucket_label
        FROM ranked
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_signal_observations_raw AS
        SELECT 'no_sector_overlay' AS overlay_signal, * FROM long_sector_leadership_base_panel
        WHERE undervalued_flag AND momentum_20_60_top20_flag
        UNION ALL
        SELECT 'current_sector_strong' AS overlay_signal, * FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strength_bucket = 'sector_strong'
        UNION ALL
        SELECT 'long_index_leadership_strong' AS overlay_signal, * FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND long_index_leadership_score >= 0.799999
        UNION ALL
        SELECT 'long_constituent_breadth_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND long_constituent_breadth_leadership_score >= 0.799999
        UNION ALL
        SELECT 'long_hybrid_leadership_strong' AS overlay_signal, * FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND long_hybrid_leadership_score >= 0.799999
        UNION ALL
        SELECT 'current_not_weak_long_hybrid_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND coalesce(sector_strength_bucket, 'sector_unknown') <> 'sector_weak'
          AND long_hybrid_leadership_score >= 0.799999
        UNION ALL
        SELECT 'current_strong_long_hybrid_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strength_bucket = 'sector_strong'
          AND long_hybrid_leadership_score >= 0.799999
        UNION ALL
        SELECT 'current_weak_long_hybrid_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strength_bucket = 'sector_weak'
          AND long_hybrid_leadership_score >= 0.799999
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_signal_observations AS
        SELECT
            raw.*,
            coalesce(terms.overlay_family, 'Other') AS overlay_family,
            coalesce(terms.overlay_display_name, raw.overlay_signal)
                AS overlay_display_name,
            coalesce(terms.display_order, 999) AS overlay_display_order
        FROM long_sector_leadership_signal_observations_raw raw
        LEFT JOIN long_sector_overlay_terms terms USING (overlay_signal)
        """
    )


def _build_annual_overlay_summary_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_overlay_summary(
            conn,
            horizon=int(horizon),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_annual_overlay_summary_columns())


def _aggregate_overlay_summary(
    conn: Any,
    *,
    horizon: int,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        WITH scoped AS (
            SELECT 'all' AS sector_scope, 'All sectors' AS sector_scope_label, *
            FROM long_sector_leadership_signal_observations
            UNION ALL
            SELECT 'ex_banks' AS sector_scope, 'ex Banks' AS sector_scope_label, *
            FROM long_sector_leadership_signal_observations
            WHERE NOT bank_sector_flag
            UNION ALL
            SELECT 'banks_only' AS sector_scope, 'Banks only' AS sector_scope_label, *
            FROM long_sector_leadership_signal_observations
            WHERE bank_sector_flag
        )
        SELECT
            {int(horizon)} AS horizon,
            market_scope,
            year,
            sector_scope,
            sector_scope_label,
            overlay_signal,
            overlay_family,
            overlay_display_name,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(forward_close_return_{int(horizon)}d_pct)
                AS mean_raw_return_pct,
            median(forward_close_return_{int(horizon)}d_pct)
                AS median_raw_return_pct,
            avg(forward_close_excess_return_{int(horizon)}d_pct)
                AS mean_forward_topix_excess_return_pct,
            median(forward_close_excess_return_{int(horizon)}d_pct)
                AS median_forward_topix_excess_return_pct,
            quantile_cont(forward_close_excess_return_{int(horizon)}d_pct, 0.10)
                AS p10_forward_topix_excess_return_pct,
            quantile_cont(forward_close_excess_return_{int(horizon)}d_pct, 0.90)
                AS p90_forward_topix_excess_return_pct,
            avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
            avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
            avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS bank_observation_share_pct,
            avg(CASE WHEN future_top5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS future_top5_sector_share_pct,
            avg(CASE WHEN future_bottom5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS future_bottom5_sector_share_pct,
            median(sector_strength_score) AS median_current_sector_score,
            median(long_index_leadership_score) AS median_long_index_score,
            median(long_constituent_breadth_leadership_score)
                AS median_long_constituent_breadth_score,
            median(long_hybrid_leadership_score) AS median_long_hybrid_score,
            any_value(overlay_display_order) AS overlay_display_order
        FROM scoped
        WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
          AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
        GROUP BY
            market_scope,
            year,
            sector_scope,
            sector_scope_label,
            overlay_signal,
            overlay_family,
            overlay_display_name
        HAVING count(*) >= ?
        ORDER BY
            horizon,
            market_scope,
            year,
            sector_scope,
            overlay_display_order
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _build_bank_concentration_df(annual_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "horizon",
        "market_scope",
        "year",
        "overlay_signal",
        "overlay_display_name",
        "all_observation_count",
        "bank_observation_share_pct",
        "all_median_forward_topix_excess_return_pct",
        "ex_banks_median_forward_topix_excess_return_pct",
        "banks_only_median_forward_topix_excess_return_pct",
        "ex_banks_minus_all_median_forward_topix_excess_return_pct",
        "banks_only_minus_all_median_forward_topix_excess_return_pct",
    ]
    if annual_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    for keys, group in annual_df.groupby(
        ["horizon", "market_scope", "year", "overlay_signal"],
        sort=False,
    ):
        by_scope = {
            str(row["sector_scope"]): row for row in group.to_dict(orient="records")
        }
        all_row = by_scope.get("all")
        if all_row is None:
            continue
        ex_row = by_scope.get("ex_banks")
        bank_row = by_scope.get("banks_only")
        all_median = _to_float(all_row.get("median_forward_topix_excess_return_pct"))
        ex_median = _to_float(
            ex_row.get("median_forward_topix_excess_return_pct")
            if ex_row is not None
            else None
        )
        bank_median = _to_float(
            bank_row.get("median_forward_topix_excess_return_pct")
            if bank_row is not None
            else None
        )
        horizon, market_scope, year, overlay_signal = keys
        records.append(
            {
                "horizon": int(horizon),
                "market_scope": str(market_scope),
                "year": str(year),
                "overlay_signal": str(overlay_signal),
                "overlay_display_name": str(all_row["overlay_display_name"]),
                "all_observation_count": int(all_row["observation_count"]),
                "bank_observation_share_pct": _to_float(
                    all_row.get("bank_observation_share_pct")
                ),
                "all_median_forward_topix_excess_return_pct": all_median,
                "ex_banks_median_forward_topix_excess_return_pct": ex_median,
                "banks_only_median_forward_topix_excess_return_pct": bank_median,
                "ex_banks_minus_all_median_forward_topix_excess_return_pct": (
                    ex_median - all_median
                    if ex_median is not None and all_median is not None
                    else None
                ),
                "banks_only_minus_all_median_forward_topix_excess_return_pct": (
                    bank_median - all_median
                    if bank_median is not None and all_median is not None
                    else None
                ),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _build_sector_contribution_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            WITH sector_rows AS (
                SELECT
                    {int(horizon)} AS horizon,
                    market_scope,
                    year,
                    overlay_signal,
                    overlay_family,
                    overlay_display_name,
                    any_value(overlay_display_order) AS overlay_display_order,
                    sector_33_name,
                    sector_33_name = '銀行業' AS bank_sector_flag,
                    sector_33_name IN ('非鉄金属', '海運業', '卸売業', '電気機器', '保険業')
                        AS future_top5_sector_flag,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    median(forward_close_return_{int(horizon)}d_pct)
                        AS median_raw_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                FROM long_sector_leadership_signal_observations
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                  AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY
                    market_scope,
                    year,
                    overlay_signal,
                    overlay_family,
                    overlay_display_name,
                    sector_33_name
                HAVING count(*) >= ?
            ),
            totals AS (
                SELECT
                    horizon,
                    market_scope,
                    year,
                    overlay_signal,
                    sum(observation_count) AS total_observation_count
                FROM sector_rows
                GROUP BY horizon, market_scope, year, overlay_signal
            )
            SELECT
                s.*,
                100.0 * s.observation_count / nullif(t.total_observation_count, 0)
                    AS sector_observation_share_pct
            FROM sector_rows s
            JOIN totals t
              ON t.horizon = s.horizon
             AND t.market_scope = s.market_scope
             AND t.year = s.year
             AND t.overlay_signal = s.overlay_signal
            ORDER BY
                s.horizon,
                s.market_scope,
                s.year,
                s.overlay_display_order,
                sector_observation_share_pct DESC
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_sector_contribution_columns())


def _build_leadership_horizon_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    leadership_windows: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for return_horizon in horizons:
        for window in leadership_windows:
            frames.append(
                conn.execute(
                    f"""
                    WITH base AS (
                        SELECT
                            {int(return_horizon)} AS horizon,
                            {int(window)} AS leadership_window,
                            market_scope,
                            year,
                            CASE
                                WHEN sector_index_{int(window)}d_rank >= 0.799999
                                    THEN 'index_long_strong'
                                WHEN sector_constituent_{int(window)}d_rank >= 0.799999
                                  OR sector_breadth_{int(window)}d_rank >= 0.799999
                                    THEN 'constituent_or_breadth_long_strong'
                                ELSE 'other'
                            END AS leadership_rule,
                            *
                        FROM long_sector_leadership_base_panel
                        WHERE undervalued_flag
                          AND momentum_20_60_top20_flag
                    )
                    SELECT
                        horizon,
                        leadership_window,
                        market_scope,
                        year,
                        leadership_rule,
                        count(*) AS observation_count,
                        count(DISTINCT code) AS code_count,
                        count(DISTINCT sector_33_name) AS sector_count,
                        avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                            AS bank_observation_share_pct,
                        avg(CASE WHEN future_top5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                            AS future_top5_sector_share_pct,
                        median(forward_close_return_{int(return_horizon)}d_pct)
                            AS median_raw_return_pct,
                        median(forward_close_excess_return_{int(return_horizon)}d_pct)
                            AS median_forward_topix_excess_return_pct,
                        avg(CASE WHEN forward_close_excess_return_{int(return_horizon)}d_pct > 0
                            THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                        avg(CASE WHEN forward_close_excess_return_{int(return_horizon)}d_pct <= ?
                            THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                    FROM base
                    WHERE leadership_rule <> 'other'
                      AND forward_close_excess_return_{int(return_horizon)}d_pct IS NOT NULL
                      AND forward_close_return_{int(return_horizon)}d_pct IS NOT NULL
                    GROUP BY
                        horizon,
                        leadership_window,
                        market_scope,
                        year,
                        leadership_rule
                    HAVING count(*) >= ?
                    ORDER BY horizon, leadership_window, market_scope, year, leadership_rule
                    """,
                    [float(severe_loss_threshold_pct), int(min_observations)],
                ).fetchdf()
            )
    return _concat_sorted(frames, columns=_leadership_horizon_columns())


def _build_current_vs_long_matrix_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            SELECT
                {int(horizon)} AS horizon,
                market_scope,
                year,
                current_sector_bucket_label,
                long_hybrid_bucket_label,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                count(DISTINCT sector_33_name) AS sector_count,
                avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                    AS bank_observation_share_pct,
                avg(CASE WHEN future_top5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                    AS future_top5_sector_share_pct,
                median(forward_close_return_{int(horizon)}d_pct)
                    AS median_raw_return_pct,
                median(forward_close_excess_return_{int(horizon)}d_pct)
                    AS median_forward_topix_excess_return_pct,
                avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                    THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                    THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
            FROM long_sector_leadership_base_panel
            WHERE undervalued_flag
              AND momentum_20_60_top20_flag
              AND forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
              AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
            GROUP BY
                market_scope,
                year,
                current_sector_bucket_label,
                long_hybrid_bucket_label
            HAVING count(*) >= ?
            ORDER BY horizon, market_scope, year, current_sector_bucket_label, long_hybrid_bucket_label
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_current_vs_long_matrix_columns())


def _build_future_top5_diagnostic_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            SELECT
                {int(horizon)} AS horizon,
                market_scope,
                year,
                CASE
                    WHEN future_top5_sector_flag THEN 'future_top5_sector'
                    WHEN future_bottom5_sector_flag THEN 'future_bottom5_sector'
                    ELSE 'other_sector'
                END AS future_sector_group,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                count(DISTINCT sector_33_name) AS sector_count,
                avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                    AS bank_observation_share_pct,
                median(sector_strength_score) AS median_current_sector_score,
                median(long_hybrid_leadership_score) AS median_long_hybrid_score,
                median(forward_close_return_{int(horizon)}d_pct)
                    AS median_raw_return_pct,
                median(forward_close_excess_return_{int(horizon)}d_pct)
                    AS median_forward_topix_excess_return_pct,
                avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                    THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                    THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
            FROM long_sector_leadership_base_panel
            WHERE undervalued_flag
              AND momentum_20_60_top20_flag
              AND forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
              AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
            GROUP BY market_scope, year, future_sector_group
            HAVING count(*) >= ?
            ORDER BY horizon, market_scope, year, future_sector_group
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_future_top5_diagnostic_columns())


def _build_overlay_comparison_df(annual_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "horizon",
        "market_scope",
        "year",
        "sector_scope",
        "comparison",
        "left_overlay_signal",
        "left_overlay_display_name",
        "right_overlay_signal",
        "right_overlay_display_name",
        "left_observation_count",
        "right_observation_count",
        "left_median_forward_topix_excess_return_pct",
        "right_median_forward_topix_excess_return_pct",
        "left_minus_right_median_forward_topix_excess_return_pct",
        "left_bank_observation_share_pct",
        "right_bank_observation_share_pct",
        "left_future_top5_sector_share_pct",
        "right_future_top5_sector_share_pct",
    ]
    if annual_df.empty:
        return pd.DataFrame(columns=columns)
    pairs = [
        (
            "long_hybrid_vs_current_strong",
            "long_hybrid_leadership_strong",
            "current_sector_strong",
        ),
        (
            "long_constituent_breadth_vs_current_strong",
            "long_constituent_breadth_leadership_strong",
            "current_sector_strong",
        ),
        (
            "current_not_weak_long_hybrid_vs_current_strong",
            "current_not_weak_long_hybrid_leadership_strong",
            "current_sector_strong",
        ),
        (
            "current_strong_long_hybrid_vs_current_strong",
            "current_strong_long_hybrid_leadership_strong",
            "current_sector_strong",
        ),
    ]
    by_key = {
        (
            int(row["horizon"]),
            str(row["market_scope"]),
            str(row["year"]),
            str(row["sector_scope"]),
            str(row["overlay_signal"]),
        ): row
        for row in annual_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    base_keys = (
        annual_df[["horizon", "market_scope", "year", "sector_scope"]]
        .drop_duplicates()
        .to_dict(orient="records")
    )
    for key in base_keys:
        key_tuple = (
            int(key["horizon"]),
            str(key["market_scope"]),
            str(key["year"]),
            str(key["sector_scope"]),
        )
        for comparison, left_signal, right_signal in pairs:
            left = by_key.get((*key_tuple, left_signal))
            right = by_key.get((*key_tuple, right_signal))
            if left is None or right is None:
                continue
            left_median = _to_float(left.get("median_forward_topix_excess_return_pct"))
            right_median = _to_float(right.get("median_forward_topix_excess_return_pct"))
            records.append(
                {
                    "horizon": key_tuple[0],
                    "market_scope": key_tuple[1],
                    "year": key_tuple[2],
                    "sector_scope": key_tuple[3],
                    "comparison": comparison,
                    "left_overlay_signal": left_signal,
                    "left_overlay_display_name": str(left["overlay_display_name"]),
                    "right_overlay_signal": right_signal,
                    "right_overlay_display_name": str(right["overlay_display_name"]),
                    "left_observation_count": int(left["observation_count"]),
                    "right_observation_count": int(right["observation_count"]),
                    "left_median_forward_topix_excess_return_pct": left_median,
                    "right_median_forward_topix_excess_return_pct": right_median,
                    "left_minus_right_median_forward_topix_excess_return_pct": (
                        left_median - right_median
                        if left_median is not None and right_median is not None
                        else None
                    ),
                    "left_bank_observation_share_pct": _to_float(
                        left.get("bank_observation_share_pct")
                    ),
                    "right_bank_observation_share_pct": _to_float(
                        right.get("bank_observation_share_pct")
                    ),
                    "left_future_top5_sector_share_pct": _to_float(
                        left.get("future_top5_sector_share_pct")
                    ),
                    "right_future_top5_sector_share_pct": _to_float(
                        right.get("future_top5_sector_share_pct")
                    ),
                }
            )
    return pd.DataFrame(records, columns=columns)


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg(CASE WHEN sector_strength_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS current_sector_score_coverage_pct,
            avg(CASE WHEN long_index_leadership_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS long_index_score_coverage_pct,
            avg(CASE WHEN long_constituent_breadth_leadership_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS long_constituent_breadth_score_coverage_pct,
            avg(CASE WHEN long_hybrid_leadership_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS long_hybrid_score_coverage_pct,
            min(date) AS first_date,
            max(date) AS last_date,
            min(CASE WHEN long_hybrid_leadership_score IS NOT NULL THEN date END)
                AS first_long_hybrid_date
        FROM long_sector_leadership_base_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_current_term_mapping_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            overlay_signal,
            overlay_family,
            overlay_display_name,
            display_order,
            CASE
                WHEN overlay_signal = 'no_sector_overlay'
                    THEN 'Undervalued + 20/60D Momentum without sector overlay.'
                WHEN overlay_signal = 'current_sector_strong'
                    THEN 'Current Daily Ranking Sector Score: Strong.'
                WHEN overlay_signal LIKE 'long_%'
                    THEN 'Anchor-date long sector leadership rank using past sector returns only.'
                WHEN overlay_signal LIKE 'current_%long_%'
                    THEN 'Current Sector Score bucket crossed with long hybrid leadership.'
                ELSE 'Long-side sector overlay variant.'
            END AS definition
        FROM long_sector_overlay_terms
        ORDER BY display_order
        """
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    return_columns = ", ".join(
        [
            f"forward_close_return_{int(horizon)}d_pct, "
            f"forward_close_excess_return_{int(horizon)}d_pct"
            for horizon in horizons
        ]
    )
    return conn.execute(
        f"""
        SELECT
            market_scope,
            year,
            date,
            code,
            company_name,
            sector_33_name,
            overlay_signal,
            overlay_display_name,
            sector_strength_bucket,
            sector_strength_score,
            long_index_leadership_score,
            long_constituent_breadth_leadership_score,
            long_hybrid_leadership_score,
            bank_sector_flag,
            future_top5_sector_flag,
            {return_columns}
        FROM long_sector_leadership_signal_observations
        ORDER BY date, overlay_display_order, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    leadership_windows: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not leadership_windows or any(int(window) <= 0 for window in leadership_windows):
        raise ValueError("leadership_windows must contain positive integers")
    if int(min_observations) < 1:
        raise ValueError("min_observations must be >= 1")
    if float(severe_loss_threshold_pct) >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if int(observation_sample_limit) < 0:
        raise ValueError("observation_sample_limit must be >= 0")


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    result = pd.concat(non_empty, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[list(columns)]


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _annual_overlay_summary_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "sector_scope",
        "sector_scope_label",
        "overlay_signal",
        "overlay_family",
        "overlay_display_name",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_raw_return_pct",
        "median_raw_return_pct",
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "p90_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
        "future_bottom5_sector_share_pct",
        "median_current_sector_score",
        "median_long_index_score",
        "median_long_constituent_breadth_score",
        "median_long_hybrid_score",
        "overlay_display_order",
    )


def _sector_contribution_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "overlay_signal",
        "overlay_family",
        "overlay_display_name",
        "overlay_display_order",
        "sector_33_name",
        "bank_sector_flag",
        "future_top5_sector_flag",
        "observation_count",
        "code_count",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "sector_observation_share_pct",
    )


def _leadership_horizon_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "leadership_window",
        "market_scope",
        "year",
        "leadership_rule",
        "observation_count",
        "code_count",
        "sector_count",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    )


def _current_vs_long_matrix_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "current_sector_bucket_label",
        "long_hybrid_bucket_label",
        "observation_count",
        "code_count",
        "sector_count",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    )


def _future_top5_diagnostic_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "future_sector_group",
        "observation_count",
        "code_count",
        "sector_count",
        "bank_observation_share_pct",
        "median_current_sector_score",
        "median_long_hybrid_score",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    )
