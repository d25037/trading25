"""Long-side sector leadership horizon decomposition for Momentum Value."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    LongLeadershipFeaturesRequest,
    SectorStrengthFeaturesRequest,
    build_long_leadership_features,
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
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    DEFAULT_HORIZONS,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)
from src.shared.utils.pandas_type_guards import required_int, required_str

RANKING_LONG_SECTOR_LEADERSHIP_HORIZON_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/ranking-long-sector-leadership-horizon-decomposition"
)
DEFAULT_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
PUBLIC_FEATURE_BUILDER = build_long_leadership_features
SECTOR_STRENGTH_FAMILY_OPTIONS: tuple[str, ...] = (
    "balanced_sector_strength",
    "long_hybrid_leadership",
    "both",
)
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
    sector_strength_family: str
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    annual_overlay_summary_df: pd.DataFrame
    selected_sector_strength_summary_df: pd.DataFrame
    bank_concentration_df: pd.DataFrame
    sector_contribution_df: pd.DataFrame
    leadership_horizon_df: pd.DataFrame
    balanced_vs_long_matrix_df: pd.DataFrame
    balanced_long_switch_attribution_df: pd.DataFrame
    long_hybrid_balanced_tolerance_df: pd.DataFrame
    future_top5_diagnostic_df: pd.DataFrame
    overlay_comparison_df: pd.DataFrame
    overlay_term_mapping_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_long_sector_leadership_horizon_decomposition_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    leadership_windows: Iterable[int] = DEFAULT_LEADERSHIP_WINDOWS,
    sector_strength_family: str = "both",
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingLongSectorLeadershipHorizonDecompositionResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_leadership_windows = tuple(
        sorted({int(window) for window in leadership_windows})
    )
    resolved_sector_strength_family = _normalize_sector_strength_family(
        sector_strength_family
    )
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
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

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-long-sector-leadership-horizon-decomposition-",
    ) as ctx:
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="long_sector_leadership",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=False,
                percentile_features=(),
            ),
        )
        signal_source = relations.ranked_signals
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="long_sector_leadership_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="long_sector_leadership_features",
                leadership_windows=resolved_leadership_windows,
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(leadership_features,),
            namespace="long_sector_leadership",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="long_sector_leadership_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="long_sector_leadership_outcomes",
        )
        _create_long_signal_tables(
            ctx.connection,
            source_name=evaluated.name,
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
            sector_strength_family=resolved_sector_strength_family,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            annual_overlay_summary_df=annual_overlay_summary_df,
            selected_sector_strength_summary_df=_build_selected_sector_strength_summary_df(
                annual_overlay_summary_df,
                sector_strength_family=resolved_sector_strength_family,
            ),
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
            balanced_vs_long_matrix_df=_build_balanced_vs_long_matrix_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            balanced_long_switch_attribution_df=(
                _build_balanced_long_switch_attribution_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            long_hybrid_balanced_tolerance_df=(
                _build_long_hybrid_balanced_tolerance_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
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
            overlay_term_mapping_df=_build_overlay_term_mapping_df(ctx.connection),
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
            "sector_strength_family": result.sector_strength_family,
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
            "selected_sector_strength_summary_df": result.selected_sector_strength_summary_df,
            "bank_concentration_df": result.bank_concentration_df,
            "sector_contribution_df": result.sector_contribution_df,
            "leadership_horizon_df": result.leadership_horizon_df,
            "balanced_vs_long_matrix_df": result.balanced_vs_long_matrix_df,
            "balanced_long_switch_attribution_df": result.balanced_long_switch_attribution_df,
            "long_hybrid_balanced_tolerance_df": result.long_hybrid_balanced_tolerance_df,
            "future_top5_diagnostic_df": result.future_top5_diagnostic_df,
            "overlay_comparison_df": result.overlay_comparison_df,
            "overlay_term_mapping_df": result.overlay_term_mapping_df,
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
        f"- sector_strength_family: `{result.sector_strength_family}`",
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
        "## Selected Sector Strength Summary",
        "",
        _top_rows_for_markdown(result.selected_sector_strength_summary_df, limit=160),
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
        "## Balanced x Long Matrix",
        "",
        _top_rows_for_markdown(result.balanced_vs_long_matrix_df, limit=260),
        "",
        "## Balanced Long Switch Attribution",
        "",
        _top_rows_for_markdown(
            result.balanced_long_switch_attribution_df,
            limit=260,
        ),
        "",
        "## Long Hybrid Balanced Tolerance",
        "",
        _top_rows_for_markdown(
            result.long_hybrid_balanced_tolerance_df,
            limit=260,
        ),
        "",
        "## Future Top 5 Diagnostic",
        "",
        _top_rows_for_markdown(result.future_top5_diagnostic_df, limit=260),
        "",
        "## Overlay Comparison",
        "",
        _top_rows_for_markdown(result.overlay_comparison_df, limit=260),
        "",
        "## Daily Ranking Overlay Terms",
        "",
        _top_rows_for_markdown(result.overlay_term_mapping_df, limit=80),
        "",
        "## Observation Sample",
        "",
        _top_rows_for_markdown(result.observation_sample_df, limit=80),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _create_long_signal_tables(
    conn: Any,
    *,
    source_name: str,
) -> None:
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
                "balanced_sector_strength_strong",
                "Balanced Sector Strength",
                "Momentum Value + Balanced Sector Strength: Strong",
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
                "balanced_not_weak_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Balanced not Weak + Long Hybrid Leadership",
                60,
            ),
            (
                "balanced_strong_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Balanced Strong + Long Hybrid Leadership",
                70,
            ),
            (
                "balanced_weak_long_hybrid_leadership_strong",
                "Long Sector Leadership",
                "Momentum Value + Balanced Weak + Long Hybrid Leadership",
                80,
            ),
        ],
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_base_panel AS
        SELECT
            r.*,
            r.forecast_per_percentile AS forward_per_percentile,
            substr(CAST(r.date AS VARCHAR), 1, 4) AS year,
            r.pbr_percentile <= 0.2
                AND r.forecast_per_percentile <= 0.2 AS undervalued_flag,
            r.sector_33_name = '銀行業' AS bank_sector_flag,
            r.sector_33_name IN ('非鉄金属', '海運業', '卸売業', '電気機器', '保険業')
                AS future_top5_sector_flag,
            r.sector_33_name IN ('空運業', '陸運業', 'パルプ・紙', '繊維製品', '医薬品')
                AS future_bottom5_sector_flag
        FROM {source_name} r
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE long_sector_leadership_signal_observations_raw AS
        SELECT 'no_sector_overlay' AS overlay_signal, * FROM long_sector_leadership_base_panel
        WHERE undervalued_flag AND momentum_20_60_top20_flag
        UNION ALL
        SELECT 'balanced_sector_strength_strong' AS overlay_signal, * FROM long_sector_leadership_base_panel
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
        SELECT 'balanced_not_weak_long_hybrid_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND coalesce(sector_strength_bucket, 'sector_unknown') <> 'sector_weak'
          AND long_hybrid_leadership_score >= 0.799999
        UNION ALL
        SELECT 'balanced_strong_long_hybrid_leadership_strong' AS overlay_signal, *
        FROM long_sector_leadership_base_panel
        WHERE undervalued_flag
          AND momentum_20_60_top20_flag
          AND sector_strength_bucket = 'sector_strong'
          AND long_hybrid_leadership_score >= 0.799999
        UNION ALL
        SELECT 'balanced_weak_long_hybrid_leadership_strong' AS overlay_signal, *
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
            median(sector_strength_score) AS median_balanced_sector_strength_score,
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
                "horizon": required_int(horizon, field="horizon"),
                "market_scope": required_str(market_scope, field="market_scope"),
                "year": required_str(year, field="year"),
                "overlay_signal": required_str(overlay_signal, field="overlay_signal"),
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


def _build_balanced_vs_long_matrix_df(
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
                balanced_sector_strength_bucket_label,
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
                balanced_sector_strength_bucket_label,
                long_hybrid_bucket_label
            HAVING count(*) >= ?
            ORDER BY horizon, market_scope, year, balanced_sector_strength_bucket_label, long_hybrid_bucket_label
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_balanced_vs_long_matrix_columns())


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
                median(sector_strength_score) AS median_balanced_sector_strength_score,
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


def _build_balanced_long_switch_attribution_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            WITH base AS (
                SELECT
                    *,
                    CASE
                        WHEN balanced_sector_strength_bucket_label = 'Balanced Strong'
                         AND long_hybrid_bucket_label = 'Long Strong'
                            THEN 'common_both_strong'
                        WHEN balanced_sector_strength_bucket_label = 'Balanced Strong'
                         AND long_hybrid_bucket_label <> 'Long Strong'
                            THEN 'dropped_balanced_strong_long_not_strong'
                        WHEN balanced_sector_strength_bucket_label <> 'Balanced Strong'
                         AND long_hybrid_bucket_label = 'Long Strong'
                            THEN 'added_balanced_not_strong_long_strong'
                        ELSE 'neither_strong'
                    END AS switch_group,
                    CASE
                        WHEN CAST(year AS INTEGER) BETWEEN 2016 AND 2021
                            THEN '2016-2021'
                        WHEN CAST(year AS INTEGER) BETWEEN 2022 AND 2026
                            THEN '2022-2026'
                        ELSE 'other'
                    END AS period_group
                FROM long_sector_leadership_base_panel
                WHERE undervalued_flag
                  AND momentum_20_60_top20_flag
                  AND forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                  AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
            ),
            scoped AS (
                SELECT 'all' AS sector_scope, 'All sectors' AS sector_scope_label, *
                FROM base
                UNION ALL
                SELECT 'ex_banks' AS sector_scope, 'ex Banks' AS sector_scope_label, *
                FROM base
                WHERE NOT bank_sector_flag
                UNION ALL
                SELECT 'banks_only' AS sector_scope, 'Banks only' AS sector_scope_label, *
                FROM base
                WHERE bank_sector_flag
            ),
            periodized AS (
                SELECT 'all_years' AS period_label, * FROM scoped
                UNION ALL
                SELECT period_group AS period_label, * FROM scoped
                WHERE period_group <> 'other'
            ),
            observation_summary AS (
                SELECT
                    {int(horizon)} AS horizon,
                    market_scope,
                    period_label,
                    sector_scope,
                    sector_scope_label,
                    switch_group,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    count(DISTINCT sector_33_name) AS sector_count,
                    avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS bank_observation_share_pct,
                    avg(CASE WHEN future_top5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS future_top5_sector_share_pct,
                    median(forward_close_return_{int(horizon)}d_pct)
                        AS median_raw_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    quantile_cont(forward_close_excess_return_{int(horizon)}d_pct, 0.10)
                        AS p10_forward_topix_excess_return_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                FROM periodized
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    sector_scope_label,
                    switch_group
                HAVING count(*) >= ?
            ),
            date_baskets AS (
                SELECT
                    market_scope,
                    period_label,
                    sector_scope,
                    switch_group,
                    date,
                    count(*) AS date_observation_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS date_equal_weight_forward_topix_excess_return_pct
                FROM periodized
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    switch_group,
                    date
            ),
            date_summary AS (
                SELECT
                    market_scope,
                    period_label,
                    sector_scope,
                    switch_group,
                    count(*) AS date_basket_count,
                    median(date_equal_weight_forward_topix_excess_return_pct)
                        AS date_level_median_forward_topix_excess_return_pct,
                    quantile_cont(date_equal_weight_forward_topix_excess_return_pct, 0.10)
                        AS date_level_p10_forward_topix_excess_return_pct,
                    avg(CASE WHEN date_equal_weight_forward_topix_excess_return_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0
                        AS date_level_win_rate_pct,
                    avg(date_equal_weight_forward_topix_excess_return_pct)
                    / nullif(stddev_samp(date_equal_weight_forward_topix_excess_return_pct), 0.0)
                        AS date_level_ir
                FROM date_baskets
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    switch_group
            )
            SELECT
                o.*,
                d.date_basket_count,
                d.date_level_median_forward_topix_excess_return_pct,
                d.date_level_p10_forward_topix_excess_return_pct,
                d.date_level_win_rate_pct,
                d.date_level_ir
            FROM observation_summary o
            LEFT JOIN date_summary d
              ON d.market_scope = o.market_scope
             AND d.period_label = o.period_label
             AND d.sector_scope = o.sector_scope
             AND d.switch_group = o.switch_group
            ORDER BY
                horizon,
                market_scope,
                period_label,
                sector_scope,
                CASE o.switch_group
                    WHEN 'common_both_strong' THEN 10
                    WHEN 'dropped_balanced_strong_long_not_strong' THEN 20
                    WHEN 'added_balanced_not_strong_long_strong' THEN 30
                    ELSE 40
                END
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(
        frames,
        columns=_balanced_long_switch_attribution_columns(),
    )


def _build_long_hybrid_balanced_tolerance_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        conn.execute(
            f"""
            WITH base AS (
                SELECT
                    *,
                    CASE
                        WHEN sector_strength_score IS NULL
                            THEN 'unknown'
                        WHEN sector_strength_score < 0.2
                            THEN 'balanced_lt_0_2'
                        WHEN sector_strength_score < 0.4
                            THEN 'balanced_0_2_to_0_4'
                        WHEN sector_strength_score < 0.6
                            THEN 'balanced_0_4_to_0_6'
                        WHEN sector_strength_score < 0.8
                            THEN 'balanced_0_6_to_0_8'
                        ELSE 'balanced_ge_0_8'
                    END AS balanced_score_band,
                    CASE
                        WHEN sector_strength_score IS NULL
                            THEN 99
                        WHEN sector_strength_score < 0.2
                            THEN 10
                        WHEN sector_strength_score < 0.4
                            THEN 20
                        WHEN sector_strength_score < 0.6
                            THEN 30
                        WHEN sector_strength_score < 0.8
                            THEN 40
                        ELSE 50
                    END AS balanced_score_band_order,
                    CASE
                        WHEN sector_strength_score IS NULL
                            THEN 'Unknown'
                        WHEN sector_strength_score < 0.2
                            THEN '<0.2'
                        WHEN sector_strength_score < 0.4
                            THEN '0.2..0.4'
                        WHEN sector_strength_score < 0.6
                            THEN '0.4..0.6'
                        WHEN sector_strength_score < 0.8
                            THEN '0.6..0.8'
                        ELSE '>=0.8'
                    END AS balanced_score_band_label,
                    CASE
                        WHEN CAST(year AS INTEGER) BETWEEN 2016 AND 2021
                            THEN '2016-2021'
                        WHEN CAST(year AS INTEGER) BETWEEN 2022 AND 2026
                            THEN '2022-2026'
                        ELSE 'other'
                    END AS period_group
                FROM long_sector_leadership_base_panel
                WHERE undervalued_flag
                  AND momentum_20_60_top20_flag
                  AND long_hybrid_bucket_label = 'Long Strong'
                  AND forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                  AND forward_close_return_{int(horizon)}d_pct IS NOT NULL
            ),
            scoped AS (
                SELECT 'all' AS sector_scope, 'All sectors' AS sector_scope_label, *
                FROM base
                UNION ALL
                SELECT 'ex_banks' AS sector_scope, 'ex Banks' AS sector_scope_label, *
                FROM base
                WHERE NOT bank_sector_flag
                UNION ALL
                SELECT 'banks_only' AS sector_scope, 'Banks only' AS sector_scope_label, *
                FROM base
                WHERE bank_sector_flag
            ),
            periodized AS (
                SELECT 'all_years' AS period_label, * FROM scoped
                UNION ALL
                SELECT period_group AS period_label, * FROM scoped
                WHERE period_group <> 'other'
            ),
            observation_summary AS (
                SELECT
                    {int(horizon)} AS horizon,
                    market_scope,
                    period_label,
                    sector_scope,
                    sector_scope_label,
                    balanced_score_band,
                    any_value(balanced_score_band_label) AS balanced_score_band_label,
                    any_value(balanced_score_band_order) AS balanced_score_band_order,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    count(DISTINCT sector_33_name) AS sector_count,
                    avg(CASE WHEN bank_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS bank_observation_share_pct,
                    avg(CASE WHEN future_top5_sector_flag THEN 1.0 ELSE 0.0 END) * 100.0
                        AS future_top5_sector_share_pct,
                    median(sector_strength_score)
                        AS median_balanced_sector_strength_score,
                    median(long_hybrid_leadership_score)
                        AS median_long_hybrid_score,
                    median(forward_close_return_{int(horizon)}d_pct)
                        AS median_raw_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_topix_excess_return_pct,
                    quantile_cont(forward_close_excess_return_{int(horizon)}d_pct, 0.10)
                        AS p10_forward_topix_excess_return_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_pct,
                    avg(CASE WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                FROM periodized
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    sector_scope_label,
                    balanced_score_band
                HAVING count(*) >= ?
            ),
            date_baskets AS (
                SELECT
                    market_scope,
                    period_label,
                    sector_scope,
                    balanced_score_band,
                    date,
                    count(*) AS date_observation_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS date_equal_weight_forward_topix_excess_return_pct
                FROM periodized
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    balanced_score_band,
                    date
            ),
            date_summary AS (
                SELECT
                    market_scope,
                    period_label,
                    sector_scope,
                    balanced_score_band,
                    count(*) AS date_basket_count,
                    median(date_equal_weight_forward_topix_excess_return_pct)
                        AS date_level_median_forward_topix_excess_return_pct,
                    quantile_cont(date_equal_weight_forward_topix_excess_return_pct, 0.10)
                        AS date_level_p10_forward_topix_excess_return_pct,
                    avg(CASE WHEN date_equal_weight_forward_topix_excess_return_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0
                        AS date_level_win_rate_pct,
                    avg(date_equal_weight_forward_topix_excess_return_pct)
                    / nullif(stddev_samp(date_equal_weight_forward_topix_excess_return_pct), 0.0)
                        AS date_level_ir
                FROM date_baskets
                GROUP BY
                    market_scope,
                    period_label,
                    sector_scope,
                    balanced_score_band
            )
            SELECT
                o.*,
                d.date_basket_count,
                d.date_level_median_forward_topix_excess_return_pct,
                d.date_level_p10_forward_topix_excess_return_pct,
                d.date_level_win_rate_pct,
                d.date_level_ir
            FROM observation_summary o
            LEFT JOIN date_summary d
              ON d.market_scope = o.market_scope
             AND d.period_label = o.period_label
             AND d.sector_scope = o.sector_scope
             AND d.balanced_score_band = o.balanced_score_band
            ORDER BY
                horizon,
                market_scope,
                period_label,
                sector_scope,
                o.balanced_score_band_order
            """,
            [float(severe_loss_threshold_pct), int(min_observations)],
        ).fetchdf()
        for horizon in horizons
    ]
    return _concat_sorted(
        frames,
        columns=_long_hybrid_balanced_tolerance_columns(),
    )


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
            "long_hybrid_vs_balanced_strong",
            "long_hybrid_leadership_strong",
            "balanced_sector_strength_strong",
        ),
        (
            "long_constituent_breadth_vs_balanced_strong",
            "long_constituent_breadth_leadership_strong",
            "balanced_sector_strength_strong",
        ),
        (
            "balanced_not_weak_long_hybrid_vs_balanced_strong",
            "balanced_not_weak_long_hybrid_leadership_strong",
            "balanced_sector_strength_strong",
        ),
        (
            "balanced_strong_long_hybrid_vs_balanced_strong",
            "balanced_strong_long_hybrid_leadership_strong",
            "balanced_sector_strength_strong",
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
            right_median = _to_float(
                right.get("median_forward_topix_excess_return_pct")
            )
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
                AS balanced_sector_strength_score_coverage_pct,
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


def _build_overlay_term_mapping_df(conn: Any) -> pd.DataFrame:
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
                WHEN overlay_signal = 'balanced_sector_strength_strong'
                    THEN 'Daily Ranking Balanced Sector Strength: Strong.'
                WHEN overlay_signal LIKE 'long_%'
                    THEN 'Anchor-date long sector leadership rank using past sector returns only.'
                WHEN overlay_signal LIKE 'balanced_%long_%'
                    THEN 'Balanced Sector Strength bucket crossed with long hybrid leadership.'
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


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _normalize_sector_strength_family(value: str) -> str:
    normalized = str(value).strip()
    if normalized not in SECTOR_STRENGTH_FAMILY_OPTIONS:
        supported = ", ".join(SECTOR_STRENGTH_FAMILY_OPTIONS)
        raise ValueError(f"sector_strength_family must be one of: {supported}")
    return normalized


def _build_selected_sector_strength_summary_df(
    annual_overlay_summary_df: pd.DataFrame,
    *,
    sector_strength_family: str,
) -> pd.DataFrame:
    if annual_overlay_summary_df.empty:
        return annual_overlay_summary_df.copy()
    if sector_strength_family == "both":
        selected_signals = {
            "balanced_sector_strength_strong",
            "long_hybrid_leadership_strong",
        }
    elif sector_strength_family == "balanced_sector_strength":
        selected_signals = {"balanced_sector_strength_strong"}
    else:
        selected_signals = {"long_hybrid_leadership_strong"}
    return annual_overlay_summary_df[
        annual_overlay_summary_df["overlay_signal"].isin(selected_signals)
    ].reset_index(drop=True)


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
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
        "median_balanced_sector_strength_score",
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


def _balanced_vs_long_matrix_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "year",
        "balanced_sector_strength_bucket_label",
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
        "median_balanced_sector_strength_score",
        "median_long_hybrid_score",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    )


def _balanced_long_switch_attribution_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "period_label",
        "sector_scope",
        "sector_scope_label",
        "switch_group",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "date_basket_count",
        "date_level_median_forward_topix_excess_return_pct",
        "date_level_p10_forward_topix_excess_return_pct",
        "date_level_win_rate_pct",
        "date_level_ir",
    )


def _long_hybrid_balanced_tolerance_columns() -> tuple[str, ...]:
    return (
        "horizon",
        "market_scope",
        "period_label",
        "sector_scope",
        "sector_scope_label",
        "balanced_score_band",
        "balanced_score_band_label",
        "balanced_score_band_order",
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
        "median_balanced_sector_strength_score",
        "median_long_hybrid_score",
        "median_raw_return_pct",
        "median_forward_topix_excess_return_pct",
        "p10_forward_topix_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "date_basket_count",
        "date_level_median_forward_topix_excess_return_pct",
        "date_level_p10_forward_topix_excess_return_pct",
        "date_level_win_rate_pct",
        "date_level_ir",
    )
