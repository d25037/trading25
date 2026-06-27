"""Moving-average replacements for Daily Ranking fixed-return technical states."""

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
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
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
from src.domains.analytics.ranking_sma5_count_long_evidence import _LONG_SCAFFOLDS
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

RANKING_MOVING_AVERAGE_REPLACEMENT_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-moving-average-replacement-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
_WARMUP_CALENDAR_DAYS = 720
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_TECHNICAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("fixed_overheat_20d_return_ge_30", "fixed_overheat_flag"),
    ("sma20_deviation_ge_30", "sma20_literal_overheat_flag"),
    ("sma20_qmatched_overheat", "sma20_qmatched_overheat_flag"),
    ("ema20_deviation_ge_30", "ema20_literal_overheat_flag"),
    ("ema20_qmatched_overheat", "ema20_qmatched_overheat_flag"),
    ("fixed_20d_pos_60d_pos", "fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'"),
    ("sma20_pos_sma60_pos", "sma_price_action_bucket = 'sma20_pos_sma60_pos'"),
    ("ema20_pos_ema60_pos", "ema_price_action_bucket = 'ema20_pos_ema60_pos'"),
    ("fixed_20d_pos_60d_neg", "fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'"),
    ("sma20_pos_sma60_neg", "sma_price_action_bucket = 'sma20_pos_sma60_neg'"),
    ("ema20_pos_ema60_neg", "ema_price_action_bucket = 'ema20_pos_ema60_neg'"),
    ("fixed_20d_neg_60d_pos", "fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'"),
    ("sma20_neg_sma60_pos", "sma_price_action_bucket = 'sma20_neg_sma60_pos'"),
    ("ema20_neg_ema60_pos", "ema_price_action_bucket = 'ema20_neg_ema60_pos'"),
    ("fixed_20d_neg_60d_neg", "fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'"),
    ("sma20_neg_sma60_neg", "sma_price_action_bucket = 'sma20_neg_sma60_neg'"),
    ("ema20_neg_ema60_neg", "ema_price_action_bucket = 'ema20_neg_ema60_neg'"),
    ("fixed_stale_rally_fade_candidate", "fixed_stale_rally_fade_candidate"),
    ("sma_stale_rally_fade_candidate", "sma_stale_rally_fade_candidate"),
    ("ema_stale_rally_fade_candidate", "ema_stale_rally_fade_candidate"),
)
_REPLACEMENT_PAIRS: tuple[tuple[str, str, str], ...] = (
    (
        "sma_overheat_qmatched",
        "fixed_overheat_20d_return_ge_30",
        "sma20_qmatched_overheat",
    ),
    (
        "ema_overheat_qmatched",
        "fixed_overheat_20d_return_ge_30",
        "ema20_qmatched_overheat",
    ),
    ("sma_overheat_literal", "fixed_overheat_20d_return_ge_30", "sma20_deviation_ge_30"),
    ("ema_overheat_literal", "fixed_overheat_20d_return_ge_30", "ema20_deviation_ge_30"),
    ("sma_dual_positive", "fixed_20d_pos_60d_pos", "sma20_pos_sma60_pos"),
    ("ema_dual_positive", "fixed_20d_pos_60d_pos", "ema20_pos_ema60_pos"),
    (
        "sma_recent20_positive_60d_negative",
        "fixed_20d_pos_60d_neg",
        "sma20_pos_sma60_neg",
    ),
    (
        "ema_recent20_positive_60d_negative",
        "fixed_20d_pos_60d_neg",
        "ema20_pos_ema60_neg",
    ),
    (
        "sma_recent20_negative_60d_positive",
        "fixed_20d_neg_60d_pos",
        "sma20_neg_sma60_pos",
    ),
    (
        "ema_recent20_negative_60d_positive",
        "fixed_20d_neg_60d_pos",
        "ema20_neg_ema60_pos",
    ),
    ("sma_dual_negative", "fixed_20d_neg_60d_neg", "sma20_neg_sma60_neg"),
    ("ema_dual_negative", "fixed_20d_neg_60d_neg", "ema20_neg_ema60_neg"),
    (
        "sma_stale_rally_fade_candidate",
        "fixed_stale_rally_fade_candidate",
        "sma_stale_rally_fade_candidate",
    ),
    (
        "ema_stale_rally_fade_candidate",
        "fixed_stale_rally_fade_candidate",
        "ema_stale_rally_fade_candidate",
    ),
)


@dataclass(frozen=True)
class RankingMovingAverageReplacementEvidenceResult:
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
    fixed_overheat_observation_count: int
    sma20_qmatched_overheat_threshold_pct: float | None
    ema20_qmatched_overheat_threshold_pct: float | None
    coverage_diagnostics_df: pd.DataFrame
    technical_condition_evidence_df: pd.DataFrame
    replacement_delta_df: pd.DataFrame
    long_candidate_moving_average_evidence_df: pd.DataFrame
    price_action_migration_df: pd.DataFrame
    overheat_overlap_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_moving_average_replacement_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingMovingAverageReplacementEvidenceResult:
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
        warmup_calendar_days=_WARMUP_CALENDAR_DAYS,
    )
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-moving-average-replacement-evidence-",
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
            include_relation_percentiles=True,
        )
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
        )
        _create_long_signal_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
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
        sma_threshold, ema_threshold, fixed_overheat_count = _create_replacement_panel(
            ctx.connection,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_moving_average_replacement_panel"
            ).fetchone()[0]
        )
        technical_condition_evidence_df = _build_technical_condition_evidence_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        result = RankingMovingAverageReplacementEvidenceResult(
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
            fixed_overheat_observation_count=fixed_overheat_count,
            sma20_qmatched_overheat_threshold_pct=sma_threshold,
            ema20_qmatched_overheat_threshold_pct=ema_threshold,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            technical_condition_evidence_df=technical_condition_evidence_df,
            replacement_delta_df=_build_replacement_delta_df(
                technical_condition_evidence_df,
            ),
            long_candidate_moving_average_evidence_df=(
                _build_long_candidate_moving_average_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            price_action_migration_df=_build_price_action_migration_df(
                ctx.connection,
                horizons=resolved_horizons,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            overheat_overlap_df=_build_overheat_overlap_df(
                ctx.connection,
                horizons=resolved_horizons,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_moving_average_replacement_evidence_bundle(
    result: RankingMovingAverageReplacementEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_MOVING_AVERAGE_REPLACEMENT_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_moving_average_replacement_evidence",
        function="run_ranking_moving_average_replacement_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
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
            "fixed_overheat_observation_count": (
                result.fixed_overheat_observation_count
            ),
            "sma20_qmatched_overheat_threshold_pct": (
                result.sma20_qmatched_overheat_threshold_pct
            ),
            "ema20_qmatched_overheat_threshold_pct": (
                result.ema20_qmatched_overheat_threshold_pct
            ),
            "fixed_return_definition": "close / close_lag_N - 1",
            "moving_average_definition": "close / SMA_N - 1 and close / EMA_N - 1",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "technical_condition_evidence_df": result.technical_condition_evidence_df,
            "replacement_delta_df": result.replacement_delta_df,
            "long_candidate_moving_average_evidence_df": (
                result.long_candidate_moving_average_evidence_df
            ),
            "price_action_migration_df": result.price_action_migration_df,
            "overheat_overlap_df": result.overheat_overlap_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingMovingAverageReplacementEvidenceResult) -> str:
    parts = [
        "# Ranking Moving Average Replacement Evidence",
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
        f"- fixed_overheat_observation_count: `{result.fixed_overheat_observation_count}`",
        "- sma20_qmatched_overheat_threshold_pct: "
        f"`{result.sma20_qmatched_overheat_threshold_pct}`",
        "- ema20_qmatched_overheat_threshold_pct: "
        f"`{result.ema20_qmatched_overheat_threshold_pct}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Technical Condition Evidence",
        "",
        _top_rows_for_markdown(result.technical_condition_evidence_df, limit=240),
        "",
        "## Replacement Delta",
        "",
        _top_rows_for_markdown(result.replacement_delta_df, limit=120),
        "",
        "## Long Candidate Moving Average Evidence",
        "",
        _top_rows_for_markdown(
            result.long_candidate_moving_average_evidence_df,
            limit=260,
        ),
        "",
        "## Price Action Migration",
        "",
        _top_rows_for_markdown(result.price_action_migration_df, limit=160),
        "",
        "## Overheat Overlap",
        "",
        _top_rows_for_markdown(result.overheat_overlap_df, limit=120),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_replacement_panel(conn: Any) -> tuple[float | None, float | None, int]:
    stock_code = normalize_code_sql("sd.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_moving_average_normalized_prices AS
        SELECT
            {stock_code} AS code,
            sd.date,
            arg_min(
                sd.close,
                CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code
            ) AS close
        FROM stock_data sd
        WHERE EXISTS (
            SELECT 1
            FROM daily_ranking_research_ranked r
            WHERE r.code = {stock_code}
        )
        GROUP BY {stock_code}, sd.date
        """
    )
    _create_ema_features_table(conn)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_moving_average_replacement_panel_base AS
        WITH sma_base AS (
            SELECT
                code,
                date,
                close,
                avg(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS sma20,
                count(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS sma20_sessions,
                avg(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS sma60,
                count(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS sma60_sessions
            FROM ranking_moving_average_normalized_prices
            WHERE close > 0
        ),
        ma_features AS (
            SELECT
                code,
                date,
                close AS ma_source_close,
                sma20,
                sma60,
                ((close / NULLIF(sma20, 0.0)) - 1.0) * 100.0
                    AS sma20_deviation_pct,
                ((close / NULLIF(sma60, 0.0)) - 1.0) * 100.0
                    AS sma60_deviation_pct
            FROM sma_base
            WHERE sma20_sessions = 20
              AND sma60_sessions = 60
              AND sma20 > 0
              AND sma60 > 0
        )
        SELECT
            r.*,
            l.sector_33_code,
            l.sector_33_name,
            l.sector_strength_bucket,
            l.sector_strength_score,
            l.sector_index_strength_score,
            l.sector_constituent_strength_score,
            l.long_index_leadership_score,
            l.long_constituent_breadth_leadership_score,
            l.long_hybrid_leadership_score,
            l.balanced_sector_strength_bucket_label,
            l.long_hybrid_bucket_label,
            coalesce(l.momentum_20_60_top20_flag, FALSE)
                AS momentum_20_60_top20_flag,
            s.atr20_pct,
            s.atr60_pct,
            s.atr20_to_atr60,
            s.atr20_change_20d_pct,
            coalesce(s.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                s.atr20_acceleration
                AND coalesce(r.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(s.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(s.weak_trend, FALSE) AS weak_trend,
            m.sma20,
            m.sma60,
            m.sma20_deviation_pct,
            m.sma60_deviation_pct,
            e.ema20,
            e.ema60,
            e.ema20_deviation_pct,
            e.ema60_deviation_pct,
            coalesce(
                r.recent_return_20d_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT},
                FALSE
            ) AS fixed_overheat_flag,
            coalesce(
                m.sma20_deviation_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT},
                FALSE
            ) AS sma20_literal_overheat_flag,
            coalesce(
                e.ema20_deviation_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT},
                FALSE
            ) AS ema20_literal_overheat_flag,
            CASE
                WHEN r.recent_return_20d_pct > 0
                 AND r.recent_return_60d_pct > 0
                    THEN 'fixed_20d_pos_60d_pos'
                WHEN r.recent_return_20d_pct > 0
                 AND r.recent_return_60d_pct < 0
                    THEN 'fixed_20d_pos_60d_neg'
                WHEN r.recent_return_20d_pct < 0
                 AND r.recent_return_60d_pct > 0
                    THEN 'fixed_20d_neg_60d_pos'
                WHEN r.recent_return_20d_pct < 0
                 AND r.recent_return_60d_pct < 0
                    THEN 'fixed_20d_neg_60d_neg'
                ELSE 'fixed_price_action_unclassified'
            END AS fixed_price_action_bucket,
            CASE
                WHEN m.sma20_deviation_pct > 0
                 AND m.sma60_deviation_pct > 0
                    THEN 'sma20_pos_sma60_pos'
                WHEN m.sma20_deviation_pct > 0
                 AND m.sma60_deviation_pct < 0
                    THEN 'sma20_pos_sma60_neg'
                WHEN m.sma20_deviation_pct < 0
                 AND m.sma60_deviation_pct > 0
                    THEN 'sma20_neg_sma60_pos'
                WHEN m.sma20_deviation_pct < 0
                 AND m.sma60_deviation_pct < 0
                    THEN 'sma20_neg_sma60_neg'
                ELSE 'sma_price_action_unclassified'
            END AS sma_price_action_bucket,
            CASE
                WHEN e.ema20_deviation_pct > 0
                 AND e.ema60_deviation_pct > 0
                    THEN 'ema20_pos_ema60_pos'
                WHEN e.ema20_deviation_pct > 0
                 AND e.ema60_deviation_pct < 0
                    THEN 'ema20_pos_ema60_neg'
                WHEN e.ema20_deviation_pct < 0
                 AND e.ema60_deviation_pct > 0
                    THEN 'ema20_neg_ema60_pos'
                WHEN e.ema20_deviation_pct < 0
                 AND e.ema60_deviation_pct < 0
                    THEN 'ema20_neg_ema60_neg'
                ELSE 'ema_price_action_unclassified'
            END AS ema_price_action_bucket,
            coalesce(
                r.liquidity_regime = 'stale_liquidity'
                AND (r.overvalued_warning OR r.no_positive_earnings_valuation)
                AND r.recent_return_20d_pct > 0
                AND r.recent_return_60d_pct > 0,
                FALSE
            ) AS fixed_stale_rally_fade_candidate,
            coalesce(
                r.liquidity_regime = 'stale_liquidity'
                AND (r.overvalued_warning OR r.no_positive_earnings_valuation)
                AND m.sma20_deviation_pct > 0
                AND m.sma60_deviation_pct > 0,
                FALSE
            ) AS sma_stale_rally_fade_candidate,
            coalesce(
                r.liquidity_regime = 'stale_liquidity'
                AND (r.overvalued_warning OR r.no_positive_earnings_valuation)
                AND e.ema20_deviation_pct > 0
                AND e.ema60_deviation_pct > 0,
                FALSE
            ) AS ema_stale_rally_fade_candidate
        FROM daily_ranking_research_ranked r
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = r.code
         AND l.date = r.date
         AND l.market_scope = r.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = r.code
         AND s.date = r.date
         AND s.market_scope = r.market_scope
        INNER JOIN ma_features m
          ON m.code = r.code
         AND m.date = r.date
        INNER JOIN ranking_moving_average_ema_features e
          ON e.code = r.code
         AND e.date = r.date
        WHERE r.recent_return_20d_pct IS NOT NULL
          AND r.recent_return_60d_pct IS NOT NULL
        """
    )
    total_count, fixed_overheat_count = conn.execute(
        """
        SELECT
            count(*)::INTEGER,
            sum(CASE WHEN fixed_overheat_flag THEN 1 ELSE 0 END)::INTEGER
        FROM ranking_moving_average_replacement_panel_base
        """
    ).fetchone()
    sma_qmatched_threshold = None
    if int(fixed_overheat_count) > 0:
        threshold_row = conn.execute(
            """
            SELECT sma20_deviation_pct
            FROM ranking_moving_average_replacement_panel_base
            WHERE sma20_deviation_pct IS NOT NULL
            ORDER BY sma20_deviation_pct DESC
            LIMIT 1 OFFSET ?
            """,
            [max(0, int(fixed_overheat_count) - 1)],
        ).fetchone()
        if threshold_row is not None:
            sma_qmatched_threshold = float(threshold_row[0])
    ema_qmatched_threshold = None
    if int(fixed_overheat_count) > 0:
        threshold_row = conn.execute(
            """
            SELECT ema20_deviation_pct
            FROM ranking_moving_average_replacement_panel_base
            WHERE ema20_deviation_pct IS NOT NULL
            ORDER BY ema20_deviation_pct DESC
            LIMIT 1 OFFSET ?
            """,
            [max(0, int(fixed_overheat_count) - 1)],
        ).fetchone()
        if threshold_row is not None:
            ema_qmatched_threshold = float(threshold_row[0])
    sma_threshold_sql = (
        "NULL" if sma_qmatched_threshold is None else f"{sma_qmatched_threshold:.12f}"
    )
    ema_threshold_sql = (
        "NULL" if ema_qmatched_threshold is None else f"{ema_qmatched_threshold:.12f}"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW ranking_moving_average_replacement_panel AS
        SELECT
            *,
            CAST({sma_threshold_sql} AS DOUBLE) AS sma20_qmatched_overheat_threshold_pct,
            CAST({ema_threshold_sql} AS DOUBLE) AS ema20_qmatched_overheat_threshold_pct,
            CASE
                WHEN CAST({sma_threshold_sql} AS DOUBLE) IS NULL THEN FALSE
                ELSE sma20_deviation_pct >= CAST({sma_threshold_sql} AS DOUBLE)
            END AS sma20_qmatched_overheat_flag,
            CASE
                WHEN CAST({ema_threshold_sql} AS DOUBLE) IS NULL THEN FALSE
                ELSE ema20_deviation_pct >= CAST({ema_threshold_sql} AS DOUBLE)
            END AS ema20_qmatched_overheat_flag
        FROM ranking_moving_average_replacement_panel_base
        """
    )
    _ = total_count
    return sma_qmatched_threshold, ema_qmatched_threshold, int(fixed_overheat_count)


def _create_ema_features_table(conn: Any) -> None:
    prices = conn.execute(
        """
        SELECT code, date, close
        FROM ranking_moving_average_normalized_prices
        WHERE close > 0
        ORDER BY code, date
        """
    ).fetchdf()
    if prices.empty:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_moving_average_ema_features (
                code TEXT,
                date DATE,
                ema20 DOUBLE,
                ema60 DOUBLE,
                ema20_deviation_pct DOUBLE,
                ema60_deviation_pct DOUBLE
            )
            """
        )
        return
    grouped = prices.groupby("code", sort=False)["close"]
    prices["ema20"] = grouped.transform(
        lambda series: series.ewm(span=20, adjust=False, min_periods=20).mean()
    )
    prices["ema60"] = grouped.transform(
        lambda series: series.ewm(span=60, adjust=False, min_periods=60).mean()
    )
    prices["ema20_deviation_pct"] = (prices["close"] / prices["ema20"] - 1.0) * 100.0
    prices["ema60_deviation_pct"] = (prices["close"] / prices["ema60"] - 1.0) * 100.0
    ema_features = prices.loc[
        prices["ema20"].notna() & prices["ema60"].notna(),
        [
            "code",
            "date",
            "ema20",
            "ema60",
            "ema20_deviation_pct",
            "ema60_deviation_pct",
        ],
    ]
    conn.register("ranking_moving_average_ema_features_df", ema_features)
    try:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_moving_average_ema_features AS
            SELECT * FROM ranking_moving_average_ema_features_df
            """
        )
    finally:
        conn.unregister("ranking_moving_average_ema_features_df")


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(sma20_deviation_pct) AS median_sma20_deviation_pct,
            median(sma60_deviation_pct) AS median_sma60_deviation_pct,
            median(ema20_deviation_pct) AS median_ema20_deviation_pct,
            median(ema60_deviation_pct) AS median_ema60_deviation_pct,
            quantile_cont(sma20_deviation_pct, 0.9) AS p90_sma20_deviation_pct,
            quantile_cont(sma60_deviation_pct, 0.9) AS p90_sma60_deviation_pct,
            quantile_cont(ema20_deviation_pct, 0.9) AS p90_ema20_deviation_pct,
            quantile_cont(ema60_deviation_pct, 0.9) AS p90_ema60_deviation_pct,
            avg(CASE WHEN fixed_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed_overheat_rate_pct,
            avg(CASE WHEN sma20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_literal_overheat_rate_pct,
            avg(CASE WHEN sma20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_qmatched_overheat_rate_pct,
            avg(CASE WHEN ema20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_literal_overheat_rate_pct,
            avg(CASE WHEN ema20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_qmatched_overheat_rate_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND sma_price_action_bucket = 'sma20_pos_sma60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND sma_price_action_bucket = 'sma20_pos_sma60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND sma_price_action_bucket = 'sma20_neg_sma60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND sma_price_action_bucket = 'sma20_neg_sma60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS sma_price_action_sign_match_rate_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND ema_price_action_bucket = 'ema20_pos_ema60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND ema_price_action_bucket = 'ema20_pos_ema60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND ema_price_action_bucket = 'ema20_neg_ema60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND ema_price_action_bucket = 'ema20_neg_ema60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS ema_price_action_sign_match_rate_pct
        FROM ranking_moving_average_replacement_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_technical_condition_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_moving_average_replacement_panel",
                lateral_sql=lateral_sql,
                match_condition="technical_condition.technical_condition_matches",
                group_select_sql=(
                    "'technical_condition' AS condition_family,\n"
                    "            technical_condition.technical_condition,\n"
                    "            technical_condition.technical_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "technical_condition.technical_condition, "
                    "technical_condition.technical_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_replacement_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_technical_condition_columns())


def _build_replacement_delta_df(evidence_df: pd.DataFrame) -> pd.DataFrame:
    if evidence_df.empty:
        return pd.DataFrame(columns=_replacement_delta_columns())
    rows: list[dict[str, Any]] = []
    for pair_name, fixed_condition, sma_condition in _REPLACEMENT_PAIRS:
        fixed_rows = evidence_df[evidence_df["technical_condition"] == fixed_condition]
        sma_rows = evidence_df[evidence_df["technical_condition"] == sma_condition]
        merged = fixed_rows.merge(
            sma_rows,
            on=["market_scope", "horizon"],
            suffixes=("_fixed", "_sma"),
        )
        for item in merged.to_dict("records"):
            fixed_count = float(item["observation_count_fixed"])
            sma_count = float(item["observation_count_sma"])
            rows.append(
                {
                    "replacement_pair": pair_name,
                    "fixed_condition": fixed_condition,
                    "sma_condition": sma_condition,
                    "market_scope": item["market_scope"],
                    "horizon": item["horizon"],
                    "fixed_observation_count": item["observation_count_fixed"],
                    "sma_observation_count": item["observation_count_sma"],
                    "observation_count_delta": sma_count - fixed_count,
                    "fixed_median_forward_excess_return_pct": item[
                        "median_forward_excess_return_pct_fixed"
                    ],
                    "sma_median_forward_excess_return_pct": item[
                        "median_forward_excess_return_pct_sma"
                    ],
                    "median_forward_excess_return_delta_pct": item[
                        "median_forward_excess_return_pct_sma"
                    ]
                    - item["median_forward_excess_return_pct_fixed"],
                    "fixed_mean_forward_excess_return_pct": item[
                        "mean_forward_excess_return_pct_fixed"
                    ],
                    "sma_mean_forward_excess_return_pct": item[
                        "mean_forward_excess_return_pct_sma"
                    ],
                    "mean_forward_excess_return_delta_pct": item[
                        "mean_forward_excess_return_pct_sma"
                    ]
                    - item["mean_forward_excess_return_pct_fixed"],
                    "fixed_severe_loss_rate_pct": item["severe_loss_rate_pct_fixed"],
                    "sma_severe_loss_rate_pct": item["severe_loss_rate_pct_sma"],
                    "severe_loss_rate_delta_pct": item["severe_loss_rate_pct_sma"]
                    - item["severe_loss_rate_pct_fixed"],
                    "fixed_excess_win_rate_pct": item["excess_win_rate_pct_fixed"],
                    "sma_excess_win_rate_pct": item["excess_win_rate_pct_sma"],
                    "excess_win_rate_delta_pct": item["excess_win_rate_pct_sma"]
                    - item["excess_win_rate_pct_fixed"],
                }
            )
    return pd.DataFrame(rows, columns=_replacement_delta_columns()).sort_values(
        ["market_scope", "horizon", "replacement_pair"],
        kind="stable",
    )


def _build_long_candidate_moving_average_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_moving_average_replacement_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND technical_condition.technical_condition_matches"
                ),
                group_select_sql=(
                    "'long_candidate_moving_average' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            technical_condition.technical_condition,\n"
                    "            technical_condition.technical_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "technical_condition.technical_condition, "
                    "technical_condition.technical_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql() + _replacement_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_candidate_moving_average_columns(),
    )


def _build_price_action_migration_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    fixed_price_action_bucket,
                    sma_price_action_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
                    median(recent_return_20d_pct) AS median_recent_return_20d_pct,
                    median(recent_return_60d_pct) AS median_recent_return_60d_pct,
                    median(sma20_deviation_pct) AS median_sma20_deviation_pct,
                    median(sma60_deviation_pct) AS median_sma60_deviation_pct
                FROM ranking_moving_average_replacement_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY market_scope, fixed_price_action_bucket, sma_price_action_bucket
                ORDER BY market_scope, fixed_price_action_bucket, sma_price_action_bucket
                """,
                [float(severe_loss_threshold_pct)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_price_action_migration_columns())


def _build_overheat_overlap_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    fixed_overheat_flag,
                    sma20_literal_overheat_flag,
                    sma20_qmatched_overheat_flag,
                    ema20_literal_overheat_flag,
                    ema20_qmatched_overheat_flag,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    median(recent_return_20d_pct) AS median_recent_return_20d_pct,
                    median(sma20_deviation_pct) AS median_sma20_deviation_pct,
                    median(ema20_deviation_pct) AS median_ema20_deviation_pct,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                FROM ranking_moving_average_replacement_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY
                    market_scope,
                    fixed_overheat_flag,
                    sma20_literal_overheat_flag,
                    sma20_qmatched_overheat_flag,
                    ema20_literal_overheat_flag,
                    ema20_qmatched_overheat_flag
                ORDER BY
                    market_scope,
                    fixed_overheat_flag DESC,
                    sma20_literal_overheat_flag DESC,
                    sma20_qmatched_overheat_flag DESC,
                    ema20_literal_overheat_flag DESC,
                    ema20_qmatched_overheat_flag DESC
                """,
                [float(severe_loss_threshold_pct)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_overheat_overlap_columns())


def _replacement_metric_sql() -> str:
    return """,
            median(sma20_deviation_pct) AS median_sma20_deviation_pct,
            median(sma60_deviation_pct) AS median_sma60_deviation_pct,
            median(ema20_deviation_pct) AS median_ema20_deviation_pct,
            median(ema60_deviation_pct) AS median_ema60_deviation_pct,
            avg(CASE WHEN fixed_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed_overheat_rate_pct,
            avg(CASE WHEN sma20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_literal_overheat_rate_pct,
            avg(CASE WHEN sma20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_qmatched_overheat_rate_pct,
            avg(CASE WHEN ema20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_literal_overheat_rate_pct,
            avg(CASE WHEN ema20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_qmatched_overheat_rate_pct,
            avg(CASE WHEN fixed_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS fixed_stale_rally_fade_rate_pct,
            avg(CASE WHEN sma_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma_stale_rally_fade_rate_pct,
            avg(CASE WHEN ema_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS ema_stale_rally_fade_rate_pct"""


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
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
            fixed_price_action_bucket,
            sma_price_action_bucket,
            recent_return_20d_pct,
            recent_return_60d_pct,
            sma20,
            sma60,
            sma20_deviation_pct,
            sma60_deviation_pct,
            ema20,
            ema60,
            ema20_deviation_pct,
            ema60_deviation_pct,
            fixed_overheat_flag,
            sma20_literal_overheat_flag,
            sma20_qmatched_overheat_flag,
            ema20_literal_overheat_flag,
            ema20_qmatched_overheat_flag,
            fixed_stale_rally_fade_candidate,
            sma_stale_rally_fade_candidate,
            ema_stale_rally_fade_candidate,
            liquidity_residual_z,
            valuation_signal,
            {horizon_columns}
        FROM ranking_moving_average_replacement_panel
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
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _technical_condition_columns() -> list[str]:
    return [
        "condition_family",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_replacement_metric_columns(),
    ]


def _replacement_metric_columns() -> list[str]:
    return [
        "median_sma20_deviation_pct",
        "median_sma60_deviation_pct",
        "median_ema20_deviation_pct",
        "median_ema60_deviation_pct",
        "fixed_overheat_rate_pct",
        "sma20_literal_overheat_rate_pct",
        "sma20_qmatched_overheat_rate_pct",
        "ema20_literal_overheat_rate_pct",
        "ema20_qmatched_overheat_rate_pct",
        "fixed_stale_rally_fade_rate_pct",
        "sma_stale_rally_fade_rate_pct",
        "ema_stale_rally_fade_rate_pct",
    ]


def _replacement_delta_columns() -> list[str]:
    return [
        "replacement_pair",
        "fixed_condition",
        "sma_condition",
        "market_scope",
        "horizon",
        "fixed_observation_count",
        "sma_observation_count",
        "observation_count_delta",
        "fixed_median_forward_excess_return_pct",
        "sma_median_forward_excess_return_pct",
        "median_forward_excess_return_delta_pct",
        "fixed_mean_forward_excess_return_pct",
        "sma_mean_forward_excess_return_pct",
        "mean_forward_excess_return_delta_pct",
        "fixed_severe_loss_rate_pct",
        "sma_severe_loss_rate_pct",
        "severe_loss_rate_delta_pct",
        "fixed_excess_win_rate_pct",
        "sma_excess_win_rate_pct",
        "excess_win_rate_delta_pct",
    ]


def _long_candidate_moving_average_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_replacement_metric_columns(),
    ]


def _price_action_migration_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "fixed_price_action_bucket",
        "sma_price_action_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_sma20_deviation_pct",
        "median_sma60_deviation_pct",
    ]


def _overheat_overlap_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "fixed_overheat_flag",
        "sma20_literal_overheat_flag",
        "sma20_qmatched_overheat_flag",
        "ema20_literal_overheat_flag",
        "ema20_qmatched_overheat_flag",
        "observation_count",
        "code_count",
        "date_count",
        "median_recent_return_20d_pct",
        "median_sma20_deviation_pct",
        "median_ema20_deviation_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
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
            "technical_condition_order",
            "long_scaffold_order",
            "fixed_price_action_bucket",
            "sma_price_action_bucket",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
