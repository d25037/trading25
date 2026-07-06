"""Trend-slope evidence for Daily Ranking fixed-return technical states."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
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

RANKING_TREND_SLOPE_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-trend-slope-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_WARMUP_CALENDAR_DAYS = 820
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_SLOPE_WINDOWS: tuple[int, ...] = (20, 60)
_MA_SLOPE_LAGS: tuple[int, ...] = (5, 20)
_HIGH_R2_THRESHOLD = 0.5
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_TECHNICAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("fixed_20d_pos_60d_pos", "fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'"),
    ("fixed_20d_pos_60d_neg", "fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'"),
    ("fixed_20d_neg_60d_pos", "fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'"),
    ("fixed_20d_neg_60d_neg", "fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'"),
    ("lr20_pos_lr60_pos", "lr_price_action_bucket = 'lr20_pos_lr60_pos'"),
    ("lr20_pos_lr60_neg", "lr_price_action_bucket = 'lr20_pos_lr60_neg'"),
    ("lr20_neg_lr60_pos", "lr_price_action_bucket = 'lr20_neg_lr60_pos'"),
    ("lr20_neg_lr60_neg", "lr_price_action_bucket = 'lr20_neg_lr60_neg'"),
    (
        "lr20_pos_lr60_pos_r2_high",
        "lr_price_action_bucket = 'lr20_pos_lr60_pos' "
        f"AND price_lr_r2_20 >= {_HIGH_R2_THRESHOLD} "
        f"AND price_lr_r2_60 >= {_HIGH_R2_THRESHOLD}",
    ),
    (
        "sma20_slope_pos_sma60_slope_pos",
        "sma_slope_bucket = 'sma20_slope_pos_sma60_slope_pos'",
    ),
    (
        "sma20_slope_neg_sma60_slope_pos",
        "sma_slope_bucket = 'sma20_slope_neg_sma60_slope_pos'",
    ),
    (
        "ema20_slope_pos_ema60_slope_pos",
        "ema_slope_bucket = 'ema20_slope_pos_ema60_slope_pos'",
    ),
    (
        "ema20_slope_neg_ema60_slope_pos",
        "ema_slope_bucket = 'ema20_slope_neg_ema60_slope_pos'",
    ),
    (
        "lr20_accel_over_lr60",
        "price_lr_slope_20_pct > price_lr_slope_60_pct",
    ),
    (
        "lr20_decel_below_lr60",
        "price_lr_slope_20_pct <= price_lr_slope_60_pct",
    ),
)


@dataclass(frozen=True)
class RankingTrendSlopeEvidenceResult:
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
    coverage_diagnostics_df: pd.DataFrame
    technical_condition_evidence_df: pd.DataFrame
    fixed_vs_slope_conflict_df: pd.DataFrame
    long_candidate_trend_slope_evidence_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_trend_slope_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingTrendSlopeEvidenceResult:
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
        warmup_calendar_days=max(_WARMUP_CALENDAR_DAYS, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-trend-slope-evidence-",
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
        _create_trend_slope_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_trend_slope_panel"
            ).fetchone()[0]
        )
        result = RankingTrendSlopeEvidenceResult(
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
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            technical_condition_evidence_df=_build_technical_condition_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            fixed_vs_slope_conflict_df=_build_fixed_vs_slope_conflict_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_candidate_trend_slope_evidence_df=(
                _build_long_candidate_trend_slope_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_trend_slope_evidence_bundle(
    result: RankingTrendSlopeEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_TREND_SLOPE_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_trend_slope_evidence",
        function="run_ranking_trend_slope_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
            "slope_windows": list(_SLOPE_WINDOWS),
            "ma_slope_lags": list(_MA_SLOPE_LAGS),
            "high_r2_threshold": _HIGH_R2_THRESHOLD,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "price_lr_definition": (
                "rolling OLS on log(close); slope pct is exp(slope*(window-1))-1"
            ),
            "ma_slope_definition": "moving_average(today) / moving_average(lag) - 1",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "technical_condition_evidence_df": result.technical_condition_evidence_df,
            "fixed_vs_slope_conflict_df": result.fixed_vs_slope_conflict_df,
            "long_candidate_trend_slope_evidence_df": (
                result.long_candidate_trend_slope_evidence_df
            ),
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingTrendSlopeEvidenceResult) -> str:
    parts = [
        "# Ranking Trend Slope Evidence",
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
        f"- high_r2_threshold: `{_HIGH_R2_THRESHOLD}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Technical Condition Evidence",
        "",
        _top_rows_for_markdown(result.technical_condition_evidence_df, limit=220),
        "",
        "## Fixed vs Slope Conflict",
        "",
        _top_rows_for_markdown(result.fixed_vs_slope_conflict_df, limit=180),
        "",
        "## Long Candidate Trend Slope Evidence",
        "",
        _top_rows_for_markdown(
            result.long_candidate_trend_slope_evidence_df,
            limit=260,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_trend_slope_panel(conn: Any) -> None:
    stock_code = normalize_code_sql("sd.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_slope_normalized_prices AS
        SELECT
            {stock_code} AS code,
            sd.date,
            arg_min(
                sd.close,
                CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code
            ) AS close
        FROM stock_data sd
        WHERE sd.date <= (
                SELECT max(date) FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
            )
          AND EXISTS (
            SELECT 1
            FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
            WHERE r.code = {stock_code}
          )
        GROUP BY {stock_code}, sd.date
        """
    )
    _create_trend_features_table(conn)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_slope_panel AS
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
            f.price_lr_slope_20_pct,
            f.price_lr_slope_60_pct,
            f.price_lr_r2_20,
            f.price_lr_r2_60,
            f.sma20,
            f.sma60,
            f.ema20,
            f.ema60,
            f.sma20_slope_5d_pct,
            f.sma20_slope_20d_pct,
            f.sma60_slope_5d_pct,
            f.sma60_slope_20d_pct,
            f.ema20_slope_5d_pct,
            f.ema20_slope_20d_pct,
            f.ema60_slope_5d_pct,
            f.ema60_slope_20d_pct,
            CASE
                WHEN r.recent_return_20d_pct > 0 THEN 'fixed20_pos'
                WHEN r.recent_return_20d_pct < 0 THEN 'fixed20_neg'
                ELSE 'fixed20_flat'
            END AS fixed_20d_sign_bucket,
            CASE
                WHEN r.recent_return_60d_pct > 0 THEN 'fixed60_pos'
                WHEN r.recent_return_60d_pct < 0 THEN 'fixed60_neg'
                ELSE 'fixed60_flat'
            END AS fixed_60d_sign_bucket,
            CASE
                WHEN f.price_lr_slope_20_pct > 0 THEN 'lr20_pos'
                WHEN f.price_lr_slope_20_pct < 0 THEN 'lr20_neg'
                ELSE 'lr20_flat'
            END AS lr20_sign_bucket,
            CASE
                WHEN f.price_lr_slope_60_pct > 0 THEN 'lr60_pos'
                WHEN f.price_lr_slope_60_pct < 0 THEN 'lr60_neg'
                ELSE 'lr60_flat'
            END AS lr60_sign_bucket,
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
                WHEN f.price_lr_slope_20_pct > 0
                 AND f.price_lr_slope_60_pct > 0
                    THEN 'lr20_pos_lr60_pos'
                WHEN f.price_lr_slope_20_pct > 0
                 AND f.price_lr_slope_60_pct < 0
                    THEN 'lr20_pos_lr60_neg'
                WHEN f.price_lr_slope_20_pct < 0
                 AND f.price_lr_slope_60_pct > 0
                    THEN 'lr20_neg_lr60_pos'
                WHEN f.price_lr_slope_20_pct < 0
                 AND f.price_lr_slope_60_pct < 0
                    THEN 'lr20_neg_lr60_neg'
                ELSE 'lr_price_action_unclassified'
            END AS lr_price_action_bucket,
            CASE
                WHEN f.sma20_slope_5d_pct > 0
                 AND f.sma60_slope_20d_pct > 0
                    THEN 'sma20_slope_pos_sma60_slope_pos'
                WHEN f.sma20_slope_5d_pct < 0
                 AND f.sma60_slope_20d_pct > 0
                    THEN 'sma20_slope_neg_sma60_slope_pos'
                WHEN f.sma20_slope_5d_pct > 0
                 AND f.sma60_slope_20d_pct < 0
                    THEN 'sma20_slope_pos_sma60_slope_neg'
                WHEN f.sma20_slope_5d_pct < 0
                 AND f.sma60_slope_20d_pct < 0
                    THEN 'sma20_slope_neg_sma60_slope_neg'
                ELSE 'sma_slope_unclassified'
            END AS sma_slope_bucket,
            CASE
                WHEN f.ema20_slope_5d_pct > 0
                 AND f.ema60_slope_20d_pct > 0
                    THEN 'ema20_slope_pos_ema60_slope_pos'
                WHEN f.ema20_slope_5d_pct < 0
                 AND f.ema60_slope_20d_pct > 0
                    THEN 'ema20_slope_neg_ema60_slope_pos'
                WHEN f.ema20_slope_5d_pct > 0
                 AND f.ema60_slope_20d_pct < 0
                    THEN 'ema20_slope_pos_ema60_slope_neg'
                WHEN f.ema20_slope_5d_pct < 0
                 AND f.ema60_slope_20d_pct < 0
                    THEN 'ema20_slope_neg_ema60_slope_neg'
                ELSE 'ema_slope_unclassified'
            END AS ema_slope_bucket,
            CASE
                WHEN r.recent_return_20d_pct > 0 AND f.price_lr_slope_20_pct > 0
                    THEN 'fixed20_pos_lr20_pos'
                WHEN r.recent_return_20d_pct > 0 AND f.price_lr_slope_20_pct <= 0
                    THEN 'fixed20_pos_lr20_neg'
                WHEN r.recent_return_20d_pct <= 0 AND f.price_lr_slope_20_pct > 0
                    THEN 'fixed20_neg_lr20_pos'
                ELSE 'fixed20_neg_lr20_neg'
            END AS fixed20_lr20_conflict_bucket,
            CASE
                WHEN r.recent_return_60d_pct > 0 AND f.price_lr_slope_60_pct > 0
                    THEN 'fixed60_pos_lr60_pos'
                WHEN r.recent_return_60d_pct > 0 AND f.price_lr_slope_60_pct <= 0
                    THEN 'fixed60_pos_lr60_neg'
                WHEN r.recent_return_60d_pct <= 0 AND f.price_lr_slope_60_pct > 0
                    THEN 'fixed60_neg_lr60_pos'
                ELSE 'fixed60_neg_lr60_neg'
            END AS fixed60_lr60_conflict_bucket
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = r.code
         AND l.date = r.date
         AND l.market_scope = r.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = r.code
         AND s.date = r.date
         AND s.market_scope = r.market_scope
        INNER JOIN ranking_trend_slope_features f
          ON f.code = r.code
         AND f.date = r.date
        WHERE r.recent_return_20d_pct IS NOT NULL
          AND r.recent_return_60d_pct IS NOT NULL
          AND f.price_lr_slope_20_pct IS NOT NULL
          AND f.price_lr_slope_60_pct IS NOT NULL
        """
    )


def _create_trend_features_table(conn: Any) -> None:
    prices = conn.execute(
        """
        SELECT code, date, close
        FROM ranking_trend_slope_normalized_prices
        WHERE close > 0
        ORDER BY code, date
        """
    ).fetchdf()
    if prices.empty:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_trend_slope_features (
                code TEXT,
                date DATE,
                price_lr_slope_20_pct DOUBLE,
                price_lr_slope_60_pct DOUBLE,
                price_lr_r2_20 DOUBLE,
                price_lr_r2_60 DOUBLE,
                sma20 DOUBLE,
                sma60 DOUBLE,
                ema20 DOUBLE,
                ema60 DOUBLE,
                sma20_slope_5d_pct DOUBLE,
                sma20_slope_20d_pct DOUBLE,
                sma60_slope_5d_pct DOUBLE,
                sma60_slope_20d_pct DOUBLE,
                ema20_slope_5d_pct DOUBLE,
                ema20_slope_20d_pct DOUBLE,
                ema60_slope_5d_pct DOUBLE,
                ema60_slope_20d_pct DOUBLE
            )
            """
        )
        return

    frames: list[pd.DataFrame] = []
    for _, group in prices.groupby("code", sort=False):
        frame = group.copy()
        close = frame["close"].astype(float)
        log_close = np.log(close.to_numpy(dtype=float))
        for window in _SLOPE_WINDOWS:
            slope, r2 = _rolling_log_slope_features(log_close, window=window)
            frame[f"price_lr_slope_{window}_pct"] = slope
            frame[f"price_lr_r2_{window}"] = r2
            frame[f"sma{window}"] = close.rolling(window, min_periods=window).mean()
            frame[f"ema{window}"] = close.ewm(
                span=window,
                adjust=False,
                min_periods=window,
            ).mean()
        for ma_prefix in ("sma20", "sma60", "ema20", "ema60"):
            for lag in _MA_SLOPE_LAGS:
                frame[f"{ma_prefix}_slope_{lag}d_pct"] = (
                    frame[ma_prefix] / frame[ma_prefix].shift(lag) - 1.0
                ) * 100.0
        frames.append(frame)

    features = pd.concat(frames, ignore_index=True)
    feature_columns = [
        "code",
        "date",
        "price_lr_slope_20_pct",
        "price_lr_slope_60_pct",
        "price_lr_r2_20",
        "price_lr_r2_60",
        "sma20",
        "sma60",
        "ema20",
        "ema60",
        "sma20_slope_5d_pct",
        "sma20_slope_20d_pct",
        "sma60_slope_5d_pct",
        "sma60_slope_20d_pct",
        "ema20_slope_5d_pct",
        "ema20_slope_20d_pct",
        "ema60_slope_5d_pct",
        "ema60_slope_20d_pct",
    ]
    features = features.loc[
        features["price_lr_slope_20_pct"].notna()
        & features["price_lr_slope_60_pct"].notna(),
        feature_columns,
    ]
    conn.register("ranking_trend_slope_features_df", features)
    try:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_trend_slope_features AS
            SELECT * FROM ranking_trend_slope_features_df
            """
        )
    finally:
        conn.unregister("ranking_trend_slope_features_df")


def _rolling_log_slope_features(
    values: np.ndarray,
    *,
    window: int,
) -> tuple[np.ndarray, np.ndarray]:
    slopes = np.full(len(values), np.nan, dtype=float)
    r2_values = np.full(len(values), np.nan, dtype=float)
    if len(values) < window:
        return slopes, r2_values

    x = np.arange(window, dtype=float)
    x_centered = x - x.mean()
    x_var = float(np.dot(x_centered, x_centered))
    for end in range(window - 1, len(values)):
        y = values[end - window + 1 : end + 1]
        if not np.isfinite(y).all():
            continue
        y_centered = y - y.mean()
        y_var = float(np.dot(y_centered, y_centered))
        if y_var <= 0.0:
            slopes[end] = 0.0
            r2_values[end] = 0.0
            continue
        slope_per_session = float(np.dot(x_centered, y_centered) / x_var)
        corr = float(np.dot(x_centered, y_centered) / np.sqrt(x_var * y_var))
        slopes[end] = (np.exp(slope_per_session * (window - 1)) - 1.0) * 100.0
        r2_values[end] = corr * corr
    return slopes, r2_values


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
            median(price_lr_slope_20_pct) AS median_price_lr_slope_20_pct,
            median(price_lr_slope_60_pct) AS median_price_lr_slope_60_pct,
            median(price_lr_r2_20) AS median_price_lr_r2_20,
            median(price_lr_r2_60) AS median_price_lr_r2_60,
            median(sma20_slope_5d_pct) AS median_sma20_slope_5d_pct,
            median(sma60_slope_20d_pct) AS median_sma60_slope_20d_pct,
            median(ema20_slope_5d_pct) AS median_ema20_slope_5d_pct,
            median(ema60_slope_20d_pct) AS median_ema60_slope_20d_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND lr_price_action_bucket = 'lr20_pos_lr60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND lr_price_action_bucket = 'lr20_pos_lr60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND lr_price_action_bucket = 'lr20_neg_lr60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND lr_price_action_bucket = 'lr20_neg_lr60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS fixed_lr_exact_label_match_rate_pct,
            avg(CASE WHEN fixed20_lr20_conflict_bucket = 'fixed20_pos_lr20_pos'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed20_pos_lr20_pos_rate_pct,
            avg(CASE WHEN fixed20_lr20_conflict_bucket = 'fixed20_pos_lr20_neg'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed20_pos_lr20_neg_rate_pct,
            avg(CASE WHEN fixed60_lr60_conflict_bucket = 'fixed60_pos_lr60_pos'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed60_pos_lr60_pos_rate_pct,
            avg(CASE WHEN fixed60_lr60_conflict_bucket = 'fixed60_pos_lr60_neg'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed60_pos_lr60_neg_rate_pct
        FROM ranking_trend_slope_panel
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
                source_name="ranking_trend_slope_panel",
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
                extra_metric_sql=_trend_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_technical_condition_columns())


def _build_fixed_vs_slope_conflict_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        for conflict_window, bucket_column in (
            (20, "fixed20_lr20_conflict_bucket"),
            (60, "fixed60_lr60_conflict_bucket"),
        ):
            frame = conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    {int(conflict_window)} AS conflict_window,
                    {bucket_column} AS conflict_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    quantile_cont(
                        forward_close_excess_return_{int(horizon)}d_pct,
                        0.1
                    ) AS p10_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
                    median(recent_return_{int(conflict_window)}d_pct)
                        AS median_recent_return_pct,
                    median(price_lr_slope_{int(conflict_window)}_pct)
                        AS median_price_lr_slope_pct,
                    median(price_lr_r2_{int(conflict_window)})
                        AS median_price_lr_r2
                FROM ranking_trend_slope_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY market_scope, {bucket_column}
                HAVING count(*) >= ?
                ORDER BY market_scope, conflict_window, conflict_bucket
                """,
                [float(severe_loss_threshold_pct), int(min_observations)],
            ).fetchdf()
            frames.append(frame)
    return _concat_sorted(frames, columns=_fixed_vs_slope_conflict_columns())


def _build_long_candidate_trend_slope_evidence_df(
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
                source_name="ranking_trend_slope_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND technical_condition.technical_condition_matches"
                ),
                group_select_sql=(
                    "'long_candidate_trend_slope' AS condition_family,\n"
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
                extra_metric_sql=_deep_dive_metric_sql() + _trend_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_candidate_trend_slope_columns(),
    )


def _trend_metric_sql() -> str:
    return """,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(price_lr_slope_20_pct) AS median_price_lr_slope_20_pct,
            median(price_lr_slope_60_pct) AS median_price_lr_slope_60_pct,
            median(price_lr_r2_20) AS median_price_lr_r2_20,
            median(price_lr_r2_60) AS median_price_lr_r2_60,
            median(sma20_slope_5d_pct) AS median_sma20_slope_5d_pct,
            median(sma60_slope_20d_pct) AS median_sma60_slope_20d_pct,
            median(ema20_slope_5d_pct) AS median_ema20_slope_5d_pct,
            median(ema60_slope_20d_pct) AS median_ema60_slope_20d_pct"""


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
            fixed_20d_sign_bucket,
            fixed_60d_sign_bucket,
            lr20_sign_bucket,
            lr60_sign_bucket,
            fixed_price_action_bucket,
            lr_price_action_bucket,
            sma_slope_bucket,
            ema_slope_bucket,
            fixed20_lr20_conflict_bucket,
            fixed60_lr60_conflict_bucket,
            recent_return_20d_pct,
            recent_return_60d_pct,
            price_lr_slope_20_pct,
            price_lr_slope_60_pct,
            price_lr_r2_20,
            price_lr_r2_60,
            sma20_slope_5d_pct,
            sma20_slope_20d_pct,
            sma60_slope_5d_pct,
            sma60_slope_20d_pct,
            ema20_slope_5d_pct,
            ema20_slope_20d_pct,
            ema60_slope_5d_pct,
            ema60_slope_20d_pct,
            liquidity_residual_z,
            valuation_signal,
            {horizon_columns}
        FROM ranking_trend_slope_panel
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
        *_trend_metric_columns(),
    ]


def _trend_metric_columns() -> list[str]:
    return [
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_price_lr_slope_20_pct",
        "median_price_lr_slope_60_pct",
        "median_price_lr_r2_20",
        "median_price_lr_r2_60",
        "median_sma20_slope_5d_pct",
        "median_sma60_slope_20d_pct",
        "median_ema20_slope_5d_pct",
        "median_ema60_slope_20d_pct",
    ]


def _fixed_vs_slope_conflict_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "conflict_window",
        "conflict_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_pct",
        "median_price_lr_slope_pct",
        "median_price_lr_r2",
    ]


def _long_candidate_trend_slope_columns() -> list[str]:
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
        *_trend_metric_columns(),
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
            "conflict_window",
            "technical_condition_order",
            "long_scaffold_order",
            "conflict_bucket",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
