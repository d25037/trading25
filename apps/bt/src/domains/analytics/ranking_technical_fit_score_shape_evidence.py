"""PIT contracts and raw panel for the Technical Fit Score shape study."""

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
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
)
from src.domains.analytics.ranking_long_scaffold_value_composite_evidence import (
    _create_value_composite_panel,
)
from src.domains.analytics.ranking_fixed_return_priority_evidence import (
    moving_block_bootstrap_ci,
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
    normalize_code_sql,
    open_readonly_analysis_connection,
    require_market_v4_compatibility,
)
from src.domains.analytics.trend_slope_features import rolling_log_slope_features
from src.shared.utils.pandas_type_guards import finite_float_or_none
from src.shared.utils.market_code_alias import MARKET_CODES_BY_SCOPE


@dataclass(frozen=True)
class CandidateRingDefinition:
    """A mutually-exclusive, fixed-free candidate quality tier."""

    name: str
    predicate: str


@dataclass(frozen=True)
class RawScoreDefinition:
    """A Prime-wide raw technical percentile retained for the experiment."""

    name: str
    family: str
    is_primary: bool


RING_REGISTRY: tuple[CandidateRingDefinition, ...] = (
    CandidateRingDefinition(
        "core_high_high",
        "value_composite_equal_score >= 0.8 AND long_hybrid_leadership_score >= 0.8",
    ),
    CandidateRingDefinition(
        "near_high_high_1",
        "value_composite_equal_score >= 0.7 AND long_hybrid_leadership_score >= 0.7 "
        "AND NOT (value_composite_equal_score >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.8)",
    ),
    CandidateRingDefinition(
        "near_high_high_2",
        "value_composite_equal_score >= 0.6 AND long_hybrid_leadership_score >= 0.6 "
        "AND NOT (value_composite_equal_score >= 0.7 "
        "AND long_hybrid_leadership_score >= 0.7)",
    ),
)

RAW_SCORE_REGISTRY: tuple[RawScoreDefinition, ...] = (
    RawScoreDefinition("fixed20_level", "fixed", False),
    RawScoreDefinition("fixed60_level", "fixed", False),
    RawScoreDefinition("fixed_equal_level", "fixed", True),
    RawScoreDefinition("ols20_level", "ols", False),
    RawScoreDefinition("ols60_level", "ols", False),
    RawScoreDefinition("ols_equal_level", "ols", True),
)

RAW_BIN_LABELS: tuple[str, ...] = ("q1", "q2", "q3", "q4", "q5")
RAW_BIN_BOUNDARIES: tuple[float, ...] = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
RAW_BIN_CENTERS: tuple[float, ...] = (0.1, 0.3, 0.5, 0.7, 0.9)
DEFAULT_MIN_TRAINING_OBSERVATIONS = 200
DEFAULT_MIN_TRAINING_DATES = 50
DEFAULT_BOOTSTRAP_RESAMPLES = 2_000
DEFAULT_BOOTSTRAP_SEED = 20260718
DEFAULT_MIN_DAILY_CANDIDATES = 10
DEFAULT_MIN_COMPARISON_SIDE = 3
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
FIRST_TRAINING_YEAR = 2017
FIRST_EVALUATION_YEAR = 2022
PRIMARY_RAW_SCORE_BY_FAMILY = {
    "fixed": "fixed_equal_level",
    "ols": "ols_equal_level",
}

DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
PRIME_EQUIVALENT_MARKET_CODES: tuple[str, ...] = tuple(
    code for code in MARKET_CODES_BY_SCOPE["prime"] if code.isdigit()
)
if set(PRIME_EQUIVALENT_MARKET_CODES) != {"0101", "0111"}:
    raise RuntimeError("Prime research must resolve to exact-date 0101/0111 membership")

_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_OLS_WINDOWS: tuple[int, ...] = (20, 60)
_WARMUP_CALENDAR_DAYS = 820
_REQUIRED_MARKET_TABLES = {
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
}

REQUIRED_BUNDLE_TABLES = {
    "ring_registry",
    "raw_score_registry",
    "coverage_attrition",
    "raw_shape_daily",
    "raw_shape_summary",
    "walkforward_mapping",
    "oos_fit_score_lift",
    "fixed_vs_ols_paired",
    "topk_operational_lift",
    "overheat_negative_diagnostics",
    "segment_stability",
    "annual_stability",
    "bootstrap_effect_ci",
    "decision_gate",
    "observation_sample",
}

_MAPPING_COLUMNS = (
    "raw_score_name",
    "evaluation_year",
    "raw_bin",
    "bin_lower",
    "bin_upper",
    "bin_center",
    "observation_count",
    "signal_date_count",
    "expectancy_pct",
    "technical_fit_score",
    "mapping_status",
    "shape_classification",
    "training_start_date",
    "training_end_date",
)


@dataclass(frozen=True)
class RankingTechnicalFitScoreShapeEvidenceResult:
    """Read-only PIT observations and frozen Task 3 evidence tables."""

    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame
    raw_shape_daily_df: pd.DataFrame
    raw_shape_summary_df: pd.DataFrame
    walkforward_mapping_df: pd.DataFrame
    oos_fit_score_lift_df: pd.DataFrame
    fixed_vs_ols_paired_df: pd.DataFrame
    topk_operational_lift_df: pd.DataFrame
    overheat_negative_diagnostics_df: pd.DataFrame
    segment_stability_df: pd.DataFrame
    annual_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame


@dataclass(frozen=True)
class TechnicalFitEvidenceTables:
    """Frozen analysis-only outputs built from the PIT candidate panel."""

    raw_shape_daily_df: pd.DataFrame
    raw_shape_summary_df: pd.DataFrame
    walkforward_mapping_df: pd.DataFrame
    oos_fit_score_lift_df: pd.DataFrame
    fixed_vs_ols_paired_df: pd.DataFrame
    topk_operational_lift_df: pd.DataFrame
    overheat_negative_diagnostics_df: pd.DataFrame
    segment_stability_df: pd.DataFrame
    annual_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame


def _as_finite_float(value: object) -> float | None:
    return finite_float_or_none(value)


def _all_explicit_true(values: pd.Series) -> bool:
    """Return true only for a non-empty series of actual true booleans."""

    return bool(
        not values.empty
        and all(
            isinstance(value, bool | np.bool_) and bool(value)
            for value in values.tolist()
        )
    )


def _has_only_explicit_booleans(values: pd.Series) -> bool:
    """Reject missing, numeric, and string truthy values at the gate boundary."""

    return bool(
        not values.empty
        and all(isinstance(value, bool | np.bool_) for value in values.tolist())
    )


def classify_candidate_ring(
    value_score: float | None, leadership_score: float | None
) -> str:
    """Return the one frozen Value/Long-Hybrid candidate ring for a row."""

    value = _as_finite_float(value_score)
    leadership = _as_finite_float(leadership_score)
    if value is None or leadership is None:
        return "missing"
    if value >= 0.8 and leadership >= 0.8:
        return "core_high_high"
    if value >= 0.7 and leadership >= 0.7:
        return "near_high_high_1"
    if value >= 0.6 and leadership >= 0.6:
        return "near_high_high_2"
    return "outside"


def classify_raw_level_bin(level: float | None) -> str:
    """Classify a closed-unit-interval percentile into one frozen raw bin."""

    numeric = _as_finite_float(level)
    if numeric is None or numeric < 0.0 or numeric > 1.0:
        return "missing"
    if numeric < 0.2:
        return "q1"
    if numeric < 0.4:
        return "q2"
    if numeric < 0.6:
        return "q3"
    if numeric < 0.8:
        return "q4"
    return "q5"


def classify_shape(
    expectancies: Sequence[float | None],
    *,
    reproduces_core_and_near: bool = False,
    positive_2022_2023: bool = False,
    positive_2024_plus: bool = False,
    severe_loss_not_worse: bool = False,
) -> str:
    """Classify a five-bin response without designating a preferred bin a priori."""

    values = [_as_finite_float(value) for value in expectancies]
    if len(values) != len(RAW_BIN_LABELS) or any(value is None for value in values):
        return "insufficient_evidence"
    finite_values = [float(value) for value in values if value is not None]
    if all(value == finite_values[0] for value in finite_values):
        return "flat"
    differences = np.diff(finite_values)
    if bool(np.all(differences >= 0.0) or np.all(differences <= 0.0)):
        return "monotonic"
    best_index = int(np.argmax(finite_values))
    if best_index in {0, len(finite_values) - 1}:
        return "unstable_shape"
    adjacent_values = (finite_values[best_index - 1], finite_values[best_index + 1])
    is_interior_winner = (
        finite_values[best_index] > max(adjacent_values)
        and finite_values[best_index] > finite_values[-1]
    )
    if (
        is_interior_winner
        and reproduces_core_and_near
        and positive_2022_2023
        and positive_2024_plus
        and severe_loss_not_worse
    ):
        return "interior_sweet_spot_confirmed"
    return "unstable_shape"


def run_ranking_technical_fit_score_shape_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = "2017-01-01",
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingTechnicalFitScoreShapeEvidenceResult:
    """Build the frozen Prime-only candidate and raw technical PIT panel."""

    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    if not resolved_horizons or any(horizon <= 0 for horizon in resolved_horizons):
        raise ValueError("horizons must contain positive integers")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=_WARMUP_CALENDAR_DAYS,
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-technical-fit-score-shape-",
    ) as ctx:
        require_market_v4_compatibility(
            ctx.connection,
            required_tables=_REQUIRED_MARKET_TABLES,
        )
        assert_daily_ranking_research_tables(ctx.connection)
        create_daily_ranking_research_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=("prime",),
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
            market_scopes=("prime",),
        )
        _create_short_red_feature_panel(ctx.connection)
        _create_value_composite_panel(ctx.connection)

        # Freeze membership using only Value and Long-Hybrid scores before any
        # raw technical or forward-outcome relation is attached.
        _create_candidate_ring_flags_table(ctx.connection)
        _create_ols_feature_table(ctx.connection)
        _create_prime_technical_rank_table(ctx.connection)
        _create_candidate_observation_table(
            ctx.connection,
            horizons=resolved_horizons,
        )

        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_technical_fit_candidate_observations"
            ).fetchone()[0]
        )
        observations = ctx.connection.execute(
            """
            SELECT *
            FROM ranking_technical_fit_candidate_observations
            ORDER BY date, ring, code
            """
        ).fetchdf()
        if "date" in observations.columns:
            observations["date"] = pd.to_datetime(observations["date"])
        evidence = build_technical_fit_evidence_tables(
            observations,
            horizons=resolved_horizons,
        )
        observation_sample = observations.head(int(observation_sample_limit)).copy()
        if "date" in observation_sample.columns:
            observation_sample["date"] = pd.to_datetime(observation_sample["date"])

        result = RankingTechnicalFitScoreShapeEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            observation_count=observation_count,
            observation_sample_df=observation_sample,
            raw_shape_daily_df=evidence.raw_shape_daily_df,
            raw_shape_summary_df=evidence.raw_shape_summary_df,
            walkforward_mapping_df=evidence.walkforward_mapping_df,
            oos_fit_score_lift_df=evidence.oos_fit_score_lift_df,
            fixed_vs_ols_paired_df=evidence.fixed_vs_ols_paired_df,
            topk_operational_lift_df=evidence.topk_operational_lift_df,
            overheat_negative_diagnostics_df=(
                evidence.overheat_negative_diagnostics_df
            ),
            segment_stability_df=evidence.segment_stability_df,
            annual_stability_df=evidence.annual_stability_df,
            bootstrap_effect_ci_df=evidence.bootstrap_effect_ci_df,
        )
    return result


def _create_candidate_ring_flags_table(conn: Any) -> None:
    """Materialize only PIT keys and the three frozen, exclusive ring flags."""

    prime_codes_sql = ", ".join(
        f"'{market_code}'" for market_code in PRIME_EQUIVALENT_MARKET_CODES
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_candidate_ring_flags AS
        SELECT
            market_scope,
            market_code,
            date,
            code,
            value_composite_equal_score >= 0.8
                AND long_hybrid_leadership_score >= 0.8
                AS core_high_high_flag,
            value_composite_equal_score >= 0.7
                AND long_hybrid_leadership_score >= 0.7
                AND NOT (
                    value_composite_equal_score >= 0.8
                    AND long_hybrid_leadership_score >= 0.8
                ) AS near_high_high_1_flag,
            value_composite_equal_score >= 0.6
                AND long_hybrid_leadership_score >= 0.6
                AND NOT (
                    value_composite_equal_score >= 0.7
                    AND long_hybrid_leadership_score >= 0.7
                ) AS near_high_high_2_flag
        FROM ranking_long_scaffold_value_composite_panel
        WHERE market_scope = 'prime'
          AND market_code IN ({prime_codes_sql})
          AND value_composite_equal_score >= 0.6
          AND long_hybrid_leadership_score >= 0.6
        """
    )


def _build_ols_feature_frame(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute shared 20D/60D rolling fitted moves and R-squared diagnostics."""

    required = {"code", "date", "close"}
    missing = required.difference(prices.columns)
    if missing:
        raise ValueError(f"prices is missing required columns: {sorted(missing)}")
    columns = [
        "code",
        "date",
        "ols_move_20d_pct",
        "ols_move_60d_pct",
        "ols_r2_20",
        "ols_r2_60",
    ]
    if prices.empty:
        return pd.DataFrame(columns=columns)

    source = prices.loc[:, ["code", "date", "close"]].copy()
    source["date"] = pd.to_datetime(source["date"], errors="coerce")
    source["close"] = pd.to_numeric(source["close"], errors="coerce")
    source = source.loc[
        source["code"].notna()
        & source["date"].notna()
        & source["close"].gt(0.0)
        & np.isfinite(source["close"])
    ].sort_values(["code", "date"])
    frames: list[pd.DataFrame] = []
    for _, group in source.groupby("code", sort=False):
        frame = group.loc[:, ["code", "date"]].copy()
        log_close = np.log(group["close"].to_numpy(dtype=float))
        for window in _OLS_WINDOWS:
            fitted_move, r2 = rolling_log_slope_features(log_close, window=window)
            frame[f"ols_move_{window}d_pct"] = fitted_move
            frame[f"ols_r2_{window}"] = r2
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True).reindex(columns=columns)


def _create_ols_feature_table(conn: Any) -> None:
    stock_code = normalize_code_sql("sd.code")
    prices = conn.execute(
        f"""
        SELECT
            {stock_code} AS code,
            sd.date,
            arg_min(
                sd.close,
                CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code
            ) AS close
        FROM stock_data sd
        WHERE sd.close > 0
          AND sd.date <= (
                SELECT max(date) FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
            )
          AND EXISTS (
                SELECT 1
                FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
                WHERE r.code = {stock_code}
            )
        GROUP BY {stock_code}, sd.date
        ORDER BY code, sd.date
        """
    ).fetchdf()
    features = _build_ols_feature_frame(prices)
    if features.empty:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_technical_fit_ols_features (
                code TEXT,
                date DATE,
                ols_move_20d_pct DOUBLE,
                ols_move_60d_pct DOUBLE,
                ols_r2_20 DOUBLE,
                ols_r2_60 DOUBLE
            )
            """
        )
        return

    conn.register("ranking_technical_fit_ols_features_df", features)
    try:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE ranking_technical_fit_ols_features AS
            SELECT
                CAST(code AS TEXT) AS code,
                CAST(date AS DATE) AS date,
                CAST(ols_move_20d_pct AS DOUBLE) AS ols_move_20d_pct,
                CAST(ols_move_60d_pct AS DOUBLE) AS ols_move_60d_pct,
                CAST(ols_r2_20 AS DOUBLE) AS ols_r2_20,
                CAST(ols_r2_60 AS DOUBLE) AS ols_r2_60
            FROM ranking_technical_fit_ols_features_df
            """
        )
    finally:
        conn.unregister("ranking_technical_fit_ols_features_df")


def _create_prime_technical_rank_table(conn: Any) -> None:
    """Rank both technical families across all exact-date Prime members."""

    prime_codes_sql = ", ".join(
        f"'{market_code}'" for market_code in PRIME_EQUIVALENT_MARKET_CODES
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_prime_ranked AS
        WITH prime_source AS (
            SELECT
                r.market_scope,
                r.market_code,
                r.date,
                r.code,
                r.recent_return_20d_pct,
                r.recent_return_60d_pct,
                f.ols_move_20d_pct,
                f.ols_move_60d_pct,
                f.ols_r2_20,
                f.ols_r2_60
            FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
            LEFT JOIN ranking_technical_fit_ols_features f
              ON f.code = r.code
             AND f.date = r.date
            WHERE r.market_scope = 'prime'
              AND r.market_code IN ({prime_codes_sql})
        ),
        levels AS (
            SELECT
                *,
                CASE WHEN recent_return_20d_pct IS NOT NULL THEN
                    rank() OVER (
                        PARTITION BY date
                        ORDER BY recent_return_20d_pct NULLS LAST
                    )::DOUBLE
                    / count(recent_return_20d_pct) OVER (PARTITION BY date)
                END AS fixed20_level,
                CASE WHEN recent_return_60d_pct IS NOT NULL THEN
                    rank() OVER (
                        PARTITION BY date
                        ORDER BY recent_return_60d_pct NULLS LAST
                    )::DOUBLE
                    / count(recent_return_60d_pct) OVER (PARTITION BY date)
                END AS fixed60_level,
                CASE WHEN ols_move_20d_pct IS NOT NULL THEN
                    rank() OVER (
                        PARTITION BY date
                        ORDER BY ols_move_20d_pct NULLS LAST
                    )::DOUBLE
                    / count(ols_move_20d_pct) OVER (PARTITION BY date)
                END AS ols20_level,
                CASE WHEN ols_move_60d_pct IS NOT NULL THEN
                    rank() OVER (
                        PARTITION BY date
                        ORDER BY ols_move_60d_pct NULLS LAST
                    )::DOUBLE
                    / count(ols_move_60d_pct) OVER (PARTITION BY date)
                END AS ols60_level
            FROM prime_source
        )
        SELECT
            *,
            CASE
                WHEN fixed20_level IS NOT NULL AND fixed60_level IS NOT NULL
                    THEN (fixed20_level + fixed60_level) / 2.0
            END AS fixed_equal_level,
            CASE
                WHEN ols20_level IS NOT NULL AND ols60_level IS NOT NULL
                    THEN (ols20_level + ols60_level) / 2.0
            END AS ols_equal_level
        FROM levels
        """
    )


def _create_candidate_observation_table(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> None:
    outcome_columns = ",\n            ".join(
        column
        for horizon in horizons
        for column in (
            f"p.forward_close_return_{int(horizon)}d_pct",
            f"p.forward_close_excess_return_{int(horizon)}d_pct",
            f"p.forward_close_n225_excess_return_{int(horizon)}d_pct",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_candidate_observations AS
        SELECT
            c.date,
            c.code,
            c.market_scope,
            c.market_code,
            CASE
                WHEN c.core_high_high_flag THEN 'core_high_high'
                WHEN c.near_high_high_1_flag THEN 'near_high_high_1'
                WHEN c.near_high_high_2_flag THEN 'near_high_high_2'
            END AS ring,
            p.company_name,
            p.sector_33_code,
            p.sector_33_name,
            p.value_composite_equal_score,
            p.long_hybrid_leadership_score,
            p.liquidity_residual_z,
            p.atr20_pct,
            p.atr20_change_20d_pct,
            t.recent_return_20d_pct,
            t.recent_return_60d_pct,
            t.fixed20_level,
            t.fixed60_level,
            t.fixed_equal_level,
            t.ols_move_20d_pct,
            t.ols_move_60d_pct,
            t.ols20_level,
            t.ols60_level,
            t.ols_equal_level,
            t.ols_r2_20,
            t.ols_r2_60,
            t.ols_move_20d_pct - t.ols_move_60d_pct
                AS ols20_minus_ols60_move_pct,
            CASE
                WHEN t.recent_return_20d_pct IS NOT NULL
                 AND t.ols_move_20d_pct IS NOT NULL
                    THEN sign(t.recent_return_20d_pct) <> sign(t.ols_move_20d_pct)
            END AS fixed20_ols20_sign_conflict,
            CASE
                WHEN t.recent_return_60d_pct IS NOT NULL
                 AND t.ols_move_60d_pct IS NOT NULL
                    THEN sign(t.recent_return_60d_pct) <> sign(t.ols_move_60d_pct)
            END AS fixed60_ols60_sign_conflict,
            CASE WHEN t.recent_return_20d_pct IS NOT NULL
                THEN t.recent_return_20d_pct < 0.0
            END AS fixed20_negative_flag,
            CASE WHEN t.recent_return_60d_pct IS NOT NULL
                THEN t.recent_return_60d_pct < 0.0
            END AS fixed60_negative_flag,
            CASE WHEN t.recent_return_20d_pct IS NOT NULL
                THEN t.recent_return_20d_pct >= 30.0
            END AS fixed20_overheat_flag,
            {outcome_columns}
        FROM ranking_technical_fit_candidate_ring_flags c
        JOIN ranking_long_scaffold_value_composite_panel p
          ON p.market_scope = c.market_scope
         AND p.market_code = c.market_code
         AND p.date = c.date
         AND p.code = c.code
        LEFT JOIN ranking_technical_fit_prime_ranked t
          ON t.market_scope = c.market_scope
         AND t.market_code = c.market_code
         AND t.date = c.date
         AND t.code = c.code
        """
    )


def build_walkforward_mapping(
    training: pd.DataFrame,
    evaluation_year: int,
    *,
    raw_level_column: str = "raw_level",
    outcome_column: str = "forward_topix_excess_20d_pct",
    date_column: str = "date",
    raw_score_name: str = "raw_level",
    min_observations: int = DEFAULT_MIN_TRAINING_OBSERVATIONS,
    min_signal_dates: int = DEFAULT_MIN_TRAINING_DATES,
) -> pd.DataFrame:
    """Learn a five-bin, strictly-prior-year Technical Fit mapping.

    Training expectancies are date-equal means.  Under-covered bins are represented
    with an explicit unavailable status and never produce an interpolation mapping.
    """

    required = {raw_level_column, outcome_column, date_column}
    missing = required.difference(training.columns)
    if missing:
        raise ValueError(f"training is missing required columns: {sorted(missing)}")
    if min_observations <= 0 or min_signal_dates <= 0:
        raise ValueError("training minimums must be positive")

    evaluation_start = pd.Timestamp(year=int(evaluation_year), month=1, day=1)
    source = training.loc[:, [date_column, raw_level_column, outcome_column]].copy()
    source[date_column] = pd.to_datetime(source[date_column], errors="coerce").dt.normalize()
    source[outcome_column] = pd.to_numeric(source[outcome_column], errors="coerce")
    source["raw_bin"] = source[raw_level_column].map(classify_raw_level_bin)
    usable = source.loc[
        source[date_column].notna()
        & source[date_column].lt(evaluation_start)
        & source[outcome_column].notna()
        & np.isfinite(source[outcome_column])
        & source["raw_bin"].ne("missing")
    ].copy()
    training_start = usable[date_column].min() if not usable.empty else pd.NaT
    training_end = usable[date_column].max() if not usable.empty else pd.NaT

    per_date = (
        usable.groupby(["raw_bin", date_column], observed=True)[outcome_column]
        .mean()
        .rename("date_equal_expectancy_pct")
        .reset_index()
    )
    counts_by_bin: dict[str, tuple[int, int]] = {
        str(raw_bin): (int(len(group)), int(group[date_column].nunique()))
        for raw_bin, group in usable.groupby("raw_bin", observed=True)
    }
    expectancies = per_date.groupby("raw_bin", observed=True)[
        "date_equal_expectancy_pct"
    ].mean()

    rows: list[dict[str, object]] = []
    for index, raw_bin in enumerate(RAW_BIN_LABELS):
        observation_count, signal_date_count = counts_by_bin.get(raw_bin, (0, 0))
        rows.append(
            {
                "raw_score_name": raw_score_name,
                "evaluation_year": int(evaluation_year),
                "raw_bin": raw_bin,
                "bin_lower": RAW_BIN_BOUNDARIES[index],
                "bin_upper": RAW_BIN_BOUNDARIES[index + 1],
                "bin_center": RAW_BIN_CENTERS[index],
                "observation_count": observation_count,
                "signal_date_count": signal_date_count,
                "expectancy_pct": (
                    float(expectancies.loc[raw_bin])
                    if raw_bin in expectancies.index
                    else float("nan")
                ),
                "technical_fit_score": float("nan"),
                "mapping_status": "insufficient_training_data",
                "shape_classification": "insufficient_evidence",
                "training_start_date": training_start,
                "training_end_date": training_end,
            }
        )
    mapping = pd.DataFrame(rows, columns=_MAPPING_COLUMNS)
    has_coverage = bool(
        mapping["observation_count"].ge(min_observations).all()
        and mapping["signal_date_count"].ge(min_signal_dates).all()
    )
    if not has_coverage:
        return mapping

    expectancy_values = mapping["expectancy_pct"].tolist()
    shape = classify_shape(expectancy_values)
    if shape == "flat":
        mapping["technical_fit_score"] = 0.5
        mapping["mapping_status"] = "flat"
    else:
        minimum = float(mapping["expectancy_pct"].min())
        maximum = float(mapping["expectancy_pct"].max())
        mapping["technical_fit_score"] = (
            mapping["expectancy_pct"] - minimum
        ) / (maximum - minimum)
        mapping["mapping_status"] = "ready"
    mapping["shape_classification"] = shape
    return mapping


def apply_walkforward_mapping(
    frame: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    raw_level_column: str = "raw_level",
    date_column: str = "date",
    raw_score_name: str = "raw_level",
    fit_score_column: str = "technical_fit_score",
) -> pd.DataFrame:
    """Apply only the mapping of each row's evaluation year, with no fallback."""

    if raw_level_column not in frame.columns or date_column not in frame.columns:
        raise ValueError("frame must contain raw-level and date columns")
    required_mapping = {
        "raw_score_name",
        "evaluation_year",
        "bin_center",
        "technical_fit_score",
        "mapping_status",
    }
    missing_mapping = required_mapping.difference(mapping.columns)
    if missing_mapping:
        raise ValueError(f"mapping is missing required columns: {sorted(missing_mapping)}")

    scored = frame.copy()
    scored[fit_score_column] = float("nan")
    scored["mapping_status"] = "missing_mapping"
    dates = pd.to_datetime(scored[date_column], errors="coerce")
    evaluation_years = dates.dt.year
    scored.loc[dates.isna(), "mapping_status"] = "missing_evaluation_year"

    selected = mapping.loc[mapping["raw_score_name"].eq(raw_score_name)].copy()
    for year in sorted(evaluation_years.dropna().unique()):
        row_mask = evaluation_years.eq(year)
        year_mapping = selected.loc[selected["evaluation_year"].eq(int(year))]
        if year_mapping.empty:
            continue
        if not year_mapping["mapping_status"].isin({"ready", "flat"}).all():
            scored.loc[row_mask, "mapping_status"] = "insufficient_training_data"
            continue
        ordered = year_mapping.sort_values("bin_center")
        centers = ordered["bin_center"].to_numpy(dtype=float)
        values = ordered["technical_fit_score"].to_numpy(dtype=float)
        if len(centers) != len(RAW_BIN_CENTERS) or not np.isfinite(values).all():
            scored.loc[row_mask, "mapping_status"] = "insufficient_training_data"
            continue
        raw_values = pd.to_numeric(scored.loc[row_mask, raw_level_column], errors="coerce")
        valid = raw_values.notna() & np.isfinite(raw_values) & raw_values.between(0.0, 1.0)
        if valid.any():
            scored.loc[raw_values.index[valid], fit_score_column] = np.interp(
                raw_values.loc[valid].to_numpy(dtype=float), centers, values
            )
            scored.loc[raw_values.index[valid], "mapping_status"] = "ready"
        if (~valid).any():
            scored.loc[raw_values.index[~valid], "mapping_status"] = "missing_raw_level"
    return scored


def _period_label(value: Any) -> str:
    year = pd.Timestamp(value).year
    if year <= 2021:
        return "training_2017_2021"
    if year <= 2023:
        return "walkforward_2022_2023"
    return "hypothesis_origin_2024_plus"


def _sector_hhi(frame: pd.DataFrame) -> float:
    if "sector_33_code" not in frame.columns:
        return float("nan")
    shares = frame["sector_33_code"].dropna().value_counts(normalize=True)
    return float((shares**2).sum()) if not shares.empty else float("nan")


def _finite_rows(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    working = frame.dropna(subset=list(columns)).copy()
    if working.empty:
        return working
    mask = pd.Series(True, index=working.index)
    for column in columns:
        numeric = pd.to_numeric(working[column], errors="coerce")
        mask &= numeric.notna() & np.isfinite(numeric)
        working[column] = numeric
    return working.loc[mask].copy()


def _build_raw_shape_tables(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_rows: list[dict[str, object]] = []
    for definition in RAW_SCORE_REGISTRY:
        if definition.name not in observations.columns:
            continue
        for horizon in horizons:
            outcome = f"forward_close_excess_return_{int(horizon)}d_pct"
            if outcome not in observations.columns:
                continue
            usable = _finite_rows(observations, [definition.name, outcome])
            usable = usable.loc[usable[definition.name].between(0.0, 1.0)]
            usable["raw_bin"] = usable[definition.name].map(classify_raw_level_bin)
            for (ring, signal_date, raw_bin), group in usable.groupby(
                ["ring", "date", "raw_bin"], observed=True, sort=True
            ):
                values = group[outcome]
                daily_rows.append(
                    {
                        "raw_score_name": definition.name,
                        "family": definition.family,
                        "is_primary": definition.is_primary,
                        "role": "primary" if definition.is_primary else "attribution_only",
                        "ring": ring,
                        "horizon": int(horizon),
                        "date": pd.Timestamp(str(signal_date)).normalize(),
                        "year": pd.Timestamp(str(signal_date)).year,
                        "segment": _period_label(signal_date),
                        "raw_bin": raw_bin,
                        "code_count": int(len(group)),
                        "mean_excess_return_pct": float(values.mean()),
                        "median_excess_return_pct": float(values.median()),
                        "win_rate_pct": float(values.gt(0.0).mean() * 100.0),
                        "p10_excess_return_pct": float(values.quantile(0.10)),
                        "p25_excess_return_pct": float(values.quantile(0.25)),
                        "severe_loss_rate_pct": float(
                            values.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean() * 100.0
                        ),
                    }
                )
    daily = pd.DataFrame(daily_rows)
    summary_rows: list[dict[str, object]] = []
    if not daily.empty:
        period_specs: tuple[tuple[str, str, pd.Series], ...] = (
            (
                "all_period",
                "all_available",
                pd.Series(True, index=daily.index),
            ),
        )
        for period_type, period_label, mask in period_specs:
            selected = daily.loc[mask]
            keys = [
                "raw_score_name",
                "family",
                "is_primary",
                "role",
                "ring",
                "horizon",
                "raw_bin",
            ]
            for group_key, group in selected.groupby(keys, observed=True, sort=True):
                row: dict[str, object] = dict(zip(keys, group_key, strict=True))
                row.update(
                    {
                        "period_type": period_type,
                        "period_label": period_label,
                        "date_count": int(group["date"].nunique()),
                        "observation_count": int(group["code_count"].sum()),
                        "date_equal_mean_excess_return_pct": float(
                            group["mean_excess_return_pct"].mean()
                        ),
                        "date_equal_median_excess_return_pct": float(
                            group["median_excess_return_pct"].median()
                        ),
                        "date_equal_win_rate_pct": float(group["win_rate_pct"].mean()),
                        "date_equal_p10_excess_return_pct": float(
                            group["p10_excess_return_pct"].mean()
                        ),
                        "date_equal_p25_excess_return_pct": float(
                            group["p25_excess_return_pct"].mean()
                        ),
                        "date_equal_severe_loss_rate_pct": float(
                            group["severe_loss_rate_pct"].mean()
                        ),
                    }
                )
                summary_rows.append(row)
        for period_type, period_column in (("segment", "segment"), ("year", "year")):
            keys = [
                "raw_score_name",
                "family",
                "is_primary",
                "role",
                "ring",
                "horizon",
                "raw_bin",
                period_column,
            ]
            for group_key, group in daily.groupby(keys, observed=True, sort=True):
                values = group_key if isinstance(group_key, tuple) else (group_key,)
                row = dict(zip(keys, values, strict=True))
                row["period_type"] = period_type
                row["period_label"] = str(row.pop(period_column))
                row.update(
                    {
                        "date_count": int(group["date"].nunique()),
                        "observation_count": int(group["code_count"].sum()),
                        "date_equal_mean_excess_return_pct": float(
                            group["mean_excess_return_pct"].mean()
                        ),
                        "date_equal_median_excess_return_pct": float(
                            group["median_excess_return_pct"].median()
                        ),
                        "date_equal_win_rate_pct": float(group["win_rate_pct"].mean()),
                        "date_equal_p10_excess_return_pct": float(
                            group["p10_excess_return_pct"].mean()
                        ),
                        "date_equal_p25_excess_return_pct": float(
                            group["p25_excess_return_pct"].mean()
                        ),
                        "date_equal_severe_loss_rate_pct": float(
                            group["severe_loss_rate_pct"].mean()
                        ),
                    }
                )
                summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)
    if not summary.empty:
        summary["shape_classification"] = "insufficient_evidence"
        shape_keys = [
            "raw_score_name",
            "family",
            "is_primary",
            "role",
            "ring",
            "horizon",
            "period_type",
            "period_label",
        ]
        for _, group in summary.groupby(shape_keys, observed=True, sort=True):
            expectancy_by_bin = group.set_index("raw_bin")[
                "date_equal_mean_excess_return_pct"
            ]
            shape = classify_shape(
                [
                    (
                        float(expectancy_by_bin.loc[raw_bin])
                        if raw_bin in expectancy_by_bin.index
                        else None
                    )
                    for raw_bin in RAW_BIN_LABELS
                ]
            )
            summary.loc[group.index, "shape_classification"] = shape
    return daily, summary


def _build_all_walkforward_mappings(
    observations: pd.DataFrame,
    *,
    min_training_observations: int,
    min_training_dates: int,
) -> pd.DataFrame:
    dates = pd.to_datetime(observations["date"], errors="coerce")
    evaluation_years = sorted(
        int(year)
        for year in dates.loc[dates.dt.year.ge(FIRST_EVALUATION_YEAR)].dt.year.dropna().unique()
    )
    outcome = "forward_close_excess_return_20d_pct"
    mappings: list[pd.DataFrame] = []
    if outcome not in observations.columns:
        return pd.DataFrame(columns=_MAPPING_COLUMNS)
    training_source = observations.loc[dates.dt.year.ge(FIRST_TRAINING_YEAR)].copy()
    for definition in RAW_SCORE_REGISTRY:
        if definition.name not in observations.columns:
            continue
        source = training_source.rename(
            columns={definition.name: "raw_level", outcome: "mapping_outcome"}
        )
        for evaluation_year in evaluation_years:
            mapping = build_walkforward_mapping(
                source,
                evaluation_year,
                raw_level_column="raw_level",
                outcome_column="mapping_outcome",
                raw_score_name=definition.name,
                min_observations=min_training_observations,
                min_signal_dates=min_training_dates,
            )
            mapping["family"] = definition.family
            mapping["is_primary"] = definition.is_primary
            mapping["role"] = (
                "primary" if definition.is_primary else "attribution_only"
            )
            mappings.append(mapping)
    if not mappings:
        return pd.DataFrame(columns=(*_MAPPING_COLUMNS, "family", "is_primary", "role"))
    return pd.concat(mappings, ignore_index=True)


def _score_walkforward_observations(
    observations: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    dates = pd.to_datetime(observations["date"], errors="coerce").dt.normalize()
    evaluation = observations.loc[dates.dt.year.ge(FIRST_EVALUATION_YEAR)].copy()
    evaluation["date"] = dates.loc[evaluation.index]
    for definition in RAW_SCORE_REGISTRY:
        if definition.name not in evaluation.columns:
            continue
        source = evaluation.rename(columns={definition.name: "raw_level"})
        scored = apply_walkforward_mapping(
            source,
            mapping,
            raw_score_name=definition.name,
        )
        scored["raw_score_name"] = definition.name
        scored["family"] = definition.family
        scored["is_primary"] = definition.is_primary
        scored["role"] = "primary" if definition.is_primary else "attribution_only"
        for horizon in horizons:
            outcome = f"forward_close_excess_return_{int(horizon)}d_pct"
            if outcome not in scored.columns:
                continue
            horizon_frame = scored.copy()
            horizon_frame["horizon"] = int(horizon)
            horizon_frame["outcome_pct"] = pd.to_numeric(
                horizon_frame[outcome], errors="coerce"
            )
            n225 = f"forward_close_n225_excess_return_{int(horizon)}d_pct"
            horizon_frame["n225_outcome_pct"] = (
                pd.to_numeric(horizon_frame[n225], errors="coerce")
                if n225 in horizon_frame.columns
                else np.nan
            )
            rows.append(horizon_frame)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _build_oos_fit_score_lift_df(scored: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(rows)
    keys = [
        "raw_score_name",
        "family",
        "is_primary",
        "role",
        "ring",
        "horizon",
        "date",
    ]
    for group_key, group in scored.groupby(keys, observed=True, sort=True):
        eligible = _finite_rows(group, ["technical_fit_score", "outcome_pct"])
        eligible = eligible.sort_values(
            ["technical_fit_score", "code"], kind="mergesort"
        )
        candidate_count = len(eligible)
        side_count = int(np.floor(candidate_count * 0.30))
        if (
            candidate_count < DEFAULT_MIN_DAILY_CANDIDATES
            or side_count < DEFAULT_MIN_COMPARISON_SIDE
        ):
            continue
        bottom = eligible.head(side_count)
        top = eligible.tail(side_count)
        row: dict[str, object] = dict(zip(keys, group_key, strict=True))
        row.update(
            {
                "candidate_count": candidate_count,
                "top_count": side_count,
                "bottom_count": side_count,
                "top_mean_excess_return_pct": float(top["outcome_pct"].mean()),
                "bottom_mean_excess_return_pct": float(bottom["outcome_pct"].mean()),
                "mean_lift_pct": float(
                    top["outcome_pct"].mean() - bottom["outcome_pct"].mean()
                ),
                "top_median_excess_return_pct": float(top["outcome_pct"].median()),
                "bottom_median_excess_return_pct": float(
                    bottom["outcome_pct"].median()
                ),
                "median_lift_pct": float(
                    top["outcome_pct"].median() - bottom["outcome_pct"].median()
                ),
                "spearman_ic": float(
                    eligible["technical_fit_score"].corr(
                        eligible["outcome_pct"], method="spearman"
                    )
                ),
                "top_win_rate_pct": float(top["outcome_pct"].gt(0).mean() * 100.0),
                "bottom_win_rate_pct": float(
                    bottom["outcome_pct"].gt(0).mean() * 100.0
                ),
                "top_p10_pct": float(top["outcome_pct"].quantile(0.10)),
                "bottom_p10_pct": float(bottom["outcome_pct"].quantile(0.10)),
                "top_p25_pct": float(top["outcome_pct"].quantile(0.25)),
                "bottom_p25_pct": float(bottom["outcome_pct"].quantile(0.25)),
                "severe_loss_rate_difference_pct": float(
                    (
                        top["outcome_pct"].le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        - bottom["outcome_pct"]
                        .le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT)
                        .mean()
                    )
                    * 100.0
                ),
                "top_fixed20_negative_share_pct": float(
                    top.get("fixed20_negative_flag", pd.Series(False, index=top.index))
                    .eq(True)
                    .mean()
                    * 100.0
                ),
                "bottom_fixed20_negative_share_pct": float(
                    bottom.get(
                        "fixed20_negative_flag", pd.Series(False, index=bottom.index)
                    )
                    .eq(True)
                    .mean()
                    * 100.0
                ),
                "top_overheat_share_pct": float(
                    top.get("fixed20_overheat_flag", pd.Series(False, index=top.index))
                    .eq(True)
                    .mean()
                    * 100.0
                ),
                "bottom_overheat_share_pct": float(
                    bottom.get(
                        "fixed20_overheat_flag", pd.Series(False, index=bottom.index)
                    )
                    .eq(True)
                    .mean()
                    * 100.0
                ),
                "top_sector_hhi": _sector_hhi(top),
                "bottom_sector_hhi": _sector_hhi(bottom),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _build_fixed_vs_ols_paired_df(oos: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "ring",
        "horizon",
        "date",
        "fixed_date",
        "ols_date",
        "fixed_raw_score_name",
        "ols_raw_score_name",
        "fixed_mean_lift_pct",
        "ols_mean_lift_pct",
        "fixed_minus_ols_lift_pct",
        "sufficient_sample",
    ]
    if oos.empty:
        return pd.DataFrame(columns=columns)
    fixed = oos.loc[oos["raw_score_name"].eq("fixed_equal_level")].copy()
    ols = oos.loc[oos["raw_score_name"].eq("ols_equal_level")].copy()
    paired = fixed.merge(
        ols,
        on=["ring", "horizon", "date"],
        how="inner",
        suffixes=("_fixed", "_ols"),
        validate="one_to_one",
    )
    if paired.empty:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(
        {
            "ring": paired["ring"],
            "horizon": paired["horizon"],
            "date": paired["date"],
            "fixed_date": paired["date"],
            "ols_date": paired["date"],
            "fixed_raw_score_name": paired["raw_score_name_fixed"],
            "ols_raw_score_name": paired["raw_score_name_ols"],
            "fixed_mean_lift_pct": paired["mean_lift_pct_fixed"],
            "ols_mean_lift_pct": paired["mean_lift_pct_ols"],
            "fixed_minus_ols_lift_pct": (
                paired["mean_lift_pct_fixed"] - paired["mean_lift_pct_ols"]
            ),
            "sufficient_sample": True,
        },
        columns=columns,
    )


def _build_topk_operational_lift_df(scored: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(rows)
    primary = scored.loc[scored["is_primary"].eq(True)].copy()
    previous_codes: dict[tuple[str, int, int], set[str]] = {}
    for (family, raw_score_name, horizon, signal_date), group in primary.groupby(
        ["family", "raw_score_name", "horizon", "date"], observed=True, sort=True
    ):
        eligible = _finite_rows(group, ["technical_fit_score", "outcome_pct"])
        eligible = eligible.drop_duplicates(["date", "code"])
        for k in (5, 10):
            if len(eligible) < 2 * k:
                continue
            selected = eligible.sort_values(
                ["technical_fit_score", "code"], ascending=[False, True]
            ).head(k)
            selected_codes = set(selected["code"].astype(str))
            turnover_key = (str(family), int(str(horizon)), int(k))
            previous = previous_codes.get(turnover_key)
            turnover = (
                float(1.0 - len(selected_codes & previous) / k) if previous else np.nan
            )
            previous_codes[turnover_key] = selected_codes
            ring_counts = selected["ring"].value_counts()
            rows.append(
                {
                    "family": family,
                    "raw_score_name": raw_score_name,
                    "role": "primary",
                    "horizon": int(str(horizon)),
                    "date": pd.Timestamp(str(signal_date)),
                    "k": int(k),
                    "eligible_count": int(len(eligible)),
                    "selected_count": int(len(selected)),
                    "eligible_mean_excess_return_pct": float(
                        eligible["outcome_pct"].mean()
                    ),
                    "selected_mean_excess_return_pct": float(
                        selected["outcome_pct"].mean()
                    ),
                    "topk_lift_pct": float(
                        selected["outcome_pct"].mean()
                        - eligible["outcome_pct"].mean()
                    ),
                    "eligible_severe_loss_rate_pct": float(
                        eligible["outcome_pct"]
                        .le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT)
                        .mean()
                        * 100.0
                    ),
                    "selected_severe_loss_rate_pct": float(
                        selected["outcome_pct"]
                        .le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT)
                        .mean()
                        * 100.0
                    ),
                    "severe_loss_rate_difference_pct": float(
                        (
                            selected["outcome_pct"]
                            .le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT)
                            .mean()
                            - eligible["outcome_pct"]
                            .le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT)
                            .mean()
                        )
                        * 100.0
                    ),
                    "eligible_sector_hhi": _sector_hhi(eligible),
                    "selected_sector_hhi": _sector_hhi(selected),
                    "turnover_rate": turnover,
                    "core_high_high_count": int(
                        ring_counts.get("core_high_high", 0)
                    ),
                    "near_high_high_1_count": int(
                        ring_counts.get("near_high_high_1", 0)
                    ),
                    "near_high_high_2_count": int(
                        ring_counts.get("near_high_high_2", 0)
                    ),
                }
            )
    return pd.DataFrame(rows)


def _diagnostic_bucket_masks(frame: pd.DataFrame) -> list[tuple[str, str, pd.Series]]:
    index = frame.index
    recent20 = pd.to_numeric(
        frame.get("recent_return_20d_pct", pd.Series(np.nan, index=index)),
        errors="coerce",
    )
    liquidity = pd.to_numeric(
        frame.get("liquidity_residual_z", pd.Series(np.nan, index=index)),
        errors="coerce",
    )
    r2 = pd.to_numeric(
        frame.get("ols_r2_20", pd.Series(np.nan, index=index)), errors="coerce"
    )
    acceleration = pd.to_numeric(
        frame.get("ols20_minus_ols60_move_pct", pd.Series(np.nan, index=index)),
        errors="coerce",
    )
    return [
        ("negative_return", "deep_pullback_le_minus10", recent20.le(-10.0)),
        (
            "negative_return",
            "shallow_negative_minus10_to_0",
            recent20.gt(-10.0) & recent20.lt(0.0),
        ),
        ("negative_return", "nonnegative", recent20.ge(0.0)),
        ("overheat", "fixed20_ge_30", recent20.ge(30.0)),
        ("ex_overheat", "fixed20_lt_30", recent20.lt(30.0)),
        ("liquidity_z_band", "z_lt_minus1", liquidity.lt(-1.0)),
        (
            "liquidity_z_band",
            "z_minus1_to_1",
            liquidity.ge(-1.0) & liquidity.lt(1.0),
        ),
        ("liquidity_z_band", "z_1_to_2", liquidity.ge(1.0) & liquidity.lt(2.0)),
        ("liquidity_z_band", "z_ge_2", liquidity.ge(2.0)),
        ("ols_r2", "r2_lt_0_5", r2.lt(0.5)),
        ("ols_r2", "r2_0_5_to_0_8", r2.ge(0.5) & r2.lt(0.8)),
        ("ols_r2", "r2_ge_0_8", r2.ge(0.8)),
        ("ols_acceleration", "positive", acceleration.gt(0.0)),
        ("ols_acceleration", "nonpositive", acceleration.le(0.0)),
        (
            "fixed_ols_conflict",
            "20d_conflict",
            frame.get(
                "fixed20_ols20_sign_conflict", pd.Series(False, index=index)
            ).eq(True),
        ),
        (
            "fixed_ols_conflict",
            "60d_conflict",
            frame.get(
                "fixed60_ols60_sign_conflict", pd.Series(False, index=index)
            ).eq(True),
        ),
    ]


def _date_fixed_effect_row(group: pd.DataFrame) -> dict[str, object]:
    controls = [
        "value_composite_equal_score",
        "long_hybrid_leadership_score",
        "liquidity_residual_z",
        "atr20_pct",
    ]
    missing_controls = [column for column in controls if column not in group.columns]
    available = [column for column in controls if column in group.columns]
    frame = group[["date", "technical_fit_score", "outcome_pct", *available]].copy()
    for column in missing_controls:
        frame[column] = np.nan
    frame = _finite_rows(frame, ["technical_fit_score", "outcome_pct", *controls])
    coefficient = float("nan")
    status = "insufficient_evidence"
    if len(frame) >= len(controls) + 2 and frame["date"].nunique() >= 2:
        numeric = ["technical_fit_score", "outcome_pct", *controls]
        demeaned = frame[numeric] - frame.groupby("date")[numeric].transform("mean")
        x = demeaned[["technical_fit_score", *controls]].to_numpy(dtype=float)
        y = demeaned["outcome_pct"].to_numpy(dtype=float)
        if np.linalg.matrix_rank(x) == x.shape[1]:
            coefficient = float(np.linalg.lstsq(x, y, rcond=None)[0][0])
            status = "ready"
    return {
        "sensitivity_type": "date_fixed_effect",
        "sensitivity_bucket": "all_candidates",
        "observation_count": int(len(frame)),
        "date_count": int(frame["date"].nunique()),
        "mean_outcome_pct": float(frame["outcome_pct"].mean()),
        "fit_effect_pct": coefficient,
        "controls": ",".join(controls),
        "diagnostic_status": status,
        "role": "sensitivity_only",
    }


def _build_diagnostics_df(scored: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(rows)
    primary = scored.loc[scored["is_primary"].eq(True)].copy()
    group_keys = ["family", "raw_score_name", "ring", "horizon"]
    for group_key, group in primary.groupby(group_keys, observed=True, sort=True):
        base = dict(zip(group_keys, group_key, strict=True))
        complete = _finite_rows(group, ["technical_fit_score", "outcome_pct"])
        for sensitivity_type, bucket, mask in _diagnostic_bucket_masks(complete):
            selected = complete.loc[mask]
            if selected.empty:
                continue
            rows.append(
                {
                    **base,
                    "sensitivity_type": sensitivity_type,
                    "sensitivity_bucket": bucket,
                    "observation_count": int(len(selected)),
                    "date_count": int(selected["date"].nunique()),
                    "mean_outcome_pct": float(selected["outcome_pct"].mean()),
                    "fit_effect_pct": float("nan"),
                    "controls": None,
                    "diagnostic_status": "ready",
                    "role": "sensitivity_only",
                }
            )
        if str(base["family"]) == "ols":
            spline_source = complete.copy()
            spline_source["raw_bin"] = spline_source["raw_level"].map(
                classify_raw_level_bin
            )
            for raw_bin, selected in spline_source.groupby(
                "raw_bin", observed=True, sort=True
            ):
                if raw_bin == "missing" or selected.empty:
                    continue
                rows.append(
                    {
                        **base,
                        "sensitivity_type": "ols_spline_shape",
                        "sensitivity_bucket": str(raw_bin),
                        "observation_count": int(len(selected)),
                        "date_count": int(selected["date"].nunique()),
                        "mean_outcome_pct": float(selected["outcome_pct"].mean()),
                        "fit_effect_pct": float("nan"),
                        "controls": None,
                        "diagnostic_status": "ready",
                        "role": "sensitivity_only",
                    }
                )
        sensitivity_frames: list[tuple[str, str, pd.DataFrame, str]] = [
            ("sector_equal", "all_sectors", complete, "outcome_pct"),
            (
                "bank_exclusion",
                "exclude_banks",
                complete.loc[
                    complete.get(
                        "sector_33_name", pd.Series("", index=complete.index)
                    ).ne("銀行業")
                ],
                "outcome_pct",
            ),
            ("benchmark", "n225_excess", complete, "n225_outcome_pct"),
        ]
        for sensitivity_type, bucket, selected, outcome_column in sensitivity_frames:
            selected = _finite_rows(selected, ["technical_fit_score", outcome_column])
            if selected.empty:
                continue
            if sensitivity_type == "sector_equal" and "sector_33_code" in selected:
                selected = (
                    selected.groupby(["date", "sector_33_code"], observed=True)
                    .agg(
                        technical_fit_score=("technical_fit_score", "mean"),
                        sensitivity_outcome=(outcome_column, "mean"),
                    )
                    .reset_index()
                )
                outcome_column = "sensitivity_outcome"
            rows.append(
                {
                    **base,
                    "sensitivity_type": sensitivity_type,
                    "sensitivity_bucket": bucket,
                    "observation_count": int(len(selected)),
                    "date_count": int(selected["date"].nunique()),
                    "mean_outcome_pct": float(selected[outcome_column].mean()),
                    "fit_effect_pct": float(
                        selected["technical_fit_score"].corr(
                            selected[outcome_column], method="spearman"
                        )
                    ),
                    "controls": None,
                    "diagnostic_status": "ready",
                    "role": "sensitivity_only",
                }
            )
        regression = _date_fixed_effect_row(complete)
        rows.append({**base, **regression})
    return pd.DataFrame(rows)


def _build_stability_tables(
    oos: pd.DataFrame,
    paired: pd.DataFrame,
    topk: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    configs = (
        (oos, "oos_fit_score_lift", "mean_lift_pct"),
        (paired, "fixed_vs_ols_paired", "fixed_minus_ols_lift_pct"),
        (topk, "topk_operational_lift", "topk_lift_pct"),
    )
    segment_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for frame, analysis, effect_column in configs:
        if frame.empty:
            continue
        working = frame.copy()
        working["segment"] = working["date"].map(_period_label)
        working["year"] = pd.to_datetime(working["date"]).dt.year
        identity = [
            column
            for column in ("family", "raw_score_name", "ring", "horizon", "k")
            if column in working.columns
        ]
        for period_column, target in (
            ("segment", segment_rows),
            ("year", annual_rows),
        ):
            keys = [*identity, period_column]
            for group_key, group in working.groupby(
                keys, observed=True, sort=True, dropna=False
            ):
                values = group_key if isinstance(group_key, tuple) else (group_key,)
                row: dict[str, object] = dict(zip(keys, values, strict=True))
                period_value = row.pop(period_column)
                effects = pd.to_numeric(group[effect_column], errors="coerce").dropna()
                row.update(
                    {
                        "analysis": analysis,
                        "period_label": str(period_value),
                        "date_count": int(group["date"].nunique()),
                        "mean_effect_pct": float(effects.mean()),
                        "median_effect_pct": float(effects.median()),
                        "positive_date_rate_pct": float(effects.gt(0.0).mean() * 100.0),
                    }
                )
                target.append(row)
    return pd.DataFrame(segment_rows), pd.DataFrame(annual_rows)


def _build_bootstrap_effect_ci_df(
    oos: pd.DataFrame,
    paired: pd.DataFrame,
    topk: pd.DataFrame,
    *,
    resamples: int,
    seed: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    configs = (
        (
            oos,
            "oos_fit_score_lift",
            ["family", "raw_score_name", "ring", "horizon"],
            "mean_lift_pct",
        ),
        (
            paired,
            "fixed_vs_ols_paired",
            ["ring", "horizon"],
            "fixed_minus_ols_lift_pct",
        ),
        (
            topk,
            "topk_operational_lift",
            ["family", "raw_score_name", "horizon", "k"],
            "topk_lift_pct",
        ),
    )
    for frame, analysis, keys, effect_column in configs:
        if frame.empty:
            continue
        for group_key, group in frame.groupby(keys, observed=True, sort=True):
            values = group_key if isinstance(group_key, tuple) else (group_key,)
            identity: dict[str, object] = dict(zip(keys, values, strict=True))
            horizon = int(str(identity["horizon"]))
            ordered = group.sort_values("date")
            point, lower, upper = moving_block_bootstrap_ci(
                ordered[effect_column].to_numpy(dtype=float),
                block_length=horizon,
                resamples=resamples,
                seed=seed,
            )
            rows.append(
                {
                    **identity,
                    "analysis": analysis,
                    "date_count": int(group["date"].nunique()),
                    "block_length": horizon,
                    "resamples": int(resamples),
                    "seed": int(seed),
                    "point_estimate_pct": point,
                    "ci_lower_pct": lower,
                    "ci_upper_pct": upper,
                }
            )
    return pd.DataFrame(rows)


def build_technical_fit_evidence_tables(
    observations: pd.DataFrame,
    *,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    min_training_observations: int = DEFAULT_MIN_TRAINING_OBSERVATIONS,
    min_training_dates: int = DEFAULT_MIN_TRAINING_DATES,
    bootstrap_resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> TechnicalFitEvidenceTables:
    """Build frozen date-equal raw shape and walk-forward OOS evidence.

    Mappings are learned once per raw score on the three-ring union, using only
    completed 20D outcomes before each evaluation year.  The equal-weight fixed
    and OLS scores remain the sole primary comparison; component scores are
    emitted for attribution only.
    """

    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    if not resolved_horizons or any(horizon <= 0 for horizon in resolved_horizons):
        raise ValueError("horizons must contain positive integers")
    if min_training_observations <= 0 or min_training_dates <= 0:
        raise ValueError("training minimums must be positive")
    if bootstrap_resamples <= 0:
        raise ValueError("bootstrap_resamples must be positive")
    required = {"date", "code", "ring"}
    missing = required.difference(observations.columns)
    if missing:
        raise ValueError(f"observations is missing required columns: {sorted(missing)}")

    source = observations.copy()
    source["date"] = pd.to_datetime(source["date"], errors="coerce").dt.normalize()
    source = source.loc[source["date"].notna()].copy()
    raw_daily, raw_summary = _build_raw_shape_tables(
        source,
        horizons=resolved_horizons,
    )
    mapping = _build_all_walkforward_mappings(
        source,
        min_training_observations=min_training_observations,
        min_training_dates=min_training_dates,
    )
    scored = _score_walkforward_observations(
        source,
        mapping,
        horizons=resolved_horizons,
    )
    oos = _build_oos_fit_score_lift_df(scored)
    paired = _build_fixed_vs_ols_paired_df(oos)
    topk = _build_topk_operational_lift_df(scored)
    diagnostics = _build_diagnostics_df(scored)
    segment, annual = _build_stability_tables(oos, paired, topk)
    bootstrap = _build_bootstrap_effect_ci_df(
        oos,
        paired,
        topk,
        resamples=bootstrap_resamples,
        seed=bootstrap_seed,
    )
    return TechnicalFitEvidenceTables(
        raw_shape_daily_df=raw_daily,
        raw_shape_summary_df=raw_summary,
        walkforward_mapping_df=mapping,
        oos_fit_score_lift_df=oos,
        fixed_vs_ols_paired_df=paired,
        topk_operational_lift_df=topk,
        overheat_negative_diagnostics_df=diagnostics,
        segment_stability_df=segment,
        annual_stability_df=annual,
        bootstrap_effect_ci_df=bootstrap,
    )


def build_decision_gate_df(
    family_evidence: pd.DataFrame,
    paired_evidence: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the frozen equal-weight Fixed-versus-OLS decision precedence."""

    required_family = {
        "family",
        "raw_score_name",
        "passes_adoption_gate",
        "sufficient_sample",
    }
    missing_family = required_family.difference(family_evidence.columns)
    if missing_family:
        raise ValueError(
            f"family_evidence is missing required columns: {sorted(missing_family)}"
        )

    family_rows: dict[str, tuple[bool, bool, bool]] = {}
    result_rows: list[dict[str, object]] = []
    for family, primary_score_name in PRIMARY_RAW_SCORE_BY_FAMILY.items():
        subset = family_evidence.loc[
            family_evidence["family"].eq(family)
            & family_evidence["raw_score_name"].eq(primary_score_name)
        ]
        valid_evidence = bool(
            not subset.empty
            and _has_only_explicit_booleans(subset["sufficient_sample"])
            and _has_only_explicit_booleans(subset["passes_adoption_gate"])
        )
        sufficient = bool(valid_evidence and _all_explicit_true(subset["sufficient_sample"]))
        passed = bool(
            sufficient and _all_explicit_true(subset["passes_adoption_gate"])
        )
        family_rows[family] = (valid_evidence, sufficient, passed)
        result_rows.append(
            {
                "decision_key": family,
                "decision": (
                    "passes_adoption_gate"
                    if passed
                    else "fails_adoption_gate"
                    if valid_evidence
                    else "insufficient_evidence"
                ),
                "sufficient_sample": sufficient,
                "passed": passed,
            }
        )

    fixed_valid, fixed_sufficient, fixed_passed = family_rows["fixed"]
    ols_valid, ols_sufficient, ols_passed = family_rows["ols"]
    if not fixed_valid or not ols_valid or not fixed_sufficient or not ols_sufficient:
        decision = "insufficient_evidence"
    elif fixed_passed and not ols_passed:
        decision = "fixed_wins"
    elif ols_passed and not fixed_passed:
        decision = "ols_wins"
    elif not fixed_passed and not ols_passed:
        decision = "neither"
    else:
        required_paired = {"sufficient_sample", "ci_lower_pct", "ci_upper_pct"}
        if required_paired.difference(paired_evidence.columns) or paired_evidence.empty:
            decision = "insufficient_evidence"
        else:
            paired_sufficient = _all_explicit_true(
                paired_evidence["sufficient_sample"]
            )
            lower = pd.to_numeric(paired_evidence["ci_lower_pct"], errors="coerce")
            upper = pd.to_numeric(paired_evidence["ci_upper_pct"], errors="coerce")
            if not paired_sufficient or not np.isfinite(lower).all() or not np.isfinite(upper).all():
                decision = "insufficient_evidence"
            elif bool(lower.gt(0.0).all()):
                decision = "fixed_wins"
            elif bool(upper.lt(0.0).all()):
                decision = "ols_wins"
            else:
                decision = "equivalent_fixed_preferred_operationally"
    result_rows.append(
        {
            "decision_key": "fixed_vs_ols",
            "decision": decision,
            "sufficient_sample": decision != "insufficient_evidence",
            "passed": decision in {"fixed_wins", "ols_wins"},
        }
    )
    return pd.DataFrame(
        result_rows,
        columns=("decision_key", "decision", "sufficient_sample", "passed"),
    )
