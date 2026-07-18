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
    """Read-only PIT candidate observations produced by the Task 2 runner."""

    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame


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
        observation_sample = ctx.connection.execute(
            """
            SELECT *
            FROM ranking_technical_fit_candidate_observations
            ORDER BY date, ring, code
            LIMIT ?
            """,
            [int(observation_sample_limit)],
        ).fetchdf()
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
