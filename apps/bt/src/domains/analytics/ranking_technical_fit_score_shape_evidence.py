"""PIT contracts and raw panel for the Technical Fit Score shape study."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from scipy.interpolate import BSpline

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_long_scaffold_value_composite_evidence import (
    _create_value_composite_panel,
)
from src.domains.analytics.ranking_fixed_return_priority_evidence import (
    moving_block_bootstrap_ci,
)
from src.domains.analytics.ranking_research_selection_contract import (
    select_frozen_topk,
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
    open_readonly_analysis_connection,
    require_market_v4_compatibility,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)
from src.domains.analytics.trend_slope_features import rolling_log_slope_features
from src.domains.analytics.ranking_technical_fit_price_projection import (
    EventTimePriceAudit,
    create_event_time_price_relations,
)
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
DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT = 0.01
FIRST_TRAINING_YEAR = 2017
FIRST_EVALUATION_YEAR = 2022
PRIMARY_RAW_SCORE_BY_FAMILY = {
    "fixed": "fixed_equal_level",
    "ols": "ols_equal_level",
}

DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
RANKING_TECHNICAL_FIT_SCORE_SHAPE_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-technical-fit-score-shape-evidence"
)
PRIME_EQUIVALENT_MARKET_CODES: tuple[str, ...] = tuple(
    code for code in MARKET_CODES_BY_SCOPE["prime"] if code.isdigit()
)
if set(PRIME_EQUIVALENT_MARKET_CODES) != {"0101", "0111"}:
    raise RuntimeError("Prime research must resolve to exact-date 0101/0111 membership")

_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_OLS_WINDOWS: tuple[int, ...] = (20, 60)
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_WARMUP_CALENDAR_DAYS = 820
_REQUIRED_MARKET_TABLES = {
    "stock_data_raw",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
}

_DATA_PLANE_SCHEMA_VERSION = 4
_STOCK_PRICE_ADJUSTMENT_MODE = "local_projection_v2_event_time"
_PIT_AS_OF_POLICY = "exact_signal_date_no_latest_fallback"
_PIT_INVALIDATION_DISPOSITION = (
    "v1_v2_historical_archive_v3_superseded_by_v4_for_price_basis_gate_ci_hardening_"
    "v4_superseded_by_v5_for_explicit_failed_shape_slices_"
    "v5_superseded_by_v6_for_lineage_disposition_hardening_"
    "v6_superseded_by_v7_for_review_fixed_frontier_and_flat_mapping_"
    "v7_superseded_by_v8_for_lineage_disposition_hardening"
)

BUNDLE_TABLE_ORDER: tuple[str, ...] = (
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
)
REQUIRED_BUNDLE_TABLES = set(BUNDLE_TABLE_ORDER)

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
    "training_completion_end_date",
)


@dataclass(frozen=True)
class PitLineageAudit:
    """Auditable event-time lineage for every consumed Prime valuation row."""

    data_plane_schema_version: int
    stock_price_adjustment_mode: str
    universe_source: str
    as_of_policy: str
    basis_dependent_sources: tuple[str, ...]
    basis_ids: tuple[str, ...]
    basis_id_sha256: str
    consumed_daily_valuation_row_count: int
    verified_basis_row_count: int
    verified_segment_row_count: int
    consumed_signal_start_date: str | None
    consumed_signal_end_date: str | None
    verification_status: str
    no_service_local_recomputation: bool
    no_basis_fallback: bool
    invalidation_disposition: str
    price_projection: EventTimePriceAudit | None

    def to_manifest_payload(self) -> dict[str, Any]:
        return {
            "data_plane": (
                f"physical_market.duckdb_schema_v{self.data_plane_schema_version}"
            ),
            "stock_price_adjustment_mode": self.stock_price_adjustment_mode,
            "universe_source": self.universe_source,
            "as_of_policy": self.as_of_policy,
            "basis_dependent_sources": list(self.basis_dependent_sources),
            "basis_ids": list(self.basis_ids),
            "basis_id_count": len(self.basis_ids),
            "basis_id_sha256": self.basis_id_sha256,
            "consumed_daily_valuation_row_count": (
                self.consumed_daily_valuation_row_count
            ),
            "verified_basis_row_count": self.verified_basis_row_count,
            "verified_segment_row_count": self.verified_segment_row_count,
            "consumed_signal_start_date": self.consumed_signal_start_date,
            "consumed_signal_end_date": self.consumed_signal_end_date,
            "verification_status": self.verification_status,
            "no_service_local_recomputation": self.no_service_local_recomputation,
            "no_basis_fallback": self.no_basis_fallback,
            "invalidation_disposition": self.invalidation_disposition,
            "price_projection": (
                self.price_projection.to_manifest_payload()
                if self.price_projection is not None
                else None
            ),
        }


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
    min_training_observations: int
    min_training_dates: int
    bootstrap_resamples: int
    bootstrap_seed: int
    observation_sample_limit: int
    observation_count: int
    pit_lineage: PitLineageAudit
    ring_registry_df: pd.DataFrame
    raw_score_registry_df: pd.DataFrame
    coverage_attrition_df: pd.DataFrame
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
    decision_gate_df: pd.DataFrame


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
    flat_tolerance_pct: float = DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT,
    reproduces_core_and_near: bool = False,
    positive_2022_2023: bool = False,
    positive_2024_plus: bool = False,
    severe_loss_not_worse: bool = False,
) -> str:
    """Classify a five-bin response without designating a preferred bin a priori."""

    if flat_tolerance_pct < 0:
        raise ValueError("flat_tolerance_pct must be non-negative")
    values = [_as_finite_float(value) for value in expectancies]
    if len(values) != len(RAW_BIN_LABELS) or any(value is None for value in values):
        return "insufficient_evidence"
    finite_values = [float(value) for value in values if value is not None]
    expectancy_spread = max(finite_values) - min(finite_values)
    if expectancy_spread <= flat_tolerance_pct or math.isclose(
        expectancy_spread,
        flat_tolerance_pct,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
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


def _audit_consumed_pit_lineage(
    conn: Any,
    price_projection: EventTimePriceAudit | None = None,
) -> PitLineageAudit:
    """Fail closed unless every Prime panel row has exact event-time lineage."""

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_consumed_lineage AS
        SELECT DISTINCT
            code,
            CAST(date AS VARCHAR) AS date,
            CAST(valuation_basis_id AS VARCHAR) AS valuation_basis_id
        FROM ranking_color_panel
        """
    )
    consumed_row = conn.execute(
        """
        SELECT count(*), min(date), max(date)
        FROM ranking_technical_fit_consumed_lineage
        """
    ).fetchone()
    consumed_count = int(consumed_row[0]) if consumed_row else 0
    consumed_start = (
        str(consumed_row[1]) if consumed_row and consumed_row[1] is not None else None
    )
    consumed_end = (
        str(consumed_row[2]) if consumed_row and consumed_row[2] is not None else None
    )

    missing_basis_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage
            WHERE valuation_basis_id IS NULL OR trim(valuation_basis_id) = ''
            """
        ).fetchone()[0]
    )
    if missing_basis_count:
        raise RuntimeError(
            "PIT lineage validation failed: missing cutoff-valid daily_valuation "
            f"basis for {missing_basis_count} consumed Prime rows; no latest/current "
            "basis fallback is allowed"
        )

    containing_basis_mismatch_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage AS consumed
            WHERE (
                SELECT count(*)
                FROM stock_adjustment_bases AS basis
                WHERE basis.code = consumed.code
                  AND basis.valid_from <= consumed.date
                  AND (
                      basis.valid_to_exclusive IS NULL
                      OR consumed.date < basis.valid_to_exclusive
                  )
            ) <> 1
               OR NOT EXISTS (
                   SELECT 1
                   FROM stock_adjustment_bases AS basis
                   WHERE basis.code = consumed.code
                     AND basis.basis_id = consumed.valuation_basis_id
                     AND basis.valid_from <= consumed.date
                     AND (
                         basis.valid_to_exclusive IS NULL
                         OR consumed.date < basis.valid_to_exclusive
                     )
               )
            """
        ).fetchone()[0]
    )
    if containing_basis_mismatch_count:
        raise RuntimeError(
            "PIT lineage validation failed: mismatched cutoff-valid basis for "
            f"{containing_basis_mismatch_count} consumed Prime rows"
        )

    unready_basis_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage AS consumed
            JOIN stock_adjustment_bases AS basis
              ON basis.code = consumed.code
             AND basis.basis_id = consumed.valuation_basis_id
            WHERE basis.status <> 'ready'
               OR basis.materialized_through_date < consumed.date
               OR basis.source_fingerprint IS NULL
               OR trim(basis.source_fingerprint) = ''
            """
        ).fetchone()[0]
    )
    if unready_basis_count:
        raise RuntimeError(
            "PIT lineage validation failed: basis is not ready and materialized "
            f"through signal date for {unready_basis_count} consumed Prime rows"
        )

    malformed_basis_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage AS consumed
            JOIN stock_adjustment_bases AS basis
              ON basis.code = consumed.code
             AND basis.basis_id = consumed.valuation_basis_id
            WHERE basis.basis_id <> (
                'event-pit-v1:' || basis.code || ':' || basis.valid_from
            )
            """
        ).fetchone()[0]
    )
    if malformed_basis_count:
        raise RuntimeError(
            "PIT lineage validation failed: malformed event-time basis_id for "
            f"{malformed_basis_count} consumed Prime rows"
        )

    segment_cardinality_mismatch_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage AS consumed
            WHERE (
                SELECT count(*)
                FROM stock_adjustment_basis_segments AS segment
                WHERE segment.code = consumed.code
                  AND segment.basis_id = consumed.valuation_basis_id
                  AND segment.source_date_from <= consumed.date
                  AND (
                      segment.source_date_to_exclusive IS NULL
                      OR consumed.date < segment.source_date_to_exclusive
                  )
            ) <> 1
            """
        ).fetchone()[0]
    )
    if segment_cardinality_mismatch_count:
        raise RuntimeError(
            "PIT lineage validation failed: expected exactly one total covering "
            "adjustment basis segment for each consumed Prime row; invalid rows="
            f"{segment_cardinality_mismatch_count}"
        )

    invalid_segment_factor_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM ranking_technical_fit_consumed_lineage AS consumed
            JOIN stock_adjustment_basis_segments AS segment
              ON segment.code = consumed.code
             AND segment.basis_id = consumed.valuation_basis_id
             AND segment.source_date_from <= consumed.date
             AND (
                 segment.source_date_to_exclusive IS NULL
                 OR consumed.date < segment.source_date_to_exclusive
             )
            WHERE segment.cumulative_factor IS NULL
               OR NOT isfinite(segment.cumulative_factor)
               OR segment.cumulative_factor <= 0
            """
        ).fetchone()[0]
    )
    if invalid_segment_factor_count:
        raise RuntimeError(
            "PIT lineage validation failed: covering adjustment basis segment "
            "cumulative_factor must be finite and positive for each consumed "
            f"Prime row; invalid rows={invalid_segment_factor_count}"
        )

    basis_ids = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT valuation_basis_id
            FROM ranking_technical_fit_consumed_lineage
            ORDER BY valuation_basis_id
            """
        ).fetchall()
    )
    basis_id_sha256 = hashlib.sha256("\n".join(basis_ids).encode()).hexdigest()
    verified_basis_row_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM (
                SELECT DISTINCT basis.code, basis.basis_id
                FROM ranking_technical_fit_consumed_lineage AS consumed
                JOIN stock_adjustment_bases AS basis
                  ON basis.code = consumed.code
                 AND basis.basis_id = consumed.valuation_basis_id
            )
            """
        ).fetchone()[0]
    )
    verified_segment_row_count = int(
        conn.execute(
            """
            SELECT count(*)
            FROM (
                SELECT DISTINCT
                    segment.code,
                    segment.basis_id,
                    segment.source_date_from
                FROM ranking_technical_fit_consumed_lineage AS consumed
                JOIN stock_adjustment_basis_segments AS segment
                  ON segment.code = consumed.code
                 AND segment.basis_id = consumed.valuation_basis_id
                 AND segment.source_date_from <= consumed.date
                 AND (
                     segment.source_date_to_exclusive IS NULL
                     OR consumed.date < segment.source_date_to_exclusive
                 )
            )
            """
        ).fetchone()[0]
    )
    return PitLineageAudit(
        data_plane_schema_version=_DATA_PLANE_SCHEMA_VERSION,
        stock_price_adjustment_mode=_STOCK_PRICE_ADJUSTMENT_MODE,
        universe_source="stock_master_daily",
        as_of_policy=_PIT_AS_OF_POLICY,
        basis_dependent_sources=("daily_valuation", "stock_data_raw"),
        basis_ids=basis_ids,
        basis_id_sha256=basis_id_sha256,
        consumed_daily_valuation_row_count=consumed_count,
        verified_basis_row_count=verified_basis_row_count,
        verified_segment_row_count=verified_segment_row_count,
        consumed_signal_start_date=consumed_start,
        consumed_signal_end_date=consumed_end,
        verification_status="verified",
        no_service_local_recomputation=True,
        no_basis_fallback=True,
        invalidation_disposition=_PIT_INVALIDATION_DISPOSITION,
        price_projection=price_projection,
    )


def run_ranking_technical_fit_score_shape_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = "2017-01-01",
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    min_training_observations: int = DEFAULT_MIN_TRAINING_OBSERVATIONS,
    min_training_dates: int = DEFAULT_MIN_TRAINING_DATES,
    bootstrap_resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingTechnicalFitScoreShapeEvidenceResult:
    """Build the frozen Prime-only candidate and raw technical PIT panel."""

    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    if not resolved_horizons or any(horizon <= 0 for horizon in resolved_horizons):
        raise ValueError("horizons must contain positive integers")
    if min_training_observations <= 0 or min_training_dates <= 0:
        raise ValueError("training minimums must be positive")
    if bootstrap_resamples <= 0:
        raise ValueError("bootstrap_resamples must be positive")
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
        price_relations, price_projection_audit = create_event_time_price_relations(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
        )
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
            event_time_basis_only=True,
            price_feature_relation=price_relations.signal_features,
            price_outcome_relation=price_relations.forward_outcomes,
        )
        pit_lineage = _audit_consumed_pit_lineage(
            ctx.connection, price_projection_audit
        )
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
            stock_return_relation=price_relations.signal_features,
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
            price_feature_relation=price_relations.signal_features,
            price_outcome_relation=price_relations.forward_outcomes,
        )
        _create_short_red_feature_panel(ctx.connection)
        _create_value_composite_panel(ctx.connection)

        # Freeze membership using only Value and Long-Hybrid scores before any
        # raw technical or forward-outcome relation is attached.
        _create_candidate_ring_flags_table(ctx.connection)
        _create_ols_feature_table(
            ctx.connection,
            price_feature_relation=price_relations.signal_features,
        )
        _create_prime_technical_rank_table(ctx.connection)
        _create_n225_forward_return_table(
            ctx.connection,
            horizons=resolved_horizons,
        )
        _create_candidate_observation_table(
            ctx.connection,
            horizons=resolved_horizons,
            price_outcome_relation=price_relations.forward_outcomes,
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
            min_training_observations=min_training_observations,
            min_training_dates=min_training_dates,
            bootstrap_resamples=bootstrap_resamples,
            bootstrap_seed=bootstrap_seed,
        )
        decision_gate = _build_result_decision_gate(evidence)
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
            min_training_observations=int(min_training_observations),
            min_training_dates=int(min_training_dates),
            bootstrap_resamples=int(bootstrap_resamples),
            bootstrap_seed=int(bootstrap_seed),
            observation_sample_limit=int(observation_sample_limit),
            observation_count=observation_count,
            pit_lineage=pit_lineage,
            ring_registry_df=_build_ring_registry_df(),
            raw_score_registry_df=_build_raw_score_registry_df(),
            coverage_attrition_df=_build_coverage_attrition_df(observations),
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
            decision_gate_df=decision_gate,
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


def _build_ols_feature_frame(  # pyright: ignore[reportUnusedFunction]
    prices: pd.DataFrame,
) -> pd.DataFrame:
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


def _create_ols_feature_table(
    conn: Any,
    *,
    price_feature_relation: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_ols_features AS
        SELECT
            code, date,
            ols_move_20d_pct,
            ols_move_60d_pct,
            ols_r2_20,
            ols_r2_60
        FROM {price_feature_relation}
        """
    )


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
    price_outcome_relation: str | None = None,
) -> None:
    outcome_alias = "o" if price_outcome_relation is not None else "a"
    outcome_columns = ",\n            ".join(
        expression
        for horizon in horizons
        for expression in (
            f"{outcome_alias}.forward_outcome_completion_date_{int(horizon)}d",
            f"{outcome_alias}.forward_close_return_{int(horizon)}d_pct",
            f"{outcome_alias}.forward_close_excess_return_{int(horizon)}d_pct",
            "CASE WHEN "
            f"{outcome_alias}.forward_close_return_{int(horizon)}d_pct IS NOT NULL "
            f"AND n.n225_close_return_{int(horizon)}d_pct IS NOT NULL "
            f"THEN {outcome_alias}.forward_close_return_{int(horizon)}d_pct "
            f"- n.n225_close_return_{int(horizon)}d_pct END "
            f"AS forward_close_n225_excess_return_{int(horizon)}d_pct",
        )
    )
    outcome_join = (
        f"LEFT JOIN {price_outcome_relation} o ON o.date = c.date AND o.code = c.code"
        if price_outcome_relation is not None
        else ""
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
        LEFT JOIN atr_expansion_panel a
          ON a.date = c.date
         AND a.code = c.code
        {outcome_join}
        LEFT JOIN ranking_technical_fit_n225_forward_returns n
          ON n.date = c.date
        LEFT JOIN ranking_technical_fit_prime_ranked t
          ON t.market_scope = c.market_scope
         AND t.market_code = c.market_code
         AND t.date = c.date
         AND t.code = c.code
        """
    )


def _create_n225_forward_return_table(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> None:
    """Build N225 forward returns from its independent full index session relation."""

    forward_exprs = ",\n                ".join(
        f"lead(close, {int(horizon)}) over (order by date) "
        f"as n225_future_close_{int(horizon)}d"
        for horizon in horizons
    )
    return_exprs = ",\n            ".join(
        f"case when close > 0 and n225_future_close_{int(horizon)}d > 0 "
        f"then (n225_future_close_{int(horizon)}d / close - 1.0) * 100.0 end "
        f"as n225_close_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_technical_fit_n225_forward_returns AS
        WITH raw_n225 AS (
            SELECT
                CAST(id.date AS DATE) AS date,
                arg_min(CAST(id.close AS DOUBLE), id.code) AS close
            FROM indices_data id
            WHERE upper(id.code) = '{_NIKKEI_SYNTHETIC_INDEX_CODE}'
              AND id.close > 0
            GROUP BY CAST(id.date AS DATE)
        ),
        featured AS (
            SELECT
                date,
                close,
                {forward_exprs}
            FROM raw_n225
        )
        SELECT
            date,
            {return_exprs}
        FROM featured
        """
    )


def build_walkforward_mapping(
    training: pd.DataFrame,
    evaluation_year: int,
    *,
    raw_level_column: str = "raw_level",
    outcome_column: str = "forward_topix_excess_20d_pct",
    date_column: str = "date",
    completion_date_column: str = "forward_outcome_completion_date_20d",
    raw_score_name: str = "raw_level",
    min_observations: int = DEFAULT_MIN_TRAINING_OBSERVATIONS,
    min_signal_dates: int = DEFAULT_MIN_TRAINING_DATES,
) -> pd.DataFrame:
    """Learn a five-bin, strictly-prior-year Technical Fit mapping.

    Training expectancies are date-equal means.  Under-covered bins are represented
    with an explicit unavailable status and never produce an interpolation mapping.
    """

    required = {
        raw_level_column,
        outcome_column,
        date_column,
        completion_date_column,
    }
    missing = required.difference(training.columns)
    if missing:
        raise ValueError(f"training is missing required columns: {sorted(missing)}")
    if min_observations <= 0 or min_signal_dates <= 0:
        raise ValueError("training minimums must be positive")

    evaluation_start = pd.Timestamp(year=int(evaluation_year), month=1, day=1)
    source = training.loc[
        :, [date_column, completion_date_column, raw_level_column, outcome_column]
    ].copy()
    source[date_column] = pd.to_datetime(
        source[date_column], errors="coerce"
    ).dt.normalize()
    source[completion_date_column] = pd.to_datetime(
        source[completion_date_column], errors="coerce"
    ).dt.normalize()
    source[outcome_column] = pd.to_numeric(source[outcome_column], errors="coerce")
    source["raw_bin"] = source[raw_level_column].map(classify_raw_level_bin)
    usable = source.loc[
        source[date_column].notna()
        & source[date_column].lt(evaluation_start)
        & source[completion_date_column].notna()
        & source[completion_date_column].lt(evaluation_start)
        & source[outcome_column].notna()
        & np.isfinite(source[outcome_column])
        & source["raw_bin"].ne("missing")
    ].copy()
    training_start = usable[date_column].min() if not usable.empty else pd.NaT
    training_end = usable[date_column].max() if not usable.empty else pd.NaT
    training_completion_end = (
        usable[completion_date_column].max() if not usable.empty else pd.NaT
    )

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
                "training_completion_end_date": training_completion_end,
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
        mapping["technical_fit_score"] = (mapping["expectancy_pct"] - minimum) / (
            maximum - minimum
        )
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
        raise ValueError(
            f"mapping is missing required columns: {sorted(missing_mapping)}"
        )

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
        raw_values = pd.to_numeric(
            scored.loc[row_mask, raw_level_column], errors="coerce"
        )
        valid = (
            raw_values.notna() & np.isfinite(raw_values) & raw_values.between(0.0, 1.0)
        )
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
                        "role": "primary"
                        if definition.is_primary
                        else "attribution_only",
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
        for year in dates.loc[dates.dt.year.ge(FIRST_EVALUATION_YEAR)]
        .dt.year.dropna()
        .unique()
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
            mapping["role"] = "primary" if definition.is_primary else "attribution_only"
            mappings.append(mapping)
    if not mappings:
        return pd.DataFrame(columns=(*_MAPPING_COLUMNS, "family", "is_primary", "role"))
    return pd.concat(mappings, ignore_index=True)


def _build_oos_shape_pair_gate_rows(
    daily: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Build auditable same-near, same-period frozen raw-shape gate slices."""

    output_columns = (
        "family",
        "raw_score_name",
        "ring",
        "horizon",
        "analysis",
        "period_label",
        "date_count",
        "mean_effect_pct",
        "median_effect_pct",
        "positive_date_rate_pct",
        "k",
    )
    required_periods = (
        "walkforward_2022_2023",
        "hypothesis_origin_2024_plus",
    )
    rows = [
        {
            "family": definition.family,
            "raw_score_name": definition.name,
            "ring": near_ring,
            "horizon": 20,
            "analysis": "raw_shape_pair_gate",
            "period_label": period_label,
            "date_count": 0,
            "mean_effect_pct": float("nan"),
            "median_effect_pct": float("nan"),
            "positive_date_rate_pct": 0.0,
            "k": float("nan"),
        }
        for definition in RAW_SCORE_REGISTRY
        for near_ring in ("near_high_high_1", "near_high_high_2")
        for period_label in required_periods
    ]
    row_by_key = {
        (str(row["raw_score_name"]), str(row["ring"]), str(row["period_label"])): row
        for row in rows
    }
    if daily.empty or mapping.empty:
        return pd.DataFrame(rows, columns=output_columns)
    ready_mapping = mapping.loc[
        mapping["mapping_status"].isin({"ready", "flat"})
    ].copy()
    selected_bins: dict[tuple[str, int], str] = {}
    for (raw_score_name, evaluation_year), group in ready_mapping.groupby(
        ["raw_score_name", "evaluation_year"], observed=True, sort=True
    ):
        maximum = group["technical_fit_score"].max()
        winners = group.loc[group["technical_fit_score"].eq(maximum), "raw_bin"]
        if len(winners) != 1:
            continue
        winner = str(winners.iloc[0])
        if winner in {RAW_BIN_LABELS[0], RAW_BIN_LABELS[-1]}:
            continue
        selected_bins[(str(raw_score_name), int(str(evaluation_year)))] = winner

    primary_daily = daily.loc[
        daily["horizon"].eq(20)
        & pd.to_datetime(daily["date"]).dt.year.ge(FIRST_EVALUATION_YEAR)
    ].copy()
    comparison_rows: list[dict[str, object]] = []
    for (raw_score_name, ring, signal_date), group in primary_daily.groupby(
        ["raw_score_name", "ring", "date"], observed=True, sort=True
    ):
        year = pd.Timestamp(str(signal_date)).year
        winner = selected_bins.get((str(raw_score_name), year))
        if winner is None:
            continue
        winner_index = RAW_BIN_LABELS.index(winner)
        control_bins = {
            RAW_BIN_LABELS[winner_index - 1],
            RAW_BIN_LABELS[winner_index + 1],
            RAW_BIN_LABELS[-1],
        }
        by_bin = group.set_index("raw_bin")
        if winner not in by_bin.index or not control_bins.issubset(by_bin.index):
            continue
        selected = by_bin.loc[[winner]].iloc[0]
        controls = by_bin.loc[sorted(control_bins)]
        selected_mean = float(selected["mean_excess_return_pct"])
        selected_severe = float(selected["severe_loss_rate_pct"])
        comparison_rows.append(
            {
                "raw_score_name": str(raw_score_name),
                "ring": str(ring),
                "date": pd.Timestamp(str(signal_date)),
                "segment": _period_label(signal_date),
                "minimum_selected_lift_pct": float(
                    (selected_mean - controls["mean_excess_return_pct"]).min()
                ),
                "maximum_severe_loss_deterioration_pct": float(
                    (selected_severe - controls["severe_loss_rate_pct"]).max()
                ),
            }
        )
    comparisons = pd.DataFrame(comparison_rows)
    if comparisons.empty:
        return pd.DataFrame(rows, columns=output_columns)

    for raw_score_name, group in comparisons.groupby(
        "raw_score_name", observed=True, sort=True
    ):
        for period_label in required_periods:
            period = group.loc[group["segment"].eq(period_label)]
            core = period.loc[period["ring"].eq("core_high_high")]
            if core.empty:
                continue
            core_lift = float(core["minimum_selected_lift_pct"].mean())
            core_severe = float(
                core["maximum_severe_loss_deterioration_pct"].mean()
            )
            for near_ring in ("near_high_high_1", "near_high_high_2"):
                near = period.loc[period["ring"].eq(near_ring)]
                if near.empty:
                    continue
                near_lift = float(near["minimum_selected_lift_pct"].mean())
                near_severe = float(
                    near["maximum_severe_loss_deterioration_pct"].mean()
                )
                minimum_pair_lift = min(core_lift, near_lift)
                maximum_pair_severe = max(core_severe, near_severe)
                pair_passes = bool(
                    minimum_pair_lift > 0.0 and maximum_pair_severe <= 1.0
                )
                row_by_key[(str(raw_score_name), near_ring, period_label)].update(
                    {
                        "date_count": min(
                            int(core["date"].nunique()), int(near["date"].nunique())
                        ),
                        "mean_effect_pct": minimum_pair_lift,
                        "median_effect_pct": maximum_pair_severe,
                        "positive_date_rate_pct": 100.0 if pair_passes else 0.0,
                    }
                )
    return pd.DataFrame(rows, columns=output_columns)


def _score_passes_oos_shape_pair_gate(
    gate_rows: pd.DataFrame,
    raw_score_name: str,
) -> bool:
    required_periods = {
        "walkforward_2022_2023",
        "hypothesis_origin_2024_plus",
    }
    selected = gate_rows.loc[
        gate_rows.get("raw_score_name", pd.Series(dtype="object")).eq(raw_score_name)
        & gate_rows.get("analysis", pd.Series(dtype="object")).eq(
            "raw_shape_pair_gate"
        )
        & gate_rows.get("horizon", pd.Series(dtype="Int64")).eq(20)
    ]
    for near_ring in ("near_high_high_1", "near_high_high_2"):
        pair = selected.loc[selected["ring"].eq(near_ring)]
        passing_periods = set(
            pair.loc[
                pd.to_numeric(pair["positive_date_rate_pct"], errors="coerce").eq(
                    100.0
                ),
                "period_label",
            ].astype(str)
        )
        if required_periods.issubset(passing_periods):
            return True
    return False


def _confirm_oos_interior_shapes(
    summary: pd.DataFrame,
    gate_rows: pd.DataFrame,
) -> pd.DataFrame:
    """Classify raw mountains using the frozen score-level same-near gate."""

    confirmed = summary.copy()
    if confirmed.empty:
        return confirmed
    for raw_score_name in confirmed["raw_score_name"].dropna().unique():
        reproduces = _score_passes_oos_shape_pair_gate(
            gate_rows, str(raw_score_name)
        )
        mask = (
            confirmed["raw_score_name"].eq(raw_score_name)
            & confirmed["horizon"].eq(20)
            & confirmed["period_type"].eq("all_period")
        )
        for _, shape_group in confirmed.loc[mask].groupby(
            ["ring"], observed=True, sort=True
        ):
            expectancy_by_bin = shape_group.set_index("raw_bin")[
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
                ],
                reproduces_core_and_near=reproduces,
                positive_2022_2023=reproduces,
                positive_2024_plus=reproduces,
                severe_loss_not_worse=reproduces,
            )
            confirmed.loc[shape_group.index, "shape_classification"] = shape
    return confirmed


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
    columns = [
        "family",
        "raw_score_name",
        "role",
        "horizon",
        "date",
        "k",
        "candidate_count",
        "candidate_outcome_count",
        "candidate_outcome_coverage_pct",
        "selected_outcome_count",
        "selected_outcome_coverage_pct",
        "outcome_status",
        "eligible_count",
        "selected_count",
        "eligible_mean_excess_return_pct",
        "selected_mean_excess_return_pct",
        "topk_lift_pct",
        "eligible_severe_loss_rate_pct",
        "selected_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
        "eligible_sector_hhi",
        "selected_sector_hhi",
        "turnover_rate",
        "core_high_high_count",
        "near_high_high_1_count",
        "near_high_high_2_count",
    ]
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(rows, columns=columns)
    primary = scored.loc[scored["is_primary"].eq(True)].copy()
    previous_codes: dict[tuple[str, int, int], set[str]] = {}
    for (family, raw_score_name, horizon, signal_date), group in primary.groupby(
        ["family", "raw_score_name", "horizon", "date"], observed=True, sort=True
    ):
        candidates = group.dropna(subset=["technical_fit_score"]).drop_duplicates(
            ["date", "code"]
        )
        for k in (5, 10):
            if len(candidates) < 2 * k:
                continue
            selection = select_frozen_topk(
                candidates,
                score_columns=("technical_fit_score",),
                outcome_column="outcome_pct",
                k=k,
                ascending=(False,),
            )
            eligible = selection.candidates
            selected = selection.selected
            candidate_outcomes = selection.candidate_outcomes
            selected_outcomes = selection.selected_outcomes
            outcome_complete = selection.outcome_status == "complete"
            selected_codes = set(selected["code"].astype(str))
            turnover_key = (str(family), int(str(horizon)), int(k))
            previous = previous_codes.get(turnover_key)
            turnover = (
                float(1.0 - len(selected_codes & previous) / k) if previous else np.nan
            )
            previous_codes[turnover_key] = selected_codes
            ring_counts = selected["ring"].value_counts()
            outcome_metrics = (
                {
                    "eligible_mean_excess_return_pct": float(
                        candidate_outcomes.mean()
                    ),
                    "selected_mean_excess_return_pct": float(
                        selected_outcomes.mean()
                    ),
                    "topk_lift_pct": float(
                        selected_outcomes.mean() - candidate_outcomes.mean()
                    ),
                    "eligible_severe_loss_rate_pct": float(
                        candidate_outcomes.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        * 100.0
                    ),
                    "selected_severe_loss_rate_pct": float(
                        selected_outcomes.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        * 100.0
                    ),
                    "severe_loss_rate_difference_pct": float(
                        (
                            selected_outcomes.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                            - candidate_outcomes.le(
                                DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                            ).mean()
                        )
                        * 100.0
                    ),
                }
                if outcome_complete
                else {
                    metric: float("nan")
                    for metric in (
                        "eligible_mean_excess_return_pct",
                        "selected_mean_excess_return_pct",
                        "topk_lift_pct",
                        "eligible_severe_loss_rate_pct",
                        "selected_severe_loss_rate_pct",
                        "severe_loss_rate_difference_pct",
                    )
                }
            )
            rows.append(
                {
                    "family": family,
                    "raw_score_name": raw_score_name,
                    "role": "primary",
                    "horizon": int(str(horizon)),
                    "date": pd.Timestamp(str(signal_date)),
                    "k": int(k),
                    "candidate_count": selection.candidate_count,
                    "candidate_outcome_count": selection.candidate_outcome_count,
                    "candidate_outcome_coverage_pct": selection.candidate_outcome_coverage_pct,
                    "selected_outcome_count": selection.selected_outcome_count,
                    "selected_outcome_coverage_pct": selection.selected_outcome_coverage_pct,
                    "outcome_status": selection.outcome_status,
                    "eligible_count": int(len(eligible)),
                    "selected_count": int(len(selected)),
                    **outcome_metrics,
                    "eligible_sector_hhi": _sector_hhi(eligible),
                    "selected_sector_hhi": _sector_hhi(selected),
                    "turnover_rate": turnover,
                    "core_high_high_count": int(ring_counts.get("core_high_high", 0)),
                    "near_high_high_1_count": int(
                        ring_counts.get("near_high_high_1", 0)
                    ),
                    "near_high_high_2_count": int(
                        ring_counts.get("near_high_high_2", 0)
                    ),
                }
            )
    return pd.DataFrame(rows, columns=columns)


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
            frame.get("fixed20_ols20_sign_conflict", pd.Series(False, index=index)).eq(
                True
            ),
        ),
        (
            "fixed_ols_conflict",
            "60d_conflict",
            frame.get("fixed60_ols60_sign_conflict", pd.Series(False, index=index)).eq(
                True
            ),
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


def _continuous_ols_spline_rows(
    base: dict[str, object], group: pd.DataFrame
) -> list[dict[str, object]]:
    """Fit the frozen cubic B-spline sensitivity with equal total weight per date."""

    degree = 3
    interior_knots = RAW_BIN_BOUNDARIES[1:-1]
    knots = np.asarray(
        [0.0] * (degree + 1) + list(interior_knots) + [1.0] * (degree + 1),
        dtype=float,
    )
    source = _finite_rows(group, ["raw_level", "outcome_pct"])
    source = source.loc[source["raw_level"].between(0.0, 1.0)].copy()
    status_row = {
        **base,
        "sensitivity_type": "ols_spline_shape",
        "sensitivity_bucket": "continuous_cubic_bspline",
        "observation_count": int(len(source)),
        "date_count": int(source["date"].nunique()) if not source.empty else 0,
        "mean_outcome_pct": (
            float(source["outcome_pct"].mean()) if not source.empty else float("nan")
        ),
        "fit_effect_pct": float("nan"),
        "controls": None,
        "diagnostic_status": "insufficient_evidence",
        "role": "sensitivity_only",
        "spline_degree": degree,
        "spline_knots": ",".join(str(value) for value in interior_knots),
        "spline_raw_level": float("nan"),
        "spline_fitted_outcome_pct": float("nan"),
    }
    basis_count = len(knots) - degree - 1
    if source["raw_level"].nunique() < basis_count:
        return [status_row]
    design = BSpline.design_matrix(
        source["raw_level"].to_numpy(dtype=float),
        knots,
        degree,
    ).toarray()
    date_counts = source.groupby("date", observed=True)["date"].transform("size")
    weights = np.sqrt(1.0 / date_counts.to_numpy(dtype=float))
    weighted_design = design * weights[:, None]
    weighted_outcome = source["outcome_pct"].to_numpy(dtype=float) * weights
    if np.linalg.matrix_rank(weighted_design) < basis_count:
        return [status_row]
    coefficients = np.linalg.lstsq(weighted_design, weighted_outcome, rcond=None)[0]
    grid = np.linspace(0.0, 1.0, 21)
    fitted = BSpline.design_matrix(grid, knots, degree).toarray() @ coefficients
    return [
        {
            **status_row,
            "diagnostic_status": "ready",
            "spline_raw_level": float(raw_level),
            "spline_fitted_outcome_pct": float(fitted_outcome),
        }
        for raw_level, fitted_outcome in zip(grid, fitted, strict=True)
    ]


def _build_diagnostics_df(scored: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(rows)
    primary = scored.loc[scored["is_primary"].eq(True)].copy()
    group_keys = ["family", "raw_score_name", "ring", "horizon"]
    for group_key, group in primary.groupby(group_keys, observed=True, sort=True):
        base: dict[str, object] = dict(zip(group_keys, group_key, strict=True))
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
            rows.extend(_continuous_ols_spline_rows(base, complete))
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
    shape_gate = _build_oos_shape_pair_gate_rows(raw_daily, mapping)
    raw_summary = _confirm_oos_interior_shapes(raw_summary, shape_gate)
    scored = _score_walkforward_observations(
        source,
        mapping,
        horizons=resolved_horizons,
    )
    oos = _build_oos_fit_score_lift_df(scored)
    paired = _build_fixed_vs_ols_paired_df(oos)
    topk = _build_topk_operational_lift_df(scored)
    complete_topk = topk.loc[topk["outcome_status"].eq("complete")].copy()
    diagnostics = _build_diagnostics_df(scored)
    segment, annual = _build_stability_tables(oos, paired, complete_topk)
    if not shape_gate.empty:
        segment = pd.concat([segment, shape_gate], ignore_index=True)
    bootstrap = _build_bootstrap_effect_ci_df(
        oos,
        paired,
        complete_topk,
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
        sufficient = bool(
            valid_evidence and _all_explicit_true(subset["sufficient_sample"])
        )
        passed = bool(sufficient and _all_explicit_true(subset["passes_adoption_gate"]))
        family_rows[family] = (valid_evidence, sufficient, passed)
        result_rows.append(
            {
                "decision_key": family,
                "decision": (
                    "passes_adoption_gate"
                    if passed
                    else "insufficient_evidence"
                    if not sufficient
                    else "fails_adoption_gate"
                    if valid_evidence and sufficient
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
            paired_sufficient = _all_explicit_true(paired_evidence["sufficient_sample"])
            lower = pd.to_numeric(paired_evidence["ci_lower_pct"], errors="coerce")
            upper = pd.to_numeric(paired_evidence["ci_upper_pct"], errors="coerce")
            if (
                not paired_sufficient
                or not np.isfinite(lower).all()
                or not np.isfinite(upper).all()
            ):
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


def _build_ring_registry_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ring": definition.name,
                "predicate": definition.predicate,
                "candidate_selection": "fixed_return_free",
                "role": "primary"
                if definition.name == "core_high_high"
                else "replication",
            }
            for definition in RING_REGISTRY
        ]
    )


def _build_raw_score_registry_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_score_name": definition.name,
                "family": definition.family,
                "is_primary": definition.is_primary,
                "role": "primary" if definition.is_primary else "attribution_only",
            }
            for definition in RAW_SCORE_REGISTRY
        ]
    )


def _build_coverage_attrition_df(observations: pd.DataFrame) -> pd.DataFrame:
    columns = {
        "ring",
        "date",
        "code",
        "market_code",
        "fixed_equal_level",
        "ols_equal_level",
        "forward_close_excess_return_20d_pct",
    }
    if observations.empty or not columns.issubset(observations.columns):
        return _typed_empty_bundle_frame("coverage_attrition")
    rows: list[dict[str, object]] = []
    for ring, group in observations.groupby("ring", observed=True, sort=True):
        rows.append(
            {
                "ring": str(ring),
                "observation_count": int(len(group)),
                "symbol_count": int(group["code"].nunique()),
                "date_count": int(group["date"].nunique()),
                "first_date": pd.to_datetime(group["date"], errors="coerce").min(),
                "last_date": pd.to_datetime(group["date"], errors="coerce").max(),
                "median_candidates_per_date": float(
                    group.groupby("date").size().median()
                ),
                "fixed_equal_coverage_pct": float(
                    group["fixed_equal_level"].notna().mean() * 100.0
                ),
                "ols_equal_coverage_pct": float(
                    group["ols_equal_level"].notna().mean() * 100.0
                ),
                "completed_20d_coverage_pct": float(
                    group["forward_close_excess_return_20d_pct"].notna().mean() * 100.0
                ),
                "market_codes": ",".join(sorted(set(group["market_code"].astype(str)))),
            }
        )
    return pd.DataFrame(rows)


def _family_adoption_evidence(evidence: TechnicalFitEvidenceTables) -> pd.DataFrame:
    """Reduce frozen primary evidence to the two family adoption verdicts."""

    rows: list[dict[str, object]] = []
    for family, raw_score_name in PRIMARY_RAW_SCORE_BY_FAMILY.items():
        oos = evidence.oos_fit_score_lift_df
        required_oos = {
            "family",
            "raw_score_name",
            "ring",
            "horizon",
            "mean_lift_pct",
            "spearman_ic",
            "severe_loss_rate_difference_pct",
        }
        if required_oos.issubset(oos.columns):
            primary = oos.loc[
                oos["family"].eq(family)
                & oos["raw_score_name"].eq(raw_score_name)
                & oos["horizon"].eq(20)
            ].copy()
        else:
            primary = pd.DataFrame()

        bootstrap = evidence.bootstrap_effect_ci_df
        required_bootstrap = {
            "family",
            "raw_score_name",
            "ring",
            "horizon",
            "analysis",
            "ci_lower_pct",
        }
        if required_bootstrap.issubset(bootstrap.columns):
            family_ci = bootstrap.loc[
                bootstrap["family"].eq(family)
                & bootstrap["raw_score_name"].eq(raw_score_name)
                & bootstrap["horizon"].eq(20)
                & bootstrap["analysis"].eq("oos_fit_score_lift")
            ]
        else:
            family_ci = pd.DataFrame()

        stability = evidence.segment_stability_df
        required_stability = {
            "family",
            "raw_score_name",
            "ring",
            "horizon",
            "analysis",
            "period_label",
            "mean_effect_pct",
        }
        if required_stability.issubset(stability.columns):
            family_segments = stability.loc[
                stability["family"].eq(family)
                & stability["raw_score_name"].eq(raw_score_name)
                & stability["horizon"].eq(20)
                & stability["analysis"].eq("oos_fit_score_lift")
            ]
        else:
            family_segments = pd.DataFrame()
        shape_gate_sufficient = False
        shape_gate_passes = False
        if required_stability.issubset(stability.columns):
            family_shape_gate = stability.loc[
                stability["family"].eq(family)
                & stability["raw_score_name"].eq(raw_score_name)
                & stability["horizon"].eq(20)
                & stability["analysis"].eq("raw_shape_pair_gate")
            ]
            required_shape_pairs = {
                (near_ring, period_label)
                for near_ring in ("near_high_high_1", "near_high_high_2")
                for period_label in (
                    "walkforward_2022_2023",
                    "hypothesis_origin_2024_plus",
                )
            }
            actual_shape_pairs = set(
                family_shape_gate[["ring", "period_label"]].itertuples(
                    index=False, name=None
                )
            )
            shape_gate_sufficient = required_shape_pairs.issubset(actual_shape_pairs)
            shape_gate_passes = _score_passes_oos_shape_pair_gate(
                family_shape_gate, raw_score_name
            )

        ring_passes: dict[str, bool] = {}
        ring_sufficient: dict[str, bool] = {}
        ring_mean_lifts: dict[str, float] = {}
        for ring in (definition.name for definition in RING_REGISTRY):
            ring_oos = primary.loc[
                primary.get("ring", pd.Series(dtype="object")).eq(ring)
            ]
            ring_ci = family_ci.loc[
                family_ci.get("ring", pd.Series(dtype="object")).eq(ring)
            ]
            ring_segments = family_segments.loc[
                family_segments.get("ring", pd.Series(dtype="object")).eq(ring)
            ]
            required_periods = {
                "walkforward_2022_2023",
                "hypothesis_origin_2024_plus",
            }
            period_means: dict[str, float] = {}
            for period_label, effect in zip(
                ring_segments.get("period_label", pd.Series(dtype="object")),
                ring_segments.get("mean_effect_pct", pd.Series(dtype="float64")),
                strict=True,
            ):
                numeric_effect = _as_finite_float(effect)
                if numeric_effect is not None:
                    period_means[str(period_label)] = numeric_effect
            mean_lift = (
                float(pd.to_numeric(ring_oos["mean_lift_pct"], errors="coerce").mean())
                if not ring_oos.empty
                else float("nan")
            )
            ring_mean_lifts[ring] = mean_lift
            sufficient = bool(
                not ring_oos.empty
                and not ring_ci.empty
                and required_periods.issubset(period_means)
            )
            ring_sufficient[ring] = sufficient
            ring_passes[ring] = bool(
                sufficient
                and mean_lift >= 0.25
                and pd.to_numeric(ring_ci["ci_lower_pct"], errors="coerce")
                .gt(0.0)
                .all()
                and float(
                    pd.to_numeric(ring_oos["spearman_ic"], errors="coerce").median()
                )
                >= 0.02
                and float(
                    pd.to_numeric(ring_oos["spearman_ic"], errors="coerce")
                    .gt(0.0)
                    .mean()
                    * 100.0
                )
                >= 52.0
                and all(period_means[period] > 0.0 for period in required_periods)
                and float(
                    pd.to_numeric(
                        ring_oos["severe_loss_rate_difference_pct"], errors="coerce"
                    ).mean()
                )
                <= 1.0
            )

        near_rings = ("near_high_high_1", "near_high_high_2")
        replicated_near = any(ring_passes[ring] for ring in near_rings)
        remaining_near_acceptable = all(
            ring_passes[ring]
            or not ring_sufficient[ring]
            or (np.isfinite(ring_mean_lifts[ring]) and ring_mean_lifts[ring] >= 0.0)
            for ring in near_rings
        )
        topk = evidence.topk_operational_lift_df
        if "outcome_status" in topk:
            topk = topk.loc[topk["outcome_status"].eq("complete")].copy()
        required_topk = {"family", "raw_score_name", "horizon", "k", "topk_lift_pct"}
        if required_topk.issubset(topk.columns):
            family_topk = topk.loc[
                topk["family"].eq(family)
                & topk["raw_score_name"].eq(raw_score_name)
                & topk["horizon"].eq(20)
            ]
            topk_means = family_topk.groupby("k", observed=True)["topk_lift_pct"].mean()
        else:
            topk_means = pd.Series(dtype="float64")
        topk_sufficient = {5, 10}.issubset(set(topk_means.index.astype(int)))
        topk_positive = bool(
            topk_sufficient and all(float(topk_means.loc[k]) > 0.0 for k in (5, 10))
        )
        sufficient_sample = bool(
            ring_sufficient["core_high_high"]
            and any(ring_sufficient[ring] for ring in near_rings)
            and topk_sufficient
            and shape_gate_sufficient
        )
        rows.append(
            {
                "family": family,
                "raw_score_name": raw_score_name,
                "passes_adoption_gate": bool(
                    sufficient_sample
                    and ring_passes["core_high_high"]
                    and replicated_near
                    and remaining_near_acceptable
                    and topk_positive
                    and shape_gate_passes
                ),
                "sufficient_sample": sufficient_sample,
            }
        )
    return pd.DataFrame(rows)


def _build_result_decision_gate(
    evidence: TechnicalFitEvidenceTables,
) -> pd.DataFrame:
    bootstrap = evidence.bootstrap_effect_ci_df
    required = {"analysis", "horizon", "ci_lower_pct", "ci_upper_pct"}
    if required.issubset(bootstrap.columns):
        paired = bootstrap.loc[
            bootstrap["analysis"].eq("fixed_vs_ols_paired")
            & bootstrap["horizon"].eq(20)
        ].copy()
        paired["sufficient_sample"] = True
    else:
        paired = pd.DataFrame(
            columns=("sufficient_sample", "ci_lower_pct", "ci_upper_pct")
        )
    return build_decision_gate_df(_family_adoption_evidence(evidence), paired)


_BUNDLE_EMPTY_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "ring_registry": (
        ("ring", "string"),
        ("predicate", "string"),
        ("candidate_selection", "string"),
        ("role", "string"),
    ),
    "raw_score_registry": (
        ("raw_score_name", "string"),
        ("family", "string"),
        ("is_primary", "bool"),
        ("role", "string"),
    ),
    "coverage_attrition": (
        ("ring", "string"),
        ("observation_count", "Int64"),
        ("symbol_count", "Int64"),
        ("date_count", "Int64"),
        ("first_date", "datetime64[ns]"),
        ("last_date", "datetime64[ns]"),
        ("median_candidates_per_date", "float64"),
        ("fixed_equal_coverage_pct", "float64"),
        ("ols_equal_coverage_pct", "float64"),
        ("completed_20d_coverage_pct", "float64"),
        ("market_codes", "string"),
    ),
    "raw_shape_daily": (
        ("raw_score_name", "string"),
        ("family", "string"),
        ("is_primary", "bool"),
        ("role", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("date", "datetime64[ns]"),
        ("year", "Int64"),
        ("segment", "string"),
        ("raw_bin", "string"),
        ("code_count", "Int64"),
        ("mean_excess_return_pct", "float64"),
        ("median_excess_return_pct", "float64"),
        ("win_rate_pct", "float64"),
        ("p10_excess_return_pct", "float64"),
        ("p25_excess_return_pct", "float64"),
        ("severe_loss_rate_pct", "float64"),
    ),
    "raw_shape_summary": (
        ("raw_score_name", "string"),
        ("family", "string"),
        ("is_primary", "bool"),
        ("role", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("raw_bin", "string"),
        ("period_type", "string"),
        ("period_label", "string"),
        ("date_count", "Int64"),
        ("observation_count", "Int64"),
        ("date_equal_mean_excess_return_pct", "float64"),
        ("date_equal_median_excess_return_pct", "float64"),
        ("date_equal_win_rate_pct", "float64"),
        ("date_equal_p10_excess_return_pct", "float64"),
        ("date_equal_p25_excess_return_pct", "float64"),
        ("date_equal_severe_loss_rate_pct", "float64"),
        ("shape_classification", "string"),
    ),
    "walkforward_mapping": (
        ("raw_score_name", "string"),
        ("evaluation_year", "Int64"),
        ("raw_bin", "string"),
        ("bin_lower", "float64"),
        ("bin_upper", "float64"),
        ("bin_center", "float64"),
        ("observation_count", "Int64"),
        ("signal_date_count", "Int64"),
        ("expectancy_pct", "float64"),
        ("technical_fit_score", "float64"),
        ("mapping_status", "string"),
        ("shape_classification", "string"),
        ("training_start_date", "datetime64[ns]"),
        ("training_end_date", "datetime64[ns]"),
        ("training_completion_end_date", "datetime64[ns]"),
        ("family", "string"),
        ("is_primary", "bool"),
        ("role", "string"),
    ),
    "oos_fit_score_lift": (
        ("raw_score_name", "string"),
        ("family", "string"),
        ("is_primary", "bool"),
        ("role", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("date", "datetime64[ns]"),
        ("candidate_count", "Int64"),
        ("top_count", "Int64"),
        ("bottom_count", "Int64"),
        ("top_mean_excess_return_pct", "float64"),
        ("bottom_mean_excess_return_pct", "float64"),
        ("mean_lift_pct", "float64"),
        ("top_median_excess_return_pct", "float64"),
        ("bottom_median_excess_return_pct", "float64"),
        ("median_lift_pct", "float64"),
        ("spearman_ic", "float64"),
        ("top_win_rate_pct", "float64"),
        ("bottom_win_rate_pct", "float64"),
        ("top_p10_pct", "float64"),
        ("bottom_p10_pct", "float64"),
        ("top_p25_pct", "float64"),
        ("bottom_p25_pct", "float64"),
        ("severe_loss_rate_difference_pct", "float64"),
        ("top_fixed20_negative_share_pct", "float64"),
        ("bottom_fixed20_negative_share_pct", "float64"),
        ("top_overheat_share_pct", "float64"),
        ("bottom_overheat_share_pct", "float64"),
        ("top_sector_hhi", "float64"),
        ("bottom_sector_hhi", "float64"),
    ),
    "fixed_vs_ols_paired": (
        ("ring", "string"),
        ("horizon", "Int64"),
        ("date", "datetime64[ns]"),
        ("fixed_date", "datetime64[ns]"),
        ("ols_date", "datetime64[ns]"),
        ("fixed_raw_score_name", "string"),
        ("ols_raw_score_name", "string"),
        ("fixed_mean_lift_pct", "float64"),
        ("ols_mean_lift_pct", "float64"),
        ("fixed_minus_ols_lift_pct", "float64"),
        ("sufficient_sample", "bool"),
    ),
    "topk_operational_lift": (
        ("family", "string"),
        ("raw_score_name", "string"),
        ("role", "string"),
        ("horizon", "Int64"),
        ("date", "datetime64[ns]"),
        ("k", "Int64"),
        ("candidate_count", "Int64"),
        ("candidate_outcome_count", "Int64"),
        ("candidate_outcome_coverage_pct", "float64"),
        ("selected_outcome_count", "Int64"),
        ("selected_outcome_coverage_pct", "float64"),
        ("outcome_status", "string"),
        ("eligible_count", "Int64"),
        ("selected_count", "Int64"),
        ("eligible_mean_excess_return_pct", "float64"),
        ("selected_mean_excess_return_pct", "float64"),
        ("topk_lift_pct", "float64"),
        ("eligible_severe_loss_rate_pct", "float64"),
        ("selected_severe_loss_rate_pct", "float64"),
        ("severe_loss_rate_difference_pct", "float64"),
        ("eligible_sector_hhi", "float64"),
        ("selected_sector_hhi", "float64"),
        ("turnover_rate", "float64"),
        ("core_high_high_count", "Int64"),
        ("near_high_high_1_count", "Int64"),
        ("near_high_high_2_count", "Int64"),
    ),
    "overheat_negative_diagnostics": (
        ("family", "string"),
        ("raw_score_name", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("sensitivity_type", "string"),
        ("sensitivity_bucket", "string"),
        ("observation_count", "Int64"),
        ("date_count", "Int64"),
        ("mean_outcome_pct", "float64"),
        ("fit_effect_pct", "float64"),
        ("controls", "string"),
        ("diagnostic_status", "string"),
        ("role", "string"),
        ("spline_degree", "float64"),
        ("spline_knots", "string"),
        ("spline_raw_level", "float64"),
        ("spline_fitted_outcome_pct", "float64"),
    ),
    "segment_stability": (
        ("family", "string"),
        ("raw_score_name", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("analysis", "string"),
        ("period_label", "string"),
        ("date_count", "Int64"),
        ("mean_effect_pct", "float64"),
        ("median_effect_pct", "float64"),
        ("positive_date_rate_pct", "float64"),
        ("k", "float64"),
    ),
    "annual_stability": (
        ("family", "string"),
        ("raw_score_name", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("analysis", "string"),
        ("period_label", "string"),
        ("date_count", "Int64"),
        ("mean_effect_pct", "float64"),
        ("median_effect_pct", "float64"),
        ("positive_date_rate_pct", "float64"),
        ("k", "float64"),
    ),
    "bootstrap_effect_ci": (
        ("family", "string"),
        ("raw_score_name", "string"),
        ("ring", "string"),
        ("horizon", "Int64"),
        ("analysis", "string"),
        ("date_count", "Int64"),
        ("block_length", "Int64"),
        ("resamples", "Int64"),
        ("seed", "Int64"),
        ("point_estimate_pct", "float64"),
        ("ci_lower_pct", "float64"),
        ("ci_upper_pct", "float64"),
        ("k", "float64"),
    ),
    "decision_gate": (
        ("decision_key", "string"),
        ("decision", "string"),
        ("sufficient_sample", "bool"),
        ("passed", "bool"),
    ),
    "observation_sample": (
        ("date", "datetime64[ns]"),
        ("code", "string"),
        ("market_scope", "string"),
        ("market_code", "string"),
        ("ring", "string"),
        ("company_name", "string"),
        ("sector_33_code", "string"),
        ("sector_33_name", "string"),
        ("value_composite_equal_score", "float64"),
        ("long_hybrid_leadership_score", "float64"),
        ("liquidity_residual_z", "float64"),
        ("atr20_pct", "float64"),
        ("atr20_change_20d_pct", "float64"),
        ("recent_return_20d_pct", "float64"),
        ("recent_return_60d_pct", "float64"),
        ("fixed20_level", "float64"),
        ("fixed60_level", "float64"),
        ("fixed_equal_level", "float64"),
        ("ols_move_20d_pct", "float64"),
        ("ols_move_60d_pct", "float64"),
        ("ols20_level", "float64"),
        ("ols60_level", "float64"),
        ("ols_equal_level", "float64"),
        ("ols_r2_20", "float64"),
        ("ols_r2_60", "float64"),
        ("ols20_minus_ols60_move_pct", "float64"),
        ("fixed20_ols20_sign_conflict", "bool"),
        ("fixed60_ols60_sign_conflict", "bool"),
        ("fixed20_negative_flag", "bool"),
        ("fixed60_negative_flag", "bool"),
        ("fixed20_overheat_flag", "bool"),
    ),
}


def _bundle_table_schema(
    table_name: str,
    *,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
) -> tuple[tuple[str, str], ...]:
    schema = _BUNDLE_EMPTY_SCHEMAS[table_name]
    if table_name != "observation_sample":
        return schema
    outcome_schema = tuple(
        column
        for horizon in tuple(sorted({int(item) for item in horizons}))
        for column in (
            (f"forward_outcome_completion_date_{horizon}d", "datetime64[ns]"),
            (f"forward_close_return_{horizon}d_pct", "float64"),
            (f"forward_close_excess_return_{horizon}d_pct", "float64"),
            (f"forward_close_n225_excess_return_{horizon}d_pct", "float64"),
        )
    )
    return (*schema, *outcome_schema)


def _typed_empty_bundle_frame(
    table_name: str,
    *,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
) -> pd.DataFrame:
    schema = _bundle_table_schema(table_name, horizons=horizons)
    return pd.DataFrame({column: pd.Series(dtype=dtype) for column, dtype in schema})


def _bundle_frame(
    table_name: str,
    frame: pd.DataFrame,
    *,
    horizons: Iterable[int],
) -> pd.DataFrame:
    if len(frame.columns) > 0:
        return frame
    return _typed_empty_bundle_frame(table_name, horizons=horizons)


def _validate_bundle_table_contract(
    tables: dict[str, pd.DataFrame],
    *,
    horizons: Iterable[int],
) -> None:
    if tuple(tables) != BUNDLE_TABLE_ORDER:
        raise RuntimeError(
            "technical fit score bundle table contract drift: "
            f"expected tables {BUNDLE_TABLE_ORDER}, received {tuple(tables)}"
        )
    for table_name, frame in tables.items():
        expected_columns = tuple(
            column
            for column, _dtype in _bundle_table_schema(
                table_name,
                horizons=horizons,
            )
        )
        actual_columns = tuple(str(column) for column in frame.columns)
        if actual_columns != expected_columns:
            raise RuntimeError(
                "technical fit score bundle column contract drift for "
                f"{table_name}: expected {expected_columns}, received {actual_columns}"
            )


def write_ranking_technical_fit_score_shape_evidence_bundle(
    result: RankingTechnicalFitScoreShapeEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    """Persist the frozen fifteen-table research contract."""

    tables = {
        "ring_registry": result.ring_registry_df,
        "raw_score_registry": result.raw_score_registry_df,
        "coverage_attrition": result.coverage_attrition_df,
        "raw_shape_daily": result.raw_shape_daily_df,
        "raw_shape_summary": result.raw_shape_summary_df,
        "walkforward_mapping": result.walkforward_mapping_df,
        "oos_fit_score_lift": result.oos_fit_score_lift_df,
        "fixed_vs_ols_paired": result.fixed_vs_ols_paired_df,
        "topk_operational_lift": result.topk_operational_lift_df,
        "overheat_negative_diagnostics": result.overheat_negative_diagnostics_df,
        "segment_stability": result.segment_stability_df,
        "annual_stability": result.annual_stability_df,
        "bootstrap_effect_ci": result.bootstrap_effect_ci_df,
        "decision_gate": result.decision_gate_df,
        "observation_sample": result.observation_sample_df,
    }
    if tuple(tables) != BUNDLE_TABLE_ORDER or set(tables) != REQUIRED_BUNDLE_TABLES:
        raise RuntimeError("technical fit score bundle table contract drift")
    typed_tables = {
        name: _bundle_frame(name, frame, horizons=result.horizons)
        for name, frame in tables.items()
    }
    _validate_bundle_table_contract(typed_tables, horizons=result.horizons)
    return write_research_bundle(
        experiment_id=RANKING_TECHNICAL_FIT_SCORE_SHAPE_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_technical_fit_score_shape_evidence",
        function="run_ranking_technical_fit_score_shape_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scope": "prime",
            "market_codes": sorted(PRIME_EQUIVALENT_MARKET_CODES),
            "candidate_rings": "fixed_return_free",
            "min_training_observations": result.min_training_observations,
            "min_training_dates": result.min_training_dates,
            "bootstrap_resamples": result.bootstrap_resamples,
            "bootstrap_seed": result.bootstrap_seed,
            "observation_sample_limit": result.observation_sample_limit,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "feature_timing": "after_close",
            "candidate_selection": "fixed_return_free",
            "walkforward_training_timing": (
                "completed_outcomes_strictly_before_evaluation_year"
            ),
            "first_training_year": FIRST_TRAINING_YEAR,
            "first_evaluation_year": FIRST_EVALUATION_YEAR,
            "primary_horizon": 20,
            "pit_lineage": result.pit_lineage.to_manifest_payload(),
        },
        result_tables=typed_tables,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def _summary_table(frame: pd.DataFrame, *, limit: int) -> str:
    if frame.empty:
        return "_該当する evidence はありません。_"
    return _top_rows_for_markdown(frame, limit=limit)


def build_summary_markdown(
    result: RankingTechnicalFitScoreShapeEvidenceResult,
) -> str:
    """Build a Japanese decision-first bundle summary from the frozen gate."""

    final_rows = result.decision_gate_df.loc[
        result.decision_gate_df.get("decision_key", pd.Series(dtype="object")).eq(
            "fixed_vs_ols"
        )
    ]
    final_decision = (
        str(final_rows["decision"].iloc[0])
        if not final_rows.empty and "decision" in final_rows.columns
        else "insufficient_evidence"
    )
    decision_explanations = {
        "fixed_wins": "fixed 20D/60D endpoint return ベースを採用候補とする。",
        "ols_wins": "OLS fitted move ベースを採用候補とする。",
        "equivalent_fixed_preferred_operationally": (
            "統計的には同等であり、運用上の単純性から fixed を優先する。"
        ),
        "neither": "どちらの Technical Fit Score も Ranking へ導入しない。",
        "insufficient_evidence": "必要な比較 coverage が不足しており導入判断を保留する。",
    }
    price_projection = result.pit_lineage.price_projection
    price_projection_lines = (
        [
            "- Physical price source: `stock_data_raw`; `stock_data` fallback なし。",
            f"- Signal price policy: `{price_projection.signal_basis_policy}`。",
            f"- Outcome price policy: `{price_projection.completion_basis_policy}`。",
            "- Price projection audit: signal rows "
            f"`{price_projection.signal_feature_row_count}` / signal basis "
            f"`{price_projection.signal_basis_row_count}` / signal segments "
            f"`{price_projection.signal_segment_row_count}` / completed outcomes "
            f"`{price_projection.completed_outcome_row_count}` / completion basis "
            f"`{price_projection.completion_basis_row_count}` / completion segments "
            f"`{price_projection.completion_segment_row_count}`。",
            f"- Price projection SHA-256: `{price_projection.price_projection_sha256}`。",
        ]
        if price_projection is not None
        else ["- Price projection audit: `missing`。"]
    )
    shape_rows = result.raw_shape_summary_df
    shape_columns = {
        "family",
        "raw_score_name",
        "ring",
        "horizon",
        "period_type",
        "shape_classification",
    }
    shape_summary_lines: list[str] = []
    for family, raw_score_name in PRIMARY_RAW_SCORE_BY_FAMILY.items():
        for ring in (definition.name for definition in RING_REGISTRY):
            if shape_columns.issubset(shape_rows.columns):
                matching = shape_rows.loc[
                    shape_rows["family"].eq(family)
                    & shape_rows["raw_score_name"].eq(raw_score_name)
                    & shape_rows["ring"].eq(ring)
                    & shape_rows["horizon"].eq(20)
                    & shape_rows["period_type"].eq("all_period")
                ]
                classifications = sorted(
                    set(matching["shape_classification"].dropna().astype(str))
                )
            else:
                classifications = []
            classification = (
                ",".join(classifications)
                if classifications
                else "insufficient_evidence"
            )
            shape_summary_lines.append(
                f"  - family=`{family}` / ring=`{ring}`: "
                f"shape_classification=`{classification}`。"
            )
    parts = [
        "# Ranking Technical Fit Score Shape Evidence",
        "",
        "## 結論",
        "",
        f"- 最終判断: `{final_decision}` — {decision_explanations[final_decision]}",
        "- 20D primary response shape（family / ring 別）:",
        *shape_summary_lines,
        "- `20D<0` と fixed `20D>=30%` overheat は診断のみで、candidate ring や primary gate を変更しない。",
        "- 本 score はシグナル日の終値確定後にのみ利用可能で、portfolio backtest の結論ではない。",
        "",
        "## 前提と再現条件",
        "",
        "- universe: exact signal-date Prime (`0101`, `0111`) のみ。",
        "- candidate rings: `fixed_return_free`。Value Score と Long Hybrid Score だけで先に凍結。",
        "- walk-forward: 各評価年より前に完了済み outcome のみを学習へ使用し、mapping fallback は行わない。",
        f"- analysis dates: `{result.analysis_start_date}` から `{result.analysis_end_date}`。",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`。",
        f"- bootstrap: `{result.bootstrap_resamples}` resamples / seed `{result.bootstrap_seed}`。",
        f"- training minimums: `{result.min_training_observations}` observations / `{result.min_training_dates}` dates per bin。",
        f"- observation count: `{result.observation_count}`。",
        "",
        "## PIT Lineage",
        "",
        "- Data plane: physical `market.duckdb` schema v4。",
        "- Adjustment mode: "
        f"`stock_price_adjustment_mode={result.pit_lineage.stock_price_adjustment_mode}`。",
        "- Universe source: `stock_master_daily` exact signal-date membership。latest/current fallback なし。",
        "- Basis-dependent sources: `daily_valuation`, `stock_data_raw`。service-local recomputation / fallback なし。",
        *price_projection_lines,
        "- Event-time basis verification: consumed Prime rows "
        f"`{result.pit_lineage.consumed_daily_valuation_row_count}` / basis IDs "
        f"`{len(result.pit_lineage.basis_ids)}` / basis rows "
        f"`{result.pit_lineage.verified_basis_row_count}` / segment rows "
        f"`{result.pit_lineage.verified_segment_row_count}`。",
        "- Catalog verification: cutoff-valid `basis_id` を "
        "`stock_adjustment_bases` と `stock_adjustment_basis_segments` に照合済み。",
        "- Exact basis IDs: `manifest.json.result_metadata.pit_lineage.basis_ids`。",
        f"- basis_id SHA-256: `{result.pit_lineage.basis_id_sha256}`。",
        "- Invalidation disposition: "
        f"`{result.pit_lineage.invalidation_disposition}`。",
        "",
        "## Decision Gate",
        "",
        _summary_table(result.decision_gate_df, limit=10),
        "",
        "## Ring Coverage",
        "",
        _summary_table(result.coverage_attrition_df, limit=20),
        "",
        "## Raw Shape Summary",
        "",
        _summary_table(result.raw_shape_summary_df, limit=120),
        "",
        "## Walk-Forward Mapping",
        "",
        _summary_table(result.walkforward_mapping_df, limit=120),
        "",
        "## OOS Fit Score Lift",
        "",
        _summary_table(result.oos_fit_score_lift_df, limit=120),
        "",
        "## Fixed vs OLS Paired",
        "",
        _summary_table(result.fixed_vs_ols_paired_df, limit=120),
        "",
        "## Top-K Operational Lift",
        "",
        _summary_table(result.topk_operational_lift_df, limit=120),
    ]
    return "\n".join(parts).rstrip() + "\n"
