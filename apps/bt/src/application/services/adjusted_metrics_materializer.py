"""Reconcile event-time adjusted fundamentals and valuation materializations."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
import math
from typing import Any, TypeVar

from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    AdjustedStatementMetric,
    DailyValuationInput,
    ForwardEpsSource,
    build_adjusted_statement_metric,
    build_daily_valuation_metric,
)
from src.domains.fundamentals.adjustment_basis import (
    RawAdjustmentPoint,
    StockAdjustmentBasis,
    StockAdjustmentBasisSegment,
    StockAdjustmentLineage,
    build_stock_adjustment_lineage,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import (
    normalize_stock_code,
    stock_code_query_candidates,
)
from src.infrastructure.db.market.valuation_writers import (
    AdjustedBasisMaterializationPlan,
    BasisSnapshot,
    FrontierExtensionBasisPlan,
    NoOpBasisPlan,
    StructuralBasisPlan,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


_T = TypeVar("_T")


@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    completed_codes: int
    total_codes: int
    basis_count: int
    published_basis_count: int = field(compare=False)
    ready_basis_count: int
    statement_rows: int
    daily_valuation_rows: int
    daily_technical_metric_rows: int
    daily_valuation_latest_date: str | None
    active_price_basis_date: str | None
    active_basis_version: str | None
    plan_counts: dict[str, int] = field(default_factory=dict, compare=False)
    mutation_stats: dict[str, MarketMutationStats] = field(
        default_factory=dict, compare=False
    )
    final_semantic_counts: dict[str, int] = field(default_factory=dict, compare=False)


class AdjustmentLineageReconstructionError(RuntimeError):
    """Raised when retained event-time bases cannot be rebuilt from raw lineage."""

    def __init__(self, code: str, missing_basis_ids: Iterable[str]) -> None:
        self.code = normalize_stock_code(code)
        self.missing_basis_ids = tuple(sorted(set(missing_basis_ids)))
        super().__init__(
            f"cannot reconstruct retained adjustment bases for code {self.code}: "
            f"{', '.join(self.missing_basis_ids)}"
        )


class AdjustmentFrontierRegressionError(RuntimeError):
    """Raised when desired coverage would move a retained basis backwards."""


class AdjustedMetricsMaterializer:
    """Build every source-derived adjustment regime and publish changed bases."""

    def __init__(self, market_db: MarketDb) -> None:
        self._market_db = market_db

    def rebuild_all(self) -> AdjustedMetricsBuildResult:
        return self.reconcile(codes=None)

    def rebuild_codes(self, codes: list[str]) -> AdjustedMetricsBuildResult:
        normalized = sorted({normalize_stock_code(code) for code in codes if code})
        return self.reconcile(codes=normalized)

    def reconcile(
        self,
        codes: list[str] | None = None,
        *,
        cancel_requested: Callable[[], bool] | None = None,
        on_progress: Callable[[int, int, str | None, int], None] | None = None,
    ) -> AdjustedMetricsBuildResult:
        target_codes = self._target_codes(codes)
        market_sessions = self._market_sessions()
        completed_codes = 0
        basis_count = 0
        published_basis_count = 0
        ready_basis_count = 0
        statement_rows = 0
        daily_valuation_rows = 0
        daily_valuation_latest_date: str | None = None
        active_price_basis_date: str | None = None
        active_basis_version: str | None = None
        plan_counts = {"structural": 0, "frontier_extension": 0, "no_op": 0}
        mutation_stats = {
            relation: MarketMutationStats.empty()
            for relation in ("basis", "segments", "statements", "valuations")
        }
        final_semantic_counts = {
            relation: 0
            for relation in ("basis", "segments", "statements", "valuations")
        }
        for code in target_codes:
            if cancel_requested is not None and cancel_requested():
                break
            if on_progress is not None:
                on_progress(
                    completed_codes,
                    len(target_codes),
                    code,
                    published_basis_count,
                )
            result = self.reconcile_code(code, market_sessions)
            completed_codes += result.completed_codes
            basis_count += result.basis_count
            published_basis_count += result.published_basis_count
            ready_basis_count += result.ready_basis_count
            statement_rows += result.statement_rows
            daily_valuation_rows += result.daily_valuation_rows
            plan_counts = _add_counts(plan_counts, result.plan_counts)
            mutation_stats = _add_mutation_stats(mutation_stats, result.mutation_stats)
            final_semantic_counts = _add_counts(
                final_semantic_counts, result.final_semantic_counts
            )
            daily_valuation_latest_date = max(
                filter(
                    None,
                    (
                        daily_valuation_latest_date,
                        result.daily_valuation_latest_date,
                    ),
                ),
                default=None,
            )
            if (result.active_price_basis_date or "", result.active_basis_version or "") > (
                active_price_basis_date or "",
                active_basis_version or "",
            ):
                active_price_basis_date = result.active_price_basis_date
                active_basis_version = result.active_basis_version
            if on_progress is not None:
                on_progress(
                    completed_codes,
                    len(target_codes),
                    code,
                    published_basis_count,
                )

        technical_rows = (
            self._market_db.rebuild_daily_technical_metrics_from_stock_data()
            if (
                (cancel_requested is None or not cancel_requested())
                and codes is None
                and self._market_db._table_exists("stock_data")
            )
            else 0
        )
        return AdjustedMetricsBuildResult(
            completed_codes=completed_codes,
            total_codes=len(target_codes),
            basis_count=basis_count,
            published_basis_count=published_basis_count,
            ready_basis_count=ready_basis_count,
            statement_rows=statement_rows,
            daily_valuation_rows=daily_valuation_rows,
            daily_technical_metric_rows=technical_rows,
            daily_valuation_latest_date=daily_valuation_latest_date,
            active_price_basis_date=active_price_basis_date,
            active_basis_version=active_basis_version,
            plan_counts=plan_counts,
            mutation_stats=mutation_stats,
            final_semantic_counts=final_semantic_counts,
        )

    def reconcile_code(
        self,
        code: str,
        market_sessions: Sequence[str],
        *,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> AdjustedMetricsBuildResult:
        normalized_code = normalize_stock_code(code)
        if cancel_requested is not None and cancel_requested():
            return _empty_build_result(total_codes=1)

        requested_codes = [normalized_code]
        points = _group_raw_points(
            self._market_db.load_raw_adjustment_points(requested_codes),
            requested_codes,
        ).get(normalized_code, ())
        lineage = build_stock_adjustment_lineage(
            normalized_code,
            points,
            market_sessions=market_sessions,
        )
        existing_snapshots = self._market_db.load_basis_snapshots(normalized_code)
        existing_catalog = {
            basis_id: snapshot.basis
            for basis_id, snapshot in existing_snapshots.items()
        }
        rebuilt_ids = {basis.basis_id for basis in lineage.bases}
        missing_basis_ids = set(existing_catalog) - rebuilt_ids
        if missing_basis_ids:
            raise AdjustmentLineageReconstructionError(
                normalized_code,
                missing_basis_ids,
            )

        statements = self._load_statement_rows(requested_codes)
        prices = self._load_raw_price_rows(requested_codes)
        point_list = list(points)
        segments_by_basis = _segments_by_basis(lineage.segments)
        statement_rows: list[dict[str, Any]] = []
        valuation_rows: list[dict[str, Any]] = []
        generated_by_basis: dict[
            str, tuple[list[dict[str, Any]], list[dict[str, Any]]]
        ] = {}
        for basis in lineage.bases:
            if basis.status != "ready":
                continue
            basis_statements = self._build_statement_rows(
                basis,
                statements,
                point_list,
            )
            basis_valuations = self._build_valuation_rows(
                basis,
                segments_by_basis.get(basis.basis_id, []),
                statements,
                prices,
                basis_statements,
            )
            statement_rows.extend(basis_statements)
            valuation_rows.extend(basis_valuations)
            generated_by_basis[basis.basis_id] = (
                basis_statements,
                basis_valuations,
            )

        basis_plans = []
        for basis in lineage.bases:
            basis_statements, basis_valuations = generated_by_basis.get(
                basis.basis_id, ([], [])
            )
            basis_plans.append(
                _plan_basis_materialization(
                    basis,
                    tuple(
                        segment
                        for segment in lineage.segments
                        if segment.basis_id == basis.basis_id
                    ),
                    basis_statements,
                    basis_valuations,
                    existing_snapshots.get(basis.basis_id),
                )
            )
        actionable_plans = tuple(plan for plan in basis_plans if plan.kind != "no_op")
        publish_result = None
        if actionable_plans:
            publish_result = self._market_db.publish_adjusted_basis_materialization(
                AdjustedBasisMaterializationPlan(
                    plans=actionable_plans,
                )
            )

        active_basis = _active_basis(lineage.bases)
        ready_bases = [basis for basis in lineage.bases if basis.status == "ready"]
        return AdjustedMetricsBuildResult(
            completed_codes=1,
            total_codes=1,
            basis_count=len(lineage.bases),
            published_basis_count=len(actionable_plans),
            ready_basis_count=len(ready_bases),
            statement_rows=len(statement_rows),
            daily_valuation_rows=len(valuation_rows),
            daily_technical_metric_rows=0,
            daily_valuation_latest_date=max(
                (str(row["date"]) for row in valuation_rows),
                default=None,
            ),
            active_price_basis_date=(
                active_basis.adjustment_through_date if active_basis else None
            ),
            active_basis_version=active_basis.basis_id if active_basis else None,
            plan_counts={
                kind: sum(plan.kind == kind for plan in basis_plans)
                for kind in ("structural", "frontier_extension", "no_op")
            },
            mutation_stats=(
                {
                    "basis": publish_result.basis.stats,
                    "segments": publish_result.segments.stats,
                    "statements": publish_result.statements.stats,
                    "valuations": publish_result.valuations.stats,
                }
                if publish_result is not None
                else {
                    relation: MarketMutationStats.empty()
                    for relation in ("basis", "segments", "statements", "valuations")
                }
            ),
            final_semantic_counts={
                "basis": len(lineage.bases),
                "segments": len(lineage.segments),
                "statements": len(statement_rows),
                "valuations": len(valuation_rows),
            },
        )

    def _market_sessions(self) -> tuple[str, ...]:
        if not self._market_db._table_exists("topix_data"):
            return ()
        return tuple(
            str(row[0])
            for row in self._market_db._fetchall(
                "SELECT date FROM topix_data WHERE date IS NOT NULL ORDER BY date"
            )
        )

    def _target_codes(self, codes: list[str] | None) -> list[str]:
        if codes is not None:
            return sorted({normalize_stock_code(code) for code in codes if code})
        return self._market_db.list_adjustment_materialization_codes()

    def _load_statement_rows(self, codes: list[str]) -> list[dict[str, Any]]:
        if not codes or not self._market_db._table_exists("statements"):
            return []
        query_codes = stock_code_query_candidates(codes)
        placeholders = ", ".join("?" for _ in query_codes)
        return self._market_db._fetchall_dicts(
            f"""
            WITH source AS (
                SELECT *
                FROM statements
                WHERE code IN ({placeholders})
            ),
            normalized AS (
                SELECT *,
                    CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                         THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY CASE
                            WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                            THEN left(code, length(code) - 1) ELSE code END,
                            disclosed_date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS alias_rank
                FROM source
            )
            SELECT * EXCLUDE (code, alias_rank), normalized_code AS code
            FROM normalized
            WHERE alias_rank = 1
            ORDER BY normalized_code, disclosed_date
            """,
            list(query_codes),
        )

    def _load_raw_price_rows(self, codes: list[str]) -> list[dict[str, Any]]:
        if not codes:
            return []
        query_codes = stock_code_query_candidates(codes)
        placeholders = ", ".join("?" for _ in query_codes)
        return self._market_db._fetchall_dicts(
            f"""
            WITH source AS (
                SELECT *
                FROM stock_data_raw
                WHERE code IN ({placeholders})
            ),
            normalized AS (
                SELECT *,
                    CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                         THEN left(code, length(code) - 1) ELSE code END AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY CASE
                            WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                            THEN left(code, length(code) - 1) ELSE code END,
                            date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS alias_rank
                FROM source
            )
            SELECT normalized_code AS code, date, open, high, low, close, volume,
                   adjustment_factor
            FROM normalized
            WHERE alias_rank = 1
              AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
              AND close IS NOT NULL AND volume IS NOT NULL
            ORDER BY normalized_code, date
            """,
            list(query_codes),
        )

    def _build_statement_rows(
        self,
        basis: StockAdjustmentBasis,
        statements: list[dict[str, Any]],
        points: list[RawAdjustmentPoint],
    ) -> list[dict[str, Any]]:
        events = [
            ShareAdjustmentEvent(point.date, float(point.adjustment_factor))
            for point in points
            if point.date <= basis.adjustment_through_date
            and point.adjustment_factor is not None
            and math.isfinite(float(point.adjustment_factor))
            and float(point.adjustment_factor) > 0
            and float(point.adjustment_factor) != 1.0
        ]
        rows: list[dict[str, Any]] = []
        for row in statements:
            disclosed = str(row["disclosed_date"])
            if basis.valid_to_exclusive is not None and disclosed >= basis.valid_to_exclusive:
                continue
            metric = build_adjusted_statement_metric(
                _statement_input(row),
                events=events,
                price_basis_date=basis.adjustment_through_date,
                basis_version=basis.basis_id,
            )
            rows.append(_statement_metric_row(metric))
        return rows

    def _build_valuation_rows(
        self,
        basis: StockAdjustmentBasis,
        segments: list[StockAdjustmentBasisSegment],
        statements: list[dict[str, Any]],
        prices: list[dict[str, Any]],
        adjusted_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        statement_by_date = {str(row["disclosed_date"]): row for row in statements}
        metrics = sorted(adjusted_rows, key=lambda row: str(row["disclosed_date"]))
        result: list[dict[str, Any]] = []
        for price in prices:
            date = str(price["date"])
            if date > basis.materialized_through_date:
                continue
            segment = next(
                (
                    item
                    for item in segments
                    if item.source_date_from <= date
                    and (
                        item.source_date_to_exclusive is None
                        or date < item.source_date_to_exclusive
                    )
                ),
                None,
            )
            if segment is None:
                continue
            known = [row for row in metrics if str(row["disclosed_date"]) <= date]
            valuation = _valuation_inputs(
                basis,
                date,
                float(price["close"]) * segment.cumulative_factor,
                known,
                statement_by_date,
            )
            result.append(_valuation_metric_row(build_daily_valuation_metric(valuation)))
        return result


def _group_raw_points(
    rows: Iterable[dict[str, Any]],
    codes: Sequence[str],
) -> dict[str, tuple[RawAdjustmentPoint, ...]]:
    grouped: dict[str, list[RawAdjustmentPoint]] = {code: [] for code in codes}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append(
            RawAdjustmentPoint(code, str(row["date"]), _optional_float(row["adjustment_factor"]))
        )
    return {code: tuple(points) for code, points in grouped.items()}


def _empty_build_result(*, total_codes: int = 0) -> AdjustedMetricsBuildResult:
    return AdjustedMetricsBuildResult(
        completed_codes=0,
        total_codes=total_codes,
        basis_count=0,
        published_basis_count=0,
        ready_basis_count=0,
        statement_rows=0,
        daily_valuation_rows=0,
        daily_technical_metric_rows=0,
        daily_valuation_latest_date=None,
        active_price_basis_date=None,
        active_basis_version=None,
    )


def _add_counts(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    return {
        key: left.get(key, 0) + right.get(key, 0)
        for key in left.keys() | right.keys()
    }


def _add_mutation_stats(
    left: dict[str, MarketMutationStats],
    right: dict[str, MarketMutationStats],
) -> dict[str, MarketMutationStats]:
    return {
        key: MarketMutationStats(
            input=left.get(key, MarketMutationStats.empty()).input
            + right.get(key, MarketMutationStats.empty()).input,
            inserted=left.get(key, MarketMutationStats.empty()).inserted
            + right.get(key, MarketMutationStats.empty()).inserted,
            updated=left.get(key, MarketMutationStats.empty()).updated
            + right.get(key, MarketMutationStats.empty()).updated,
            unchanged=left.get(key, MarketMutationStats.empty()).unchanged
            + right.get(key, MarketMutationStats.empty()).unchanged,
            deleted=left.get(key, MarketMutationStats.empty()).deleted
            + right.get(key, MarketMutationStats.empty()).deleted,
        )
        for key in left.keys() | right.keys()
    }


def _segments_by_basis(
    segments: Iterable[StockAdjustmentBasisSegment],
) -> dict[str, list[StockAdjustmentBasisSegment]]:
    grouped: dict[str, list[StockAdjustmentBasisSegment]] = {}
    for segment in segments:
        grouped.setdefault(segment.basis_id, []).append(segment)
    return grouped


_BASIS_STRUCTURAL_COLUMNS = (
    "code",
    "basis_id",
    "valid_from",
    "valid_to_exclusive",
    "adjustment_through_date",
    "source_fingerprint",
    "status",
)


def _plan_basis_materialization(
    basis: StockAdjustmentBasis,
    segments: tuple[StockAdjustmentBasisSegment, ...],
    statement_rows: list[dict[str, Any]],
    valuation_rows: list[dict[str, Any]],
    snapshot: BasisSnapshot | None,
) -> StructuralBasisPlan | FrontierExtensionBasisPlan | NoOpBasisPlan:
    lineage = StockAdjustmentLineage(
        code=basis.code,
        bases=(basis,),
        segments=segments,
    )
    if snapshot is None:
        return StructuralBasisPlan(
            kind="structural",
            lineage=lineage,
            adjusted_statement_rows=tuple(statement_rows),
            daily_valuation_rows=tuple(valuation_rows),
            expected_snapshot=None,
        )
    old_frontier = str(snapshot.basis["materialized_through_date"])
    new_frontier = basis.materialized_through_date
    if new_frontier < old_frontier:
        raise AdjustmentFrontierRegressionError(
            f"adjusted basis frontier regressed for {basis.basis_id}: "
            f"{old_frontier} -> {new_frontier}"
        )
    expected_catalog = {
        "code": basis.code,
        "basis_id": basis.basis_id,
        "valid_from": basis.valid_from,
        "valid_to_exclusive": basis.valid_to_exclusive,
        "adjustment_through_date": basis.adjustment_through_date,
        "source_fingerprint": basis.source_fingerprint,
        "status": basis.status,
    }
    structural_catalog_matches = all(
        snapshot.basis.get(column) == expected_catalog[column]
        for column in _BASIS_STRUCTURAL_COLUMNS
    )
    exact_segments = _canonical_rows(
        snapshot.segments,
        (
            "code",
            "basis_id",
            "source_date_from",
            "source_date_to_exclusive",
            "cumulative_factor",
        ),
    ) == _canonical_rows(
        (
            {
                "code": segment.code,
                "basis_id": segment.basis_id,
                "source_date_from": segment.source_date_from,
                "source_date_to_exclusive": segment.source_date_to_exclusive,
                "cumulative_factor": segment.cumulative_factor,
            }
            for segment in segments
        ),
        (
            "code",
            "basis_id",
            "source_date_from",
            "source_date_to_exclusive",
            "cumulative_factor",
        ),
    )
    desired_statements = tuple(statement_rows)
    desired_valuations = tuple(valuation_rows)
    exact_statements = _canonical_rows(
        snapshot.statement_rows, STATEMENT_METRICS_ADJUSTED_COLUMNS
    ) == _canonical_rows(desired_statements, STATEMENT_METRICS_ADJUSTED_COLUMNS)
    exact_valuations = _canonical_rows(
        snapshot.valuation_rows, DAILY_VALUATION_COLUMNS
    ) == _canonical_rows(desired_valuations, DAILY_VALUATION_COLUMNS)
    if (
        structural_catalog_matches
        and exact_segments
        and new_frontier == old_frontier
        and exact_statements
        and exact_valuations
    ):
        return NoOpBasisPlan(kind="no_op", code=basis.code, basis_id=basis.basis_id)
    if structural_catalog_matches and exact_segments and new_frontier > old_frontier:
        statement_delta = _append_only_delta(
            snapshot.statement_rows,
            desired_statements,
            key_column="disclosed_date",
            frontier=old_frontier,
            columns=STATEMENT_METRICS_ADJUSTED_COLUMNS,
            allow_suffix_updates=True,
        )
        valuation_delta = _append_only_delta(
            snapshot.valuation_rows,
            desired_valuations,
            key_column="date",
            frontier=old_frontier,
            columns=DAILY_VALUATION_COLUMNS,
            allow_suffix_updates=False,
        )
        if statement_delta is not None and valuation_delta is not None:
            return FrontierExtensionBasisPlan(
                kind="frontier_extension",
                basis=basis,
                segments=segments,
                adjusted_statement_rows=statement_delta,
                daily_valuation_rows=valuation_delta,
                expected_snapshot=snapshot,
            )
    return StructuralBasisPlan(
        kind="structural",
        lineage=lineage,
        adjusted_statement_rows=desired_statements,
        daily_valuation_rows=desired_valuations,
        expected_snapshot=snapshot,
    )


def _append_only_delta(
    existing_rows: Sequence[dict[str, Any]],
    desired_rows: Sequence[dict[str, Any]],
    *,
    key_column: str,
    frontier: str,
    columns: Sequence[str],
    allow_suffix_updates: bool,
) -> tuple[dict[str, Any], ...] | None:
    existing = {str(row[key_column]): row for row in existing_rows}
    desired = {str(row[key_column]): row for row in desired_rows}
    delta: list[dict[str, Any]] = []
    for key, row in existing.items():
        candidate = desired.get(key)
        if candidate is None:
            return None
        changed = _canonical_rows((row,), columns) != _canonical_rows(
            (candidate,), columns
        )
        if changed:
            if key <= frontier or not allow_suffix_updates:
                return None
            delta.append(candidate)
    additions = tuple(row for key, row in desired.items() if key not in existing)
    if any(str(row[key_column]) <= frontier for row in additions):
        return None
    if not allow_suffix_updates and any(
        str(row[key_column]) > frontier for row in existing_rows
    ):
        return None
    return tuple(delta) + additions


def _active_basis(bases: Iterable[StockAdjustmentBasis]) -> StockAdjustmentBasis | None:
    active = [
        basis
        for basis in bases
        if basis.status == "ready" and basis.valid_to_exclusive is None
    ]
    return max(active, key=lambda basis: (basis.materialized_through_date, basis.code), default=None)


def _canonical_rows(
    rows: Iterable[dict[str, Any]],
    columns: Sequence[str],
) -> list[tuple[Any, ...]]:
    compare_columns = [column for column in columns if column != "created_at"]
    return sorted(
        (tuple(row.get(column) for column in compare_columns) for row in rows),
        key=repr,
    )


def _statement_input(row: dict[str, Any]) -> AdjustedStatementInput:
    disclosed = str(row["disclosed_date"])
    document = str(row.get("type_of_document") or "")
    period_type = str(row.get("type_of_current_period") or "")
    if "EarnForecastRevision" in document:
        forecast_eps = _first_not_none(
            row.get("forecast_eps"),
            row.get("next_year_forecast_earnings_per_share"),
        )
    elif period_type.upper() == "FY":
        forecast_eps = _first_not_none(
            row.get("next_year_forecast_earnings_per_share"),
            row.get("forecast_eps"),
        )
    else:
        forecast_eps = row.get("forecast_eps")
    return AdjustedStatementInput(
        code=normalize_stock_code(str(row["code"])),
        disclosed_date=disclosed,
        period_end=disclosed,
        period_type=period_type,
        eps=_optional_float(row.get("earnings_per_share")),
        bps=_optional_float(row.get("bps")),
        forecast_eps=_optional_float(forecast_eps),
        dividend_fy=_optional_float(row.get("dividend_fy")),
        shares_outstanding=_optional_float(row.get("shares_outstanding")),
        treasury_shares=_optional_float(row.get("treasury_shares")),
    )


def _valuation_inputs(
    basis: StockAdjustmentBasis,
    date: str,
    close: float,
    metrics: list[dict[str, Any]],
    statements: dict[str, dict[str, Any]],
) -> DailyValuationInput:
    fy_metrics = [row for row in metrics if str(row["period_type"]).upper() == "FY"]
    actual = _latest(fy_metrics, lambda row: row.get("adjusted_eps") is not None)
    bps = _latest(fy_metrics, lambda row: row.get("adjusted_bps") is not None)
    anchors = [
        row
        for row in fy_metrics
        if "EarnForecastRevision" not in str(
            statements.get(str(row["disclosed_date"]), {}).get("type_of_document") or ""
        )
        and (
            _positive(row.get("adjusted_eps"))
            or _positive(row.get("adjusted_bps"))
            or _positive(statements.get(str(row["disclosed_date"]), {}).get("sales"))
        )
    ]
    anchor = _latest(anchors)
    shares = _latest(metrics, lambda row: row.get("adjusted_shares_outstanding") is not None)
    forward_candidates = [
        row
        for row in metrics
        if row.get("adjusted_forecast_eps") is not None
        and (
            str(row["period_type"]).upper() != "FY"
            or row in anchors
            or "EarnForecastRevision" in str(
                statements.get(str(row["disclosed_date"]), {}).get("type_of_document") or ""
            )
        )
    ]
    forward = _latest(forward_candidates)
    forward_source = _forward_source(forward, statements)
    forward_valid = _forecast_valid(forward, forward_source, anchor)
    actual_sales = _latest_raw_metric(anchors, statements, "sales")
    actual_op = _latest_raw_metric(anchors, statements, "operating_profit")
    if actual_op is not None and anchor is not None:
        if actual_op[0]["disclosed_date"] != anchor["disclosed_date"]:
            actual_op = None
    forward_sales = _latest_forward_raw(metrics, anchors, statements, "sales")
    forward_op = _latest_forward_raw(metrics, anchors, statements, "operating_profit")
    valid_forward_sales = _raw_forecast_valid(forward_sales, anchor)
    valid_forward_op = _raw_forecast_valid(forward_op, anchor)
    return DailyValuationInput(
        code=basis.code,
        date=date,
        price_basis_date=basis.adjustment_through_date,
        close=close,
        eps=_optional_float(actual.get("adjusted_eps")) if actual else None,
        bps=_optional_float(bps.get("adjusted_bps")) if bps else None,
        forward_eps=(
            _optional_float(forward.get("adjusted_forecast_eps")) if forward_valid and forward else None
        ),
        sales=_optional_float(actual_sales[1]) if actual_sales else None,
        forward_sales=_optional_float(forward_sales[1]) if valid_forward_sales and forward_sales else None,
        operating_profit=_optional_float(actual_op[1]) if actual_op else None,
        forward_operating_profit=_optional_float(forward_op[1]) if valid_forward_op and forward_op else None,
        shares_outstanding=(
            _optional_float(shares.get("adjusted_shares_outstanding")) if shares else None
        ),
        treasury_shares=(
            _optional_float(shares.get("adjusted_treasury_shares")) if shares else None
        ),
        statement_disclosed_date=(
            max(
                (str(row["disclosed_date"]) for row in (actual, bps) if row is not None),
                default=None,
            )
        ),
        forward_eps_disclosed_date=(str(forward["disclosed_date"]) if forward_valid and forward else None),
        forward_eps_source=forward_source if forward_valid else None,
        forward_sales_disclosed_date=(str(forward_sales[0]["disclosed_date"]) if valid_forward_sales and forward_sales else None),
        forward_sales_source=(forward_sales[2] if valid_forward_sales and forward_sales else None),
        basis_version=basis.basis_id,
    )


def _latest(
    rows: Iterable[dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any] | None:
    eligible = [row for row in rows if predicate is None or predicate(row)]
    return max(eligible, key=lambda row: str(row["disclosed_date"]), default=None)


def _latest_raw_metric(
    anchors: Iterable[dict[str, Any]],
    statements: dict[str, dict[str, Any]],
    field: str,
) -> tuple[dict[str, Any], Any] | None:
    rows = [
        (anchor, statements.get(str(anchor["disclosed_date"]), {}).get(field))
        for anchor in anchors
    ]
    return max(
        (item for item in rows if item[1] is not None),
        key=lambda item: str(item[0]["disclosed_date"]),
        default=None,
    )


def _latest_forward_raw(
    metrics: Iterable[dict[str, Any]],
    anchors: list[dict[str, Any]],
    statements: dict[str, dict[str, Any]],
    field: str,
) -> tuple[dict[str, Any], Any, ForwardEpsSource] | None:
    candidates: list[tuple[dict[str, Any], Any, ForwardEpsSource]] = []
    for metric in metrics:
        raw = statements.get(str(metric["disclosed_date"]), {})
        document = str(raw.get("type_of_document") or "")
        period_type = str(metric["period_type"]).upper()
        if "EarnForecastRevision" in document:
            value = _first_not_none(
                raw.get(f"forecast_{field}"),
                raw.get(f"next_year_forecast_{field}"),
            )
            source = "revised"
        elif period_type == "FY":
            if metric not in anchors:
                continue
            value = _first_not_none(
                raw.get(f"next_year_forecast_{field}"),
                raw.get(f"forecast_{field}"),
            )
            source = "fy"
        else:
            value = raw.get(f"forecast_{field}")
            source = "revised"
        if value is not None:
            candidates.append((metric, value, source))
    return max(candidates, key=lambda item: str(item[0]["disclosed_date"]), default=None)


def _forward_source(
    metric: dict[str, Any] | None,
    statements: dict[str, dict[str, Any]],
) -> ForwardEpsSource | None:
    if metric is None:
        return None
    raw = statements.get(str(metric["disclosed_date"]), {})
    if "EarnForecastRevision" in str(raw.get("type_of_document") or ""):
        return "revised"
    return "fy" if str(metric["period_type"]).upper() == "FY" else "revised"


def _forecast_valid(
    metric: dict[str, Any] | None,
    source: str | None,
    anchor: dict[str, Any] | None,
) -> bool:
    if metric is None or anchor is None:
        return False
    disclosed = str(metric["disclosed_date"])
    anchor_date = str(anchor["disclosed_date"])
    return (source == "fy" and disclosed == anchor_date) or (
        source == "revised" and disclosed > anchor_date
    )


def _raw_forecast_valid(
    metric: tuple[dict[str, Any], Any, ForwardEpsSource] | None,
    anchor: dict[str, Any] | None,
) -> bool:
    return metric is not None and _forecast_valid(metric[0], metric[2], anchor)


def _positive(value: Any) -> bool:
    return value is not None and float(value) > 0


def _statement_metric_row(metric: AdjustedStatementMetric) -> dict[str, Any]:
    return {
        "code": metric.code,
        "disclosed_date": metric.disclosed_date,
        "period_end": metric.period_end,
        "period_type": metric.period_type,
        "price_basis_date": metric.price_basis_date,
        "raw_eps": metric.raw_eps,
        "adjusted_eps": metric.adjusted_eps,
        "raw_bps": metric.raw_bps,
        "adjusted_bps": metric.adjusted_bps,
        "raw_forecast_eps": metric.raw_forecast_eps,
        "adjusted_forecast_eps": metric.adjusted_forecast_eps,
        "raw_dividend_fy": metric.raw_dividend_fy,
        "adjusted_dividend_fy": metric.adjusted_dividend_fy,
        "raw_shares_outstanding": metric.raw_shares_outstanding,
        "adjusted_shares_outstanding": metric.adjusted_shares_outstanding,
        "raw_treasury_shares": metric.raw_treasury_shares,
        "adjusted_treasury_shares": metric.adjusted_treasury_shares,
        "adjustment_factor_cumulative": metric.adjustment_factor_cumulative,
        "basis_version": metric.basis_version,
    }


def _valuation_metric_row(metric: Any) -> dict[str, Any]:
    return {
        column: getattr(metric, column)
        for column in DAILY_VALUATION_COLUMNS
        if column != "created_at"
    }


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _first_not_none(*values: _T | None) -> _T | None:
    return next((value for value in values if value is not None), None)
