"""Pure corporate-action adjustment basis lineage construction."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
from typing import Literal


BasisStatus = Literal["building", "ready", "invalid"]


@dataclass(frozen=True)
class RawAdjustmentPoint:
    code: str
    date: str
    adjustment_factor: float | None


@dataclass(frozen=True)
class StockAdjustmentBasis:
    code: str
    basis_id: str
    valid_from: str
    valid_to_exclusive: str | None
    adjustment_through_date: str
    source_fingerprint: str
    materialized_through_date: str
    status: BasisStatus


@dataclass(frozen=True)
class StockAdjustmentBasisSegment:
    code: str
    basis_id: str
    source_date_from: str
    source_date_to_exclusive: str | None
    cumulative_factor: float


@dataclass(frozen=True)
class StockAdjustmentLineage:
    code: str
    bases: tuple[StockAdjustmentBasis, ...]
    segments: tuple[StockAdjustmentBasisSegment, ...]


def _normalize_stock_code(code: str) -> str:
    if len(code) in {5, 6} and code.endswith("0"):
        return code[:-1]
    return code


def _factor_token(factor: float | None) -> str:
    if factor is None:
        return "null"
    value = float(factor)
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return repr(value)


def _positive_factor(factor: float | None) -> float | None:
    if factor is None:
        return None
    value = float(factor)
    return value if math.isfinite(value) and value > 0 else None


def _valid_factor(factor: float | None) -> bool:
    return _positive_factor(factor) is not None


def _fingerprint(points: Sequence[RawAdjustmentPoint]) -> str:
    payload = [(point.date, _factor_token(point.adjustment_factor)) for point in points]
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalized_points(
    code: str,
    rows: Sequence[RawAdjustmentPoint],
) -> tuple[str, list[RawAdjustmentPoint]]:
    normalized_code = _normalize_stock_code(code)
    by_date: dict[str, RawAdjustmentPoint] = {}
    for row in rows:
        row_code = _normalize_stock_code(row.code)
        if row_code != normalized_code:
            raise ValueError(f"adjustment point code {row.code!r} does not match {code!r}")
        try:
            date.fromisoformat(row.date)
        except ValueError as exc:
            raise ValueError(f"invalid adjustment point date: {row.date!r}") from exc
        existing = by_date.get(row.date)
        if existing is not None:
            if _factor_token(existing.adjustment_factor) != _factor_token(row.adjustment_factor):
                raise ValueError(
                    f"conflicting adjustment factors for {normalized_code} on {row.date}"
                )
            continue
        by_date[row.date] = RawAdjustmentPoint(
            code=normalized_code,
            date=row.date,
            adjustment_factor=row.adjustment_factor,
        )
    return normalized_code, sorted(by_date.values(), key=lambda point: point.date)


def build_stock_adjustment_lineage(
    code: str,
    rows: Sequence[RawAdjustmentPoint],
    *,
    market_sessions: Sequence[str] = (),
) -> StockAdjustmentLineage:
    """Build immutable event-time adjustment regimes from raw price facts."""
    normalized_code, points = _normalized_points(code, rows)
    if not points:
        return StockAdjustmentLineage(code=normalized_code, bases=(), segments=())

    regime_points = []
    for index, point in enumerate(points):
        factor = _positive_factor(point.adjustment_factor)
        if index == 0 or factor is None or factor != 1.0:
            regime_points.append(point)
    invalid_from = next(
        (point.date for point in points if not _valid_factor(point.adjustment_factor)),
        None,
    )
    bases: list[StockAdjustmentBasis] = []
    segments: list[StockAdjustmentBasisSegment] = []
    for index, regime in enumerate(regime_points):
        next_regime_date = (
            regime_points[index + 1].date if index + 1 < len(regime_points) else None
        )
        covered_points = [
            point
            for point in points
            if point.date >= regime.date
            and (next_regime_date is None or point.date < next_regime_date)
        ]
        covered_sessions = [
            session
            for session in market_sessions
            if next_regime_date is None or session < next_regime_date
        ]
        materialized_through_date = max(
            covered_points[-1].date,
            max(covered_sessions, default=covered_points[-1].date),
        )
        # The fingerprint identifies the immutable corporate-action graph, not
        # the moving materialization frontier.  Ordinary factor-1 sessions are
        # deliberately excluded; only the origin/event points known through
        # this basis' adjustment date participate.
        source_points = [point for point in regime_points if point.date <= regime.date]
        status: BasisStatus = (
            "invalid" if invalid_from is not None and regime.date >= invalid_from else "ready"
        )
        basis_id = f"event-pit-v1:{normalized_code}:{regime.date}"
        bases.append(
            StockAdjustmentBasis(
                code=normalized_code,
                basis_id=basis_id,
                valid_from=regime.date,
                valid_to_exclusive=next_regime_date,
                adjustment_through_date=regime.date,
                source_fingerprint=_fingerprint(source_points),
                materialized_through_date=materialized_through_date,
                status=status,
            )
        )
        if status != "ready":
            continue

        boundaries = [
            point.date
            for point in regime_points
            if point.date <= regime.date
        ]
        for boundary_index, source_from in enumerate(boundaries):
            source_to = (
                boundaries[boundary_index + 1]
                if boundary_index + 1 < len(boundaries)
                else None
            )
            factors: list[float] = []
            for point in regime_points:
                point_factor = _positive_factor(point.adjustment_factor)
                if source_from < point.date <= regime.date and point_factor is not None:
                    factors.append(point_factor)
            factor = math.prod(factors)
            segments.append(
                StockAdjustmentBasisSegment(
                    code=normalized_code,
                    basis_id=basis_id,
                    source_date_from=source_from,
                    source_date_to_exclusive=source_to,
                    cumulative_factor=factor,
                )
            )

    return StockAdjustmentLineage(
        code=normalized_code,
        bases=tuple(bases),
        segments=tuple(segments),
    )
