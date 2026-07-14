"""Reconcile event-time adjusted fundamentals and valuation materializations."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
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
from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.db.market.valuation_writers import (
    AdjustedBasisMaterializationPlan,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


_CATALOG_COMPARE_COLUMNS = (
    "code",
    "basis_id",
    "valid_from",
    "valid_to_exclusive",
    "adjustment_through_date",
    "source_fingerprint",
    "materialized_through_date",
    "status",
)
_T = TypeVar("_T")


@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    completed_codes: int
    total_codes: int
    basis_count: int
    ready_basis_count: int
    statement_rows: int
    daily_valuation_rows: int
    daily_technical_metric_rows: int
    daily_valuation_latest_date: str | None
    active_price_basis_date: str | None
    active_basis_version: str | None


class AdjustedMetricsMaterializer:
    """Build every source-derived adjustment regime and publish changed bases."""

    def __init__(self, market_db: MarketDb) -> None:
        self._market_db = market_db

    def rebuild_all(self) -> AdjustedMetricsBuildResult:
        return self.reconcile(codes=None)

    def rebuild_codes(self, codes: list[str]) -> AdjustedMetricsBuildResult:
        normalized = sorted({normalize_stock_code(code) for code in codes if code})
        return self.reconcile(codes=normalized)

    def reconcile(self, codes: list[str] | None = None) -> AdjustedMetricsBuildResult:
        target_codes = self._target_codes(codes)
        market_sessions = self._market_sessions()
        completed_codes = 0
        basis_count = 0
        ready_basis_count = 0
        statement_rows = 0
        daily_valuation_rows = 0
        daily_valuation_latest_date: str | None = None
        active_price_basis_date: str | None = None
        active_basis_version: str | None = None
        for code in target_codes:
            result = self.reconcile_code(code, market_sessions)
            completed_codes += result.completed_codes
            basis_count += result.basis_count
            ready_basis_count += result.ready_basis_count
            statement_rows += result.statement_rows
            daily_valuation_rows += result.daily_valuation_rows
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

        technical_rows = (
            self._market_db.rebuild_daily_technical_metrics_from_stock_data()
            if codes is None and self._market_db._table_exists("stock_data")
            else 0
        )
        return AdjustedMetricsBuildResult(
            completed_codes=completed_codes,
            total_codes=len(target_codes),
            basis_count=basis_count,
            ready_basis_count=ready_basis_count,
            statement_rows=statement_rows,
            daily_valuation_rows=daily_valuation_rows,
            daily_technical_metric_rows=technical_rows,
            daily_valuation_latest_date=daily_valuation_latest_date,
            active_price_basis_date=active_price_basis_date,
            active_basis_version=active_basis_version,
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

        existing_catalog = self._existing_catalog(requested_codes)
        rebuilt_ids = {basis.basis_id for basis in lineage.bases}
        retained_ids = set(existing_catalog) - rebuilt_ids
        changed_catalog_ids = {
            basis.basis_id
            for basis in lineage.bases
            if _catalog_changed(basis, existing_catalog.get(basis.basis_id))
        }
        replace_ids = set(changed_catalog_ids)
        for basis_id, (basis_statements, basis_valuations) in generated_by_basis.items():
            if self._materialized_rows_changed(
                basis_id,
                basis_statements,
                basis_valuations,
            ):
                replace_ids.add(basis_id)

        # A disappeared raw event cannot authorize deleting or rewriting its retained
        # event-time graph. Leave the code unchanged for validation/recovery instead.
        if retained_ids:
            changed_catalog_ids.clear()
            replace_ids.clear()

        changed_lineages = (
            (_select_lineage(lineage, changed_catalog_ids),)
            if changed_catalog_ids
            else ()
        )
        if changed_lineages or replace_ids:
            self._market_db.publish_adjusted_basis_materialization(
                AdjustedBasisMaterializationPlan(
                    lineages=changed_lineages,
                    adjusted_statement_rows=tuple(
                        row
                        for basis_id in sorted(replace_ids)
                        for row in generated_by_basis.get(basis_id, ([], []))[0]
                    ),
                    daily_valuation_rows=tuple(
                        row
                        for basis_id in sorted(replace_ids)
                        for row in generated_by_basis.get(basis_id, ([], []))[1]
                    ),
                    replace_basis_ids=_ids_by_code(replace_ids),
                    orphan_basis_ids={},
                )
            )

        active_basis = _active_basis(lineage.bases)
        ready_bases = [basis for basis in lineage.bases if basis.status == "ready"]
        return AdjustedMetricsBuildResult(
            completed_codes=1,
            total_codes=1,
            basis_count=len(lineage.bases),
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

    def _existing_catalog(self, codes: list[str]) -> dict[str, dict[str, Any]]:
        if not codes:
            return {}
        placeholders = ", ".join("?" for _ in codes)
        rows = self._market_db._fetchall_dicts(
            f"""
            SELECT {', '.join(_CATALOG_COMPARE_COLUMNS)}, created_at, updated_at
            FROM stock_adjustment_bases
            WHERE code IN ({placeholders})
            """,
            codes,
        )
        return {str(row["basis_id"]): row for row in rows}

    def _load_statement_rows(self, codes: list[str]) -> list[dict[str, Any]]:
        if not codes or not self._market_db._table_exists("statements"):
            return []
        placeholders = ", ".join("?" for _ in codes)
        return self._market_db._fetchall_dicts(
            f"""
            WITH normalized AS (
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
                FROM statements
            )
            SELECT * EXCLUDE (code, alias_rank), normalized_code AS code
            FROM normalized
            WHERE alias_rank = 1 AND normalized_code IN ({placeholders})
            ORDER BY normalized_code, disclosed_date
            """,
            codes,
        )

    def _load_raw_price_rows(self, codes: list[str]) -> list[dict[str, Any]]:
        if not codes:
            return []
        placeholders = ", ".join("?" for _ in codes)
        return self._market_db._fetchall_dicts(
            f"""
            WITH normalized AS (
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
                FROM stock_data_raw
            )
            SELECT normalized_code AS code, date, open, high, low, close, volume,
                   adjustment_factor
            FROM normalized
            WHERE alias_rank = 1 AND normalized_code IN ({placeholders})
              AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
              AND close IS NOT NULL AND volume IS NOT NULL
            ORDER BY normalized_code, date
            """,
            codes,
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

    def _materialized_rows_changed(
        self,
        basis_id: str,
        statement_rows: list[dict[str, Any]],
        valuation_rows: list[dict[str, Any]],
    ) -> bool:
        existing_statements = self._market_db._fetchall_dicts(
            f"SELECT {', '.join(STATEMENT_METRICS_ADJUSTED_COLUMNS)} "
            "FROM statement_metrics_adjusted WHERE basis_version = ?",
            [basis_id],
        )
        existing_valuations = self._market_db._fetchall_dicts(
            f"SELECT {', '.join(DAILY_VALUATION_COLUMNS)} "
            "FROM daily_valuation WHERE basis_version = ?",
            [basis_id],
        )
        return (
            _canonical_rows(existing_statements, STATEMENT_METRICS_ADJUSTED_COLUMNS)
            != _canonical_rows(statement_rows, STATEMENT_METRICS_ADJUSTED_COLUMNS)
            or _canonical_rows(existing_valuations, DAILY_VALUATION_COLUMNS)
            != _canonical_rows(valuation_rows, DAILY_VALUATION_COLUMNS)
        )


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
        ready_basis_count=0,
        statement_rows=0,
        daily_valuation_rows=0,
        daily_technical_metric_rows=0,
        daily_valuation_latest_date=None,
        active_price_basis_date=None,
        active_basis_version=None,
    )


def _segments_by_basis(
    segments: Iterable[StockAdjustmentBasisSegment],
) -> dict[str, list[StockAdjustmentBasisSegment]]:
    grouped: dict[str, list[StockAdjustmentBasisSegment]] = {}
    for segment in segments:
        grouped.setdefault(segment.basis_id, []).append(segment)
    return grouped


def _catalog_changed(basis: StockAdjustmentBasis, existing: dict[str, Any] | None) -> bool:
    if existing is None:
        return True
    expected = {
        "code": basis.code,
        "basis_id": basis.basis_id,
        "valid_from": basis.valid_from,
        "valid_to_exclusive": basis.valid_to_exclusive,
        "adjustment_through_date": basis.adjustment_through_date,
        "source_fingerprint": basis.source_fingerprint,
        "materialized_through_date": basis.materialized_through_date,
        "status": basis.status,
    }
    return any(existing.get(column) != expected[column] for column in _CATALOG_COMPARE_COLUMNS)


def _select_lineage(
    lineage: StockAdjustmentLineage,
    basis_ids: set[str],
) -> StockAdjustmentLineage:
    selected = tuple(basis for basis in lineage.bases if basis.basis_id in basis_ids)
    selected_ids = {basis.basis_id for basis in selected}
    return StockAdjustmentLineage(
        code=lineage.code,
        bases=selected,
        segments=tuple(
            segment for segment in lineage.segments if segment.basis_id in selected_ids
        ),
    )


def _ids_by_code(basis_ids: Iterable[str]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for basis_id in basis_ids:
        parts = basis_id.split(":", 2)
        if len(parts) != 3:
            raise ValueError(f"invalid event-time basis id: {basis_id}")
        grouped.setdefault(parts[1], []).append(basis_id)
    return {code: tuple(sorted(ids)) for code, ids in grouped.items()}


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
