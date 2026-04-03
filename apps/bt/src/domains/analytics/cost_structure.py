"""Cost structure analysis domain logic."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Literal

from src.domains.analytics.regression_core import ols_regression
from src.shared.models.types import normalize_period_type

SourcePeriodType = Literal["1Q", "2Q", "3Q", "FY"]
AnalysisPeriodType = Literal["1Q", "2Q", "3Q", "4Q", "FY"]
CostStructureAnalysisView = Literal["recent", "same_quarter", "fiscal_year_only", "all"]

_PERIOD_ORDER: dict[SourcePeriodType, int] = {
    "1Q": 1,
    "2Q": 2,
    "3Q": 3,
    "FY": 4,
}
_SKIPPED_WARNING_PATTERN = re.compile(r"^Skipped (?P<fiscal_year>\d{4}) (?P<period>1Q|2Q|3Q|4Q|FY):")


@dataclass(frozen=True)
class CostStructureStatement:
    """Statement row required for cost structure normalization."""

    disclosed_date: str
    period_type: SourcePeriodType
    sales: float | None
    operating_profit: float | None


@dataclass(frozen=True)
class CostStructurePoint:
    """Analysis point used for regression."""

    period_end: str
    disclosed_date: str
    fiscal_year: str
    analysis_period_type: AnalysisPeriodType
    sales: float
    operating_profit: float
    operating_margin: float | None
    is_derived: bool


@dataclass(frozen=True)
class CostStructureRegression:
    """Regression summary for cost structure analysis."""

    sample_count: int
    slope: float
    intercept: float
    r_squared: float
    contribution_margin_ratio: float
    variable_cost_ratio: float
    fixed_cost: float | None
    break_even_sales: float | None
    warnings: list[str]


@dataclass(frozen=True)
class CostStructureAnalysis:
    """Complete cost structure analysis output."""

    points: list[CostStructurePoint]
    latest_point: CostStructurePoint
    regression: CostStructureRegression
    warnings: list[str]
    date_from: str
    date_to: str


@dataclass(frozen=True)
class _StatementCycle:
    """Fiscal-cycle bucket anchored by FY disclosures when available."""

    fiscal_year: str
    statements: list[CostStructureStatement]
    closed: bool
    left_censored: bool


def _append_warning(warnings: list[str], message: str) -> None:
    if message not in warnings:
        warnings.append(message)


def _normalize_statement(statement: CostStructureStatement) -> CostStructureStatement | None:
    normalized_period = normalize_period_type(statement.period_type)
    if normalized_period not in _PERIOD_ORDER:
        return None

    return CostStructureStatement(
        disclosed_date=statement.disclosed_date,
        period_type=normalized_period,
        sales=statement.sales,
        operating_profit=statement.operating_profit,
    )


def _is_left_censored_cycle(
    statements: list[CostStructureStatement],
    previous_fy_year: int | None,
) -> bool:
    if previous_fy_year is not None or not statements:
        return False
    earliest_period = min(_PERIOD_ORDER[row.period_type] for row in statements)
    return earliest_period > 1


def build_statement_cycles(statements: list[CostStructureStatement]) -> list[_StatementCycle]:
    """Bucket statements into fiscal cycles using FY disclosures as anchors."""
    normalized_rows = [
        row
        for statement in statements
        if (row := _normalize_statement(statement)) is not None
    ]
    if not normalized_rows:
        return []

    sorted_rows = sorted(
        normalized_rows,
        key=lambda item: (item.disclosed_date, _PERIOD_ORDER[item.period_type]),
    )

    fy_indexes = [index for index, row in enumerate(sorted_rows) if row.period_type == "FY"]
    if not fy_indexes:
        return [
            _StatementCycle(
                fiscal_year=_infer_trailing_fiscal_year(sorted_rows, previous_fy_year=None),
                statements=sorted_rows,
                closed=False,
                left_censored=False,
            )
        ]

    cycles: list[_StatementCycle] = []
    start = 0
    previous_fy_year: int | None = None
    for fy_index in fy_indexes:
        cycle_rows = sorted_rows[start : fy_index + 1]
        if not cycle_rows:
            start = fy_index + 1
            continue

        fy_year = _year_from_date(sorted_rows[fy_index].disclosed_date)
        if (
            cycles
            and all(row.period_type == "FY" for row in cycle_rows)
            and cycles[-1].fiscal_year == str(fy_year)
        ):
            previous_cycle = cycles[-1]
            cycles[-1] = _StatementCycle(
                fiscal_year=previous_cycle.fiscal_year,
                statements=[*previous_cycle.statements, *cycle_rows],
                closed=True,
                left_censored=previous_cycle.left_censored,
            )
            start = fy_index + 1
            previous_fy_year = fy_year
            continue

        cycles.append(
            _StatementCycle(
                fiscal_year=str(fy_year),
                statements=cycle_rows,
                closed=True,
                left_censored=_is_left_censored_cycle(cycle_rows, previous_fy_year),
            )
        )
        previous_fy_year = fy_year
        start = fy_index + 1

    trailing_rows = sorted_rows[start:]
    if trailing_rows:
        cycles.append(
            _StatementCycle(
                fiscal_year=_infer_trailing_fiscal_year(trailing_rows, previous_fy_year),
                statements=trailing_rows,
                closed=False,
                left_censored=_is_left_censored_cycle(trailing_rows, previous_fy_year),
            )
        )

    return cycles


def _year_from_date(value: str) -> int:
    return int(value[:4])


def _infer_trailing_fiscal_year(
    statements: list[CostStructureStatement],
    previous_fy_year: int | None,
) -> str:
    if previous_fy_year is not None:
        return str(previous_fy_year + 1)

    latest_year = _year_from_date(statements[-1].disclosed_date)
    highest_period = max(_PERIOD_ORDER[row.period_type] for row in statements)
    if highest_period in (1, 2):
        return str(latest_year + 1)
    return str(latest_year)


def _is_usable_statement(statement: CostStructureStatement) -> bool:
    return _is_valid_sales(statement.sales) and _is_valid_operating_profit(statement.operating_profit)


def _dedupe_cycle_statements(cycle: _StatementCycle) -> dict[SourcePeriodType, CostStructureStatement]:
    latest_by_period: dict[SourcePeriodType, CostStructureStatement] = {}
    latest_usable_by_period: dict[SourcePeriodType, CostStructureStatement] = {}
    for statement in cycle.statements:
        latest_by_period[statement.period_type] = statement
        if _is_usable_statement(statement):
            latest_usable_by_period[statement.period_type] = statement

    deduped: dict[SourcePeriodType, CostStructureStatement] = {}
    for period_type, statement in latest_by_period.items():
        deduped[period_type] = latest_usable_by_period.get(period_type, statement)
    return deduped


def _is_valid_sales(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


def _is_valid_operating_profit(value: float | None) -> bool:
    return value is not None and math.isfinite(value)


def _format_cycle_period_label(cycle: _StatementCycle, period_type: str) -> str:
    return f"{cycle.fiscal_year} {period_type}"


def _should_suppress_missing_predecessor_warning(
    cycle: _StatementCycle,
    period_rows: dict[SourcePeriodType, CostStructureStatement],
    predecessor_period_type: SourcePeriodType,
) -> bool:
    return cycle.left_censored and predecessor_period_type not in period_rows


def _validate_source_statement(
    cycle: _StatementCycle,
    statement: CostStructureStatement | None,
    warnings: list[str],
) -> CostStructureStatement | None:
    if statement is None:
        return None
    if not _is_valid_sales(statement.sales):
        _append_warning(
            warnings,
            f"Skipped {_format_cycle_period_label(cycle, statement.period_type)}: sales was missing, non-finite, or non-positive.",
        )
        return None
    if not _is_valid_operating_profit(statement.operating_profit):
        _append_warning(
            warnings,
            f"Skipped {_format_cycle_period_label(cycle, statement.period_type)}: operating profit was missing or non-finite.",
        )
        return None
    return statement


def _build_operating_margin(sales: float, operating_profit: float) -> float | None:
    if not math.isfinite(sales) or sales <= 0 or not math.isfinite(operating_profit):
        return None
    return (operating_profit / sales) * 100


def _build_raw_point(
    cycle: _StatementCycle,
    source: CostStructureStatement,
) -> CostStructurePoint:
    assert source.sales is not None
    assert source.operating_profit is not None
    return CostStructurePoint(
        period_end=source.disclosed_date,
        disclosed_date=source.disclosed_date,
        fiscal_year=cycle.fiscal_year,
        analysis_period_type="1Q",
        sales=float(source.sales),
        operating_profit=float(source.operating_profit),
        operating_margin=_build_operating_margin(float(source.sales), float(source.operating_profit)),
        is_derived=False,
    )


def _build_derived_point(
    cycle: _StatementCycle,
    source: CostStructureStatement,
    predecessor: CostStructureStatement,
    analysis_period_type: AnalysisPeriodType,
    warnings: list[str],
) -> CostStructurePoint | None:
    assert source.sales is not None and source.operating_profit is not None
    assert predecessor.sales is not None and predecessor.operating_profit is not None

    sales = float(source.sales) - float(predecessor.sales)
    operating_profit = float(source.operating_profit) - float(predecessor.operating_profit)

    label = _format_cycle_period_label(cycle, analysis_period_type)
    if not math.isfinite(sales) or sales <= 0:
        _append_warning(
            warnings,
            f"Skipped {label}: normalized sales was non-finite or non-positive after cumulative diff.",
        )
        return None
    if not math.isfinite(operating_profit):
        _append_warning(
            warnings,
            f"Skipped {label}: normalized operating profit was non-finite after cumulative diff.",
        )
        return None

    return CostStructurePoint(
        period_end=source.disclosed_date,
        disclosed_date=source.disclosed_date,
        fiscal_year=cycle.fiscal_year,
        analysis_period_type=analysis_period_type,
        sales=sales,
        operating_profit=operating_profit,
        operating_margin=_build_operating_margin(sales, operating_profit),
        is_derived=True,
    )


def _build_fiscal_year_point(
    cycle: _StatementCycle,
    source: CostStructureStatement,
) -> CostStructurePoint:
    assert source.sales is not None
    assert source.operating_profit is not None
    sales = float(source.sales)
    operating_profit = float(source.operating_profit)
    return CostStructurePoint(
        period_end=source.disclosed_date,
        disclosed_date=source.disclosed_date,
        fiscal_year=cycle.fiscal_year,
        analysis_period_type="FY",
        sales=sales,
        operating_profit=operating_profit,
        operating_margin=_build_operating_margin(sales, operating_profit),
        is_derived=False,
    )


def _select_cost_structure_points(
    points: list[CostStructurePoint],
    *,
    view: CostStructureAnalysisView,
    window_quarters: int,
) -> list[CostStructurePoint]:
    if view == "all":
        selected = list(points)
    elif view == "same_quarter":
        if not points:
            selected = []
        else:
            latest_period_type = points[-1].analysis_period_type
            selected = [point for point in points if point.analysis_period_type == latest_period_type]
    elif view == "fiscal_year_only":
        selected = [point for point in points if point.analysis_period_type == "FY"]
    else:
        selected = points[-window_quarters:]

    if len(selected) < 3:
        raise ValueError(
            f"Insufficient usable data for {view} view: {len(selected)} normalized points (minimum 3)"
        )
    return selected


def _filter_warnings_for_selected_points(
    warnings: list[str],
    points: list[CostStructurePoint],
) -> list[str]:
    if not points:
        return []

    selected_fiscal_years = {point.fiscal_year for point in points}
    selected_periods = {point.analysis_period_type for point in points}
    filtered: list[str] = []
    for warning in warnings:
        match = _SKIPPED_WARNING_PATTERN.match(warning)
        if match is None:
            filtered.append(warning)
            continue

        fiscal_year = match.group("fiscal_year")
        period_type = match.group("period")
        if fiscal_year in selected_fiscal_years and period_type in selected_periods:
            filtered.append(warning)
    return filtered


def build_fiscal_year_cost_structure_points(
    statements: list[CostStructureStatement],
) -> tuple[list[CostStructurePoint], list[str]]:
    """Build comparable fiscal-year cumulative points."""
    warnings: list[str] = []
    points: list[CostStructurePoint] = []

    for cycle in build_statement_cycles(statements):
        period_rows = _dedupe_cycle_statements(cycle)
        full_year = _validate_source_statement(cycle, period_rows.get("FY"), warnings)
        if full_year is None:
            continue
        points.append(_build_fiscal_year_point(cycle, full_year))

    points.sort(key=lambda point: (point.disclosed_date, point.analysis_period_type))
    return points, warnings


def normalize_cost_structure_points(
    statements: list[CostStructureStatement],
) -> tuple[list[CostStructurePoint], list[str]]:
    """Normalize cumulative statement rows into comparable single-quarter points."""
    warnings: list[str] = []
    points: list[CostStructurePoint] = []

    for cycle in build_statement_cycles(statements):
        period_rows = _dedupe_cycle_statements(cycle)

        first_quarter = _validate_source_statement(cycle, period_rows.get("1Q"), warnings)
        second_quarter = _validate_source_statement(cycle, period_rows.get("2Q"), warnings)
        third_quarter = _validate_source_statement(cycle, period_rows.get("3Q"), warnings)
        full_year = _validate_source_statement(cycle, period_rows.get("FY"), warnings)

        if first_quarter is not None:
            points.append(_build_raw_point(cycle, first_quarter))

        if second_quarter is not None:
            if first_quarter is None:
                if not _should_suppress_missing_predecessor_warning(cycle, period_rows, "1Q"):
                    _append_warning(
                        warnings,
                        f"Skipped {_format_cycle_period_label(cycle, '2Q')}: missing valid 1Q predecessor for single-quarter normalization.",
                    )
            else:
                derived = _build_derived_point(cycle, second_quarter, first_quarter, "2Q", warnings)
                if derived is not None:
                    points.append(derived)

        if third_quarter is not None:
            if second_quarter is None:
                if not _should_suppress_missing_predecessor_warning(cycle, period_rows, "2Q"):
                    _append_warning(
                        warnings,
                        f"Skipped {_format_cycle_period_label(cycle, '3Q')}: missing valid 2Q predecessor for single-quarter normalization.",
                    )
            else:
                derived = _build_derived_point(cycle, third_quarter, second_quarter, "3Q", warnings)
                if derived is not None:
                    points.append(derived)

        if full_year is not None:
            if third_quarter is None:
                if not _should_suppress_missing_predecessor_warning(cycle, period_rows, "3Q"):
                    _append_warning(
                        warnings,
                        f"Skipped {_format_cycle_period_label(cycle, '4Q')}: missing valid 3Q predecessor for single-quarter normalization.",
                    )
            else:
                derived = _build_derived_point(cycle, full_year, third_quarter, "4Q", warnings)
                if derived is not None:
                    points.append(derived)

    points.sort(key=lambda point: (point.disclosed_date, point.analysis_period_type))
    return points, warnings


def calculate_cost_structure_regression(points: list[CostStructurePoint]) -> CostStructureRegression:
    """Run OLS regression over normalized cost-structure points."""
    if len(points) < 3:
        raise ValueError(
            f"Insufficient usable data: {len(points)} normalized points (minimum 3)"
        )

    x = [point.sales for point in points]
    y = [point.operating_profit for point in points]
    result = ols_regression(y, x)

    warnings: list[str] = []
    slope = float(result.beta)
    intercept = float(result.alpha)
    r_squared = float(result.r_squared)
    fixed_cost: float | None = None
    if math.isfinite(intercept) and intercept < 0:
        fixed_cost = -intercept
    else:
        _append_warning(
            warnings,
            "Fixed cost could not be interpreted because regression intercept was non-negative.",
        )

    break_even_sales: float | None = None
    if fixed_cost is not None and math.isfinite(slope) and slope > 0:
        candidate = fixed_cost / slope
        if math.isfinite(candidate) and candidate > 0:
            break_even_sales = candidate
        else:
            _append_warning(
                warnings,
                "Break-even sales could not be interpreted because the derived value was non-positive or non-finite.",
            )
    elif not math.isfinite(slope) or slope <= 0:
        _append_warning(
            warnings,
            "Break-even sales could not be interpreted because contribution margin ratio was not positive.",
        )
    else:
        _append_warning(
            warnings,
            "Break-even sales could not be interpreted because fixed cost was not economically interpretable.",
        )

    return CostStructureRegression(
        sample_count=len(points),
        slope=slope,
        intercept=intercept,
        r_squared=r_squared,
        contribution_margin_ratio=slope,
        variable_cost_ratio=1 - slope,
        fixed_cost=fixed_cost,
        break_even_sales=break_even_sales,
        warnings=warnings,
    )


def analyze_cost_structure(
    statements: list[CostStructureStatement],
    *,
    view: CostStructureAnalysisView = "recent",
    window_quarters: int = 12,
) -> CostStructureAnalysis:
    """Build normalized points and regression summary for one stock."""
    normalized_points, normalization_warnings = normalize_cost_structure_points(statements)
    fiscal_year_points, fiscal_year_warnings = build_fiscal_year_cost_structure_points(statements)
    if view == "fiscal_year_only":
        selected_points = _select_cost_structure_points(
            fiscal_year_points,
            view=view,
            window_quarters=window_quarters,
        )
        selected_warnings = fiscal_year_warnings
    else:
        selected_points = _select_cost_structure_points(
            normalized_points,
            view=view,
            window_quarters=window_quarters,
        )
        selected_warnings = normalization_warnings

    regression = calculate_cost_structure_regression(selected_points)
    warnings = _filter_warnings_for_selected_points(selected_warnings, selected_points) + [
        warning
        for warning in regression.warnings
        if warning not in selected_warnings
    ]
    latest_point = max(selected_points, key=lambda point: (point.disclosed_date, point.analysis_period_type))
    date_from = min(point.period_end for point in selected_points)
    date_to = max(point.period_end for point in selected_points)
    return CostStructureAnalysis(
        points=selected_points,
        latest_point=latest_point,
        regression=regression,
        warnings=warnings,
        date_from=date_from,
        date_to=date_to,
    )
