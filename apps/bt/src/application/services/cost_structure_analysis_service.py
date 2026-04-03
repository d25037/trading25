"""Cost structure analysis service."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import SupportsFloat, SupportsIndex, cast

from src.application.services.analytics_provenance import build_market_provenance
from src.domains.analytics.cost_structure import (
    CostStructurePoint as DomainCostStructurePoint,
    CostStructureAnalysisView,
    CostStructureStatement,
    SourcePeriodType,
    analyze_cost_structure,
)
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.entrypoints.http.schemas.cost_structure import (
    CostStructureDateRange,
    CostStructurePoint,
    CostStructureRegressionSummary,
    CostStructureResponse,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import stock_code_candidates


class CostStructureAnalysisService:
    """Analyze stock cost structure from local market statements."""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader

    def _get_stock_row(self, symbol: str):
        codes = stock_code_candidates(symbol)
        placeholders = ",".join("?" for _ in codes)
        return self._reader.query_one(
            f"""
            SELECT code, company_name
            FROM stocks
            WHERE code IN ({placeholders})
            ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
            LIMIT 1
            """,
            tuple(codes),
        )

    @staticmethod
    def _to_millions(value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(cast(str | SupportsFloat | SupportsIndex, value)) / 1_000_000
        except (TypeError, ValueError):
            return None

    def _get_statement_rows(self, symbol: str) -> list[CostStructureStatement]:
        codes = stock_code_candidates(symbol)
        placeholders = ",".join("?" for _ in codes)
        rows = self._reader.query(
            f"""
            WITH ranked AS (
                SELECT
                    code,
                    disclosed_date,
                    type_of_current_period,
                    sales,
                    operating_profit,
                    ROW_NUMBER() OVER (
                        PARTITION BY disclosed_date, type_of_current_period
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM statements
                WHERE code IN ({placeholders})
            )
            SELECT
                code,
                disclosed_date,
                type_of_current_period,
                sales,
                operating_profit
            FROM ranked
            WHERE rn = 1
            ORDER BY disclosed_date ASC
            """,
            tuple(codes),
        )

        statements: list[CostStructureStatement] = []
        for row in rows:
            period_type = str(row["type_of_current_period"] or "").strip()
            if period_type not in {"1Q", "2Q", "3Q", "FY"}:
                continue
            statements.append(
                CostStructureStatement(
                    disclosed_date=str(row["disclosed_date"]),
                    period_type=cast(SourcePeriodType, period_type),
                    sales=self._to_millions(row["sales"]),
                    operating_profit=(
                        self._to_millions(row["operating_profit"])
                    ),
                )
            )
        return statements

    @staticmethod
    def _build_effective_period_type(
        view: CostStructureAnalysisView,
        window_quarters: int,
    ) -> str:
        if view == "recent":
            return f"normalized_single_quarter_recent_{window_quarters}q"
        if view == "same_quarter":
            return "normalized_single_quarter_same_quarter"
        if view == "fiscal_year_only":
            return "fiscal_year_cumulative_only"
        return "normalized_single_quarter_all_history"

    @staticmethod
    def _to_response_point(point: DomainCostStructurePoint) -> CostStructurePoint:
        return CostStructurePoint(
            periodEnd=point.period_end,
            disclosedDate=point.disclosed_date,
            fiscalYear=point.fiscal_year,
            analysisPeriodType=point.analysis_period_type,
            sales=point.sales,
            operatingProfit=point.operating_profit,
            operatingMargin=point.operating_margin,
            isDerived=point.is_derived,
        )

    def analyze_stock(
        self,
        symbol: str,
        *,
        view: CostStructureAnalysisView = "recent",
        window_quarters: int = 12,
    ) -> CostStructureResponse:
        stock_row = self._get_stock_row(symbol)
        if stock_row is None:
            raise ValueError(f"Stock not found: {symbol}")

        statements = self._get_statement_rows(symbol)
        analysis = analyze_cost_structure(
            statements,
            view=view,
            window_quarters=window_quarters,
        )

        provenance = build_market_provenance(
            reference_date=analysis.latest_point.disclosed_date,
            loaded_domains=("statements", "stocks"),
            warnings=analysis.warnings,
        )

        return CostStructureResponse(
            symbol=symbol,
            companyName=str(stock_row["company_name"]) if stock_row["company_name"] is not None else None,
            points=[self._to_response_point(point) for point in analysis.points],
            latestPoint=self._to_response_point(analysis.latest_point),
            regression=CostStructureRegressionSummary(
                sampleCount=analysis.regression.sample_count,
                slope=analysis.regression.slope,
                intercept=analysis.regression.intercept,
                rSquared=analysis.regression.r_squared,
                contributionMarginRatio=analysis.regression.contribution_margin_ratio,
                variableCostRatio=analysis.regression.variable_cost_ratio,
                fixedCost=analysis.regression.fixed_cost,
                breakEvenSales=analysis.regression.break_even_sales,
            ),
            dateRange=CostStructureDateRange(
                **{
                    "from": analysis.date_from,
                    "to": analysis.date_to,
                }
            ),
            lastUpdated=datetime.now(UTC).isoformat(),
            provenance=provenance,
            diagnostics=ResponseDiagnostics(
                used_fields=["statements.sales", "statements.operating_profit"],
                effective_period_type=self._build_effective_period_type(view, window_quarters),
                warnings=analysis.warnings,
            ),
        )
