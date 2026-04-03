"""Schemas for cost structure analysis."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from src.entrypoints.http.schemas.analytics_common import DataProvenance, ResponseDiagnostics


class CostStructureDateRange(BaseModel):
    """Date range used in cost structure analysis."""

    from_: str = Field(alias="from")
    to: str

    model_config = {"populate_by_name": True}


class CostStructurePoint(BaseModel):
    """Single cost structure analysis point."""

    periodEnd: str = Field(..., description="Source period end surrogate date (YYYY-MM-DD)")
    disclosedDate: str = Field(..., description="Disclosure date (YYYY-MM-DD)")
    fiscalYear: str = Field(..., description="Inferred fiscal year label")
    analysisPeriodType: Literal["1Q", "2Q", "3Q", "4Q", "FY"] = Field(
        ...,
        description="Analysis period type (single-quarter normalized or fiscal-year cumulative)",
    )
    sales: float = Field(..., description="Analysis sales value (millions JPY)")
    operatingProfit: float = Field(..., description="Analysis operating profit value (millions JPY)")
    operatingMargin: float | None = Field(None, description="Operating margin (%)")
    isDerived: bool = Field(..., description="Whether point was derived from cumulative diff")


class CostStructureRegressionSummary(BaseModel):
    """Regression summary for cost structure analysis."""

    sampleCount: int = Field(..., description="Number of normalized points used for regression")
    slope: float = Field(..., description="Regression slope")
    intercept: float = Field(..., description="Regression intercept")
    rSquared: float = Field(..., description="Coefficient of determination (0-1)")
    contributionMarginRatio: float = Field(..., description="Contribution margin ratio from slope")
    variableCostRatio: float = Field(..., description="Variable cost ratio (= 1 - slope)")
    fixedCost: float | None = Field(None, description="Estimated fixed cost (millions JPY)")
    breakEvenSales: float | None = Field(None, description="Estimated break-even sales (millions JPY)")


class CostStructureResponse(BaseModel):
    """Cost structure analysis response."""

    symbol: str = Field(..., description="Stock code")
    companyName: str | None = Field(None, description="Company name")
    points: list[CostStructurePoint] = Field(..., description="Analysis points for the selected view")
    latestPoint: CostStructurePoint = Field(..., description="Most recent normalized point")
    regression: CostStructureRegressionSummary
    dateRange: CostStructureDateRange
    lastUpdated: str = Field(..., description="Last updated timestamp (ISO 8601)")
    provenance: DataProvenance
    diagnostics: ResponseDiagnostics = Field(default_factory=ResponseDiagnostics)
