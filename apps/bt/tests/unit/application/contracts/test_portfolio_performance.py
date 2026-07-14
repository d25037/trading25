from src.application.contracts.portfolio_performance import (
    BenchmarkResult,
    BenchmarkTimeSeriesPoint,
    DateRange,
    HoldingDetail,
    PerformanceSummary,
    PortfolioPerformanceResponse,
    TimeSeriesPoint,
)


def _complete_response() -> PortfolioPerformanceResponse:
    return PortfolioPerformanceResponse(
        portfolioId=1,
        portfolioName="Core",
        portfolioDescription=None,
        summary=PerformanceSummary(
            totalCost=1000.0,
            currentValue=1100.0,
            totalPnL=100.0,
            returnRate=10.0,
        ),
        holdings=[
            HoldingDetail(
                code="7203",
                companyName="Toyota Motor",
                quantity=10,
                purchasePrice=100.0,
                currentPrice=110.0,
                cost=1000.0,
                marketValue=1100.0,
                pnl=100.0,
                returnRate=10.0,
                weight=100.0,
                purchaseDate="2026-01-05",
                account=None,
            )
        ],
        timeSeries=[
            TimeSeriesPoint(
                date="2026-01-05",
                dailyReturn=1.0,
                cumulativeReturn=10.0,
            )
        ],
        benchmark=BenchmarkResult(
            code="TOPIX",
            name="TOPIX",
            beta=1.0,
            alpha=0.1,
            correlation=0.9,
            rSquared=0.81,
            benchmarkReturn=8.0,
            relativeReturn=2.0,
        ),
        benchmarkTimeSeries=[
            BenchmarkTimeSeriesPoint(
                date="2026-01-05",
                portfolioReturn=10.0,
                benchmarkReturn=8.0,
            )
        ],
        analysisDate="2026-07-14",
        dateRange=DateRange(from_="2026-01-05", to="2026-07-14"),
        dataPoints=1,
        warnings=[],
    )


def test_complete_portfolio_performance_response_and_date_range_alias() -> None:
    response = _complete_response()
    from_alias = DateRange(**{"from": "2026-01-05"}, to="2026-07-14")
    from_name = DateRange(from_="2026-01-05", to="2026-07-14")

    assert response.analysisDate == "2026-07-14"
    assert response.holdings[0].account is None
    assert response.portfolioDescription is None
    assert response.dateRange is not None
    assert from_alias == from_name
    for date_range in (from_alias, from_name):
        dumped = date_range.model_dump(by_alias=True)
        assert dumped["from"] == "2026-01-05"
        assert "from_" not in dumped


def test_portfolio_performance_model_property_and_required_orders() -> None:
    expected_properties = {
        PerformanceSummary: ["totalCost", "currentValue", "totalPnL", "returnRate"],
        HoldingDetail: [
            "code",
            "companyName",
            "quantity",
            "purchasePrice",
            "currentPrice",
            "cost",
            "marketValue",
            "pnl",
            "returnRate",
            "weight",
            "purchaseDate",
            "account",
        ],
        TimeSeriesPoint: ["date", "dailyReturn", "cumulativeReturn"],
        BenchmarkResult: [
            "code",
            "name",
            "beta",
            "alpha",
            "correlation",
            "rSquared",
            "benchmarkReturn",
            "relativeReturn",
        ],
        BenchmarkTimeSeriesPoint: [
            "date",
            "portfolioReturn",
            "benchmarkReturn",
        ],
        DateRange: ["from", "to"],
        PortfolioPerformanceResponse: [
            "portfolioId",
            "portfolioName",
            "portfolioDescription",
            "summary",
            "holdings",
            "timeSeries",
            "benchmark",
            "benchmarkTimeSeries",
            "analysisDate",
            "dateRange",
            "dataPoints",
            "warnings",
        ],
    }

    for model, property_order in expected_properties.items():
        assert list(model.model_json_schema()["properties"]) == property_order

    assert PortfolioPerformanceResponse.model_json_schema()["required"] == [
        "portfolioId",
        "portfolioName",
        "summary",
        "holdings",
        "timeSeries",
        "analysisDate",
        "dataPoints",
        "warnings",
    ]


def test_portfolio_responses_do_not_share_list_values() -> None:
    first = _complete_response()
    second = _complete_response()

    first.holdings.clear()
    first.timeSeries.clear()
    assert first.benchmarkTimeSeries is not None
    first.benchmarkTimeSeries.clear()
    first.warnings.append("changed")

    assert len(second.holdings) == 1
    assert len(second.timeSeries) == 1
    assert second.benchmarkTimeSeries is not None
    assert len(second.benchmarkTimeSeries) == 1
    assert second.warnings == []
