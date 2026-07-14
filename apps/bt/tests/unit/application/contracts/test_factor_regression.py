from src.application.contracts import factor_regression as factor_contracts
from src.application.contracts import portfolio_factor_regression as portfolio_contracts


def _assert_field_and_schema_orders(
    model: type,
    *,
    fields: list[str],
    properties: list[str] | None = None,
    required: list[str],
) -> None:
    schema = model.model_json_schema()

    assert list(model.model_fields) == fields
    assert list(schema["properties"]) == (properties or fields)
    assert schema["required"] == required


def test_stock_factor_regression_contract_shape_is_frozen() -> None:
    _assert_field_and_schema_orders(
        factor_contracts.DateRange,
        fields=["from_", "to"],
        properties=["from", "to"],
        required=["from", "to"],
    )
    _assert_field_and_schema_orders(
        factor_contracts.IndexMatch,
        fields=["indexCode", "indexName", "category", "rSquared", "beta"],
        required=["indexCode", "indexName", "category", "rSquared", "beta"],
    )
    _assert_field_and_schema_orders(
        factor_contracts.FactorRegressionResponse,
        fields=[
            "stockCode",
            "companyName",
            "marketBeta",
            "marketRSquared",
            "sector17Matches",
            "sector33Matches",
            "topixStyleMatches",
            "analysisDate",
            "dataPoints",
            "dateRange",
        ],
        required=[
            "stockCode",
            "marketBeta",
            "marketRSquared",
            "sector17Matches",
            "sector33Matches",
            "topixStyleMatches",
            "analysisDate",
            "dataPoints",
            "dateRange",
        ],
    )


def test_portfolio_factor_regression_contract_shape_is_frozen() -> None:
    _assert_field_and_schema_orders(
        portfolio_contracts.StockWeight,
        fields=["code", "companyName", "weight", "latestPrice", "marketValue", "quantity"],
        required=["code", "companyName", "weight", "latestPrice", "marketValue", "quantity"],
    )
    _assert_field_and_schema_orders(
        portfolio_contracts.ExcludedStock,
        fields=["code", "companyName", "reason"],
        required=["code", "companyName", "reason"],
    )
    _assert_field_and_schema_orders(
        portfolio_contracts.IndexMatch,
        fields=["code", "name", "rSquared"],
        required=["code", "name", "rSquared"],
    )
    _assert_field_and_schema_orders(
        portfolio_contracts.DateRange,
        fields=["from_", "to"],
        properties=["from", "to"],
        required=["from", "to"],
    )
    response_fields = [
        "portfolioId",
        "portfolioName",
        "weights",
        "totalValue",
        "stockCount",
        "includedStockCount",
        "marketBeta",
        "marketRSquared",
        "sector17Matches",
        "sector33Matches",
        "topixStyleMatches",
        "analysisDate",
        "dataPoints",
        "dateRange",
        "excludedStocks",
    ]
    _assert_field_and_schema_orders(
        portfolio_contracts.PortfolioFactorRegressionResponse,
        fields=response_fields,
        required=response_fields,
    )


def test_factor_regression_contract_docstrings_are_frozen() -> None:
    assert factor_contracts.DateRange.__doc__ == "分析期間"
    assert factor_contracts.IndexMatch.__doc__ == "指数マッチ結果"
    assert factor_contracts.FactorRegressionResponse.__doc__ == "ファクター回帰分析レスポンス"
    assert portfolio_contracts.StockWeight.__doc__ is None
    assert portfolio_contracts.ExcludedStock.__doc__ is None
    assert portfolio_contracts.IndexMatch.__doc__ is None
    assert portfolio_contracts.DateRange.__doc__ is None
    assert portfolio_contracts.PortfolioFactorRegressionResponse.__doc__ is None


def test_stock_factor_regression_nested_serialization_is_frozen() -> None:
    response = factor_contracts.FactorRegressionResponse(
        stockCode="7203",
        companyName="Toyota Motor",
        marketBeta=1.15,
        marketRSquared=0.81,
        sector17Matches=[
            factor_contracts.IndexMatch(
                indexCode="TOPIX-17-AUTO",
                indexName="Automobiles & Transportation Equipment",
                category="sector17",
                rSquared=0.72,
                beta=1.08,
            )
        ],
        sector33Matches=[
            factor_contracts.IndexMatch(
                indexCode="TOPIX-33-TRANSPORT",
                indexName="Transportation Equipment",
                category="sector33",
                rSquared=0.69,
                beta=1.04,
            )
        ],
        topixStyleMatches=[
            factor_contracts.IndexMatch(
                indexCode="TOPIX-LARGE70",
                indexName="TOPIX Large70",
                category="topix_style",
                rSquared=0.77,
                beta=0.97,
            )
        ],
        analysisDate="2026-07-14",
        dataPoints=250,
        dateRange=factor_contracts.DateRange(from_="2025-07-14", to="2026-07-14"),
    )

    assert response.model_dump(by_alias=True) == {
        "stockCode": "7203",
        "companyName": "Toyota Motor",
        "marketBeta": 1.15,
        "marketRSquared": 0.81,
        "sector17Matches": [
            {
                "indexCode": "TOPIX-17-AUTO",
                "indexName": "Automobiles & Transportation Equipment",
                "category": "sector17",
                "rSquared": 0.72,
                "beta": 1.08,
            }
        ],
        "sector33Matches": [
            {
                "indexCode": "TOPIX-33-TRANSPORT",
                "indexName": "Transportation Equipment",
                "category": "sector33",
                "rSquared": 0.69,
                "beta": 1.04,
            }
        ],
        "topixStyleMatches": [
            {
                "indexCode": "TOPIX-LARGE70",
                "indexName": "TOPIX Large70",
                "category": "topix_style",
                "rSquared": 0.77,
                "beta": 0.97,
            }
        ],
        "analysisDate": "2026-07-14",
        "dataPoints": 250,
        "dateRange": {"from": "2025-07-14", "to": "2026-07-14"},
    }


def test_portfolio_factor_regression_nested_serialization_and_mutability_are_frozen() -> None:
    weight = portfolio_contracts.StockWeight(
        code="7203",
        companyName="Toyota Motor",
        weight=0.6,
        latestPrice=3100.0,
        marketValue=1_860_000.0,
        quantity=600,
    )
    weight.weight = 0.625
    response = portfolio_contracts.PortfolioFactorRegressionResponse(
        portfolioId=42,
        portfolioName="Core Japan",
        weights=[weight],
        totalValue=2_976_000.0,
        stockCount=2,
        includedStockCount=1,
        marketBeta=0.91,
        marketRSquared=0.74,
        sector17Matches=[
            portfolio_contracts.IndexMatch(code="S17-08", name="Automobiles", rSquared=0.66)
        ],
        sector33Matches=[
            portfolio_contracts.IndexMatch(code="S33-19", name="Transportation", rSquared=0.63)
        ],
        topixStyleMatches=[
            portfolio_contracts.IndexMatch(code="TOPIX100", name="TOPIX 100", rSquared=0.71)
        ],
        analysisDate="2026-07-14",
        dataPoints=240,
        dateRange=portfolio_contracts.DateRange(**{"from": "2025-07-14", "to": "2026-07-14"}),
        excludedStocks=[
            portfolio_contracts.ExcludedStock(
                code="9999",
                companyName="No History Co.",
                reason="Insufficient price history",
            )
        ],
    )

    assert weight.weight == 0.625
    assert response.model_dump(by_alias=True) == {
        "portfolioId": 42,
        "portfolioName": "Core Japan",
        "weights": [
            {
                "code": "7203",
                "companyName": "Toyota Motor",
                "weight": 0.625,
                "latestPrice": 3100.0,
                "marketValue": 1_860_000.0,
                "quantity": 600,
            }
        ],
        "totalValue": 2_976_000.0,
        "stockCount": 2,
        "includedStockCount": 1,
        "marketBeta": 0.91,
        "marketRSquared": 0.74,
        "sector17Matches": [{"code": "S17-08", "name": "Automobiles", "rSquared": 0.66}],
        "sector33Matches": [{"code": "S33-19", "name": "Transportation", "rSquared": 0.63}],
        "topixStyleMatches": [{"code": "TOPIX100", "name": "TOPIX 100", "rSquared": 0.71}],
        "analysisDate": "2026-07-14",
        "dataPoints": 240,
        "dateRange": {"from": "2025-07-14", "to": "2026-07-14"},
        "excludedStocks": [
            {
                "code": "9999",
                "companyName": "No History Co.",
                "reason": "Insufficient price history",
            }
        ],
    }


def test_date_ranges_accept_field_name_and_alias_and_serialize_alias() -> None:
    for model in (factor_contracts.DateRange, portfolio_contracts.DateRange):
        from_field_name = model(from_="2025-01-01", to="2025-12-31")
        from_alias = model(**{"from": "2024-01-01", "to": "2024-12-31"})

        assert from_field_name.model_dump(by_alias=True) == {
            "from": "2025-01-01",
            "to": "2025-12-31",
        }
        assert from_alias.from_ == "2024-01-01"
        assert from_alias.model_dump(by_alias=True) == {
            "from": "2024-01-01",
            "to": "2024-12-31",
        }
