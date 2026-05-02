from __future__ import annotations

import pytest

from src.application.services.cost_structure_analysis_service import CostStructureAnalysisService


class _FakeReader:
    def query_one(self, sql: str, params: tuple[object, ...] = ()) -> dict[str, object] | None:
        if "FROM stocks" in sql:
            return {
                "code": "7203",
                "company_name": "Toyota Motor",
            }
        raise AssertionError(f"Unexpected query_one SQL: {sql} / {params}")

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        if "FROM ranked" in sql:
            return [
                {
                    "code": "7203",
                    "disclosed_date": "2024-05-08",
                    "type_of_current_period": "1Q",
                    "sales": 300_000_000,
                    "operating_profit": 30_000_000,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2024-08-06",
                    "type_of_current_period": "2Q",
                    "sales": 700_000_000,
                    "operating_profit": 80_000_000,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2024-11-05",
                    "type_of_current_period": "3Q",
                    "sales": 1_200_000_000,
                    "operating_profit": 150_000_000,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2025-05-08",
                    "type_of_current_period": "FY",
                    "sales": 2_000_000_000,
                    "operating_profit": 260_000_000,
                },
            ]
        raise AssertionError(f"Unexpected query SQL: {sql} / {params}")


class _BlankPeriodReader(_FakeReader):
    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        rows = super().query(sql, params)
        return [
            *rows,
            {
                "code": "7203",
                "disclosed_date": "2025-08-07",
                "type_of_current_period": " ",
                "sales": 2_500_000_000,
                "operating_profit": None,
            },
        ]


class _ForecastRevisionReader(_FakeReader):
    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        rows = super().query(sql, params)
        return [
            *rows,
            {
                "code": "7203",
                "disclosed_date": "2025-06-01",
                "type_of_document": "EarnForecastRevision",
                "type_of_current_period": "FY",
                "sales": 9_999_000_000,
                "operating_profit": 9_999_000_000,
            },
        ]


class _MissingStockReader(_FakeReader):
    def query_one(self, sql: str, params: tuple[object, ...] = ()) -> dict[str, object] | None:
        if "FROM stocks" in sql:
            return None
        return super().query_one(sql, params)


class _MultiFyReader(_FakeReader):
    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        if "FROM ranked" in sql:
            return [
                {
                    "code": "7203",
                    "disclosed_date": "2022-05-08",
                    "type_of_current_period": "FY",
                    "sales": 1_800_000_000,
                    "operating_profit": 180_000_000,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2023-05-08",
                    "type_of_current_period": "FY",
                    "sales": 2_000_000_000,
                    "operating_profit": 240_000_000,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2024-05-08",
                    "type_of_current_period": "FY",
                    "sales": 2_300_000_000,
                    "operating_profit": 310_000_000,
                },
            ]
        return super().query(sql, params)


def test_analyze_stock_converts_statement_amounts_to_millions() -> None:
    service = CostStructureAnalysisService(_FakeReader())

    result = service.analyze_stock("7203")

    assert result.symbol == "7203"
    assert result.companyName == "Toyota Motor"
    assert result.points[0].sales == 300.0
    assert result.points[0].operatingProfit == 30.0
    assert result.points[1].sales == 400.0
    assert result.points[1].operatingProfit == 50.0
    assert result.latestPoint.analysisPeriodType == "4Q"
    assert result.latestPoint.sales == 800.0
    assert result.latestPoint.operatingProfit == 110.0
    assert result.diagnostics.effective_period_type == "normalized_single_quarter_recent_12q"


def test_get_statement_rows_skips_blank_period_types() -> None:
    service = CostStructureAnalysisService(_BlankPeriodReader())

    rows = service._get_statement_rows("7203")

    assert len(rows) == 4
    assert all(row.period_type.strip() for row in rows)


def test_get_statement_rows_skips_forecast_revision_documents() -> None:
    service = CostStructureAnalysisService(_ForecastRevisionReader())

    rows = service._get_statement_rows("7203")

    assert len(rows) == 4
    assert all(row.sales != 9999.0 for row in rows)


def test_to_millions_returns_none_for_null() -> None:
    assert CostStructureAnalysisService._to_millions(None) is None


@pytest.mark.parametrize(
    ("view", "window_quarters", "expected"),
    [
        ("recent", 20, "normalized_single_quarter_recent_20q"),
        ("same_quarter", 12, "normalized_single_quarter_same_quarter"),
        ("fiscal_year_only", 12, "fiscal_year_cumulative_only"),
        ("all", 12, "normalized_single_quarter_all_history"),
    ],
)
def test_build_effective_period_type_variants(
    view: str,
    window_quarters: int,
    expected: str,
) -> None:
    assert (
        CostStructureAnalysisService._build_effective_period_type(view, window_quarters)  # type: ignore[arg-type]
        == expected
    )


def test_analyze_stock_raises_when_stock_is_missing() -> None:
    service = CostStructureAnalysisService(_MissingStockReader())

    with pytest.raises(ValueError, match="Stock not found: 9999"):
        service.analyze_stock("9999")


def test_analyze_stock_fiscal_year_only_uses_cumulative_fy_values() -> None:
    service = CostStructureAnalysisService(_MultiFyReader())

    result = service.analyze_stock("7203", view="fiscal_year_only")

    assert [point.analysisPeriodType for point in result.points] == ["FY", "FY", "FY"]
    assert [point.sales for point in result.points] == [1800.0, 2000.0, 2300.0]
    assert result.latestPoint.analysisPeriodType == "FY"
    assert result.latestPoint.sales == 2300.0
    assert result.diagnostics.effective_period_type == "fiscal_year_cumulative_only"
