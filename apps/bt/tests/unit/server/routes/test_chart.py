"""Chart route structured error tests."""

from fastapi.testclient import TestClient

from src.application.services.market_data_errors import MarketDataError
from src.entrypoints.http.app import create_app
from src.entrypoints.http.schemas.chart import (
    IndexDataResponse,
    IndexInfo,
    IndexOHLCRecord,
    IndicesListResponse,
    SectorStockItem,
    SectorStocksResponse,
    StockSearchResponse,
    StockSearchResultItem,
)


class DummyChartService:
    def __init__(
        self,
        *,
        stock_error: MarketDataError | None = None,
        topix_error: MarketDataError | None = None,
        indices_list: IndicesListResponse | None = None,
        index_data: IndexDataResponse | None = None,
        search_result: StockSearchResponse | None = None,
        sector_stocks: SectorStocksResponse | None = None,
    ) -> None:
        self.stock_error = stock_error
        self.topix_error = topix_error
        self.indices_list = indices_list
        self.index_data = index_data
        self.search_result = search_result
        self.sector_stocks = sector_stocks

    def get_indices_list(self):  # noqa: ANN001, ANN201
        return self.indices_list

    async def get_topix_data(self, from_date=None, to_date=None):  # noqa: ANN001, ANN201
        del from_date, to_date
        if self.topix_error is None:
            raise AssertionError("topix_error must be configured in this test")
        raise self.topix_error

    def get_index_data(self, code):  # noqa: ANN001, ANN201
        del code
        return self.index_data

    def search_stocks(self, query, limit):  # noqa: ANN001, ANN201
        del query, limit
        if self.search_result is None:
            raise AssertionError("search_result must be configured in this test")
        return self.search_result

    async def get_stock_data(self, symbol, timeframe="daily", adjusted=True):  # noqa: ANN001, ANN201
        del symbol, timeframe, adjusted
        if self.stock_error is None:
            raise AssertionError("stock_error must be configured in this test")
        raise self.stock_error

    def get_sector_stocks(self, **kwargs):  # noqa: ANN003, ANN201
        del kwargs
        return self.sector_stocks


def _client_with_chart_service(service: DummyChartService) -> TestClient:
    app = create_app()
    app.state.chart_service = service
    return TestClient(app)


def test_stock_route_returns_local_stock_data_missing_details() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            stock_error=MarketDataError(
                "Stock data not available in local market snapshot",
                reason="local_stock_data_missing",
                recovery="stock_refresh",
            )
        )
    )

    response = client.get("/api/chart/stocks/7203")

    assert response.status_code == 404
    assert response.json()["message"] == "Stock data not available in local market snapshot"
    assert response.json()["details"] == [
        {"field": "reason", "message": "local_stock_data_missing"},
        {"field": "recovery", "message": "stock_refresh"},
    ]


def test_stock_route_returns_stock_not_found_details() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            stock_error=MarketDataError(
                "Stock symbol not found",
                reason="stock_not_found",
            )
        )
    )

    response = client.get("/api/chart/stocks/9999")

    assert response.status_code == 404
    assert response.json()["message"] == "Stock symbol not found"
    assert response.json()["details"] == [
        {"field": "reason", "message": "stock_not_found"},
    ]


def test_topix_route_returns_market_db_sync_details() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            topix_error=MarketDataError(
                "TOPIX data not available in local market snapshot",
                reason="topix_data_missing",
                recovery="market_db_sync",
            )
        )
    )

    response = client.get("/api/chart/indices/topix")

    assert response.status_code == 404
    assert response.json()["message"] == "TOPIX data not available in local market snapshot"
    assert response.json()["details"] == [
        {"field": "reason", "message": "topix_data_missing"},
        {"field": "recovery", "message": "market_db_sync"},
    ]


def test_indices_route_returns_payload() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            indices_list=IndicesListResponse(
                indices=[
                    IndexInfo(
                        code="TOPIX",
                        name="TOPIX",
                        nameEnglish="Tokyo Stock Price Index",
                        category="broad",
                        dataStartDate="2020-01-01",
                    )
                ],
                lastUpdated="2026-03-12T00:00:00Z",
            )
        )
    )

    response = client.get("/api/chart/indices")

    assert response.status_code == 200
    assert response.json()["indices"][0]["code"] == "TOPIX"


def test_index_route_returns_payload() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            index_data=IndexDataResponse(
                code="TOPIX",
                name="TOPIX",
                data=[
                    IndexOHLCRecord(
                        date="2026-03-12",
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.5,
                    )
                ],
                lastUpdated="2026-03-12T00:00:00Z",
            )
        )
    )

    response = client.get("/api/chart/indices/TOPIX")

    assert response.status_code == 200
    assert response.json()["code"] == "TOPIX"


def test_search_route_returns_payload() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            search_result=StockSearchResponse(
                query="toy",
                count=1,
                results=[
                    StockSearchResultItem(
                        code="7203",
                        companyName="Toyota Motor",
                        companyNameEnglish="TOYOTA MOTOR",
                        marketCode="0111",
                        marketName="Prime",
                        sector33Name="輸送用機器",
                    )
                ],
            )
        )
    )

    response = client.get("/api/chart/stocks/search?q=toy")

    assert response.status_code == 200
    assert response.json()["results"][0]["code"] == "7203"


def test_sector_stocks_route_returns_payload() -> None:
    client = _client_with_chart_service(
        DummyChartService(
            sector_stocks=SectorStocksResponse(
                sector33Name="輸送用機器",
                sector17Name="輸送･機器",
                markets=["prime"],
                lookbackDays=5,
                sortBy="tradingValue",
                sortOrder="desc",
                stocks=[
                    SectorStockItem(
                        rank=1,
                        code="7203",
                        companyName="Toyota Motor",
                        marketCode="0111",
                        sector33Name="輸送用機器",
                        currentPrice=100.0,
                        volume=1000.0,
                        tradingValue=100000.0,
                    )
                ],
                lastUpdated="2026-03-12T00:00:00Z",
            )
        )
    )

    response = client.get("/api/analytics/sector-stocks")

    assert response.status_code == 200
    assert response.json()["stocks"][0]["code"] == "7203"
