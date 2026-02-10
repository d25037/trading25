"""
ChartService branch coverage tests for fallback/error paths.
"""

import pytest

from src.server.services.chart_service import ChartService


class FakeReader:
    def __init__(self, query_one_results=None, query_results=None):
        self.query_one_results = list(query_one_results or [])
        self.query_results = list(query_results or [])

    def query_one(self, sql, params=()):  # noqa: ANN001, ANN201
        if self.query_one_results:
            return self.query_one_results.pop(0)
        return None

    def query(self, sql, params=()):  # noqa: ANN001, ANN201
        if self.query_results:
            return self.query_results.pop(0)
        return []


class FakeJQuants:
    def __init__(self, responses=None, raise_paths=None):
        self.responses = responses or {}
        self.raise_paths = set(raise_paths or [])
        self.calls = []

    async def get(self, path, params=None):  # noqa: ANN001, ANN201
        self.calls.append((path, params))
        if path in self.raise_paths:
            raise RuntimeError("jquants error")
        return self.responses.get(path, {})


class TestReaderNonePaths:
    def test_indices_return_none_when_reader_is_none(self):
        service = ChartService(None, FakeJQuants())
        assert service.get_indices_list() is None
        assert service.get_index_data("0000") is None

    def test_stock_from_db_returns_none_when_reader_is_none(self):
        service = ChartService(None, FakeJQuants())
        assert service._get_stock_from_db("7203", "daily") is None  # noqa: SLF001

    def test_sector_stocks_return_none_when_reader_is_none(self):
        service = ChartService(None, FakeJQuants())
        assert service.get_sector_stocks() is None


class TestTopixFallbackPaths:
    @pytest.mark.asyncio
    async def test_topix_falls_back_to_jquants_when_db_unavailable(self):
        jq = FakeJQuants(
            responses={
                "/indices/bars/daily/topix": {
                    "data": [{"Date": "2026-02-06", "O": 100, "H": 110, "L": 95, "C": 105}]
                }
            }
        )
        service = ChartService(None, jq)
        result = await service.get_topix_data("2026-01-01", "2026-02-06")
        assert result is not None
        assert len(result.topix) == 1
        assert jq.calls[0][0] == "/indices/bars/daily/topix"
        assert jq.calls[0][1] == {"from": "20260101", "to": "20260206"}

    @pytest.mark.asyncio
    async def test_topix_jquants_error_and_empty_return_none(self):
        service_err = ChartService(None, FakeJQuants(raise_paths={"/indices/bars/daily/topix"}))
        assert await service_err._get_topix_from_jquants(None, None) is None  # noqa: SLF001

        service_empty = ChartService(None, FakeJQuants(responses={"/indices/bars/daily/topix": {"data": []}}))
        assert await service_empty._get_topix_from_jquants(None, None) is None  # noqa: SLF001

    def test_topix_from_db_returns_none_on_empty(self):
        service = ChartService(FakeReader(query_results=[[]]), FakeJQuants())
        assert service._get_topix_from_db(None, None) is None  # noqa: SLF001


class TestStockFallbackPaths:
    @pytest.mark.asyncio
    async def test_get_stock_data_falls_back_to_jquants(self):
        reader = FakeReader(query_one_results=[None])
        jq = FakeJQuants(
            responses={
                "/equities/bars/daily": {"data": [{"Date": "2026-02-06", "AdjO": 10, "AdjH": 11, "AdjL": 9, "AdjC": 10.5, "AdjVo": 1000}]},
                "/equities/master": {"data": [{"CoName": "Test Corp"}]},
            }
        )
        service = ChartService(reader, jq)
        result = await service.get_stock_data("7203", "daily", adjusted=True)
        assert result is not None
        assert result.companyName == "Test Corp"

    @pytest.mark.asyncio
    async def test_stock_from_jquants_handles_error_empty_and_unadjusted(self):
        service_err = ChartService(None, FakeJQuants(raise_paths={"/equities/bars/daily"}))
        assert await service_err._get_stock_from_jquants("7203", "daily", adjusted=True) is None  # noqa: SLF001

        service_empty = ChartService(None, FakeJQuants(responses={"/equities/bars/daily": {"data": []}}))
        assert await service_empty._get_stock_from_jquants("7203", "daily", adjusted=True) is None  # noqa: SLF001

        service_unadj = ChartService(
            None,
            FakeJQuants(
                responses={
                    "/equities/bars/daily": {
                        "data": [
                            {"Date": "2026-02-05", "O": 10, "H": 11, "L": 9, "C": 0, "Vo": 1000},
                            {"Date": "2026-02-06", "O": 10, "H": 11, "L": 9, "C": 10.5, "Vo": 2000},
                        ]
                    },
                    "/equities/master": {"data": []},
                }
            ),
        )
        result = await service_unadj._get_stock_from_jquants("7203", "daily", adjusted=False)  # noqa: SLF001
        assert result is not None
        assert len(result.data) == 1

    def test_stock_from_db_returns_none_when_rows_empty(self):
        reader = FakeReader(query_one_results=[{"code": "72030", "company_name": "Toyota"}], query_results=[[]])
        service = ChartService(reader, FakeJQuants())
        assert service._get_stock_from_db("7203", "daily") is None  # noqa: SLF001


class TestMiscBranches:
    def test_search_stocks_returns_empty_for_blank_query(self):
        service = ChartService(FakeReader(), FakeJQuants())
        result = service.search_stocks("   ")
        assert result.count == 0

    def test_sector_stocks_returns_none_when_no_latest_date(self):
        reader = FakeReader(query_one_results=[{"max_date": None}])
        service = ChartService(reader, FakeJQuants())
        assert service.get_sector_stocks() is None

    def test_sector_stocks_with_base_date_and_sector17_filter(self):
        reader = FakeReader(
            query_one_results=[
                {"max_date": "2026-02-06"},
                {"date": "2026-02-05"},
                {"date": "2026-01-16"},
            ],
            query_results=[
                [
                    {
                        "code": "72030",
                        "company_name": "トヨタ自動車",
                        "market_code": "prime",
                        "sector_33_name": "輸送用機器",
                        "current_price": 2500.0,
                        "volume": 1000000,
                        "trading_value": 2.5e9,
                        "base_price": 2480.0,
                        "change_amount": 20.0,
                        "change_percentage": 0.81,
                    }
                ]
            ],
        )
        service = ChartService(reader, FakeJQuants())
        result = service.get_sector_stocks(sector17_name="輸送･機器", markets="prime", lookback_days=1)
        assert result is not None
        assert len(result.stocks) == 1
        assert result.stocks[0].basePrice == 2480.0
