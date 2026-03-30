"""ChartService branch coverage tests."""

import pytest

from src.application.services.chart_service import ChartService
from src.application.services.market_data_errors import MarketDataError


class FakeReader:
    def __init__(self, query_one_results=None, query_results=None):
        self.query_one_results = list(query_one_results or [])
        self.query_results = list(query_results or [])

    def query_one(self, sql, params=()):  # noqa: ANN001, ANN201
        del sql, params
        if self.query_one_results:
            return self.query_one_results.pop(0)
        return None

    def query(self, sql, params=()):  # noqa: ANN001, ANN201
        del sql, params
        if self.query_results:
            return self.query_results.pop(0)
        return []


class TestReaderNonePaths:
    def test_indices_return_none_when_reader_is_none(self):
        service = ChartService(None)
        assert service.get_indices_list() is None
        assert service.get_index_data("0000") is None

    def test_stock_from_db_raises_when_reader_is_none(self):
        service = ChartService(None)
        with pytest.raises(MarketDataError, match="ローカルOHLCVデータがありません") as exc_info:
            service._get_stock_from_db("7203", "daily")  # noqa: SLF001
        assert exc_info.value.reason == "local_stock_data_missing"
        assert exc_info.value.recovery == "market_db_sync"

    def test_sector_stocks_return_none_when_reader_is_none(self):
        service = ChartService(None)
        assert service.get_sector_stocks() is None


class TestTopixPaths:
    @pytest.mark.asyncio
    async def test_topix_raises_when_db_unavailable(self):
        service = ChartService(None)
        with pytest.raises(MarketDataError, match="TOPIX のローカルデータがありません") as exc_info:
            await service.get_topix_data("2026-01-01", "2026-02-06")
        assert exc_info.value.reason == "topix_data_missing"
        assert exc_info.value.recovery == "market_db_sync"

    def test_topix_from_db_raises_on_empty(self):
        service = ChartService(FakeReader(query_results=[[]]))
        with pytest.raises(MarketDataError, match="TOPIX のローカルデータがありません") as exc_info:
            service._get_topix_from_db(None, None)  # noqa: SLF001
        assert exc_info.value.reason == "topix_data_missing"
        assert exc_info.value.recovery == "market_db_sync"


class TestStockPaths:
    @pytest.mark.asyncio
    async def test_get_stock_data_returns_db_rows_only(self):
        reader = FakeReader(
            query_one_results=[{"code": "7203", "company_name": "Test Corp"}],
            query_results=[
                [
                    {
                        "date": "2026-02-06",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "volume": 1000,
                    }
                ]
            ],
        )
        service = ChartService(reader)
        result = await service.get_stock_data("7203", "daily", adjusted=True)
        assert result is not None
        assert result.companyName == "Test Corp"
        assert len(result.data) == 1

    @pytest.mark.asyncio
    async def test_get_stock_data_uses_stock_data_when_stock_metadata_is_missing(self):
        reader = FakeReader(
            query_one_results=[None],
            query_results=[
                [
                    {
                        "date": "2026-02-06",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "volume": 1000,
                    }
                ]
            ],
        )
        service = ChartService(reader)

        result = await service.get_stock_data("7203", "daily", adjusted=True)

        assert result.companyName == ""
        assert len(result.data) == 1

    def test_stock_from_db_raises_when_rows_empty(self):
        reader = FakeReader(
            query_one_results=[{"code": "72030", "company_name": "Toyota"}],
            query_results=[[]],
        )
        service = ChartService(reader)
        with pytest.raises(MarketDataError, match="ローカルOHLCVデータがありません") as exc_info:
            service._get_stock_from_db("7203", "daily")  # noqa: SLF001
        assert exc_info.value.reason == "local_stock_data_missing"
        assert exc_info.value.recovery == "stock_refresh"

    def test_has_stock_metadata_checks_stocks_table(self):
        reader = FakeReader(query_one_results=[{"code": "7203"}])
        service = ChartService(reader)
        assert service.has_stock_metadata("7203") is True

    def test_has_stock_metadata_returns_false_when_stock_missing(self):
        service = ChartService(FakeReader())
        assert service.has_stock_metadata("9999") is False


class TestMiscBranches:
    def test_get_indices_list_builds_response(self):
        reader = FakeReader(
            query_one_results=[None],
            query_results=[
                [
                    {
                        "code": "TOPIX",
                        "name": "TOPIX",
                        "name_english": "Tokyo Stock Price Index",
                        "category": "broad",
                        "data_start_date": "2020-01-01",
                    }
                ]
            ]
        )
        service = ChartService(reader)

        result = service.get_indices_list()

        assert result is not None
        assert result.indices[0].code == "TOPIX"
        assert result.indices[0].nameEnglish == "Tokyo Stock Price Index"

    def test_get_indices_list_appends_nt_ratio_when_local_data_exists(self):
        reader = FakeReader(
            query_one_results=[
                {"data_start_date": "2026-02-06"},
                {"data_start_date": "2026-02-06"},
            ],
            query_results=[
                [
                    {
                        "code": "N225_UNDERPX",
                        "name": "日経平均",
                        "name_english": "Nikkei 225 (UnderPx derived)",
                        "category": "synthetic",
                        "data_start_date": "2026-02-06",
                    }
                ]
            ],
        )
        service = ChartService(reader)

        result = service.get_indices_list()

        assert result is not None
        assert [item.code for item in result.indices] == ["N225_UNDERPX", "N225_VI", "NT_RATIO"]
        assert result.indices[1].name == "日経VI"
        assert result.indices[1].dataStartDate == "2026-02-06"
        assert result.indices[2].name == "NT倍率"
        assert result.indices[2].dataStartDate == "2026-02-06"

    def test_get_index_data_builds_response(self):
        reader = FakeReader(
            query_one_results=[{"code": "TOPIX", "name": "TOPIX"}],
            query_results=[
                [
                    {
                        "date": "2026-02-06",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                    }
                ]
            ],
        )
        service = ChartService(reader)

        result = service.get_index_data("TOPIX")

        assert result is not None
        assert result.code == "TOPIX"
        assert len(result.data) == 1
        assert result.data[0].close == 100.5

    def test_get_index_data_builds_nt_ratio_response(self):
        reader = FakeReader(
            query_results=[
                [
                    {
                        "date": "2026-02-06",
                        "value": 14.1284,
                    }
                ]
            ],
        )
        service = ChartService(reader)

        result = service.get_index_data("NT_RATIO")

        assert result is not None
        assert result.code == "NT_RATIO"
        assert result.name == "NT倍率"
        assert len(result.data) == 1
        assert result.data[0].open == 14.1284
        assert result.data[0].close == 14.1284

    def test_get_index_data_builds_vi_response(self):
        reader = FakeReader(
            query_results=[
                [
                    {
                        "date": "2026-02-06",
                        "value": 23.456789,
                    }
                ]
            ],
        )
        service = ChartService(reader)

        result = service.get_index_data("N225_VI")

        assert result is not None
        assert result.code == "N225_VI"
        assert result.name == "日経VI"
        assert len(result.data) == 1
        assert result.data[0].open == 23.456789
        assert result.data[0].close == 23.456789

    def test_search_stocks_returns_ranked_results(self):
        reader = FakeReader(
            query_results=[
                [
                    {
                        "code": "7203",
                        "company_name": "Toyota Motor",
                        "company_name_english": "TOYOTA MOTOR",
                        "market_code": "0111",
                        "market_name": "Prime",
                        "sector_33_name": "輸送用機器",
                    }
                ]
            ]
        )
        service = ChartService(reader)

        result = service.search_stocks("toy")

        assert result.count == 1
        assert result.results[0].code == "7203"
        assert result.results[0].marketCode == "0111"

    def test_search_stocks_returns_empty_for_blank_query(self):
        service = ChartService(FakeReader())
        result = service.search_stocks("   ")
        assert result.count == 0

    def test_sector_stocks_returns_none_when_no_latest_date(self):
        reader = FakeReader(query_one_results=[{"max_date": None}])
        service = ChartService(reader)
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
        service = ChartService(reader)
        result = service.get_sector_stocks(
            sector17_name="輸送･機器",
            markets="prime",
            lookback_days=1,
        )
        assert result is not None
        assert len(result.stocks) == 1
        assert result.stocks[0].basePrice == 2480.0
