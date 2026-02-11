"""
MarketDataService Unit Tests
"""

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.market_data_service import MarketDataService, _stock_code_candidates


class TestStockCodeCandidates:
    def test_4digit_candidates(self):
        assert _stock_code_candidates("7203") == ("7203", "72030")

    def test_5digit_candidates(self):
        assert _stock_code_candidates("72030") == ("7203", "72030")

    def test_3digit_candidates(self):
        assert _stock_code_candidates("123") == ("123",)


@pytest.fixture
def service(market_db_path):
    """テスト用 MarketDataService"""
    reader = MarketDbReader(market_db_path)
    svc = MarketDataService(reader)
    yield svc
    reader.close()


class TestGetStockInfo:
    def test_found_4digit(self, service):
        """4桁コードで銘柄情報取得"""
        result = service.get_stock_info("7203")
        assert result is not None
        assert result.code == "72030"
        assert result.companyName == "トヨタ自動車"
        assert result.companyNameEnglish == "TOYOTA MOTOR"
        assert result.marketCode == "prime"
        assert result.sector17Code == "S17_1"
        assert result.listedDate == "1949-05-16"

    def test_found_5digit(self, service):
        """5桁コードで銘柄情報取得"""
        result = service.get_stock_info("72030")
        assert result is not None
        assert result.code == "72030"

    def test_not_found(self, service):
        """存在しない銘柄"""
        result = service.get_stock_info("0000")
        assert result is None

    def test_null_scale_category(self, service):
        """scale_category が NULL の場合"""
        result = service.get_stock_info("9984")
        assert result is not None
        assert result.scaleCategory == ""


class TestGetStockOhlcv:
    def test_all_data(self, service):
        """全期間の OHLCV データ取得"""
        result = service.get_stock_ohlcv("7203")
        assert result is not None
        assert len(result) == 3
        assert result[0].date == "2024-01-15"
        assert result[0].volume == 1000000
        assert isinstance(result[0].volume, int)

    def test_with_date_range(self, service):
        """日付範囲指定"""
        result = service.get_stock_ohlcv("7203", start_date="2024-01-16", end_date="2024-01-16")
        assert result is not None
        assert len(result) == 1
        assert result[0].date == "2024-01-16"

    def test_stock_not_found(self, service):
        """存在しない銘柄は None"""
        result = service.get_stock_ohlcv("0000")
        assert result is None

    def test_no_data_in_range(self, service):
        """範囲内にデータなし → 空リスト"""
        result = service.get_stock_ohlcv("7203", start_date="2025-01-01")
        assert result is not None
        assert len(result) == 0

    def test_ordered_by_date(self, service):
        """日付昇順"""
        result = service.get_stock_ohlcv("7203")
        assert result is not None
        dates = [r.date for r in result]
        assert dates == sorted(dates)

    def test_handles_null_volume_as_zero(self):
        """volume が NULL でも 0 にフォールバックして返す"""
        class MockReader:
            def query_one(self, _sql, _params=()):
                return {"code": "285A0"}

            def query(self, _sql, _params=()):
                return [
                    {
                        "date": "2024-01-15",
                        "open": 100.0,
                        "high": 110.0,
                        "low": 95.0,
                        "close": 105.0,
                        "volume": None,
                    }
                ]

        svc = MarketDataService(MockReader())  # type: ignore[arg-type]
        result = svc.get_stock_ohlcv("285A")
        assert result is not None
        assert result[0].date == "2024-01-15"
        assert result[0].volume == 0


class TestGetAllStocks:
    def test_prime_market(self, service):
        """プライム市場の全銘柄データ"""
        result = service.get_all_stocks(market="prime", history_days=300)
        assert result is not None
        assert len(result) == 2  # 72030 + 67580
        codes = [s.code for s in result]
        assert "72030" in codes
        assert "67580" in codes

    def test_standard_market(self, service):
        """スタンダード市場"""
        result = service.get_all_stocks(market="standard", history_days=300)
        assert result is not None
        # 99840 にはデータがないので空
        assert len(result) == 0

    def test_nonexistent_market(self, service):
        """存在しない市場は None"""
        result = service.get_all_stocks(market="growth", history_days=300)
        assert result is None

    def test_stock_data_has_company_name(self, service):
        """company_name フィールドの存在確認"""
        result = service.get_all_stocks(market="prime")
        assert result is not None
        assert len(result) > 0
        assert result[0].company_name != ""

    def test_stock_data_has_ohlcv(self, service):
        """OHLCV データの存在確認"""
        result = service.get_all_stocks(market="prime")
        assert result is not None
        for stock in result:
            assert len(stock.data) > 0
            for rec in stock.data:
                assert rec.date != ""
                assert isinstance(rec.volume, int)


class TestGetTopix:
    def test_all_data(self, service):
        """全期間の TOPIX データ取得"""
        result = service.get_topix()
        assert result is not None
        assert len(result) == 3
        assert result[0].date == "2024-01-15"
        # volume フィールドがないことを確認
        assert not hasattr(result[0], "volume") or "volume" not in result[0].model_fields

    def test_with_date_range(self, service):
        """日付範囲指定"""
        result = service.get_topix(start_date="2024-01-16", end_date="2024-01-17")
        assert result is not None
        assert len(result) == 2

    def test_no_data(self, service):
        """データなしは None"""
        result = service.get_topix(start_date="2025-01-01")
        assert result is None
