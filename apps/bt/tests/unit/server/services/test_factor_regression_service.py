"""
Factor Regression Service Unit Tests
"""

import math
import sqlite3

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.factor_regression_service import (
    DailyReturn,
    FactorRegressionService,
    _align_returns,
    _calculate_daily_returns,
    _ols_regression,
)


class TestOLSRegression:
    def test_perfect_fit(self):
        """完全一致の回帰"""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]  # y = 2x
        result = _ols_regression(y, x)
        assert abs(result.beta - 2.0) < 0.001
        assert abs(result.r_squared - 1.0) < 0.001

    def test_zero_variance(self):
        """定数x"""
        x = [1.0, 1.0, 1.0, 1.0]
        y = [2.0, 3.0, 4.0, 5.0]
        result = _ols_regression(y, x)
        assert result.beta == 0.0
        assert result.r_squared == 0.0

    def test_length_mismatch(self):
        with pytest.raises(ValueError, match="same length"):
            _ols_regression([1.0, 2.0], [1.0])

    def test_insufficient_data(self):
        with pytest.raises(ValueError, match="At least 2"):
            _ols_regression([1.0], [1.0])

    def test_r_squared_range(self):
        """R² は [0, 1] にクランプ"""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [1.5, 3.2, 4.8, 7.1, 9.3]
        result = _ols_regression(y, x)
        assert 0 <= result.r_squared <= 1


class TestCalculateDailyReturns:
    def test_basic(self):
        prices = [("2024-01-01", 100.0), ("2024-01-02", 110.0), ("2024-01-03", 105.0)]
        returns = _calculate_daily_returns(prices)
        assert len(returns) == 2
        assert abs(returns[0].ret - math.log(110 / 100)) < 0.001
        assert abs(returns[1].ret - math.log(105 / 110)) < 0.001

    def test_single_point(self):
        assert _calculate_daily_returns([("2024-01-01", 100.0)]) == []

    def test_zero_price_skipped(self):
        prices = [("2024-01-01", 100.0), ("2024-01-02", 0.0), ("2024-01-03", 110.0)]
        returns = _calculate_daily_returns(prices)
        # 0 価格はスキップ
        assert len(returns) == 0


class TestAlignReturns:
    def test_basic(self):
        stock = [DailyReturn("2024-01-01", 0.01), DailyReturn("2024-01-02", 0.02), DailyReturn("2024-01-03", 0.03)]
        index = [DailyReturn("2024-01-02", 0.005), DailyReturn("2024-01-03", 0.01)]
        dates, aligned_s, aligned_i = _align_returns(stock, index)
        assert dates == ["2024-01-02", "2024-01-03"]
        assert aligned_s == [0.02, 0.03]
        assert aligned_i == [0.005, 0.01]

    def test_no_overlap(self):
        stock = [DailyReturn("2024-01-01", 0.01)]
        index = [DailyReturn("2024-01-02", 0.005)]
        dates, _, _ = _align_returns(stock, index)
        assert dates == []


@pytest.fixture
def factor_db(tmp_path):
    """factor-regression テスト用DB"""
    import random
    random.seed(42)

    db_path = str(tmp_path / "factor.db")
    conn = sqlite3.connect(db_path)

    conn.execute("""CREATE TABLE stocks (
        code TEXT PRIMARY KEY, company_name TEXT, company_name_english TEXT,
        market_code TEXT, market_name TEXT, sector_17_code TEXT, sector_17_name TEXT,
        sector_33_code TEXT, sector_33_name TEXT, scale_category TEXT, listed_date TEXT,
        created_at TEXT, updated_at TEXT)""")
    conn.execute("""CREATE TABLE stock_data (
        code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
        volume INTEGER, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date))""")
    conn.execute("""CREATE TABLE topix_data (
        date TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, created_at TEXT)""")
    conn.execute("""CREATE TABLE index_master (
        code TEXT PRIMARY KEY, name TEXT, name_english TEXT, category TEXT, data_start_date TEXT)""")
    conn.execute("""CREATE TABLE indices_data (
        code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
        sector_name TEXT, created_at TEXT, PRIMARY KEY (code, date))""")

    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("72030", "トヨタ", "TOYOTA", "prime", "P", "S17", "輸送", "S33", "輸送用機器", None, "2000-01-01", None, None))

    # 100日分のデータ
    from datetime import date, timedelta
    d = date(2023, 6, 1)
    dates = []
    while len(dates) < 100:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += timedelta(days=1)

    stock_price = 2500.0
    topix_price = 2500.0
    idx_price = 1200.0

    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0000", "TOPIX", "TOPIX", "topix", "2008-01-01"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0040", "水産", "Fishery", "sector33", "2010-01-01"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0080", "食品", "Foods", "sector17", "2010-01-01"))

    for d_str in dates:
        stock_price *= 1 + random.uniform(-0.02, 0.025)
        topix_price *= 1 + random.uniform(-0.01, 0.015)
        idx_price *= 1 + random.uniform(-0.015, 0.02)

        conn.execute("INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                     ("72030", d_str, stock_price * 0.99, stock_price * 1.01, stock_price * 0.98, stock_price, 1000000, 1.0, None))
        conn.execute("INSERT INTO topix_data VALUES (?,?,?,?,?,?)",
                     (d_str, topix_price * 0.99, topix_price * 1.01, topix_price * 0.98, topix_price, None))

        for idx_code, ip in [("0000", topix_price), ("0040", idx_price), ("0080", idx_price * 1.1)]:
            conn.execute("INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                         (idx_code, d_str, ip * 0.99, ip * 1.01, ip * 0.98, ip, None, None))

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def factor_service(factor_db):
    reader = MarketDbReader(factor_db)
    yield FactorRegressionService(reader)
    reader.close()


class TestFactorRegressionService:
    def test_analyze_stock(self, factor_service):
        result = factor_service.analyze_stock("7203", lookback_days=80)
        assert result.stockCode == "7203"
        assert result.companyName == "トヨタ"
        assert 0 <= result.marketRSquared <= 1
        assert isinstance(result.marketBeta, float)
        assert result.dataPoints <= 80
        assert result.dateRange.to > result.dateRange.from_

    def test_sector_matches(self, factor_service):
        result = factor_service.analyze_stock("7203")
        # sector33/17 マッチがある
        assert isinstance(result.sector33Matches, list)
        assert isinstance(result.sector17Matches, list)

    def test_not_found(self, factor_service):
        with pytest.raises(ValueError, match="not found"):
            factor_service.analyze_stock("0000")

    def test_lookback_limits_data(self, factor_service):
        result = factor_service.analyze_stock("7203", lookback_days=60)
        assert result.dataPoints <= 60
