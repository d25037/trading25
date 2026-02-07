"""
Ranking Service Unit Tests
"""

import sqlite3

import pytest

from src.server.db.market_reader import MarketDbReader
from src.server.services.ranking_service import RankingService


@pytest.fixture
def ranking_db(tmp_path):
    """ランキングテスト用DB"""
    db_path = str(tmp_path / "ranking.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT,
            market_code TEXT NOT NULL, market_name TEXT NOT NULL,
            sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT NOT NULL,
            scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    # 3 銘柄
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("72030", "トヨタ", "TOYOTA", "prime", "P", "S17", "輸送", "S33", "輸送用機器", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("67580", "ソニー", "SONY", "prime", "P", "S17", "電気", "S33", "電気機器", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("99840", "テスト", "TEST", "standard", "S", "S17", "情報", "S33", "情報通信", None, "2000-01-01", None, None))

    # 5日分のOHLCVデータ
    dates = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
    for code, base_v in [("72030", 2000000), ("67580", 1500000), ("99840", 100000)]:
        for i, d in enumerate(dates):
            price = 2500.0 + i * 10 if code == "72030" else (13000.0 + i * 50 if code == "67580" else 500.0 + i * 5)
            vol = base_v + i * 10000
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                (code, d, price, price + 20, price - 10, price, vol, 1.0, None),
            )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(ranking_db):
    reader = MarketDbReader(ranking_db)
    yield RankingService(reader)
    reader.close()


class TestGetRankings:
    def test_default(self, service):
        result = service.get_rankings()
        assert result.date == "2024-01-19"
        assert result.markets == ["prime"]
        assert result.lookbackDays == 1
        assert result.periodDays == 250

    def test_trading_value_ranking(self, service):
        result = service.get_rankings(markets="prime", limit=10)
        items = result.rankings.tradingValue
        assert len(items) >= 1
        # 売買代金降順
        if len(items) >= 2:
            assert items[0].tradingValue >= items[1].tradingValue

    def test_gainers_losers(self, service):
        result = service.get_rankings(markets="prime")
        gainers = result.rankings.gainers
        losers = result.rankings.losers
        assert isinstance(gainers, list)
        assert isinstance(losers, list)

    def test_with_date(self, service):
        result = service.get_rankings(date="2024-01-17")
        assert result.date == "2024-01-17"

    def test_market_filter(self, service):
        result = service.get_rankings(markets="standard")
        # standard は 99840 のみ
        items = result.rankings.tradingValue
        for item in items:
            assert item.marketCode == "standard"

    def test_lookback_days(self, service):
        result = service.get_rankings(lookback_days=3)
        assert result.lookbackDays == 3
        # N日平均ランキング
        items = result.rankings.tradingValue
        assert len(items) >= 1

    def test_limit(self, service):
        result = service.get_rankings(limit=1)
        assert len(result.rankings.tradingValue) <= 1
        assert len(result.rankings.gainers) <= 1

    def test_no_data_raises(self, tmp_path):
        """データなしDBの場合"""
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT, company_name_english TEXT,
            market_code TEXT, market_name TEXT, sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT, scale_category TEXT, listed_date TEXT,
            created_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE stock_data (
            code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date))""")
        conn.commit()
        conn.close()

        reader = MarketDbReader(db_path)
        service = RankingService(reader)
        with pytest.raises(ValueError, match="No trading data"):
            service.get_rankings()
        reader.close()
