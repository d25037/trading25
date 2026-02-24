"""
Ranking Service Unit Tests
"""

import sqlite3

import pytest

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.ranking_service import RankingService


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
    conn.execute("""
        CREATE TABLE statements (
            code TEXT NOT NULL,
            disclosed_date TEXT NOT NULL,
            earnings_per_share REAL,
            profit REAL,
            equity REAL,
            type_of_current_period TEXT,
            type_of_document TEXT,
            next_year_forecast_earnings_per_share REAL,
            bps REAL,
            sales REAL,
            operating_profit REAL,
            ordinary_profit REAL,
            operating_cash_flow REAL,
            dividend_fy REAL,
            forecast_dividend_fy REAL,
            next_year_forecast_dividend_fy REAL,
            payout_ratio REAL,
            forecast_payout_ratio REAL,
            next_year_forecast_payout_ratio REAL,
            forecast_eps REAL,
            investing_cash_flow REAL,
            financing_cash_flow REAL,
            cash_and_equivalents REAL,
            total_assets REAL,
            shares_outstanding REAL,
            treasury_shares REAL,
            PRIMARY KEY (code, disclosed_date)
        )
    """)

    # 3 銘柄
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("72030", "トヨタ", "TOYOTA", "prime", "P", "S17", "輸送", "S33", "輸送用機器", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("67580", "ソニー", "SONY", "prime", "P", "S17", "電気", "S33", "電気機器", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("83060", "Numeric Prime", "NPRIME", "0111", "P", "S17", "銀行", "S33", "銀行業", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("46890", "Alt Prime", "APRIME", "prime", "P", "S17", "情報", "S33", "情報通信", None, "2000-01-01", None, None))
    conn.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("99840", "テスト", "TEST", "standard", "S", "S17", "情報", "S33", "情報通信", None, "2000-01-01", None, None))

    # 5日分のOHLCVデータ
    dates = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
    for code, base_v in [("72030", 2000000), ("67580", 1500000), ("83060", 1100000), ("46890", 900000), ("99840", 100000)]:
        for i, d in enumerate(dates):
            price = 2500.0 + i * 10 if code == "72030" else (13000.0 + i * 50 if code == "67580" else 500.0 + i * 5)
            vol = base_v + i * 10000
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                (code, d, price, price + 20, price - 10, price, vol, 1.0, None),
            )

    # statements (FY + quarter revisions)
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("72030", "2024-05-10", 100.0, "FY", 120.0, 118.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("72030", "2024-08-10", "1Q", 140.0, 100.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, shares_outstanding
        )
        VALUES (?,?,?,?,?,?)
        """,
        ("67580", "2024-05-12", 200.0, "FY", 220.0, 200.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("67580", "2024-08-12", "Q1", None, 250.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("83060", "2024-05-15", -50.0, "FY", -40.0, -42.0, 50.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("83060", "2024-08-15", "Q2", -20.0, 50.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("46890", "2024-05-18", 80.0, "FY", None, None, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, next_year_forecast_earnings_per_share, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("46890", "2024-08-18", "2Q", 95.0, 100.0),
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

    def test_market_filter_alias_prime_includes_numeric_codes(self, service):
        result = service.get_rankings(markets="prime", limit=20)
        market_codes = {item.marketCode for item in result.rankings.tradingValue}
        assert "prime" in market_codes
        assert "0111" in market_codes

    def test_market_filter_alias_0111_includes_legacy_codes(self, service):
        result = service.get_rankings(markets="0111", limit=20)
        market_codes = {item.marketCode for item in result.rankings.tradingValue}
        assert "prime" in market_codes
        assert "0111" in market_codes

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


class TestGetFundamentalRankings:
    def test_default_shape(self, service):
        result = service.get_fundamental_rankings()
        assert result.date == "2024-01-19"
        assert result.markets == ["prime"]
        assert "forecastHigh" in result.rankings.model_dump()
        assert "forecastLow" in result.rankings.model_dump()
        assert "actualHigh" in result.rankings.model_dump()
        assert "actualLow" in result.rankings.model_dump()

    def test_revised_forecast_is_prioritized(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        toyota = next((item for item in result.rankings.forecastHigh if item.code == "72030"), None)
        assert toyota is not None
        assert toyota.source == "revised"
        assert toyota.epsValue == 140.0
        assert toyota.periodType == "1Q"
        assert toyota.disclosedDate == "2024-08-10"

    def test_fy_forecast_fallback_when_revision_missing(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        sony = next((item for item in result.rankings.forecastHigh if item.code == "67580"), None)
        assert sony is not None
        assert sony.source == "fy"
        assert sony.periodType == "FY"
        assert sony.disclosedDate == "2024-05-12"
        # 220 * (200 / 250) = 176.0
        assert sony.epsValue == 176.0

    def test_revised_quarter_uses_next_year_forecast_fallback(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        alt = next((item for item in result.rankings.forecastHigh if item.code == "46890"), None)
        assert alt is not None
        assert alt.source == "revised"
        assert alt.periodType == "2Q"
        assert alt.disclosedDate == "2024-08-18"
        assert alt.epsValue == 95.0

    def test_actual_high_low_ordering(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        assert result.rankings.actualHigh[0].epsValue >= result.rankings.actualHigh[1].epsValue
        assert result.rankings.actualLow[0].epsValue <= result.rankings.actualLow[1].epsValue

    def test_market_filter_alias_prime_includes_numeric_codes(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        market_codes = {item.marketCode for item in result.rankings.actualHigh}
        assert "prime" in market_codes
        assert "0111" in market_codes
