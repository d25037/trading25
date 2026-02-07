"""
MarketDbReader Unit Tests

in-memory SQLite を使用したテスト。
"""

import sqlite3

import pytest

from src.server.db.market_reader import MarketDbReader


@pytest.fixture
def reader(market_db_path):
    """テスト用 MarketDbReader"""
    r = MarketDbReader(market_db_path)
    yield r
    r.close()


class TestMarketDbReader:
    def test_connect_readonly(self, reader):
        """読み取り専用接続の確認"""
        rows = reader.query("SELECT COUNT(*) as cnt FROM stocks")
        assert rows[0]["cnt"] == 3

    def test_query_one(self, reader):
        """query_one で 1 行取得"""
        row = reader.query_one("SELECT code FROM stocks WHERE code = ?", ("72030",))
        assert row is not None
        assert row["code"] == "72030"

    def test_query_one_not_found(self, reader):
        """query_one で該当なし"""
        row = reader.query_one("SELECT code FROM stocks WHERE code = ?", ("00000",))
        assert row is None

    def test_query_multiple_rows(self, reader):
        """複数行取得"""
        rows = reader.query("SELECT code FROM stocks ORDER BY code")
        assert len(rows) == 3

    def test_close_and_reconnect(self, market_db_path):
        """close 後に再接続"""
        r = MarketDbReader(market_db_path)
        r.query("SELECT 1")
        r.close()
        # close 後の再利用: conn プロパティで再接続
        rows = r.query("SELECT COUNT(*) as cnt FROM stocks")
        assert rows[0]["cnt"] == 3
        r.close()

    def test_readonly_write_fails(self, reader):
        """read-only 接続で書き込み不可"""
        with pytest.raises(sqlite3.OperationalError):
            reader.query("INSERT INTO stocks (code, company_name, market_code, market_name, sector_17_code, sector_17_name, sector_33_code, sector_33_name, listed_date) VALUES ('99990', 'test', 'prime', 'p', 's17', 's17n', 's33', 's33n', '2024-01-01')")
