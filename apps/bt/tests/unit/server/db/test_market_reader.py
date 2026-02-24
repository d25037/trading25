"""
MarketDbReader Unit Tests

in-memory SQLite を使用したテスト。
"""

import sqlite3

import pytest

from src.infrastructure.db.market.market_reader import MarketDbReader


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

    def test_get_latest_price(self, reader):
        """最新終値を取得できること（4桁/5桁候補解決を含む）"""
        assert reader.get_latest_price("7203") == 2525.0
        assert reader.get_latest_price("72030") == 2525.0

    def test_get_latest_price_not_found(self, reader):
        """該当銘柄がない場合はNone"""
        assert reader.get_latest_price("0000") is None

    def test_get_stock_prices_by_date(self, reader):
        """日次終値を日付昇順で取得"""
        rows = reader.get_stock_prices_by_date("72030")
        assert rows == [
            ("2024-01-15", 2505.0),
            ("2024-01-16", 2515.0),
            ("2024-01-17", 2525.0),
        ]

    def test_get_stock_prices_by_date_prefers_4digit(self, market_db_path):
        """4桁/5桁が同居する場合は4桁を優先マージ"""
        conn = sqlite3.connect(market_db_path)
        conn.execute(
            "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "7203",
                "トヨタ自動車(4桁)",
                "TOYOTA MOTOR 4D",
                "prime",
                "プライム",
                "S17_1",
                "輸送用機器",
                "S33_1",
                "輸送用機器",
                "TOPIX Large70",
                "1949-05-16",
                None,
                None,
            ),
        )
        for i, d in enumerate(("2024-01-15", "2024-01-16", "2024-01-17")):
            base = 2600.0 + i * 10
            conn.execute(
                "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("7203", d, base, base + 20, base - 10, base + 5, 999999 + i, 1.0, None),
            )
        conn.commit()
        conn.close()

        reader = MarketDbReader(market_db_path)
        try:
            rows = reader.get_stock_prices_by_date("7203")
            assert rows == [
                ("2024-01-15", 2605.0),
                ("2024-01-16", 2615.0),
                ("2024-01-17", 2625.0),
            ]
        finally:
            reader.close()

    def test_get_stock_prices_by_date_not_found(self, reader):
        """該当銘柄がない場合は空配列"""
        assert reader.get_stock_prices_by_date("0000") == []
