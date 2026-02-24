"""DatasetDb 拡張メソッド（Phase 3D-1）のユニットテスト"""

from __future__ import annotations

import os
import sqlite3

import pytest

from src.infrastructure.db.market.dataset_db import DatasetDb


@pytest.fixture
def dataset_db(tmp_path):
    """テスト用 DatasetDb を作成（テーブル + サンプルデータ付き）"""
    db_path = os.path.join(str(tmp_path), "test.db")
    conn = sqlite3.connect(db_path)

    # テーブル作成
    conn.executescript("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            adjustment_factor REAL,
            created_at TEXT,
            PRIMARY KEY (code, date)
        );
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            created_at TEXT
        );
        CREATE TABLE indices_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            sector_name TEXT,
            created_at TEXT,
            PRIMARY KEY (code, date)
        );
        CREATE TABLE dataset_info (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT
        );
        CREATE TABLE margin_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            long_margin_volume REAL,
            short_margin_volume REAL,
            PRIMARY KEY (code, date)
        );
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
        );
    """)

    # サンプルデータ挿入
    conn.executescript("""
        INSERT INTO stocks VALUES ('7203', 'トヨタ自動車', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('9984', 'ソフトバンクグループ', 'SOFTBANK', '0111', 'プライム', '9', '情報・通信業', '3700', '情報・通信業', 'TOPIX Core30', '1994-07-22', NULL, NULL);
        INSERT INTO stocks VALUES ('6758', 'ソニーグループ', 'SONY', '0111', 'プライム', '4', '電気機器', '3650', '電気機器', 'TOPIX Core30', '1958-12-01', NULL, NULL);

        INSERT INTO stock_data VALUES ('7203', '2024-01-04', 2700, 2750, 2680, 2720, 1000000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('7203', '2024-01-05', 2720, 2780, 2700, 2760, 1100000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('9984', '2024-01-04', 6500, 6600, 6450, 6550, 500000, 1.0, NULL);

        INSERT INTO topix_data VALUES ('2024-01-04', 2500, 2520, 2490, 2510, NULL);
        INSERT INTO topix_data VALUES ('2024-01-05', 2510, 2530, 2500, 2520, NULL);

        INSERT INTO indices_data VALUES ('0010', '2024-01-04', 100, 102, 99, 101, '食料品', NULL);
        INSERT INTO indices_data VALUES ('0010', '2024-01-05', 101, 103, 100, 102, '食料品', NULL);
        INSERT INTO indices_data VALUES ('0020', '2024-01-04', 200, 205, 198, 203, '化学', NULL);

        INSERT INTO margin_data VALUES ('7203', '2024-01-04', 50000, 30000);
        INSERT INTO margin_data VALUES ('7203', '2024-01-05', 52000, 31000);
        INSERT INTO margin_data VALUES ('9984', '2024-01-04', 40000, 20000);

        INSERT INTO statements VALUES ('7203', '2024-01-30', 150.0, 2000000, 5000000, 'FY', 'AnnualReport', 160.0, 3000, 20000000, 1500000, 1600000, 1800000, 60.0, 62.0, 64.0, 30.0, 32.0, 34.0, 165.0, -500000, -300000, 4000000, 50000000, 330000000, 10000000);
        INSERT INTO statements VALUES ('9984', '2024-01-30', 100.0, 1500000, 3000000, 'FY', 'AnnualReport', 110.0, 2500, 15000000, 1200000, 1300000, 1400000, 50.0, 51.0, 53.0, 25.0, 27.0, 28.0, 115.0, -400000, -200000, 3000000, 40000000, 200000000, 5000000);

        INSERT INTO dataset_info VALUES ('preset', 'primeMarket', NULL);
        INSERT INTO dataset_info VALUES ('created_at', '2024-01-01T00:00:00', NULL);
    """)

    conn.close()
    return DatasetDb(db_path)


class TestDatasetDbExtended:
    def test_get_stock_list_with_counts(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.get_stock_list_with_counts(min_records=0)
        assert len(rows) == 3
        # 7203 has 2 records
        toyota = next(r for r in rows if r.stockCode == "7203")
        assert toyota.record_count == 2
        assert toyota.start_date == "2024-01-04"
        assert toyota.end_date == "2024-01-05"

    def test_get_stock_list_with_counts_filter(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.get_stock_list_with_counts(min_records=2)
        assert len(rows) == 1
        assert rows[0].stockCode == "7203"

    def test_get_index_list_with_counts(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.get_index_list_with_counts(min_records=0)
        assert len(rows) == 2
        food = next(r for r in rows if r.indexCode == "0010")
        assert food.record_count == 2
        assert food.indexName == "食料品"

    def test_get_margin_list(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.get_margin_list(min_records=0)
        assert len(rows) == 2
        toyota = next(r for r in rows if r.stockCode == "7203")
        assert toyota.record_count == 2

    def test_search_stocks_exact(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.search_stocks("7203", exact=True)
        assert len(rows) == 1
        assert rows[0].code == "7203"
        assert rows[0].match_type == "exact"

    def test_search_stocks_partial(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.search_stocks("ソニー", exact=False)
        assert len(rows) == 1
        assert rows[0].code == "6758"

    def test_search_stocks_no_match(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.search_stocks("存在しない", exact=True)
        assert len(rows) == 0

    def test_get_sample_codes(self, dataset_db: DatasetDb) -> None:
        codes = dataset_db.get_sample_codes(size=2, seed=42)
        assert len(codes) == 2
        # Deterministic with seed
        codes2 = dataset_db.get_sample_codes(size=2, seed=42)
        assert codes == codes2

    def test_get_sample_codes_larger_than_available(self, dataset_db: DatasetDb) -> None:
        codes = dataset_db.get_sample_codes(size=100)
        assert len(codes) == 3  # Only 3 stocks

    def test_get_table_counts(self, dataset_db: DatasetDb) -> None:
        counts = dataset_db.get_table_counts()
        assert counts["stocks"] == 3
        assert counts["stock_data"] == 3
        assert counts["topix_data"] == 2
        assert counts["indices_data"] == 3
        assert counts["margin_data"] == 3
        assert counts["statements"] == 2
        assert counts["dataset_info"] == 2

    def test_get_date_range(self, dataset_db: DatasetDb) -> None:
        dr = dataset_db.get_date_range()
        assert dr is not None
        assert dr["min"] == "2024-01-04"
        assert dr["max"] == "2024-01-05"

    def test_get_sectors_with_count(self, dataset_db: DatasetDb) -> None:
        rows = dataset_db.get_sectors_with_count()
        assert len(rows) >= 2  # 輸送用機器, 情報・通信業, 電気機器
        names = [r.sectorName for r in rows]
        assert "輸送用機器" in names

    def test_get_stocks_with_quotes_count(self, dataset_db: DatasetDb) -> None:
        count = dataset_db.get_stocks_with_quotes_count()
        assert count == 2  # 7203 and 9984

    def test_get_dataset_info(self, dataset_db: DatasetDb) -> None:
        info = dataset_db.get_dataset_info()
        assert info["preset"] == "primeMarket"
