"""
Tests for DatasetDb
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert

from src.infrastructure.db.market.dataset_db import DatasetDb
from src.infrastructure.db.market.tables import (
    dataset_info,
    dataset_meta,
    ds_indices_data,
    ds_stock_data,
    ds_stocks,
    ds_topix_data,
    margin_data,
    statements,
)


def _create_test_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "dataset.db")
    engine = create_engine(f"sqlite:///{db_path}")
    dataset_meta.create_all(engine)

    with engine.begin() as conn:
        # Stocks
        conn.execute(insert(ds_stocks).values(
            code="7203", company_name="トヨタ", market_code="0111",
            market_name="プライム", sector_17_code="6", sector_17_name="自動車",
            sector_33_code="3700", sector_33_name="輸送用機器", listed_date="1949-05-16",
        ))
        conn.execute(insert(ds_stocks).values(
            code="6758", company_name="ソニー", market_code="0111",
            market_name="プライム", sector_17_code="5", sector_17_name="電機",
            sector_33_code="3650", sector_33_name="電気機器", listed_date="1958-12-01",
        ))

        # Stock data
        for date in ["2024-01-15", "2024-01-16", "2024-01-17"]:
            conn.execute(insert(ds_stock_data).values(
                code="7203", date=date, open=2500.0, high=2510.0,
                low=2490.0, close=2505.0, volume=1000000,
            ))

        # TOPIX
        for date in ["2024-01-15", "2024-01-16"]:
            conn.execute(insert(ds_topix_data).values(
                date=date, open=2500.0, high=2510.0, low=2490.0, close=2505.0,
            ))

        # Indices
        conn.execute(insert(ds_indices_data).values(
            code="0000", date="2024-01-15", open=2500.0, high=2510.0,
            low=2490.0, close=2505.0, sector_name="TOPIX",
        ))

        # Margin
        conn.execute(insert(margin_data).values(
            code="7203", date="2024-01-15",
            long_margin_volume=50000.0, short_margin_volume=20000.0,
        ))

        # Statements
        conn.execute(insert(statements).values(
            code="7203",
            disclosed_date="2024-01-15",
            earnings_per_share=150.0,
            profit=2000000.0,
            equity=10000000.0,
            type_of_current_period="FY",
        ))
        conn.execute(insert(statements).values(
            code="7203",
            disclosed_date="2024-04-15",
            earnings_per_share=40.0,
            profit=500000.0,
            equity=10200000.0,
            type_of_current_period="1Q",
        ))
        conn.execute(insert(statements).values(
            code="7203",
            disclosed_date="2024-07-15",
            earnings_per_share=45.0,
            profit=550000.0,
            equity=10300000.0,
            type_of_current_period="Q1",
        ))
        # Forecast-only row (actual_only=True should exclude this)
        conn.execute(insert(statements).values(
            code="7203",
            disclosed_date="2024-10-15",
            type_of_current_period="FY",
            next_year_forecast_earnings_per_share=180.0,
        ))

        # Dataset info
        conn.execute(insert(dataset_info).values(key="schema_version", value="2.0.0"))
        conn.execute(insert(dataset_info).values(key="name", value="test_dataset"))

    engine.dispose()
    return db_path


@pytest.fixture()
def ds_db(tmp_path: Path) -> DatasetDb:
    db_path = _create_test_db(tmp_path)
    db = DatasetDb(db_path)
    yield db  # type: ignore[misc]
    db.close()


@pytest.fixture()
def legacy_ds_db(tmp_path: Path) -> DatasetDb:
    """旧 statements スキーマ（配当/配当性向予想カラムなし）の DatasetDb。"""
    db_path = tmp_path / "legacy-dataset.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
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
            forecast_eps REAL,
            investing_cash_flow REAL,
            financing_cash_flow REAL,
            cash_and_equivalents REAL,
            total_assets REAL,
            shares_outstanding REAL,
            treasury_shares REAL,
            PRIMARY KEY (code, disclosed_date)
        );

        INSERT INTO statements VALUES (
            '7203', '2024-01-15',
            150.0, 2000000.0, 10000000.0, 'FY', 'AnnualReport',
            160.0, 3000.0, 20000000.0, 1500000.0, 1600000.0, 1800000.0,
            60.0, 165.0, -500000.0, -300000.0, 4000000.0, 50000000.0,
            330000000.0, 10000000.0
        );
    """)
    conn.close()

    db = DatasetDb(str(db_path))
    yield db
    db.close()


@pytest.fixture()
def invalid_schema_ds_db(tmp_path: Path) -> DatasetDb:
    """必須列が不足した statements スキーマ（破損想定）。"""
    db_path = tmp_path / "invalid-schema.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE statements (
            code TEXT NOT NULL,
            earnings_per_share REAL
        );

        INSERT INTO statements VALUES ('7203', 150.0);
    """)
    conn.close()

    db = DatasetDb(str(db_path))
    yield db
    db.close()


class TestDatasetDbStocks:
    def test_get_all_stocks(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stocks()
        assert len(result) == 2

    def test_get_stocks_by_sector(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stocks(sector="輸送用機器")
        assert len(result) == 1
        assert result[0].code == "7203"

    def test_get_stocks_by_market(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stocks(market="0111")
        assert len(result) == 2

    def test_get_stocks_empty_sector(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stocks(sector="存在しないセクター")
        assert len(result) == 0


class TestDatasetDbOHLCV:
    def test_get_stock_ohlcv(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stock_ohlcv("7203")
        assert len(result) == 3

    def test_get_stock_ohlcv_with_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stock_ohlcv("7203", start="2024-01-16")
        assert len(result) == 2

    def test_get_stock_ohlcv_with_end(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stock_ohlcv("7203", end="2024-01-16")
        assert len(result) == 2

    def test_get_stock_ohlcv_5digit(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stock_ohlcv("72030")  # 5桁→4桁自動変換
        assert len(result) == 3

    def test_get_ohlcv_batch(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_ohlcv_batch(["7203", "6758"])
        assert len(result["7203"]) == 3
        assert len(result["6758"]) == 0

    def test_get_stock_ohlcv_empty(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stock_ohlcv("9999")
        assert len(result) == 0


class TestDatasetDbTopix:
    def test_get_topix(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_topix()
        assert len(result) == 2

    def test_get_topix_with_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_topix(start="2024-01-16")
        assert len(result) == 1

    def test_get_topix_with_end(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_topix(end="2024-01-15")
        assert len(result) == 1


class TestDatasetDbIndices:
    def test_get_indices(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_indices()
        assert len(result) == 1
        assert result[0].code == "0000"

    def test_get_index_data(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_index_data("0000")
        assert len(result) == 1

    def test_get_index_data_with_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_index_data("0000", start="2024-01-01", end="2024-01-20")
        assert len(result) == 1


class TestDatasetDbMargin:
    def test_get_margin(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_margin(code="7203")
        assert len(result) == 1
        assert result[0].long_margin_volume == 50000.0

    def test_get_margin_all(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_margin()
        assert len(result) == 1

    def test_get_margin_batch(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_margin_batch(["7203", "6758"])
        assert len(result["7203"]) == 1
        assert len(result["6758"]) == 0

    def test_get_margin_with_date_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_margin(code="7203", start="2024-01-01", end="2024-01-20")
        assert len(result) == 1


class TestDatasetDbStatements:
    def test_get_statements(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements("7203")
        assert len(result) == 3
        assert result[0].earnings_per_share == 150.0

    def test_get_statements_batch(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements_batch(["7203", "6758"])
        assert len(result["7203"]) == 3
        assert len(result["6758"]) == 0

    def test_get_statements_batch_empty_codes(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements_batch([])
        assert result == {}

    def test_get_statements_period_type_fy(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements("7203", period_type="FY")
        assert len(result) == 1
        assert all(row.type_of_current_period == "FY" for row in result)

    def test_get_statements_period_type_1q_includes_legacy_q1(
        self, ds_db: DatasetDb
    ) -> None:
        result = ds_db.get_statements("7203", period_type="1Q")
        assert len(result) == 2
        assert {row.type_of_current_period for row in result} == {"1Q", "Q1"}

    def test_get_statements_actual_only_false_includes_forecast_only(
        self, ds_db: DatasetDb
    ) -> None:
        result = ds_db.get_statements("7203", actual_only=False)
        assert len(result) == 4
        assert any(row.disclosed_date == "2024-10-15" for row in result)

    def test_get_statements_with_date_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements(
            "7203",
            start="2024-04-01",
            end="2024-08-01",
        )
        assert len(result) == 2
        assert [row.disclosed_date for row in result] == ["2024-04-15", "2024-07-15"]

    def test_get_statements_batch_applies_filters(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements_batch(
            ["7203", "6758"],
            period_type="FY",
            actual_only=False,
        )
        assert len(result["7203"]) == 2
        assert {row.disclosed_date for row in result["7203"]} == {
            "2024-01-15",
            "2024-10-15",
        }
        assert len(result["6758"]) == 0

    def test_get_statements_legacy_schema_fills_missing_columns_with_none(
        self, legacy_ds_db: DatasetDb
    ) -> None:
        result = legacy_ds_db.get_statements("7203", actual_only=False)
        assert len(result) == 1
        row = result[0]
        assert row.forecast_dividend_fy is None
        assert row.next_year_forecast_dividend_fy is None
        assert row.payout_ratio is None
        assert row.forecast_payout_ratio is None
        assert row.next_year_forecast_payout_ratio is None

    def test_get_statements_batch_legacy_schema_does_not_fail(
        self, legacy_ds_db: DatasetDb
    ) -> None:
        result = legacy_ds_db.get_statements_batch(
            ["7203", "6758"],
            actual_only=False,
        )
        assert len(result["7203"]) == 1
        assert result["7203"][0].forecast_dividend_fy is None
        assert len(result["6758"]) == 0

    def test_get_statements_invalid_schema_raises_runtime_error(
        self, invalid_schema_ds_db: DatasetDb
    ) -> None:
        with pytest.raises(
            RuntimeError,
            match="missing required columns: disclosed_date",
        ):
            invalid_schema_ds_db.get_statements("7203")


class TestDatasetDbSectors:
    def test_get_sectors(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_sectors()
        assert len(result) == 2

    def test_get_sector_mapping(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_sector_mapping()
        assert "3700" in result
        assert result["3700"] == "輸送用機器"

    def test_get_sector_stock_mapping(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_sector_stock_mapping()
        assert "輸送用機器" in result
        assert "7203" in result["輸送用機器"]

    def test_get_sector_stocks(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_sector_stocks("輸送用機器")
        assert len(result) == 1


class TestDatasetDbInfo:
    def test_get_dataset_info(self, ds_db: DatasetDb) -> None:
        info = ds_db.get_dataset_info()
        assert info["schema_version"] == "2.0.0"
        assert info["name"] == "test_dataset"

    def test_get_stock_count(self, ds_db: DatasetDb) -> None:
        assert ds_db.get_stock_count() == 2


class TestDatasetDbExtended:
    def test_search_stocks_partial(self, ds_db: DatasetDb) -> None:
        result = ds_db.search_stocks("トヨ")
        assert len(result) == 1
        assert result[0].code == "7203"
        assert result[0].match_type == "partial"

    def test_search_stocks_exact(self, ds_db: DatasetDb) -> None:
        result = ds_db.search_stocks("7203", exact=True)
        assert len(result) == 1
        assert result[0].code == "7203"
        assert result[0].match_type == "exact"

    def test_get_sample_codes(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_sample_codes(size=10, seed=42)
        assert len(result) == 2
        assert set(result) == {"7203", "6758"}

    def test_get_table_counts(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_table_counts()
        assert result["stocks"] == 2
        assert result["stock_data"] == 3
        assert result["topix_data"] == 2
        assert result["indices_data"] == 1
        assert result["margin_data"] == 1
        assert result["statements"] == 4
        assert result["dataset_info"] == 2

    def test_get_date_range(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_date_range()
        assert result == {"min": "2024-01-15", "max": "2024-01-17"}

    def test_get_stocks_with_quotes_count(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_stocks_with_quotes_count()
        assert result == 1

    def test_get_date_range_returns_none_when_no_stock_data(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "dataset-empty.db")
        engine = create_engine(f"sqlite:///{db_path}")
        dataset_meta.create_all(engine)
        engine.dispose()

        db = DatasetDb(db_path)
        try:
            result = db.get_date_range()
            assert result is None
            assert db.get_sample_codes() == []
        finally:
            db.close()
