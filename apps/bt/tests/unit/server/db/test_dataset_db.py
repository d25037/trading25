"""
Tests for DatasetDb
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert

from src.server.db.dataset_db import DatasetDb
from src.server.db.tables import (
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
            code="7203", disclosed_date="2024-01-15",
            earnings_per_share=150.0, profit=2000000.0, equity=10000000.0,
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


class TestDatasetDbIndices:
    def test_get_indices(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_indices()
        assert len(result) == 1
        assert result[0].code == "0000"

    def test_get_index_data(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_index_data("0000")
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


class TestDatasetDbStatements:
    def test_get_statements(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements("7203")
        assert len(result) == 1
        assert result[0].earnings_per_share == 150.0

    def test_get_statements_batch(self, ds_db: DatasetDb) -> None:
        result = ds_db.get_statements_batch(["7203", "6758"])
        assert len(result["7203"]) == 1
        assert len(result["6758"]) == 0


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
