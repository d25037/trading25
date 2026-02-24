"""Tests for DatasetWriter."""

import os
import tempfile

import pytest
from sqlalchemy import text

from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter


@pytest.fixture
def writer() -> DatasetWriter:
    """Temporary database writer."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    w = DatasetWriter(path)
    yield w  # type: ignore[misc]
    w.close()
    os.unlink(path)


def test_ensure_schema(writer: DatasetWriter) -> None:
    """Schema tables are created on init."""
    with writer.engine.connect() as conn:
        tables = [r[0] for r in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()]
    assert "stocks" in tables
    assert "stock_data" in tables
    assert "dataset_info" in tables


def test_upsert_stocks(writer: DatasetWriter) -> None:
    count = writer.upsert_stocks([
        {"code": "7203", "company_name": "Toyota", "market_code": "111", "market_name": "プライム",
         "sector_17_code": "7", "sector_17_name": "自動車", "sector_33_code": "3700",
         "sector_33_name": "輸送用機器", "listed_date": "2000-01-01", "created_at": "2024-01-01"},
    ])
    assert count == 1
    assert writer.get_stock_count() == 1


def test_upsert_stocks_empty(writer: DatasetWriter) -> None:
    assert writer.upsert_stocks([]) == 0


def test_upsert_stock_data(writer: DatasetWriter) -> None:
    count = writer.upsert_stock_data([
        {"code": "7203", "date": "2024-01-04", "open": 100, "high": 110,
         "low": 90, "close": 105, "volume": 1000, "created_at": "2024-01-04"},
    ])
    assert count == 1
    assert writer.get_stock_data_count() == 1


def test_upsert_topix_data(writer: DatasetWriter) -> None:
    count = writer.upsert_topix_data([
        {"date": "2024-01-04", "open": 2500, "high": 2520, "low": 2480, "close": 2510, "created_at": "2024-01-04"},
    ])
    assert count == 1


def test_upsert_margin_data(writer: DatasetWriter) -> None:
    count = writer.upsert_margin_data([
        {"code": "7203", "date": "2024-01-04", "long_margin_volume": 50000, "short_margin_volume": 30000},
    ])
    assert count == 1


def test_upsert_statements(writer: DatasetWriter) -> None:
    count = writer.upsert_statements([
        {"code": "7203", "disclosed_date": "2024-03-15", "earnings_per_share": 250.0},
    ])
    assert count == 1


def test_set_dataset_info(writer: DatasetWriter) -> None:
    writer.set_dataset_info("preset", "quickTesting")
    with writer.engine.connect() as conn:
        row = conn.execute(text("SELECT value FROM dataset_info WHERE key = 'preset'")).fetchone()
    assert row is not None
    assert row[0] == "quickTesting"


def test_upsert_stock_data_replace(writer: DatasetWriter) -> None:
    """Duplicate key should replace."""
    writer.upsert_stock_data([
        {"code": "7203", "date": "2024-01-04", "open": 100, "high": 110,
         "low": 90, "close": 105, "volume": 1000, "created_at": "2024-01-04"},
    ])
    writer.upsert_stock_data([
        {"code": "7203", "date": "2024-01-04", "open": 200, "high": 210,
         "low": 190, "close": 205, "volume": 2000, "created_at": "2024-01-04"},
    ])
    assert writer.get_stock_data_count() == 1


def test_upsert_indices_data(writer: DatasetWriter) -> None:
    count = writer.upsert_indices_data([
        {"code": "I201", "date": "2024-01-04", "open": 1000, "high": 1010,
         "low": 990, "close": 1005, "created_at": "2024-01-04"},
    ])
    assert count == 1
