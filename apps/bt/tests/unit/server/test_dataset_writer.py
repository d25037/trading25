"""Tests for DatasetWriter."""

from collections.abc import Generator
import importlib
import os
from pathlib import Path
import shutil
import tempfile

import pytest

import src.infrastructure.db.dataset_io.dataset_writer as dataset_writer_module
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter


def _connect_duckdb(writer: DatasetWriter):
    duckdb = importlib.import_module("duckdb")
    return duckdb.connect(str(writer.duckdb_path))


def _source_statement_payload(code: str, disclosed_date: str, **values: object) -> tuple[object, ...]:
    columns = dataset_writer_module._DatasetDuckDbStore._STATEMENT_COLUMNS
    payload: dict[str, object | None] = {column: None for column in columns}
    payload["code"] = code
    payload["disclosed_date"] = disclosed_date
    payload.update(values)
    return tuple(payload[column] for column in columns)


def _create_source_market_duckdb(tmp_path: Path) -> Path:
    duckdb = importlib.import_module("duckdb")
    source_path = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    columns = dataset_writer_module._DatasetDuckDbStore._STATEMENT_COLUMNS
    try:
        conn.execute(
            """
            CREATE TABLE stock_data (
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adjustment_factor DOUBLE,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE margin_data (
                code TEXT,
                date TEXT,
                long_margin_volume DOUBLE,
                short_margin_volume DOUBLE,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE statements (
                {", ".join(
                    f"{column} {'TEXT' if column in ('code', 'disclosed_date', 'type_of_current_period', 'type_of_document') else 'DOUBLE'}"
                    for column in columns
                )},
                PRIMARY KEY (code, disclosed_date)
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 10.0, 12.0, 9.0, 11.0, 1000, 1.0, "2026-01-01T00:00:00+00:00"),
                ("2222", "2026-01-01", 20.0, 22.0, 19.0, 21.0, None, 1.0, "2026-01-01T00:00:00+00:00"),
                ("22220", "2026-01-01", None, None, None, None, 2000, 1.1, "2026-01-01T01:00:00+00:00"),
                ("3333", "2026-01-01", 30.0, None, 29.0, 30.5, 3000, 1.0, "2026-01-01T00:00:00+00:00"),
                ("44440", "2026-01-01", 40.0, 42.0, 39.0, 41.0, 4000, 1.0, "2026-01-01T00:00:00+00:00"),
            ],
        )
        conn.executemany(
            "INSERT INTO margin_data VALUES (?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 1000.0, None),
                ("11110", "2026-01-01", 9999.0, 500.0),
                ("22220", "2026-01-01", 300.0, 200.0),
            ],
        )
        conn.executemany(
            f"INSERT INTO statements VALUES ({', '.join('?' for _ in columns)})",
            [
                _source_statement_payload(
                    "1111",
                    "2026-01-31",
                    earnings_per_share=10.0,
                    profit=None,
                    type_of_document="AnnualReport",
                ),
                _source_statement_payload(
                    "11110",
                    "2026-01-31",
                    earnings_per_share=99.0,
                    profit=500.0,
                    forecast_eps=12.0,
                ),
                _source_statement_payload(
                    "22220",
                    "2026-01-31",
                    earnings_per_share=20.0,
                    profit=600.0,
                    forecast_eps=21.0,
                ),
            ],
        )
    finally:
        conn.close()
    return source_path


@pytest.fixture
def writer() -> Generator[DatasetWriter, None, None]:
    """Temporary database writer."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    w = DatasetWriter(path)
    yield w
    w.close()
    os.unlink(path)
    shutil.rmtree(w.snapshot_dir)


def test_ensure_schema(writer: DatasetWriter) -> None:
    """Schema tables are created on init."""
    conn = _connect_duckdb(writer)
    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    finally:
        conn.close()
    assert "stocks" in tables
    assert "stock_data" in tables
    assert "dataset_info" in tables
    assert writer.duckdb_path.exists() is True
    assert writer.parquet_dir.exists() is True


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
    conn = _connect_duckdb(writer)
    try:
        row = conn.execute("SELECT value FROM dataset_info WHERE key = 'preset'").fetchone()
    finally:
        conn.close()
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


def test_upsert_empty_rows_return_zero(writer: DatasetWriter) -> None:
    assert writer.upsert_stock_data([]) == 0
    assert writer.upsert_topix_data([]) == 0
    assert writer.upsert_indices_data([]) == 0
    assert writer.upsert_margin_data([]) == 0
    assert writer.upsert_statements([]) == 0


def test_existing_code_helpers_and_topix_presence(writer: DatasetWriter) -> None:
    assert writer.get_existing_stock_data_codes() == set()
    assert writer.get_existing_index_codes() == set()
    assert writer.get_existing_margin_codes() == set()
    assert writer.get_existing_statement_codes() == set()
    assert writer.has_topix_data() is False

    writer.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-01-04",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 1000,
            "created_at": "2024-01-04",
        },
        {
            "code": "9984",
            "date": "2024-01-04",
            "open": 200,
            "high": 210,
            "low": 190,
            "close": 205,
            "volume": 2000,
            "created_at": "2024-01-04",
        },
    ])
    writer.upsert_topix_data([
        {"date": "2024-01-04", "open": 2500, "high": 2520, "low": 2480, "close": 2510, "created_at": "2024-01-04"},
    ])
    writer.upsert_indices_data([
        {"code": "0040", "date": "2024-01-04", "open": 1000, "high": 1010, "low": 990, "close": 1005},
    ])
    writer.upsert_margin_data([
        {"code": "7203", "date": "2024-01-04", "long_margin_volume": 50000, "short_margin_volume": 30000},
    ])
    writer.upsert_statements([
        {"code": "7203", "disclosed_date": "2024-03-15", "earnings_per_share": 250.0},
    ])

    assert writer.get_existing_stock_data_codes() == {"7203", "9984"}
    assert writer.has_topix_data() is True
    assert writer.get_existing_index_codes() == {"0040"}
    assert writer.get_existing_margin_codes() == {"7203"}
    assert writer.get_existing_statement_codes() == {"7203"}


def test_close_exports_parquet_bundle() -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    writer = DatasetWriter(path)
    try:
        writer.upsert_stocks([
            {
                "code": "7203",
                "company_name": "Toyota",
                "market_code": "111",
                "market_name": "プライム",
                "sector_17_code": "7",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "listed_date": "2000-01-01",
                "created_at": "2024-01-01",
            }
        ])
        writer.upsert_stock_data([
            {
                "code": "7203",
                "date": "2024-01-04",
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "volume": 1000,
                "created_at": "2024-01-04",
            }
        ])
        writer.close()
        assert writer.duckdb_path.exists() is True
        assert (writer.parquet_dir / "stocks.parquet").exists() is True
        assert (writer.parquet_dir / "stock_data.parquet").exists() is True
    finally:
        os.unlink(path)
        shutil.rmtree(writer.snapshot_dir)


def test_copy_stock_data_from_source_merges_alias_rows_and_tracks_invalid_rows(
    writer: DatasetWriter,
    tmp_path: Path,
) -> None:
    source_path = _create_source_market_duckdb(tmp_path)

    result = writer.copy_stock_data_from_source(
        source_duckdb_path=str(source_path),
        normalized_codes=["1111", "2222", "3333", "4444", "9999"],
    )

    assert result.inserted_rows == 3
    assert result.code_stats["1111"].valid_rows == 1
    assert result.code_stats["2222"].valid_rows == 1
    assert result.code_stats["2222"].skipped_rows == 0
    assert result.code_stats["3333"].total_rows == 1
    assert result.code_stats["3333"].valid_rows == 0
    assert result.code_stats["3333"].skipped_rows == 1
    assert result.code_stats["4444"].valid_rows == 1
    assert result.code_stats["9999"].total_rows == 0

    conn = _connect_duckdb(writer)
    try:
        rows = conn.execute(
            """
            SELECT code, date, open, high, low, close, volume, adjustment_factor
            FROM stock_data
            ORDER BY code, date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("1111", "2026-01-01", 10.0, 12.0, 9.0, 11.0, 1000, 1.0),
        ("2222", "2026-01-01", 20.0, 22.0, 19.0, 21.0, 2000, 1.0),
        ("4444", "2026-01-01", 40.0, 42.0, 39.0, 41.0, 4000, 1.0),
    ]


def test_copy_from_source_uses_temp_copy_when_source_db_is_already_open(
    writer: DatasetWriter, tmp_path: Path
) -> None:
    duckdb = importlib.import_module("duckdb")
    source_path = _create_source_market_duckdb(tmp_path)
    source_conn = duckdb.connect(str(source_path))
    try:
        stock_result = writer.copy_stock_data_from_source(
            source_duckdb_path=str(source_path),
            normalized_codes=["1111", "2222", "3333", "4444"],
        )
        statement_rows = writer.copy_statements_from_source(
            source_duckdb_path=str(source_path),
            normalized_codes=["1111", "2222"],
        )
        margin_rows = writer.copy_margin_data_from_source(
            source_duckdb_path=str(source_path),
            normalized_codes=["1111", "2222"],
        )

        assert stock_result.inserted_rows == 3
        assert statement_rows == 2
        assert margin_rows == 2

        conn = _connect_duckdb(writer)
        try:
            stock_codes = conn.execute("SELECT code FROM stock_data ORDER BY code").fetchall()
            statement_codes = conn.execute("SELECT code FROM statements ORDER BY code").fetchall()
            margin_codes = conn.execute("SELECT code FROM margin_data ORDER BY code").fetchall()
        finally:
            conn.close()

        assert stock_codes == [("1111",), ("2222",), ("4444",)]
        assert statement_codes == [("1111",), ("2222",)]
        assert margin_codes == [("1111",), ("2222",)]
    finally:
        source_conn.close()


def test_copy_statements_from_source_merges_alias_rows(writer: DatasetWriter, tmp_path: Path) -> None:
    source_path = _create_source_market_duckdb(tmp_path)

    inserted_rows = writer.copy_statements_from_source(
        source_duckdb_path=str(source_path),
        normalized_codes=["1111", "2222"],
    )

    assert inserted_rows == 2
    conn = _connect_duckdb(writer)
    try:
        rows = conn.execute(
            """
            SELECT code, disclosed_date, earnings_per_share, profit, forecast_eps, type_of_document
            FROM statements
            ORDER BY code, disclosed_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("1111", "2026-01-31", 10.0, 500.0, 12.0, "AnnualReport"),
        ("2222", "2026-01-31", 20.0, 600.0, 21.0, None),
    ]


def test_copy_margin_data_from_source_merges_alias_rows(writer: DatasetWriter, tmp_path: Path) -> None:
    source_path = _create_source_market_duckdb(tmp_path)

    inserted_rows = writer.copy_margin_data_from_source(
        source_duckdb_path=str(source_path),
        normalized_codes=["1111", "2222"],
    )

    assert inserted_rows == 2
    conn = _connect_duckdb(writer)
    try:
        rows = conn.execute(
            """
            SELECT code, date, long_margin_volume, short_margin_volume
            FROM margin_data
            ORDER BY code, date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("1111", "2026-01-01", 1000.0, 500.0),
        ("2222", "2026-01-01", 300.0, 200.0),
    ]
