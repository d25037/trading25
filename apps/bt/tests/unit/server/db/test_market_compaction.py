from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.market.market_compaction import (
    compact_market_duckdb,
    compact_market_duckdb_in_place_if_needed,
)


def test_compact_market_duckdb_creates_independent_copy(tmp_path: Path) -> None:
    source_path = tmp_path / "market.duckdb"
    output_path = tmp_path / "market.compact.duckdb"
    conn = duckdb.connect(str(source_path))
    conn.execute("CREATE TABLE stock_data(code VARCHAR, date DATE, close DOUBLE)")
    conn.execute("INSERT INTO stock_data VALUES ('7203', DATE '2026-01-05', 3000.0)")
    conn.close()

    result = compact_market_duckdb(source_path, output_path)

    assert result.source_path == source_path
    assert result.output_path == output_path
    assert result.source_bytes > 0
    assert result.output_bytes > 0
    assert result.table_count == 1
    copied = duckdb.connect(str(output_path), read_only=True)
    try:
        rows = copied.execute("SELECT code, date, close FROM stock_data").fetchall()
    finally:
        copied.close()
    assert rows == [("7203", date(2026, 1, 5), 3000.0)]


def test_compact_market_duckdb_rejects_in_place_output(tmp_path: Path) -> None:
    source_path = tmp_path / "market.duckdb"
    duckdb.connect(str(source_path)).close()

    with pytest.raises(ValueError, match="must differ"):
        compact_market_duckdb(source_path, source_path)


def test_compact_market_duckdb_in_place_skips_when_free_space_is_below_threshold(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    conn.execute("CREATE TABLE stock_data(code VARCHAR, date DATE, close DOUBLE)")
    conn.execute("INSERT INTO stock_data VALUES ('7203', DATE '2026-01-05', 3000.0)")
    conn.close()

    result = compact_market_duckdb_in_place_if_needed(
        source_path,
        min_free_bytes=1,
        min_free_ratio=0.01,
    )

    assert result.compacted is False
    assert result.reason == "below_threshold"
    assert result.before_bytes == result.after_bytes
    assert result.after_free_bytes == result.before_free_bytes


def test_compact_market_duckdb_in_place_replaces_source_when_free_space_crosses_threshold(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    conn.execute("CREATE TABLE stock_data(code VARCHAR, date DATE, close DOUBLE)")
    conn.execute("INSERT INTO stock_data VALUES ('7203', DATE '2026-01-05', 3000.0)")
    conn.close()

    result = compact_market_duckdb_in_place_if_needed(
        source_path,
        min_free_bytes=0,
        min_free_ratio=0,
    )

    assert result.compacted is True
    assert result.reason == "compacted"
    assert result.table_count == 1
    assert result.before_bytes > 0
    assert result.after_bytes > 0
    assert result.after_free_bytes == 0
    copied = duckdb.connect(str(source_path), read_only=True)
    try:
        rows = copied.execute("SELECT code, date, close FROM stock_data").fetchall()
    finally:
        copied.close()
    assert rows == [("7203", date(2026, 1, 5), 3000.0)]
