from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.market.market_compaction import compact_market_duckdb


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
