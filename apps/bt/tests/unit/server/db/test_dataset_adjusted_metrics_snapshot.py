from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

import duckdb

from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetSnapshotReader,
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def test_dataset_snapshot_copies_adjusted_metrics_and_reader_exposes_them(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    try:
        conn.execute("""
            CREATE TABLE statement_metrics_adjusted (
                code TEXT,
                disclosed_date TEXT,
                period_end TEXT,
                period_type TEXT,
                price_basis_date TEXT,
                raw_eps DOUBLE,
                adjusted_eps DOUBLE,
                raw_bps DOUBLE,
                adjusted_bps DOUBLE,
                raw_forecast_eps DOUBLE,
                adjusted_forecast_eps DOUBLE,
                raw_dividend_fy DOUBLE,
                adjusted_dividend_fy DOUBLE,
                raw_shares_outstanding DOUBLE,
                adjusted_shares_outstanding DOUBLE,
                raw_treasury_shares DOUBLE,
                adjusted_treasury_shares DOUBLE,
                adjustment_factor_cumulative DOUBLE,
                basis_version TEXT,
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE daily_valuation (
                code TEXT,
                date TEXT,
                price_basis_date TEXT,
                close DOUBLE,
                eps DOUBLE,
                bps DOUBLE,
                forward_eps DOUBLE,
                per DOUBLE,
                forward_per DOUBLE,
                pbr DOUBLE,
                market_cap DOUBLE,
                free_float_market_cap DOUBLE,
                statement_disclosed_date TEXT,
                forward_eps_disclosed_date TEXT,
                forward_eps_source TEXT,
                basis_version TEXT,
                created_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO statement_metrics_adjusted VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "72030",
                "2024-05-10",
                "2024-03-31",
                "FY",
                "2024-12-30",
                100.0,
                50.0,
                1000.0,
                500.0,
                120.0,
                60.0,
                30.0,
                15.0,
                10_000_000.0,
                20_000_000.0,
                1_000_000.0,
                2_000_000.0,
                0.5,
                "adjusted-v1:2024-12-30",
                None,
            ),
        )
        conn.execute(
            "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "72030",
                "2024-12-30",
                "2024-12-30",
                500.0,
                50.0,
                500.0,
                60.0,
                10.0,
                8.3333,
                1.0,
                10_000_000_000.0,
                None,
                "2024-05-10",
                "2024-05-10",
                "fy",
                "adjusted-v1:2024-12-30",
                None,
            ),
        )
    finally:
        conn.close()

    snapshot_dir = tmp_path / "snapshot"
    writer = DatasetWriter(str(snapshot_dir))
    result = writer.copy_adjusted_metrics_from_source(
        source_duckdb_path=str(source_path),
        normalized_codes=["7203"],
    )
    writer.close()

    assert result == {
        "statement_metrics_adjusted": 1,
        "daily_valuation": 1,
    }

    inspection = inspect_dataset_snapshot_duckdb(snapshot_dir / "dataset.duckdb")
    parquet_dir = snapshot_dir / "parquet"
    parquet_checksums = {
        path.name: _sha256(path)
        for path in sorted(parquet_dir.glob("*.parquet"))
    }
    (snapshot_dir / "manifest.v2.json").write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "generatedAt": datetime.now(UTC).isoformat(),
                "dataset": {
                    "name": "adjusted-test",
                    "preset": "unit",
                    "duckdbFile": "dataset.duckdb",
                    "parquetDir": "parquet",
                },
                "source": {"backend": "duckdb-parquet"},
                "counts": inspection.counts.model_dump(),
                "coverage": inspection.coverage.model_dump(),
                "checksums": {
                    "duckdbSha256": _sha256(snapshot_dir / "dataset.duckdb"),
                    "logicalSha256": build_dataset_snapshot_logical_checksum(
                        counts=inspection.counts,
                        coverage=inspection.coverage,
                        date_range=inspection.date_range,
                    ),
                    "parquet": parquet_checksums,
                },
                "dateRange": None,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        adjusted = reader.get_adjusted_statement_metrics("7203")
        valuation = reader.get_daily_valuation("7203")
    finally:
        reader.close()

    assert adjusted[0]["adjusted_eps"] == 50.0
    assert valuation[0]["per"] == 10.0
