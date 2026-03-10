from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetSnapshotReader,
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
    validate_dataset_snapshot,
)


def _create_snapshot(tmp_path: Path) -> Path:
    writer = DatasetWriter(str(tmp_path / "sample.db"))
    writer.upsert_stocks([
        {
            "code": "7203",
            "company_name": "Toyota",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "7",
            "sector_17_name": "輸送用機器",
            "sector_33_code": "3050",
            "sector_33_name": "輸送用機器",
            "listed_date": "1949-05-16",
        }
    ])
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()

    snapshot_dir = tmp_path / "sample"
    compatibility_path = snapshot_dir / "dataset.db"
    duckdb_path = snapshot_dir / "dataset.duckdb"
    duckdb_sha = hashlib.sha256(duckdb_path.read_bytes()).hexdigest()
    compatibility_sha = hashlib.sha256(compatibility_path.read_bytes()).hexdigest()
    parquet_sha = hashlib.sha256((snapshot_dir / "parquet" / "stocks.parquet").read_bytes()).hexdigest()
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)

    manifest = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T00:00:00+00:00",
        "dataset": {
            "name": "sample",
            "preset": "quickTesting",
            "duckdbFile": "dataset.duckdb",
            "compatibilityDbFile": "dataset.db",
            "parquetDir": "parquet",
        },
        "source": {
            "backend": "duckdb-parquet",
            "compatibilityArtifact": "dataset.db",
        },
        "counts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": duckdb_sha,
            "compatibilityDbSha256": compatibility_sha,
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                "stocks.parquet": parquet_sha,
            },
        },
    }
    if inspection.date_range is not None:
        manifest["dateRange"] = inspection.date_range.model_dump()
    (snapshot_dir / "manifest.v1.json").write_text(json.dumps(manifest), encoding="utf-8")
    return snapshot_dir


def test_validate_dataset_snapshot_accepts_valid_bundle(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)

    manifest = validate_dataset_snapshot(snapshot_dir)

    assert manifest.dataset.name == "sample"


def test_dataset_snapshot_reader_uses_compatibility_db(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)

    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.get_dataset_info()["preset"] == "quickTesting"
    finally:
        reader.close()


def test_dataset_snapshot_reader_prefers_duckdb_over_compatibility_db(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    compatibility_path = snapshot_dir / "dataset.db"
    conn = sqlite3.connect(compatibility_path)
    conn.execute("UPDATE dataset_info SET value = 'stale' WHERE key = 'preset'")
    conn.commit()
    conn.close()
    manifest_path = snapshot_dir / "manifest.v1.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["checksums"]["compatibilityDbSha256"] = hashlib.sha256(
        compatibility_path.read_bytes()
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.get_dataset_info()["preset"] == "quickTesting"
    finally:
        reader.close()


def test_validate_dataset_snapshot_rejects_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    (snapshot_dir / "dataset.duckdb").write_text("tampered", encoding="utf-8")

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_logical_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest_path = snapshot_dir / "manifest.v1.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["checksums"]["logicalSha256"] = "stale"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RuntimeError, match="logical checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_compatibility_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    compatibility_path = snapshot_dir / "dataset.db"
    conn = sqlite3.connect(compatibility_path)
    conn.execute("UPDATE dataset_info SET value = 'tampered' WHERE key = 'preset'")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="dataset.db checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)
