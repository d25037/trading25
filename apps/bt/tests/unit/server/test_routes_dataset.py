"""Dataset Management ルートの統合テスト"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.application.services.dataset_resolver import DatasetResolver
from src.entrypoints.http.app import create_app
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)


def _write_manifest_v2(snapshot_dir: Path, name: str) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest = {
        "schemaVersion": 2,
        "generatedAt": "2026-03-14T00:00:00+00:00",
        "dataset": {
            "name": name,
            "preset": "primeMarket",
            "duckdbFile": "dataset.duckdb",
            "parquetDir": "parquet",
        },
        "source": {
            "backend": "duckdb-parquet",
        },
        "counts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": hashlib.sha256(duckdb_path.read_bytes()).hexdigest(),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                parquet_file.name: hashlib.sha256(parquet_file.read_bytes()).hexdigest()
                for parquet_file in sorted(parquet_dir.glob("*.parquet"))
            },
        },
    }
    if inspection.date_range is not None:
        manifest["dateRange"] = inspection.date_range.model_dump()
    (snapshot_dir / "manifest.v2.json").write_text(json.dumps(manifest), encoding="utf-8")


def _build_snapshot(base_dir: Path, name: str) -> None:
    writer = DatasetWriter(str(base_dir / name))
    writer.upsert_stocks([
        {
            "code": "7203",
            "company_name": "トヨタ自動車",
            "company_name_english": "TOYOTA",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "7",
            "sector_17_name": "輸送用機器",
            "sector_33_code": "3050",
            "sector_33_name": "輸送用機器",
            "scale_category": "TOPIX Core30",
            "listed_date": "1949-05-16",
        },
        {
            "code": "9984",
            "company_name": "ソフトバンク",
            "company_name_english": "SB",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "9",
            "sector_17_name": "情報・通信業",
            "sector_33_code": "3700",
            "sector_33_name": "情報・通信業",
            "scale_category": "TOPIX Core30",
            "listed_date": "1994-07-22",
        },
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
            "adjustment_factor": 1.0,
        },
        {
            "code": "9984",
            "date": "2024-01-04",
            "open": 200,
            "high": 210,
            "low": 190,
            "close": 205,
            "volume": 500,
            "adjustment_factor": 1.0,
        },
    ])
    writer.set_dataset_info("preset", "primeMarket")
    writer.set_dataset_info("created_at", "2026-01-01T00:00:00+00:00")
    writer.set_dataset_info("stock_count", "2")
    writer.close()
    _write_manifest_v2(base_dir / name, name)


@pytest.fixture
def test_dataset_dir(tmp_path: Path):
    """テスト用のデータセットディレクトリ"""
    _build_snapshot(Path(tmp_path), "test-market")
    return str(tmp_path)


@pytest.fixture
def client(test_dataset_dir: str):
    app = create_app()
    app.state.dataset_resolver = DatasetResolver(test_dataset_dir)
    return TestClient(app, raise_server_exceptions=False)


class TestDatasetManagementRoutes:
    def test_list_datasets(self, client: TestClient) -> None:
        resp = client.get("/api/dataset")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test-market"
        assert data[0]["fileSize"] > 0
        assert data[0]["preset"] == "primeMarket"
        assert data[0]["createdAt"] == "2026-01-01T00:00:00+00:00"
        assert data[0]["backend"] == "duckdb-parquet"

    def test_list_datasets_handles_unsupported_partial_snapshot(self, client: TestClient, test_dataset_dir: str) -> None:
        broken_dir = Path(test_dataset_dir) / "broken"
        broken_dir.mkdir()
        (broken_dir / "dataset.duckdb").write_text("", encoding="utf-8")

        resp = client.get("/api/dataset")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert all(item["name"] != "broken" for item in data)

    def test_dataset_info(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test-market"
        assert data["snapshot"]["createdAt"] == "2026-01-01T00:00:00+00:00"
        assert data["snapshot"]["totalStocks"] == 2
        assert data["snapshot"]["preset"] == "primeMarket"
        assert data["snapshot"]["validation"]["isValid"] is True
        assert data["stats"]["totalStocks"] == 2
        assert data["stats"]["totalQuotes"] == 2
        assert data["stats"]["dateRange"]["from"] == "2024-01-04"
        assert data["stats"]["dateRange"]["to"] == "2024-01-04"
        assert data["storage"]["backend"] == "duckdb-parquet"
        assert data["storage"]["primaryPath"].endswith("/test-market")
        assert data["storage"]["duckdbPath"].endswith("/test-market/dataset.duckdb")
        assert data["storage"]["manifestPath"].endswith("/test-market/manifest.v2.json")
        assert data["validation"]["details"]["dataCoverage"]["stocksWithQuotes"] == 2
        assert data["validation"]["details"]["stockCountValidation"]["isWithinRange"] is True

    def test_dataset_info_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/nonexistent/info")
        assert resp.status_code == 404

    def test_dataset_sample(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["codes"]) == 2

    def test_dataset_sample_deterministic(self, client: TestClient) -> None:
        resp1 = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        resp2 = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        assert resp1.json()["codes"] == resp2.json()["codes"]

    def test_dataset_search(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/search?q=トヨタ")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) >= 1
        assert data["results"][0]["code"] == "7203"

    def test_dataset_search_by_code(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/search?q=7203")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) >= 1

    def test_dataset_search_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/nonexistent/search?q=test")
        assert resp.status_code == 404

    def test_delete_dataset(self, client: TestClient, test_dataset_dir: str) -> None:
        delete_dir = Path(test_dataset_dir) / "to-delete"
        _build_snapshot(Path(test_dataset_dir), "to-delete")
        legacy_db = Path(test_dataset_dir) / "to-delete.db"
        legacy_db.write_text("", encoding="utf-8")

        resp = client.delete("/api/dataset/to-delete")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert not delete_dir.exists()
        assert not legacy_db.exists()

    def test_delete_nonexistent(self, client: TestClient) -> None:
        resp = client.delete("/api/dataset/nonexistent")
        assert resp.status_code == 404
