"""Dataset Management ルートの統合テスト"""

from __future__ import annotations

import importlib
import hashlib
import json
from pathlib import Path
import shutil
import time
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from src.application.services.dataset_builder_service import dataset_job_manager
from src.application.services.dataset_resolver import DatasetResolver
from src.entrypoints.http.app import create_app
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)
from src.infrastructure.db.market.market_reader import MarketDbReader


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


def _create_market_source_duckdb(base_dir: Path) -> Path:
    duckdb = importlib.import_module("duckdb")
    source_path = base_dir / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    try:
        conn.execute(
            """
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
            )
            """
        )
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
            CREATE TABLE topix_data (
                date TEXT PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE indices_data (
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                sector_name TEXT,
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
            """
            CREATE TABLE statements (
                code TEXT,
                disclosed_date TEXT,
                earnings_per_share DOUBLE,
                profit DOUBLE,
                equity DOUBLE,
                type_of_current_period TEXT,
                type_of_document TEXT,
                next_year_forecast_earnings_per_share DOUBLE,
                bps DOUBLE,
                sales DOUBLE,
                operating_profit DOUBLE,
                ordinary_profit DOUBLE,
                operating_cash_flow DOUBLE,
                dividend_fy DOUBLE,
                forecast_dividend_fy DOUBLE,
                next_year_forecast_dividend_fy DOUBLE,
                payout_ratio DOUBLE,
                forecast_payout_ratio DOUBLE,
                next_year_forecast_payout_ratio DOUBLE,
                forecast_eps DOUBLE,
                investing_cash_flow DOUBLE,
                financing_cash_flow DOUBLE,
                cash_and_equivalents DOUBLE,
                total_assets DOUBLE,
                shares_outstanding DOUBLE,
                treasury_shares DOUBLE,
                PRIMARY KEY (code, disclosed_date)
            )
            """
        )

        conn.executemany(
            "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "Alpha", "ALPHA", "0111", "プライム", "7", "輸送用機器", "3050", "輸送用機器", "TOPIX Core30", "2001-01-01", None, None),
                ("2222", "Beta", "BETA", "0111", "プライム", "9", "情報・通信業", "5250", "情報・通信業", "TOPIX Large70", "2002-02-02", None, None),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 10.0, 12.0, 9.0, 11.0, 1000, 1.0, "2026-01-01T00:00:00+00:00"),
                ("2222", "2026-01-01", 20.0, None, 19.0, 20.5, 2000, 1.0, "2026-01-01T00:00:00+00:00"),
            ],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            ("2026-01-01", 2000.0, 2010.0, 1990.0, 2005.0, "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0040", "2026-01-01", 500.0, 510.0, 495.0, 505.0, "Sector 40", "2026-01-01T00:00:00+00:00"),
        )
        conn.executemany(
            "INSERT INTO margin_data VALUES (?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 1000.0, 500.0),
                ("2222", "2026-01-01", 300.0, 200.0),
            ],
        )
        conn.executemany(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "2026-01-31", 10.0, 500.0, None, "FY", "AnnualReport", None, None, None, None, None, None, None, None, None, None, None, None, 12.0, None, None, None, None, None, None),
                ("2222", "2026-01-31", 20.0, 600.0, None, "FY", "AnnualReport", None, None, None, None, None, None, None, None, None, None, None, None, 21.0, None, None, None, None, None, None),
            ],
        )
    finally:
        conn.close()
    return source_path


@pytest.fixture(scope="module")
def dataset_template_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    template_dir = tmp_path_factory.mktemp("dataset-routes-template")
    _build_snapshot(template_dir, "test-market")
    return template_dir


@pytest.fixture(scope="module")
def market_source_template_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _create_market_source_duckdb(tmp_path_factory.mktemp("dataset-routes-market"))


@pytest.fixture(scope="module")
def app_client() -> Generator[TestClient, None, None]:
    app = create_app()
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def test_dataset_dir(tmp_path: Path, dataset_template_dir: Path) -> str:
    """テスト用のデータセットディレクトリ"""
    dataset_dir = tmp_path / "datasets"
    shutil.copytree(dataset_template_dir, dataset_dir)
    return str(dataset_dir)


@pytest.fixture
def client(app_client: TestClient, test_dataset_dir: str) -> TestClient:
    app_client.app.state.dataset_resolver = DatasetResolver(test_dataset_dir)
    return app_client


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
        assert data["storage"]["primaryPath"].endswith("/test-market/dataset.duckdb")
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

    def test_create_dataset_route_builds_valid_snapshot(
        self,
        tmp_path: Path,
        market_source_template_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dataset_root = tmp_path / "datasets"
        dataset_root.mkdir(parents=True, exist_ok=True)
        source_path = tmp_path / "market.duckdb"
        shutil.copyfile(market_source_template_path, source_path)

        app = create_app()
        with TestClient(app) as client:
            app.state.dataset_resolver = DatasetResolver(str(dataset_root))
            market_reader = MarketDbReader(str(source_path))
            app.state.market_reader = market_reader
            monkeypatch.setattr(
                "src.entrypoints.http.routes.dataset._get_market_duckdb_path",
                lambda: str(source_path),
            )

            create_resp = client.post(
                "/api/dataset",
                json={"name": "created-direct", "preset": "quickTesting", "overwrite": True},
            )
            assert create_resp.status_code == 202
            job_id = create_resp.json()["jobId"]
            job = dataset_job_manager.get_job(job_id)

            assert job is not None
            for _ in range(100):
                if job.status.value in {"completed", "failed", "cancelled"}:
                    break
                time.sleep(0.002)

            assert job.status.value == "completed"
            assert job.result is not None
            assert job.result.success is True

            info_resp = client.get("/api/dataset/created-direct/info")
            assert info_resp.status_code == 200
            info = info_resp.json()
            assert info["validation"]["isValid"] is True
            assert info["stats"]["totalStocks"] == 2
            assert info["stats"]["totalQuotes"] == 1
            market_reader.close()
