"""DatasetResolver のユニットテスト"""

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import threading

import pytest

import src.infrastructure.db.market.dataset_snapshot_reader as snapshot_reader_module
import src.application.services.dataset_resolver as dataset_resolver_module
from src.application.services.dataset_resolver import DatasetResolver
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)
from tests.unit.server.db.test_dataset_event_time_basis_snapshot import (
    _build_readable_provider_snapshot,
)
from tests.unit.server.test_dataset_snapshot_reader import _set_v4_source_info


def _write_manifest(snapshot_dir: Path, name: str, *, schema_version: int = 4) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest = {
        "schemaVersion": schema_version,
        "generatedAt": "2026-03-14T00:00:00+00:00",
        "dataset": {
            "name": name,
            "preset": "quickTesting",
            "duckdbFile": "dataset.duckdb",
            "parquetDir": "parquet",
        },
        "source": inspection.source.model_dump(),
        "logicalCounts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": hashlib.sha256(duckdb_path.read_bytes()).hexdigest(),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                source=inspection.source,
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


@pytest.fixture
def resolver_dir(tmp_path: Path) -> str:
    """テスト用のデータセットディレクトリ"""
    for name in ["test-market", "prime_v2"]:
        writer = DatasetWriter(str(tmp_path / name))
        writer.set_dataset_info("preset", "quickTesting")
        _set_v4_source_info(
            writer, coverage_start="2026-03-14", coverage_end="2026-03-14"
        )
        writer.close()
        _write_manifest(tmp_path / name, name)

    compat_snapshot_dir = tmp_path / "compat_only"
    compat_snapshot_dir.mkdir()
    (compat_snapshot_dir / "dataset.db").write_text("", encoding="utf-8")
    return str(tmp_path)


class TestDatasetResolver:
    def test_support_validation_cache_reuses_typed_proof(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0
        original = dataset_resolver_module.validate_supported_dataset_snapshot_proof

        def counted(path):
            nonlocal calls
            calls += 1
            return original(path)

        monkeypatch.setattr(
            dataset_resolver_module,
            "validate_supported_dataset_snapshot_proof",
            counted,
        )
        resolver = DatasetResolver(resolver_dir)

        first = resolver.resolve("test-market")
        assert first is not None
        assert resolver.exists("test-market") is True
        assert resolver.resolve("test-market") is first
        assert calls == 1
        assert "test-market" in resolver.list_datasets()
        assert "test-market" in resolver.list_datasets()
        assert calls == 2  # one validation per distinct valid bundle listed

    def test_artifact_stat_change_revalidates_and_evicts_reader(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0
        original = dataset_resolver_module.validate_supported_dataset_snapshot_proof

        def counted(path):
            nonlocal calls
            calls += 1
            return original(path)

        monkeypatch.setattr(
            dataset_resolver_module,
            "validate_supported_dataset_snapshot_proof",
            counted,
        )
        resolver = DatasetResolver(resolver_dir)
        first = resolver.resolve("test-market")
        assert first is not None and calls == 1
        duckdb_path = Path(resolver_dir) / "test-market" / "dataset.duckdb"
        duckdb_path.touch()

        second = resolver.resolve("test-market")

        assert second is not None and second is not first
        assert calls == 2

    def test_concurrent_artifact_change_during_validation_fails_closed(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        original = dataset_resolver_module.validate_supported_dataset_snapshot_proof

        def mutate_after_validation(path):
            proof = original(path)
            (Path(path) / "manifest.v2.json").touch()
            return proof

        monkeypatch.setattr(
            dataset_resolver_module,
            "validate_supported_dataset_snapshot_proof",
            mutate_after_validation,
        )
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert resolver.resolve("test-market") is None

    def test_cached_reader_changed_during_final_check_is_never_returned(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolver = DatasetResolver(resolver_dir)
        first = resolver.resolve("test-market")
        assert first is not None
        original = dataset_resolver_module.build_dataset_artifact_fingerprint
        calls = 0

        def mutate_during_final_check(path):
            nonlocal calls
            calls += 1
            if calls == 3:
                (Path(path) / "dataset.duckdb").touch()
            return original(path)

        monkeypatch.setattr(
            dataset_resolver_module,
            "build_dataset_artifact_fingerprint",
            mutate_during_final_check,
        )

        second = resolver.resolve("test-market")

        assert second is not None and second is not first

    def test_resolve_bounds_persistent_final_fingerprint_mismatch(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolver = DatasetResolver(resolver_dir)
        normalized = "test-market"
        snapshot_dir = resolver.get_snapshot_dir(normalized)
        proof = resolver._validation_proof(normalized, snapshot_dir)
        assert proof is not None
        proof_calls = 0
        fingerprint_calls = 0

        def counted_proof(requested_name, requested_path):
            nonlocal proof_calls
            assert requested_name == normalized
            assert requested_path == snapshot_dir
            proof_calls += 1
            return proof

        def always_mutating_fingerprint(path):
            nonlocal fingerprint_calls
            assert str(path) == snapshot_dir
            fingerprint_calls += 1
            return object()

        monkeypatch.setattr(resolver, "_validation_proof", counted_proof)
        monkeypatch.setattr(
            dataset_resolver_module,
            "build_dataset_artifact_fingerprint",
            always_mutating_fingerprint,
        )

        assert resolver.resolve(normalized) is None
        assert proof_calls == 2
        assert fingerprint_calls == 2
        assert normalized not in resolver._cache
        assert resolver._retired == []

    def test_never_returned_candidates_do_not_accumulate_as_retired_readers(
        self, resolver_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolver = DatasetResolver(resolver_dir)
        normalized = "test-market"
        snapshot_dir = resolver.get_snapshot_dir(normalized)
        proof = resolver._validation_proof(normalized, snapshot_dir)
        assert proof is not None
        proof_calls = 0
        fingerprint_calls = 0

        def counted_proof(requested_name, requested_path):
            nonlocal proof_calls
            assert requested_name == normalized
            assert requested_path == snapshot_dir
            proof_calls += 1
            return proof

        def mutate_after_candidate_construction(path):
            nonlocal fingerprint_calls
            assert str(path) == snapshot_dir
            fingerprint_calls += 1
            if fingerprint_calls % 2:
                return proof.fingerprint
            return object()

        monkeypatch.setattr(resolver, "_validation_proof", counted_proof)
        monkeypatch.setattr(
            dataset_resolver_module,
            "build_dataset_artifact_fingerprint",
            mutate_after_candidate_construction,
        )

        for _ in range(3):
            assert resolver.resolve(normalized) is None

        assert proof_calls == 6
        assert fingerprint_calls == 12
        assert normalized not in resolver._cache
        assert resolver._retired == []

    @pytest.mark.parametrize(
        "artifact",
        ["manifest.v2.json", "dataset.duckdb", "parquet/stocks.parquet"],
    )
    def test_discovery_rejects_symlink_artifacts(
        self, resolver_dir: str, artifact: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        artifact_path = snapshot_dir / artifact
        replacement = Path(resolver_dir) / f"outside-{artifact_path.name}"
        replacement.write_bytes(artifact_path.read_bytes())
        artifact_path.unlink()
        artifact_path.symlink_to(replacement)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    def test_discovery_rejects_symlink_snapshot_root(self, resolver_dir: str) -> None:
        alias = Path(resolver_dir) / "alias-market"
        alias.symlink_to(Path(resolver_dir) / "test-market", target_is_directory=True)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("alias-market") is False
        assert "alias-market" not in resolver.list_datasets()
        assert resolver.resolve("alias-market") is None

    def test_discovery_rejects_symlink_parquet_directory(
        self, resolver_dir: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        parquet_dir = snapshot_dir / "parquet"
        real_parquet_dir = snapshot_dir / "parquet-real"
        parquet_dir.rename(real_parquet_dir)
        parquet_dir.symlink_to(real_parquet_dir, target_is_directory=True)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    def test_changed_reader_is_retired_while_query_is_active(
        self,
        resolver_dir: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        resolver = DatasetResolver(resolver_dir)
        first = resolver.resolve("test-market")
        assert first is not None
        entered = threading.Event()
        release = threading.Event()
        close_calls = 0
        original_query = first.query
        original_close = first.close

        def blocked_query(sql, params=()):
            entered.set()
            assert release.wait(timeout=5)
            return original_query(sql, params)

        def counted_close():
            nonlocal close_calls
            close_calls += 1
            original_close()

        monkeypatch.setattr(first, "query", blocked_query)
        monkeypatch.setattr(first, "close", counted_close)
        worker = threading.Thread(target=lambda: first.query("SELECT 1"))
        worker.start()
        assert entered.wait(timeout=5)

        (Path(resolver_dir) / "test-market" / "dataset.duckdb").touch()
        second = resolver.resolve("test-market")

        assert second is not None and second is not first
        assert close_calls == 0
        release.set()
        worker.join(timeout=5)
        assert not worker.is_alive()
        resolver.close_all()
        assert close_calls == 1

    def test_list_datasets(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        names = resolver.list_datasets()
        assert sorted(names) == ["prime_v2", "test-market"]

    def test_resolve_existing(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("test-market")
        assert db is not None

    def test_resolve_rejects_db_extension(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("test-market.db")

    def test_resolve_nonexistent(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("nonexistent")
        assert db is None

    def test_resolve_caches(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db1 = resolver.resolve("test-market")
        db2 = resolver.resolve("test-market")
        assert db1 is db2

    def test_validate_name_invalid(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("../etc/passwd")

    def test_validate_name_path_traversal(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("../../secret")

    def test_evict(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db1 = resolver.resolve("test-market")
        assert db1 is not None
        resolver.evict("test-market")
        db2 = resolver.resolve("test-market")
        assert db2 is not None
        assert db1 is not db2

    def test_close_all(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        resolver.resolve("test-market")
        resolver.resolve("prime_v2")
        resolver.close_all()
        assert len(resolver._cache) == 0

    def test_get_duckdb_path(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_duckdb_path("test-market")
        assert path.endswith("test-market/dataset.duckdb")

    def test_get_dataset_path_prefers_snapshot_dir(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("test-market")
        assert path.endswith("test-market")

    def test_exists_checks_supported_snapshots_only(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        assert resolver.exists("test-market") is True
        assert resolver.exists("prime_v2") is True
        assert resolver.exists("compat_only") is False
        assert resolver.exists("missing") is False

    def test_discovery_rejects_schema_version_two_without_opening_duckdb(
        self, resolver_dir: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        _write_manifest(snapshot_dir, "test-market", schema_version=2)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    def test_discovery_rejects_missing_provider_adjustment_mode(
        self, resolver_dir: str
    ) -> None:
        manifest_path = Path(resolver_dir) / "test-market" / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        del manifest["source"]["stockPriceAdjustmentMode"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()

    def test_discovery_rejects_v4_bundle_without_provider_vintage(
        self, resolver_dir: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        duckdb = importlib.import_module("duckdb")
        conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
        try:
            conn.execute(
                "DELETE FROM dataset_info WHERE key = ?",
                ["provider_as_of"],
            )
        finally:
            conn.close()
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    @pytest.mark.parametrize(
        "corruption",
        ["missing_table", "checksum_mismatch"],
    )
    def test_discovery_rejects_db_inconsistent_with_valid_v3_manifest(
        self, resolver_dir: str, corruption: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        duckdb = importlib.import_module("duckdb")
        conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
        try:
            if corruption == "missing_table":
                conn.execute("DROP TABLE statements")
            else:
                conn.execute(
                    "UPDATE dataset_info SET value = 'tampered' WHERE key = 'preset'"
                )
        finally:
            conn.close()
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    def test_discovery_rejects_invalid_provider_payload_lineage(
        self, tmp_path: Path
    ) -> None:
        snapshot_dir = _build_readable_provider_snapshot(tmp_path)
        duckdb_path = snapshot_dir / "dataset.duckdb"
        duckdb = importlib.import_module("duckdb")
        conn = duckdb.connect(str(duckdb_path))
        try:
            conn.execute("UPDATE stock_data_raw SET adjusted_close = adjusted_close + 1")
        finally:
            conn.close()
        manifest_path = snapshot_dir / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["checksums"]["duckdbSha256"] = hashlib.sha256(
            duckdb_path.read_bytes()
        ).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        resolver = DatasetResolver(str(tmp_path))

        assert resolver.exists(snapshot_dir.name) is False
        assert snapshot_dir.name not in resolver.list_datasets()
        assert resolver.resolve(snapshot_dir.name) is None

    @pytest.mark.parametrize(
        ("field_path", "value"),
        [
            (("schemaVersion",), 3.0),
            (("schemaVersion",), True),
            (("source", "marketSchemaVersion"), 4.0),
            (("source", "marketSchemaVersion"), True),
        ],
    )
    def test_discovery_rejects_coercible_non_integer_lineage_versions(
        self,
        resolver_dir: str,
        field_path: tuple[str, ...],
        value: float | bool,
    ) -> None:
        manifest_path = Path(resolver_dir) / "test-market" / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        target = manifest
        for key in field_path[:-1]:
            target = target[key]
        target[field_path[-1]] = value
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    @pytest.mark.parametrize(
        ("section", "mutation"),
        [
            ("dataset", lambda manifest: manifest.pop("dataset")),
            ("logicalCounts", lambda manifest: manifest.pop("logicalCounts")),
            ("coverage", lambda manifest: manifest.pop("coverage")),
            ("checksums", lambda manifest: manifest.pop("checksums")),
            (
                "dataset.duckdbFile",
                lambda manifest: manifest["dataset"].__setitem__("duckdbFile", "dataset.db"),
            ),
            (
                "logicalCounts.stocks",
                lambda manifest: manifest["logicalCounts"].__setitem__("stocks", -1),
            ),
            (
                "coverage.totalStocks",
                lambda manifest: manifest["coverage"].__setitem__("totalStocks", -1),
            ),
            (
                "checksums.duckdbSha256",
                lambda manifest: manifest["checksums"].__setitem__(
                    "duckdbSha256", "not-a-sha256"
                ),
            ),
        ],
    )
    def test_discovery_rejects_partial_or_malformed_v3_manifest_without_opening_duckdb(
        self,
        resolver_dir: str,
        monkeypatch: pytest.MonkeyPatch,
        section: str,
        mutation,
    ) -> None:
        del section
        manifest_path = Path(resolver_dir) / "test-market" / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        mutation(manifest)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_connect = snapshot_reader_module._connect_duckdb

        def guarded_connect(duckdb_path, *, read_only):
            if Path(duckdb_path).parent.name == "test-market":
                pytest.fail("discovery opened malformed Dataset DuckDB")
            return original_connect(duckdb_path, read_only=read_only)

        monkeypatch.setattr(snapshot_reader_module, "_connect_duckdb", guarded_connect)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()

    def test_discovery_rejects_partial_snapshot_without_manifest(self, resolver_dir: str) -> None:
        partial = Path(resolver_dir) / "cancelled"
        writer = DatasetWriter(str(partial))
        writer.close()
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("cancelled") is False
        assert "cancelled" not in resolver.list_datasets()

    def test_resolve_unsupported_compatibility_snapshot_returns_none(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        assert resolver.resolve("compat_only") is None

    def test_get_dataset_path_for_unsupported_snapshot_dir(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("compat_only")
        assert path.endswith("compat_only")

    def test_empty_dir(self, tmp_path: Path) -> None:
        resolver = DatasetResolver(str(tmp_path))
        assert resolver.list_datasets() == []

    def test_nonexistent_dir(self) -> None:
        resolver = DatasetResolver("/nonexistent/path")
        assert resolver.list_datasets() == []
