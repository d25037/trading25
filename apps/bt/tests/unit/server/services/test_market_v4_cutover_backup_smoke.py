"""Market v4 cutover backup smoke tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.errors import (
    WorkerShutdownError,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _changing_code_version,
)


def test_preflight_fails_closed_when_server_or_jobs_are_active(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)

    with pytest.raises(CutoverSafetyError, match="FastAPI process must be stopped"):
        _service(data_root, runtime=FakeRuntime(stopped=False)).preflight()
    with pytest.raises(CutoverSafetyError, match="active jobs: sync"):
        _service(data_root, runtime=FakeRuntime(active_jobs=("sync",))).preflight()


def test_preflight_requires_exclusive_checkpoint_and_empty_wal(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    with pytest.raises(CutoverSafetyError, match="exclusive writable"):
        _service(
            data_root,
            duckdb=FakeDuckDb(checkpoint_error=RuntimeError("locked")),
        ).preflight()

    with pytest.raises(CutoverSafetyError, match="WAL"):
        _service(data_root, duckdb=FakeDuckDb(leave_wal=True)).preflight()


def test_preflight_unjoined_worker_transfers_active_lease(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        retained_guard_fd = -1

        def checkpoint_exclusive(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            del directory_fd, filename
            self.retained_guard_fd = os.dup(guard_lease_fd)
            raise WorkerShutdownError(
                "injected unjoined checkpoint worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb()
    service = _service(data_root, duckdb=duckdb)

    with pytest.raises(CutoverSafetyError):
        service.preflight()

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_preflight_rejects_market_root_symlink_before_duckdb_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-market"
    shutil.move(data_root / "market-timeseries", external)
    (data_root / "market-timeseries").symlink_to(external, target_is_directory=True)
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(data_root, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert (external / "market.duckdb").read_bytes() == b"duckdb-v3"


def test_preflight_rejects_selected_data_root_symlink_before_external_mutation(
    tmp_path: Path,
) -> None:
    external = _market_root(tmp_path / "external")
    selected = tmp_path / "selected-root"
    selected.symlink_to(external, target_is_directory=True)
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(selected, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert not (external / ".market-timeseries.operation.lock").exists()


def test_preflight_rejects_symlink_in_selected_root_ancestor(tmp_path: Path) -> None:
    external_parent = tmp_path / "external-parent"
    data_root = _market_root(external_parent)
    alias = tmp_path / "alias"
    alias.symlink_to(external_parent, target_is_directory=True)
    selected = alias / data_root.name
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(selected, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert not (data_root / ".market-timeseries.operation.lock").exists()


def test_preflight_requires_capacity_for_backup_rebuild_and_staging(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    with pytest.raises(CutoverSafetyError, match="Insufficient free space"):
        _service(data_root, free_bytes=1).preflight()


def test_backup_is_recursive_checksummed_and_immutable(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)

    result = service.backup("backup-001")

    backup_dir = data_root / "operations/market-v4-cutover/backups/backup-001"
    manifest = json.loads((backup_dir / "manifest.json").read_text())
    assert result.backup_id == "backup-001"
    assert [entry["path"] for entry in manifest["files"]] == [
        "market.duckdb",
        "parquet/stock_data/part.parquet",
    ]
    assert manifest["source"] == {
        "schemaVersion": 3,
        "stockPriceAdjustmentMode": "local_projection_v1",
    }
    assert all(len(entry["sha256"]) == 64 for entry in manifest["files"])
    assert not (backup_dir.stat().st_mode & 0o200)
    assert service.verify_backup("backup-001").backup_id == "backup-001"


def test_backup_manifest_uses_operation_start_code_identity(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    code_version, calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service = _service(data_root)
    service.code_version = code_version

    service.backup("captured-identity")

    manifest = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/backups/captured-identity/manifest.json"
        ).read_text()
    )
    assert manifest["codeVersion"] == "deadbeef"
    assert calls == ["deadbeef"]


def test_backup_fails_for_existing_destination_symlink_and_checksum_mismatch(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    backup_parent = data_root / "operations/market-v4-cutover/backups"
    (backup_parent / "existing").mkdir(parents=True)
    with pytest.raises(CutoverSafetyError, match="already exists"):
        service.backup("existing")

    symlink = data_root / "market-timeseries/parquet/link"
    symlink.symlink_to(data_root / "market-timeseries/market.duckdb")
    with pytest.raises(CutoverSafetyError, match="symlink"):
        service.backup("with-link")
    symlink.unlink()

    service.backup("corrupt")
    copied_db = backup_parent / "corrupt/payload/market.duckdb"
    copied_db.chmod(0o600)
    copied_db.write_bytes(b"duckdb-X3")
    with pytest.raises(CutoverSafetyError, match="checksum mismatch"):
        service.verify_backup("corrupt")


@pytest.mark.parametrize("redirected_component", ["operations", "backups"])
def test_backup_rejects_redirected_operation_component_without_external_writes(
    tmp_path: Path,
    redirected_component: str,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / f"external-{redirected_component}"
    external.mkdir()
    if redirected_component == "operations":
        (data_root / "operations").symlink_to(external, target_is_directory=True)
    else:
        cutover_root = data_root / "operations/market-v4-cutover"
        cutover_root.mkdir(parents=True)
        (cutover_root / "backups").symlink_to(external, target_is_directory=True)

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(data_root).backup("must-not-escape")

    assert list(external.iterdir()) == []


def test_restore_requires_explicit_verified_backup_and_preserves_it(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    with pytest.raises(CutoverSafetyError, match="explicit backup ID"):
        service.restore(None)

    service.backup("before")
    market = data_root / "market-timeseries"
    shutil.rmtree(market)
    market.mkdir()
    (market / "market.duckdb").write_bytes(b"failed-v4")

    result = service.restore("before")

    assert (market / "market.duckdb").read_bytes() == b"duckdb-v3"
    assert market.stat().st_mode & 0o200
    assert (market / "market.duckdb").stat().st_mode & 0o200
    assert (data_root / "operations/market-v4-cutover/backups/before").exists()
    assert result.quarantine_path is not None
    assert (
        data_root / result.quarantine_path / "market.duckdb"
    ).read_bytes() == b"failed-v4"


def test_smoke_requires_market_v4_exact_lineage_and_semantic_api_parity(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
    )
    config = SmokeConfig(
        symbol="7203", strategy="production/smoke", dataset_preset="primeMarket"
    )

    api = FakeApi()
    result = service.smoke(api, config, operation_id="smoke-001")

    assert result.schema_version == 4
    assert result.adjustment_mode == "local_projection_v2_event_time"
    assert result.checks == (
        "market_metadata",
        "adjusted_metrics_lineage",
        "fundamentals_parity",
        "screening",
        "fundamental_ranking",
        "dataset_create_info_open",
    )
    assert ("GET", "/api/analytics/screening/jobs/screen-1", None) in api.calls

    with pytest.raises(CutoverSafetyError, match="adjusted-metric lineage"):
        service.smoke(FakeApi(invalid_lineage=True), config, operation_id="smoke-002")
    with pytest.raises(CutoverSafetyError, match="Fundamentals GET/POST parity"):
        service.smoke(FakeApi(parity=False), config, operation_id="smoke-003")


def test_smoke_reads_adjustment_mode_directly_from_duckdb(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v1")),
    )
    with pytest.raises(CutoverSafetyError, match="adjustment mode"):
        service.smoke(
            FakeApi(),
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            operation_id="smoke-mode",
        )


def test_smoke_uses_operation_scoped_create_only_dataset(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    api = FakeApi()
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
    )

    service.smoke(
        api,
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        operation_id="report-ABC",
    )

    create = next(call for call in api.calls if call[:2] == ("POST", "/api/dataset"))
    assert create[2] == {
        "name": "cutover-smoke-report-ABC",
        "preset": "primeMarket",
        "overwrite": False,
    }
    assert all(call[1] != "/api/dataset/cutover-smoke/info" for call in api.calls)


def test_standalone_smoke_worker_unjoined_transfers_shared_guard_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        retained_guard_fd = -1

        def inspect(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            del directory_fd, filename
            self.retained_guard_fd = os.dup(guard_lease_fd)
            raise WorkerShutdownError(
                "injected unjoined inspect worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(4, "local_projection_v2_event_time")
    )
    service = _service(data_root, duckdb=duckdb)

    with pytest.raises(WorkerShutdownError):
        service.smoke(
            FakeApi(),
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            operation_id="standalone-worker-transfer",
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass
