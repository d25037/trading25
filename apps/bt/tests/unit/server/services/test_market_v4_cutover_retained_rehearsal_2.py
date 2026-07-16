"""Market v4 cutover retained rehearsal tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import src.application.services.market_v4_cutover.project_paths as project_paths
from src.application.services.market_v4_cutover.contracts import (
    SmokeResult,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
    WorkerShutdownError,
)
from src.application.services.market_v4_cutover.filesystem import (
    _DIR_OPEN_FLAGS,
)
from src.application.services.market_v4_cutover.service import MarketV4CutoverService
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _retained_source,
)


@pytest.mark.parametrize(
    "relative_target",
    [
        "market-timeseries/market.duckdb",
        "market-timeseries/parquet/stock_data/part.parquet",
    ],
)
def test_market_tree_identity_rejects_same_content_replacement_during_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_target: str,
) -> None:
    data_root = _market_root(tmp_path)
    _service, retained_root, _config = _retained_source(data_root)
    target = retained_root / relative_target
    target_inode = target.stat().st_ino
    original_read = os.read
    replaced = False

    def replace_during_read(fd: int, size: int) -> bytes:
        nonlocal replaced
        if not replaced and os.fstat(fd).st_ino == target_inode:
            replaced = True
            payload = target.read_bytes()
            target.rename(target.with_suffix(target.suffix + ".replaced"))
            target.write_bytes(payload)
        return original_read(fd, size)

    monkeypatch.setattr(os, "read", replace_during_read)
    root_fd = os.open(retained_root, _DIR_OPEN_FLAGS)
    try:
        with pytest.raises(CutoverSafetyError, match="changed during identity hashing"):
            MarketV4CutoverService._market_tree_identity(root_fd)
    finally:
        os.close(root_fd)
    assert replaced is True


def test_regular_file_identity_reports_path_failure_class_and_metadata_deltas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "identity-root"
    target = root / "parquet/stock_data/part.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"stable payload")
    original_inode = target.stat().st_ino
    original_read = os.read
    replaced = False

    def replace_during_read(fd: int, size: int) -> bytes:
        nonlocal replaced
        if not replaced and os.fstat(fd).st_ino == original_inode:
            replaced = True
            payload = target.read_bytes()
            target.rename(target.with_suffix(".replaced"))
            target.write_bytes(payload)
        return original_read(fd, size)

    monkeypatch.setattr(os, "read", replace_during_read)
    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError) as error:
            MarketV4CutoverService._regular_file_identity(
                managed,
                Path("parquet/stock_data/part.parquet"),
            )

    message = str(error.value)
    assert "path=parquet/stock_data/part.parquet" in message
    assert "failure=metadata_changed" in message
    diagnostic = json.loads(message.split("metadata=", 1)[1])
    assert diagnostic["before"]["inode"] == original_inode
    assert set(diagnostic["afterDelta"]) <= {"ctimeNs"}
    if "ctimeNs" in diagnostic["afterDelta"]:
        assert (
            diagnostic["afterDelta"]["ctimeNs"]["before"]
            == (diagnostic["before"]["ctimeNs"])
        )
    assert diagnostic["currentDelta"]["inode"] == {
        "before": original_inode,
        "current": target.stat().st_ino,
    }
    assert replaced is True


def test_prepare_retained_runtime_uses_repository_config_when_active_override_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.parent.mkdir(parents=True)
    strategy.write_text("name: smoke\n")
    repository_root = tmp_path / "repository/apps/bt"
    repository_config = repository_root / "config/default.yaml"
    repository_config.parent.mkdir(parents=True)
    repository_config.write_text("execution:\n  cash: 1000000\n")
    (repository_root / "src/application/services/market_v4_cutover").mkdir(parents=True)
    (repository_root / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    project_paths.bt_project_root.cache_clear()
    monkeypatch.setattr(
        project_paths,
        "__file__",
        str(repository_root / "src/application/services/market_v4_cutover/rebuild.py"),
    )
    service = _service(data_root)
    runtime_name = ".cutover-runtime-config-fallback"

    with service._managed_root_scope():
        service._active_code_version = "deadbeef"
        expected_fingerprint = service.configuration_fingerprint(data_root)
        service._prepare_retained_runtime(
            data_root,
            runtime_name=runtime_name,
            root_fd=service._managed().fd,
        )
        runtime_root = data_root / "market-timeseries" / runtime_name
        assert service.configuration_fingerprint(runtime_root) == expected_fingerprint

    assert not (data_root / "config/default.yaml").exists()
    assert (
        data_root / "market-timeseries" / runtime_name / "config/default.yaml"
    ).read_bytes() == repository_config.read_bytes()
    project_paths.bt_project_root.cache_clear()


@pytest.mark.parametrize("unjoined_process", ["server", "worker"])
def test_rehearse_retained_unjoined_process_keeps_competing_lease_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unjoined_process: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)

    class LeaseHoldingRuntime(FakeRuntime):
        retained_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            self.start_calls += 1
            if unjoined_process == "server":
                self.retained_lease_fd = os.dup(int(kwargs["lease_fd"]))
                raise RuntimeStopError(
                    "injected unjoined server",
                    process_joined=False,
                )
            return super().start(**kwargs)

    runtime = LeaseHoldingRuntime(apis=[FakeApi()])
    service.runtime = runtime

    if unjoined_process == "worker":

        def unjoined_smoke(
            *_args: object,
            **kwargs: object,
        ) -> SmokeResult:
            runtime.retained_lease_fd = os.dup(int(kwargs["guard_lease_fd"]))
            raise WorkerShutdownError(
                "injected unjoined worker",
                process_joined=False,
            )

        monkeypatch.setattr(service, "smoke", unjoined_smoke)

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            f"retained-unjoined-{unjoined_process}",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                retained_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_lease_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        retained_root,
        exclusive=True,
    ):
        pass
