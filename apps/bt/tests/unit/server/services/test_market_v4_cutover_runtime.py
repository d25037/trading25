"""Market v4 cutover runtime tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

import src.application.services.market_v4_cutover.runtime as cutover_module
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.duckdb_identity import (
    DefaultDuckDbAdapter,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
    WorkerShutdownError,
)
from src.application.services.market_v4_cutover.runtime import (
    HttpApiAdapter,
    SubprocessRuntimeAdapter,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    _screening_job_response,
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
)


def test_default_runtime_adapter_is_available() -> None:
    assert DefaultDuckDbAdapter is not None
    assert SubprocessRuntimeAdapter is not None


def test_owned_server_argv_uses_uvicorn_without_cli_port_kill_path() -> None:
    argv = SubprocessRuntimeAdapter.server_argv(
        41234,
        market_fd=8,
    )
    assert argv.count("--port") == 1
    assert argv[-2:] == ["--port", "41234"]
    assert "bt" not in argv
    assert "8" in argv


def test_owned_server_passes_root_and_lease_fds_to_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class ExitedProcess:
        def poll(self) -> int:
            return 1

        def wait(self, timeout: float) -> int:
            del timeout
            return 1

    def fake_popen(argv: list[str], **kwargs: object) -> ExitedProcess:
        captured["argv"] = argv
        captured.update(kwargs)
        return ExitedProcess()

    root = tmp_path / "stage-root"
    root.mkdir()
    (root / "market-timeseries").mkdir()
    lock = root / ".market-timeseries.operation.lock"
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    market_fd = os.open(root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY)
    lease_fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    retained_lock = tmp_path / "retained.operation.lock"
    retained_lease_fd = os.open(retained_lock, os.O_CREAT | os.O_RDWR, 0o600)
    log_fd = os.open(tmp_path / "server.log", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        monkeypatch.setattr(cutover_module.subprocess, "Popen", fake_popen)
        runtime = SubprocessRuntimeAdapter()
        with pytest.raises(CutoverSafetyError, match="exited during startup"):
            runtime.start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                retained_lease_fd=retained_lease_fd,
                environment={},
                log_path=tmp_path / "server.log",
                log_fd=log_fd,
            )
    finally:
        os.close(log_fd)
        os.close(retained_lease_fd)
        os.close(lease_fd)
        os.close(market_fd)
        os.close(root_fd)

    assert set(captured["pass_fds"]) == {
        root_fd,
        market_fd,
        lease_fd,
        retained_lease_fd,
    }
    assert captured["env"]["TRADING25_RETAINED_MARKET_OPERATION_LOCK_FD"] == str(
        retained_lease_fd
    )
    assert str(market_fd) in captured["argv"]


@pytest.mark.darwin_capability
@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="requires Darwin F_GETPATH inherited-root resolution",
)
def test_owned_server_real_bootstrap_runs_from_inherited_root_fd(
    tmp_path: Path,
) -> None:
    root = tmp_path / "stage-root"
    for relative in (
        "market-timeseries",
        "datasets",
        "config",
        "strategies",
        "backtest",
    ):
        (root / relative).mkdir(parents=True, exist_ok=True)
    (root / "config/default.yaml").write_text("default: {}\n")
    runtime_name = ".cutover-runtime-bootstrap"
    for relative in ("datasets", "config", "strategies", "backtest"):
        (root / "market-timeseries" / runtime_name / relative).mkdir(
            parents=True,
            exist_ok=True,
        )
    (root / "market-timeseries" / runtime_name / "config/default.yaml").write_text(
        "default: {}\n"
    )
    log_path = tmp_path / "real-server.log"
    log_fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    runtime = SubprocessRuntimeAdapter(
        startup_timeout_seconds=20,
    )
    with market_operation_lease.MarketOperationLease.acquire(
        root,
        exclusive=True,
    ) as lease:
        market_fd = os.open(
            root / "market-timeseries",
            os.O_RDONLY | os.O_DIRECTORY,
        )
        environment = dict(os.environ)
        environment.update(
            {
                "XDG_DATA_HOME": f"{runtime_name}/xdg-data-home",
                "TRADING25_DATA_DIR": runtime_name,
                "MARKET_TIMESERIES_DIR": ".",
                "MARKET_DB_PATH": "market.duckdb",
                "DATASET_BASE_PATH": f"{runtime_name}/datasets",
                "PORTFOLIO_DB_PATH": f"{runtime_name}/portfolio.db",
                "TRADING25_STRATEGIES_DIR": f"{runtime_name}/strategies",
                "TRADING25_BACKTEST_DIR": f"{runtime_name}/backtest",
                "TRADING25_DEFAULT_CONFIG_PATH": f"{runtime_name}/config/default.yaml",
            }
        )
        try:
            api = runtime.start(
                root_fd=lease.root_fd,
                market_fd=market_fd,
                lease_fd=lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            assert api.request("GET", "/api/health")["status"] == "healthy"
            runtime.stop(api)
        finally:
            os.close(market_fd)
            os.close(log_fd)


def test_owned_server_log_redacts_secrets_and_local_paths(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    log.write_text("key=super-secret db=/Users/me/active/market.duckdb\n")
    log_fd = os.open(log, os.O_RDWR)
    try:
        SubprocessRuntimeAdapter.redact_log_fd(
            log_fd,
            {
                "JQUANTS_API_KEY": "super-secret",
                "MARKET_DB_PATH": "/Users/me/active/market.duckdb",
            },
        )
    finally:
        os.close(log_fd)
    retained = log.read_text()
    assert "super-secret" not in retained
    assert "/Users/me" not in retained
    assert "<redacted-secret>" in retained
    assert log.stat().st_mode & 0o777 == 0o600


def test_owned_server_log_does_not_redact_relative_dot_path_as_punctuation(
    tmp_path: Path,
) -> None:
    log = tmp_path / "server.log"
    log.write_text("127.0.0.1 loaded module.py; key=super-secret\n")
    log_fd = os.open(log, os.O_RDWR)
    try:
        SubprocessRuntimeAdapter.redact_log_fd(
            log_fd,
            {
                "JQUANTS_API_KEY": "super-secret",
                "MARKET_TIMESERIES_DIR": ".",
                "MARKET_DB_PATH": "market.duckdb",
            },
        )
    finally:
        os.close(log_fd)

    retained = log.read_text()
    assert "127.0.0.1 loaded module.py" in retained
    assert "super-secret" not in retained
    assert "<redacted-secret>" in retained


def test_fixed_port_health_is_not_probed_after_root_scoped_lease(
    monkeypatch, tmp_path: Path
) -> None:
    def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("fixed-port health probe must not run")

    monkeypatch.setattr(cutover_module, "urlopen", must_not_probe)
    runtime = SubprocessRuntimeAdapter()
    runtime.assert_quiescent(tmp_path)


def test_http_adapter_tracks_exact_job_id_field_for_each_create_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        "/api/db/sync": {"jobId": "sync-1"},
        "/api/db/adjusted-metrics/materialize": {"jobId": "materialize-1"},
        "/api/analytics/screening/jobs": _screening_job_response("pending"),
        "/api/dataset": {"jobId": "dataset-1"},
    }

    class JsonResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def __enter__(self) -> JsonResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(self.payload).encode()

    def fake_urlopen(request: object, *, timeout: float) -> JsonResponse:
        assert timeout == 30.0
        path = getattr(request, "selector")
        return JsonResponse(payloads[path])

    monkeypatch.setattr(cutover_module, "urlopen", fake_urlopen)
    api = HttpApiAdapter("http://unused")

    for path in payloads:
        api.request("POST", path, {})

    assert api.owned_jobs == {
        "sync": "sync-1",
        "materialize": "materialize-1",
        "screening": "screen-1",
        "dataset": "dataset-1",
    }


def test_http_adapter_does_not_accept_camel_case_screening_job_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class JsonResponse:
        def __enter__(self) -> JsonResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"jobId":"legacy-screen"}'

    monkeypatch.setattr(
        cutover_module,
        "urlopen",
        lambda *_args, **_kwargs: JsonResponse(),
    )
    api = HttpApiAdapter("http://unused")

    api.request("POST", "/api/analytics/screening/jobs", {})

    assert api.owned_jobs == {}


def test_operation_lease_blocks_unrecognized_server_and_allows_owner(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    lease_cls = market_operation_lease.MarketOperationLease

    with lease_cls.acquire(data_root, exclusive=True) as lease:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            lease_cls.acquire(data_root, exclusive=False)
        inherited = lease_cls.adopt_inherited(data_root, os.dup(lease.fd))
        inherited.release()
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            lease_cls.acquire(data_root, exclusive=False)

    with lease_cls.acquire(data_root, exclusive=False):
        pass


def test_operation_lease_transfer_holds_lock_until_inherited_fd_closes(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    lease = market_operation_lease.MarketOperationLease.acquire(
        data_root, exclusive=True
    )
    inherited_server_fd = os.dup(lease.fd)
    inherited_worker_fd = os.dup(lease.fd)
    lease.unlock_on_release = False
    lease.release()
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
        os.close(inherited_server_fd)
        inherited_server_fd = -1
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
    finally:
        if inherited_server_fd >= 0:
            os.close(inherited_server_fd)
        os.close(inherited_worker_fd)

    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_rehearsal_unjoined_server_transfers_lease_to_inherited_fd(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise RuntimeError("injected rebuild failure")

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_lease_fd = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise RuntimeStopError(
                "injected unjoined server",
                process_joined=False,
            )

    runtime = LeaseHoldingRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-lease-transfer",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root
                / "operations/market-v4-cutover/rehearsals/rehearsal-lease-transfer/root",
                exclusive=False,
            )
    finally:
        os.close(runtime.inherited_lease_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        data_root
        / "operations/market-v4-cutover/rehearsals/rehearsal-lease-transfer/root",
        exclusive=True,
    ):
        pass


def test_rehearsal_unjoined_worker_transfers_lease_to_worker_guard_fd(
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
                "injected unjoined rehearsal worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(4, "local_projection_v2_event_time")
    )
    service = _service(
        data_root,
        duckdb=duckdb,
        runtime=FakeRuntime(apis=[FakeApi()]),
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-worker-transfer",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    rehearsal_root = (
        data_root
        / "operations/market-v4-cutover/rehearsals/rehearsal-worker-transfer/root"
    )
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-worker-transfer/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_cleanup_deferred"
    assert report["workerProcessJoined"] is False
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                rehearsal_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        rehearsal_root,
        exclusive=True,
    ):
        pass


def test_duckdb_worker_inherits_guard_lease_fd_until_process_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    directory_fd = os.open(
        data_root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY
    )
    lease = market_operation_lease.MarketOperationLease.acquire(
        data_root, exclusive=True
    )
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_worker_argv",
        staticmethod(
            lambda _operation, _directory_fd, _guard_lease_fd, _filename: [
                cutover_module.sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ]
        ),
    )
    process = None
    try:
        process = DefaultDuckDbAdapter._start_worker(
            "inspect",
            directory_fd,
            "market.duckdb",
            guard_lease_fd=lease.fd,
        )
        lease.unlock_on_release = False
        lease.release()
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root, exclusive=False
            )
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=5)
        if lease.fd >= 0:
            lease.release()
        os.close(directory_fd)

    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_inherited_unlocked_matching_fd_establishes_exclusive_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    lock_path = data_root / ".market-timeseries.operation.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    inherited = market_operation_lease.MarketOperationLease.adopt_inherited(
        data_root, fd
    )
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root, exclusive=False
            )
    finally:
        inherited.release()


def test_inherited_matching_fd_rejects_competing_shared_lease(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    shared = market_operation_lease.MarketOperationLease.acquire(
        data_root, exclusive=False
    )
    unlocked_fd = os.open(shared.path, os.O_RDWR)
    try:
        with pytest.raises(CutoverSafetyError, match="exclusive|operation lease"):
            market_operation_lease.MarketOperationLease.adopt_inherited(
                data_root, unlocked_fd
            )
    finally:
        os.close(unlocked_fd)
        shared.release()


def test_inherited_root_fd_avoids_reopening_swapped_lexical_root(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    parent = market_operation_lease.MarketOperationLease.acquire(
        data_root,
        exclusive=True,
    )
    inherited_lock_fd = os.dup(parent.fd)
    inherited_root_fd = os.dup(parent.root_fd)
    detached = tmp_path / "data-root-detached"
    data_root.rename(detached)
    external = tmp_path / "external-root"
    external.mkdir()
    data_root.symlink_to(external, target_is_directory=True)
    try:
        with pytest.raises(CutoverSafetyError, match="exactly match"):
            market_operation_lease.MarketOperationLease.adopt_inherited(
                data_root,
                inherited_lock_fd,
                root_fd=inherited_root_fd,
            )
    finally:
        parent.release()

    assert list(external.iterdir()) == []
