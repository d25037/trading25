"""Fresh-process recovery for interrupted Market v5 activation."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil

import pytest

from src.application.services.market_v4_cutover.contracts import (
    ActivationState,
    MarketSourceMetadata,
    SmokeConfig,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeApi,
    FakeDuckDb,
    FakeRuntime,
    _TestAtomicExchange,
    _market_root,
    _service,
)


_CRASH_EXIT_CODE = 75
_REPORT_ID = "crash-recovery-active"
_REHEARSAL_ID = "crash-recovery-rehearsal"
_BACKUP_ID = "crash-recovery-backup"
_CONFIG = SmokeConfig("7203", "production/smoke", "primeMarket")


def _retained_runtime(data_root: Path) -> Path:
    return (
        data_root
        / "operations/market-v5-cutover/recovery-runtime-quarantine"
        / _REPORT_ID
    )


def _runtime_tree_evidence(root: Path) -> dict[str, tuple[int, str | None]]:
    evidence = {".": (root.stat().st_ino, None)}
    for path in sorted(root.rglob("*")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        evidence[path.relative_to(root).as_posix()] = (path.stat().st_ino, digest)
    return evidence


class _CrashAtomicExchange(_TestAtomicExchange):
    def __init__(self, crash_point: str) -> None:
        self._crash_point = crash_point

    def exchange(self, managed_root, left: Path, right: Path) -> None:
        if self._crash_point == "before_exchange":
            os._exit(_CRASH_EXIT_CODE)
        super().exchange(managed_root, left, right)
        if self._crash_point == "after_exchange":
            os._exit(_CRASH_EXIT_CODE)


def _run_crashing_cutover(data_root: Path, crash_point: str) -> None:
    child_pid = os.fork()
    if child_pid == 0:
        atomic_exchange = (
            _CrashAtomicExchange(crash_point)
            if crash_point in {"before_exchange", "after_exchange"}
            else _TestAtomicExchange()
        )
        service = _service(
            data_root,
            duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
            runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
            atomic_exchange=atomic_exchange,
        )
        if crash_point in {"after_prepared", "after_activated"}:
            real_append = service._activation._journal.append

            def crash_after_journal_state(attempt, state):
                record = real_append(attempt, state)
                expected_state = (
                    ActivationState.PREPARED
                    if crash_point == "after_prepared"
                    else ActivationState.ACTIVATED
                )
                if state is expected_state:
                    os._exit(_CRASH_EXIT_CODE)
                return record

            service._activation._journal.append = crash_after_journal_state
        elif crash_point == "after_quarantine":
            real_rename = service._workspace._secure_rename

            def crash_after_quarantine(source: Path, target: Path) -> None:
                real_rename(source, target)
                if target.name == f"pre-cutover-{_REPORT_ID}":
                    os._exit(_CRASH_EXIT_CODE)

            service._workspace._secure_rename = crash_after_quarantine
        elif crash_point == "after_runtime_rename":
            real_rename = service._workspace._secure_rename

            def crash_after_runtime_rename(source: Path, target: Path) -> None:
                real_rename(source, target)
                if (
                    source.name == f"runtime-template-{_REPORT_ID}"
                    and target.name == f".cutover-runtime-{_REPORT_ID}"
                ):
                    os._exit(_CRASH_EXIT_CODE)

            service._workspace._secure_rename = crash_after_runtime_rename
        elif crash_point == "after_report":
            real_publish = service._reports._write_or_adopt_exact_report

            def crash_after_report(*args, **kwargs):
                result = real_publish(*args, **kwargs)
                os._exit(_CRASH_EXIT_CODE)
                return result

            service._reports._write_or_adopt_exact_report = crash_after_report
        try:
            service.cutover(
                _REPORT_ID,
                rehearsal_report_id=_REHEARSAL_ID,
                backup_id=_BACKUP_ID,
                config=_CONFIG,
                inherited_environment={},
            )
        except BaseException:
            os._exit(74)
        os._exit(0)

    waited_pid, status = os.waitpid(child_pid, 0)
    assert waited_pid == child_pid
    assert os.WIFEXITED(status)
    assert os.WEXITSTATUS(status) == _CRASH_EXIT_CODE


@pytest.mark.parametrize(
    "crash_point",
    (
        "after_prepared",
        "before_exchange",
        "after_exchange",
        "after_quarantine",
        "after_runtime_rename",
        "after_activated",
        "after_report",
    ),
)
def test_fresh_service_recovers_exact_same_cutover_after_process_death(
    tmp_path: Path,
    crash_point: str,
) -> None:
    data_root = _market_root(tmp_path)
    original_market = (data_root / "market-timeseries/market.duckdb").read_bytes()
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})

    _run_crashing_cutover(data_root, crash_point)

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    result = fresh.cutover(
        _REPORT_ID,
        rehearsal_report_id=_REHEARSAL_ID,
        backup_id=_BACKUP_ID,
        config=_CONFIG,
        inherited_environment={},
    )

    assert result.report_id == _REPORT_ID
    report_path = data_root / result.report_path
    report = json.loads(report_path.read_text())
    assert report["status"] == "passed"
    assert report["reportId"] == _REPORT_ID
    assert report["rehearsalReportId"] == _REHEARSAL_ID
    assert report["backupId"] == _BACKUP_ID

    active_market = data_root / "market-timeseries"
    quarantine = (
        data_root
        / f"operations/market-v5-cutover/quarantine/pre-cutover-{_REPORT_ID}"
    )
    backup = data_root / f"operations/market-v5-cutover/backups/{_BACKUP_ID}"
    staged_market = (
        data_root
        / f"operations/market-v5-cutover/staging/{_REPORT_ID}/root/market-timeseries"
    )
    assert (active_market / "market.duckdb").read_bytes() != original_market
    assert (quarantine / "market.duckdb").read_bytes() == original_market
    assert (backup / "payload/market.duckdb").read_bytes() == original_market
    assert (backup / "manifest.json").is_file()
    assert not staged_market.exists()

    journal_dir = (
        data_root
        / f"operations/market-v5-cutover/activation-journals/{_REPORT_ID}"
    )
    assert [path.name for path in sorted(journal_dir.iterdir())] == [
        "00000001-prepared.json",
        "00000002-exchange_started.json",
        "00000003-activated.json",
        "00000004-reported.json",
    ]
    retained_runtime = _retained_runtime(data_root)
    active_runtime = active_market / f".cutover-runtime-{_REPORT_ID}"
    assert retained_runtime.is_dir()
    assert not active_runtime.exists()

    report_bytes = report_path.read_bytes()
    active_identity = active_market.stat().st_ino
    retained_identity = retained_runtime.stat().st_ino
    reported = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(),
    ).cutover(
        _REPORT_ID,
        rehearsal_report_id=_REHEARSAL_ID,
        backup_id=_BACKUP_ID,
        config=_CONFIG,
        inherited_environment={},
    )
    assert reported == result
    assert report_path.read_bytes() == report_bytes
    assert active_market.stat().st_ino == active_identity
    assert retained_runtime.stat().st_ino == retained_identity
    assert not active_runtime.exists()


def test_recovery_rejects_mismatched_attempt_arguments_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    original_market = (data_root / "market-timeseries/market.duckdb").read_bytes()
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "before_exchange")

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="attempt"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=SmokeConfig("6758", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original_market
    assert (
        data_root
        / f"operations/market-v5-cutover/staging/{_REPORT_ID}/root/market-timeseries"
    ).is_dir()
    assert not (
        data_root
        / f"operations/market-v5-cutover/quarantine/pre-cutover-{_REPORT_ID}"
    ).exists()


def test_recovery_rejects_changed_target_fingerprint_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "before_exchange")
    original_market = (data_root / "market-timeseries/market.duckdb").read_bytes()
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("changed: true\n")

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="rehearsal"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original_market


def test_recovery_rejects_changed_staging_configuration_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "before_exchange")
    original_market = (data_root / "market-timeseries/market.duckdb").read_bytes()
    staged_config = (
        data_root
        / f"operations/market-v5-cutover/staging/{_REPORT_ID}/root/config/default.yaml"
    )
    staged_config.write_text("changed: true\n")

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="configuration"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original_market
    assert not (
        data_root
        / f"operations/market-v5-cutover/quarantine/pre-cutover-{_REPORT_ID}"
    ).exists()


def test_recovery_rejects_ambiguous_quarantine_without_mutation(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "before_exchange")
    original_market = (data_root / "market-timeseries/market.duckdb").read_bytes()
    quarantine = (
        data_root
        / f"operations/market-v5-cutover/quarantine/pre-cutover-{_REPORT_ID}"
    )
    quarantine.mkdir()
    (quarantine / "market.duckdb").write_bytes(b"ambiguous")

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="layout"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original_market
    assert (quarantine / "market.duckdb").read_bytes() == b"ambiguous"


def test_recovery_never_adopts_or_overwrites_mismatched_published_report(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_report")
    report_path = (
        data_root / f"operations/market-v5-cutover/reports/{_REPORT_ID}/report.json"
    )
    report = json.loads(report_path.read_text())
    report["backupId"] = "mismatched-backup"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    mismatched = report_path.read_bytes()
    active_market = data_root / "market-timeseries/market.duckdb"
    active_bytes = active_market.read_bytes()

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="report"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert active_market.read_bytes() == active_bytes
    assert report_path.read_bytes() == mismatched


def test_joined_smoke_failure_never_uses_partial_recursive_runtime_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecoverySmokeFailure(RuntimeError):
        pass

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise RecoverySmokeFailure("injected joined recovery smoke failure")

    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_activated")

    failed = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FailingApi()]),
    )

    recursive_cleanup_called = False

    def partially_remove_runtime(market_fd: int, runtime_name: str) -> None:
        nonlocal recursive_cleanup_called
        recursive_cleanup_called = True
        runtime_fd = os.open(runtime_name, os.O_RDONLY, dir_fd=market_fd)
        try:
            config_fd = os.open("config", os.O_RDONLY, dir_fd=runtime_fd)
            try:
                os.unlink("default.yaml", dir_fd=config_fd)
            finally:
                os.close(config_fd)
        finally:
            os.close(runtime_fd)
        raise OSError("injected failure after partial recursive mutation")

    monkeypatch.setattr(
        failed._workspace,
        "_remove_market_runtime",
        partially_remove_runtime,
    )
    with pytest.raises(RecoverySmokeFailure, match="joined recovery smoke"):
        failed.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )
    assert recursive_cleanup_called is False

    runtime_root = (
        data_root / f"market-timeseries/.cutover-runtime-{_REPORT_ID}"
    )
    retained_runtime = _retained_runtime(data_root)
    assert not runtime_root.exists()
    assert (retained_runtime / "config/default.yaml").is_file()
    journal_dir = (
        data_root
        / f"operations/market-v5-cutover/activation-journals/{_REPORT_ID}"
    )
    assert [path.name for path in sorted(journal_dir.iterdir())][-1] == (
        "00000003-activated.json"
    )

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    result = fresh.cutover(
        _REPORT_ID,
        rehearsal_report_id=_REHEARSAL_ID,
        backup_id=_BACKUP_ID,
        config=_CONFIG,
        inherited_environment={},
    )

    assert result.report_id == _REPORT_ID
    assert not runtime_root.exists()
    assert (retained_runtime / "config/default.yaml").is_file()
    assert [path.name for path in sorted(journal_dir.iterdir())][-1] == (
        "00000004-reported.json"
    )


def test_recovery_fences_unknown_baseexception_during_server_startup(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_activated")

    class InterruptingRecoveryStartRuntime(FakeRuntime):
        retained_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            self.retained_lease_fd = os.dup(int(kwargs["lease_fd"]))
            raise KeyboardInterrupt("operator interrupted recovery startup")

    runtime = InterruptingRecoveryStartRuntime()
    failed = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )

    caught_error: BaseException | None = None
    try:
        failed.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )
    except BaseException as exc:
        caught_error = exc

    assert isinstance(caught_error, CutoverSafetyError)
    assert "recovery is deferred" in str(caught_error)
    active_runtime = (
        data_root / f"market-timeseries/.cutover-runtime-{_REPORT_ID}"
    )
    assert (active_runtime / "config/default.yaml").is_file()
    assert not _retained_runtime(data_root).exists()
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_lease_fd)

    recovered = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    ).cutover(
        _REPORT_ID,
        rehearsal_report_id=_REHEARSAL_ID,
        backup_id=_BACKUP_ID,
        config=_CONFIG,
        inherited_environment={},
    )
    assert recovered.report_id == _REPORT_ID


def test_recovery_rejects_tampered_runtime_without_deleting_it(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_runtime_rename")
    runtime_config = (
        data_root
        / f"market-timeseries/.cutover-runtime-{_REPORT_ID}/config/default.yaml"
    )
    runtime_config.write_text("tampered: true\n")
    tampered = runtime_config.read_bytes()

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="runtime.*exact"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert runtime_config.read_bytes() == tampered


def test_runtime_retirement_rename_failure_leaves_active_for_fresh_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_activated")

    failed = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    real_rename = failed._workspace._secure_rename

    def fail_retirement_rename(source: Path, target: Path) -> None:
        if target == _retained_runtime(data_root):
            raise OSError("injected atomic runtime retirement failure")
        real_rename(source, target)

    monkeypatch.setattr(
        failed._workspace,
        "_secure_rename",
        fail_retirement_rename,
    )
    with pytest.raises(CutoverSafetyError, match="retirement failure") as caught:
        failed.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )
    assert isinstance(caught.value.__cause__, OSError)

    active_runtime = (
        data_root / f"market-timeseries/.cutover-runtime-{_REPORT_ID}"
    )
    retained_runtime = _retained_runtime(data_root)
    assert (active_runtime / "config/default.yaml").is_file()
    assert not retained_runtime.exists()

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    result = fresh.cutover(
        _REPORT_ID,
        rehearsal_report_id=_REHEARSAL_ID,
        backup_id=_BACKUP_ID,
        config=_CONFIG,
        inherited_environment={},
    )
    assert result.report_id == _REPORT_ID
    assert not active_runtime.exists()
    assert (retained_runtime / "config/default.yaml").is_file()


def test_recovery_rejects_tampered_retained_runtime_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_runtime_rename")
    active_runtime = (
        data_root / f"market-timeseries/.cutover-runtime-{_REPORT_ID}"
    )
    retained_runtime = _retained_runtime(data_root)
    retained_runtime.parent.mkdir()
    os.rename(active_runtime, retained_runtime)
    retained_config = retained_runtime / "config/default.yaml"
    retained_config.write_text("tampered: retained\n")
    tampered = retained_config.read_bytes()

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="runtime.*exact"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert not active_runtime.exists()
    assert retained_config.read_bytes() == tampered


def test_recovery_rejects_active_and_retained_runtime_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    setup = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi()]),
    )
    setup.backup(_BACKUP_ID)
    setup.rehearse(_REHEARSAL_ID, _CONFIG, inherited_environment={})
    _run_crashing_cutover(data_root, "after_runtime_rename")
    active_runtime = (
        data_root / f"market-timeseries/.cutover-runtime-{_REPORT_ID}"
    )
    retained_runtime = _retained_runtime(data_root)
    retained_runtime.parent.mkdir()
    shutil.copytree(active_runtime, retained_runtime)
    active_before = _runtime_tree_evidence(active_runtime)
    retained_before = _runtime_tree_evidence(retained_runtime)

    fresh = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    with pytest.raises(CutoverSafetyError, match="ownership is ambiguous"):
        fresh.cutover(
            _REPORT_ID,
            rehearsal_report_id=_REHEARSAL_ID,
            backup_id=_BACKUP_ID,
            config=_CONFIG,
            inherited_environment={},
        )

    assert _runtime_tree_evidence(active_runtime) == active_before
    assert _runtime_tree_evidence(retained_runtime) == retained_before
