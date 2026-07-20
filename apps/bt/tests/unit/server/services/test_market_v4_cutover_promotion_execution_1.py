"""Market v4 cutover promotion execution tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

import pytest

import src.application.services.market_v4_cutover.filesystem as filesystem_module
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    PromotionAppendStatus,
    PromotionState,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.application.services.market_v4_cutover.promotion_contracts import (
    RetainedPromotionContext,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _retained_promotion_source,
    _TestAtomicExchange,
    _run_retained_promotion,
    _market_identity_at_root,
)


def test_public_promote_retained_runs_gated_promotion_and_recovers_same_id(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    result = service.promote_retained(
        "market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    )

    assert result.report_id == "market-v4-active-20260716"
    assert runtime.start_calls == 1
    fresh_service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
    )
    assert (
        fresh_service.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
        == result
    )
    assert runtime.start_calls == 1


def test_promote_retained_atomically_activates_exact_payload_without_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    api = FakeApi()
    runtime = FakeRuntime(apis=[api])
    service._workspace.runtime = runtime
    monkeypatch.setattr(
        service._runtime_smoke,
        "run_rebuild",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retained promotion must not rebuild or sync")
        ),
    )

    result, states = _run_retained_promotion(
        service,
        config,
        inherited_environment={
            "PATH": os.environ.get("PATH", ""),
            "JQUANTS_API_KEY": "forbidden",
            "SERVICE_TOKEN": "forbidden",
            "AWS_SECRET_ACCESS_KEY": "forbidden",
            "CREDENTIAL_FILE": "forbidden",
            "JQUANTS_PLAN": "forbidden",
        },
    )

    active_after = _market_identity_at_root(service, data_root)
    quarantine = data_root / (
        "operations/market-v4-cutover/quarantine/market-v4-active-20260716"
    )
    assert result.report_id == "market-v4-active-20260716"
    assert active_after == retained_before
    quarantine_db = quarantine / "market.duckdb"
    active_before_db = active_before["marketDuckdb"]
    assert isinstance(active_before_db, dict)
    assert quarantine_db.stat().st_ino == active_before_db["inode"]
    assert service._workspace._sha256(quarantine_db) == active_before_db["sha256"]
    quarantine_fd = os.open(quarantine, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert service._market_identity._market_payload_identity(quarantine_fd) == active_before
    finally:
        os.close(quarantine_fd)
    assert set(path.name for path in (data_root / "market-timeseries").iterdir()) == {
        "market.duckdb",
        "parquet",
    }
    assert runtime.start_calls == runtime.stop_calls == 1
    assert len(runtime.retained_lease_fds) == 1
    environment = runtime.environments[0]
    assert not any(
        token in name.upper()
        for name in environment
        for token in ("JQUANTS", "KEY", "TOKEN", "SECRET", "CREDENTIAL", "PLAN")
    )
    forbidden_paths = (
        "/api/db/sync",
        "/api/db/stocks/refresh",
        "/api/db/intraday/sync",
    )
    assert all(
        not any(path.startswith(forbidden) for forbidden in forbidden_paths)
        for _method, path, _payload in api.calls
    )
    log = data_root / (
        "operations/market-v4-cutover/reports/market-v4-active-20260716/active-smoke.log"
    )
    assert "jquants_fetch" not in log.read_text().lower()
    assert states == (
        PromotionState.VALIDATED,
        PromotionState.RUNTIMES_DETACHED,
        PromotionState.PREPARED,
        PromotionState.EXCHANGED,
        PromotionState.QUARANTINED,
        PromotionState.ACTIVE_SMOKE_PASSED,
        PromotionState.CLEANUP_STAGED,
        PromotionState.REPORT_PERSISTED,
        PromotionState.COMMITTED,
    )


def test_promotion_routes_owned_duckdb_temp_into_isolated_runtime(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    retained_market = retained_root / "market-timeseries"
    (retained_market / "duckdb-tmp").mkdir()

    class TempCreatingRuntime(FakeRuntime):
        def start(self, **kwargs: object) -> FakeApi:
            environment = cast(dict[str, str], kwargs["environment"])
            temp_relative = environment.get(
                "TRADING25_DUCKDB_TEMP_DIR",
                "duckdb-tmp",
            )
            os.mkdir(temp_relative, dir_fd=cast(int, kwargs["market_fd"]))
            return super().start(**kwargs)  # type: ignore[arg-type]

    runtime = TempCreatingRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    result, states = _run_retained_promotion(service, config)

    assert result.report_id == "market-v4-active-20260716"
    assert states[-1] is PromotionState.COMMITTED
    environment = runtime.environments[0]
    assert environment["TRADING25_RUNTIME_CAPABILITY"] == "retained_market_smoke"
    assert environment["TRADING25_DUCKDB_TEMP_DIR"] == (
        ".cutover-runtime-market-v4-active-20260716/duckdb-tmp"
    )
    assert set(path.name for path in (data_root / "market-timeseries").iterdir()) == {
        "market.duckdb",
        "parquet",
    }


def test_promotion_recovery_detects_swap_after_prepared_before_exchanged_record(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    report_id = "market-v4-active-20260716"

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service._workspace.atomic_exchange.exchange(
            service._workspace.managed(),
            Path("market-timeseries"),
            service._workspace._managed_relative(retained_root / "market-timeseries"),
        )
        context = RetainedPromotionContext(
            preparation=preparation,
            journal=journal,
        )

        service._promotion._rollback._rollback_retained_promotion(context, processes_joined=True)

        assert _market_identity_at_root(service, data_root) == active_before
        assert _market_identity_at_root(service, retained_root) == retained_before
        assert tuple(record.state for record in journal.read_validated())[-2:] == (
            PromotionState.EXCHANGED_BACK,
            PromotionState.ROLLED_BACK,
        )


def test_promotion_smoke_failure_exchanges_back_and_restores_exact_v3(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi(parity=False)])

    with pytest.raises(CutoverSafetyError, match="parity failed"):
        _run_retained_promotion(service, config)

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before
    assert not (
        data_root / "operations/market-v4-cutover/consumed/"
        "market-v4-retained-20260715-r13.json"
    ).exists()
    journal = PromotionJournal(
        service._workspace._managed_root_fd
        if service._workspace._managed_root_fd is not None
        else managed_root.ManagedRootFd.open(data_root),
        "market-v4-active-20260716",
        now=service._workspace.now,
    )
    managed = journal._managed_root
    try:
        latest = journal.recovery_attempt_id()
        assert journal.recover(latest).status is PromotionAppendStatus.COMMITTED
        assert tuple(record.state for record in journal.read_validated())[-2:] == (
            PromotionState.EXCHANGED_BACK,
            PromotionState.ROLLED_BACK,
        )
    finally:
        if managed is not service._workspace._managed_root_fd:
            managed.close()


def test_promotion_unjoined_runtime_defers_rollback_and_fences_both_leases(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise RuntimeStopError("injected unjoined runtime", process_joined=False)

    service._workspace.runtime = UnjoinedRuntime(apis=[FakeApi()])
    leaked_fds: tuple[int, int]

    with pytest.raises(CutoverSafetyError, match="deferred"):
        with service._promotion._transaction._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._workspace._active_lease is not None
            assert service._workspace._retained_lease is not None
            leaked_fds = (service._workspace._active_lease.fd, service._workspace._retained_lease.fd)
            journal = PromotionJournal(
                service._workspace.managed(), "market-v4-active-20260716", now=service._workspace.now
            )
            preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
            service._promotion._promote_retained_under_leases(
                preparation,
                journal=journal,
                config=config,
                inherited_environment={},
            )

    try:
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire_existing(
                    root, exclusive=True
                )
    finally:
        for fd in leaked_fds:
            os.close(fd)


def test_public_promotion_deferred_recovery_waits_for_both_inherited_leases(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            raise RuntimeStopError("injected unjoined runtime", process_joined=False)

    service._workspace.runtime = UnjoinedRuntime(apis=[FakeApi()])
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    with pytest.raises(CutoverSafetyError, match="deferred"):
        service.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
    assert service._workspace._active_lease is None
    inherited_fds = (
        *service._workspace.runtime.active_lease_fds,
        *service._workspace.runtime.retained_lease_fds,
    )

    fresh = _service(data_root)
    with pytest.raises(CutoverSafetyError, match="operation lease"):
        fresh.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )

    for fd in inherited_fds:
        os.close(fd)
    deferred_active = _market_identity_at_root(fresh, data_root)
    with pytest.raises(CutoverSafetyError, match="missing|identity|retained report"):
        fresh.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r12",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
    assert _market_identity_at_root(fresh, data_root) == deferred_active
    with pytest.raises(CutoverSafetyError, match="rolled back"):
        fresh.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
    assert _market_identity_at_root(fresh, data_root) == active_before
    assert _market_identity_at_root(fresh, retained_root) == retained_before


@pytest.mark.parametrize(
    "failure_suffix",
    ["moved", "source_fsynced", "holding_fsynced"],
)
@pytest.mark.parametrize("artifact_index", range(4))
def test_public_promotion_partial_detach_restores_before_releasing_leases(
    tmp_path: Path,
    failure_suffix: str,
    artifact_index: int,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    market = retained_root / "market-timeseries"
    (market / ".cutover-runtime-market-v4-rehearsal-20260715-r10").mkdir()
    (market / "duckdb-tmp").mkdir()
    (market / "market.duckdb.wal").touch()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    runtime_names = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name.startswith(".cutover-runtime-")
    }
    injected = False
    observed_artifacts = 0

    def fail_after_first_artifact(stage: str) -> None:
        nonlocal injected, observed_artifacts
        if stage.startswith("detach_artifact_") and stage.endswith(failure_suffix):
            if observed_artifacts != artifact_index:
                observed_artifacts += 1
                return
            injected = True
            raise CutoverSafetyError(f"injected {failure_suffix}")

    service._workspace._promotion_boundary_hook = fail_after_first_artifact

    with pytest.raises(CutoverSafetyError, match=f"injected {failure_suffix}"):
        service.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )

    assert injected is True
    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before
    assert runtime_names <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }
    assert not (
        data_root / "operations/market-v4-cutover/holding/market-v4-active-20260716"
    ).exists()


def test_partial_detach_unrestorable_layout_fences_both_leases_and_journals_deferred(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    market = retained_root / "market-timeseries"
    (market / ".cutover-runtime-market-v4-rehearsal-20260715-r10").mkdir()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    original_rename = filesystem_module._rename_exclusive_at
    leaked_fds: tuple[int, int] | None = None

    def fail_restoration(*_args: object) -> None:
        raise OSError("injected restoration failure")

    def interrupt_after_move(stage: str) -> None:
        nonlocal leaked_fds
        if leaked_fds is None and stage.endswith(":moved"):
            assert service._workspace._active_lease is not None
            assert service._workspace._retained_lease is not None
            leaked_fds = (service._workspace._active_lease.fd, service._workspace._retained_lease.fd)
            monkeypatch.setattr(
                filesystem_module, "_rename_exclusive_at", fail_restoration
            )
            raise CutoverSafetyError("injected split layout")

    service._workspace._promotion_boundary_hook = interrupt_after_move

    with pytest.raises(CutoverSafetyError, match="deferred"):
        service.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )

    assert leaked_fds is not None
    try:
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire_existing(
                    root, exclusive=True
                )
        journal_records = sorted(
            (
                data_root
                / "operations/market-v4-cutover/journals/market-v4-active-20260716"
            ).glob("*.json")
        )
        unresolved = json.loads(journal_records[-1].read_text())
        assert unresolved["state"] in {
            PromotionState.VALIDATED.value,
            PromotionState.ROLLBACK_DEFERRED.value,
        }
        assert unresolved["identities"]["detached_artifacts"]
    finally:
        monkeypatch.setattr(filesystem_module, "_rename_exclusive_at", original_rename)
        for fd in leaked_fds:
            os.close(fd)

    fresh = _service(data_root)
    with pytest.raises(CutoverSafetyError, match="rolled back"):
        fresh.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
    assert _market_identity_at_root(fresh, data_root) == active_before
    assert _market_identity_at_root(fresh, retained_root) == retained_before


def test_promotion_recovery_matching_incomplete_journal_rolls_back(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    report_id = "market-v4-active-20260716"

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service._workspace.atomic_exchange.exchange(
            service._workspace.managed(),
            Path("market-timeseries"),
            service._workspace._managed_relative(retained_root / "market-timeseries"),
        )

    assert (
        service._promotion._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_recovery_rejects_mismatched_identity_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"
    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    with pytest.raises(CutoverSafetyError, match="identity|retained report"):
        service._promotion._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r12",
            backup_id="market-v3-pre-v4-20260716",
        )

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_recovery_valid_committed_report_rejects_replay_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    result, _states = _run_retained_promotion(service, config)
    active_before = _market_identity_at_root(service, data_root)

    recovered = service._promotion._recover_retained_promotion(
        result.report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    )

    assert recovered == result
    assert _market_identity_at_root(service, data_root) == active_before


@pytest.mark.parametrize(
    "durable_boundary",
    [
        "exchange_fsynced",
        "exchanged_journaled",
        "quarantine_fsynced",
        "quarantined_journaled",
        "smoke_joined",
        "smoke_journaled",
        "held_cleanup_fsynced",
        "report_fsynced",
        "report_journaled",
        "consumed_marker_fsynced",
    ],
)
def test_promotion_failure_at_durable_boundary_restores_exact_v3(
    tmp_path: Path,
    durable_boundary: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    retained_artifacts_before = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name not in {"market.duckdb", "parquet"}
    }

    def fail_at_boundary(stage: str) -> None:
        if stage == durable_boundary:
            raise CutoverSafetyError(f"injected durable boundary: {stage}")

    service._workspace._promotion_boundary_hook = fail_at_boundary

    with pytest.raises(CutoverSafetyError, match="injected durable boundary"):
        _run_retained_promotion(service, config)

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before
    assert retained_artifacts_before <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }
    assert not (
        data_root / "operations/market-v4-cutover/consumed/"
        "market-v4-retained-20260715-r13.json"
    ).exists()


def test_promotion_rollback_uses_verified_backup_only_after_exchange_back_fails(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("injected exchange-back failure")

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._workspace.managed(), "market-v4-active-20260716", now=service._workspace.now
        )
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service._workspace.atomic_exchange.exchange(
            service._workspace.managed(),
            Path("market-timeseries"),
            service._workspace._managed_relative(retained_root / "market-timeseries"),
        )
        service._workspace.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._promotion._rollback._rollback_retained_promotion(
            RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )

    active_after = _market_identity_at_root(service, data_root)
    assert service._promotion._promotion_evidence._payload_manifest_entries(active_after) == (
        service._promotion._promotion_evidence._payload_manifest_entries(active_before)
    )
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_rollback_reports_terminal_failure_when_both_paths_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("injected exchange-back failure")

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._workspace.managed(), "market-v4-active-20260716", now=service._workspace.now
        )
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service._workspace.atomic_exchange.exchange(
            service._workspace.managed(),
            Path("market-timeseries"),
            service._workspace._managed_relative(retained_root / "market-timeseries"),
        )
        service._workspace.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        monkeypatch.setattr(
            service._backups,
            "_restore_under_lease",
            lambda _backup_id: (_ for _ in ()).throw(
                CutoverSafetyError("injected restore failure")
            ),
        )

        with pytest.raises(CutoverSafetyError, match="Terminal promotion recovery"):
            service._promotion._rollback._rollback_retained_promotion(
                RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )


def test_promotion_report_failure_restores_exact_staged_artifacts(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    runtime_names = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name.startswith(".cutover-runtime-")
    }

    def fail_after_report(stage: str) -> None:
        if stage == "report_fsynced":
            raise CutoverSafetyError("injected report crash")

    service._workspace._promotion_boundary_hook = fail_after_report

    with pytest.raises(CutoverSafetyError, match="injected report crash"):
        _run_retained_promotion(service, config)

    assert runtime_names <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }
    assert not (
        data_root / "operations/market-v4-cutover/cleanup-staging/"
        "market-v4-active-20260716"
    ).exists()
