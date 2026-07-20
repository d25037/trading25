"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import cast, NoReturn

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    ApiAdapter,
    OperationResult,
    SmokeConfig,
    SmokeResult,
)
from .errors import RetainedMarketMutationError, RuntimeStopError, WorkerShutdownError
from .duckdb_service import MarketIdentityService
from .evidence import MarketEvidence
from .filesystem import _DIR_OPEN_FLAGS
from .reports import CutoverReportRepository
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


@dataclass
class _RetainedRunState:
    api: ApiAdapter | None = None
    market_fd: int | None = None
    source_market_identity_before: dict[str, object] | None = None
    source_market_identity_after: dict[str, object] | None = None
    completed_api_checks: tuple[str, ...] = ()
    completed_phases: tuple[dict[str, object], ...] = ()
    completed_evidence: dict[str, object] | None = None
    report_reserved: bool = False
    runtime_reserved: bool = False
    runtime_start_attempted: bool = False


class RetainedRehearsalService:
    """Smoke a retained full-rebuild root without mutating its Market tree."""

    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
        market_identity: MarketIdentityService,
        reports: CutoverReportRepository,
        runtime_smoke: RuntimeSmokeService,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence
        self._market_identity = market_identity
        self._reports = reports
        self._runtime_smoke = runtime_smoke

    def rehearse_retained(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._workspace.managed_root_scope():
            return self._rehearse_retained_managed(
                report_id,
                source_rehearsal_report_id=source_rehearsal_report_id,
                config=config,
                inherited_environment=inherited_environment,
            )

    def _rehearse_retained_managed(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        workspace = self._workspace
        report_id = workspace._validate_id(report_id, label="report")
        source_rehearsal_report_id = workspace._validate_id(
            source_rehearsal_report_id,
            label="source rehearsal report",
        )
        if report_id == source_rehearsal_report_id:
            raise _managed_root.CutoverSafetyError(
                "Retained rehearsal requires a new report ID"
            )
        code_version = workspace._require_code_identity()
        workspace._validate_active_roots()
        target_root_fingerprint = self._evidence.root_fingerprint(
            workspace.data_root
        )
        source_report = self._reports._read_report(source_rehearsal_report_id)
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        source_code_version = source_report.get("codeVersion")
        if not (
            source_report.get("reportId") == source_rehearsal_report_id
            and source_report.get("phase") == "rehearsal"
            and source_report.get("status") in {"passed", "failed"}
            and source_report.get("smokeConfig") == expected_smoke_config
            and source_report.get("targetRootFingerprint") == target_root_fingerprint
            and source_report.get("serverProcessJoined") is True
            and source_report.get("workerProcessJoined") is True
            and isinstance(source_code_version, str)
            and bool(source_code_version)
        ):
            raise _managed_root.CutoverSafetyError(
                "An eligible retained rehearsal report is required"
            )
        retained_root = self._market_identity.retained_rehearsal_root(
            source_rehearsal_report_id
        )
        if self._evidence.configuration_fingerprint(
            retained_root
        ) != self._evidence.configuration_fingerprint(workspace.data_root):
            raise _managed_root.CutoverSafetyError(
                "Retained rehearsal configuration differs from active configuration"
            )
        source_retained_root_fingerprint = self._evidence.root_fingerprint(
            retained_root
        )
        with _market_operation_lease.MarketOperationLease.acquire(
            retained_root, exclusive=True
        ) as lease:
            self._market_identity.assert_retained_root_identity(
                retained_root,
                lease.root_fd,
            )
            leased_root_fingerprint = self._market_identity.root_fingerprint_at(
                lease.root_fd
            )
            if leased_root_fingerprint != source_retained_root_fingerprint:
                raise _managed_root.CutoverSafetyError(
                    "Retained rehearsal root changed before lease acquisition"
                )
            return self._rehearse_retained_under_lease(
                report_id,
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_code_version,
                source_retained_root_fingerprint=leased_root_fingerprint,
                retained_root=retained_root,
                config=config,
                inherited_environment=inherited_environment,
                lease=lease,
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
            )

    def _rehearse_retained_under_lease(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        source_rehearsal_code_version: str,
        source_retained_root_fingerprint: str,
        retained_root: Path,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        lease: _market_operation_lease.MarketOperationLease,
        target_root_fingerprint: str,
        code_version: str,
    ) -> OperationResult:
        runtime_name, report_dir, log_path = self._retained_rehearsal_paths(report_id)
        started = time.monotonic()
        state = _RetainedRunState()
        try:
            report_path = self._run_retained_smoke(
                report_id=report_id,
                started=started,
                config=config,
                code_version=code_version,
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_rehearsal_code_version,
                source_retained_root_fingerprint=source_retained_root_fingerprint,
                target_root_fingerprint=target_root_fingerprint,
                retained_root=retained_root,
                lease=lease,
                inherited_environment=inherited_environment,
                runtime_name=runtime_name,
                report_dir=report_dir,
                log_path=log_path,
                state=state,
            )
        except Exception as exc:
            self._handle_retained_rehearsal_failure(
                exc=exc,
                report_id=report_id,
                report_dir=report_dir,
                runtime_name=runtime_name,
                runtime_start_attempted=state.runtime_start_attempted,
                runtime_reserved=state.runtime_reserved,
                report_reserved=state.report_reserved,
                api=state.api,
                market_fd=state.market_fd,
                lease=lease,
                started=started,
                completed_api_checks=state.completed_api_checks,
                completed_phases=state.completed_phases,
                completed_evidence=state.completed_evidence,
                config=config,
                code_version=code_version,
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_rehearsal_code_version,
                source_retained_root_fingerprint=source_retained_root_fingerprint,
                source_market_identity_before=state.source_market_identity_before,
                inherited_environment=inherited_environment,
                target_root_fingerprint=target_root_fingerprint,
            )
        finally:
            if state.market_fd is not None:
                os.close(state.market_fd)
        return self._operation_result(report_id, report_path)

    def _run_retained_smoke(
        self,
        *,
        report_id: str,
        started: float,
        config: SmokeConfig,
        code_version: str,
        source_rehearsal_report_id: str,
        source_rehearsal_code_version: str,
        source_retained_root_fingerprint: str,
        target_root_fingerprint: str,
        retained_root: Path,
        lease: _market_operation_lease.MarketOperationLease,
        inherited_environment: dict[str, str],
        runtime_name: str,
        report_dir: Path,
        log_path: Path,
        state: _RetainedRunState,
    ) -> Path:
        state.market_fd, state.source_market_identity_before = (
            self._open_retained_market_for_rehearsal(report_dir, lease)
        )

        def mark_runtime_reserved() -> None:
            state.runtime_reserved = True

        try:
            self._market_identity._prepare_retained_runtime(
                retained_root,
                runtime_name=runtime_name,
                root_fd=lease.root_fd,
                on_reserved=mark_runtime_reserved,
            )
        except Exception:
            if state.runtime_reserved:
                with _managed_root.ManagedRootFd(
                    Path("."),
                    os.dup(lease.root_fd),
                ) as retained:
                    retained.remove_tree(
                        Path("market-timeseries") / runtime_name,
                        missing_ok=True,
                    )
                state.runtime_reserved = False
            raise
        self._workspace._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._workspace._prepare_managed_directory(report_dir, exist_ok=False)
        state.report_reserved = True
        self._validate_retained_before_runtime(
            retained_root,
            lease,
            target_root_fingerprint=target_root_fingerprint,
            code_version=code_version,
        )
        environment = self._runtime_smoke.isolated_environment(
            inherited_environment,
            lease_fd=lease.fd,
            root_fd=lease.root_fd,
            runtime_name=runtime_name,
        )
        environment["TRADING25_RUNTIME_CAPABILITY"] = "retained_market_smoke"
        log_fd = self._workspace.managed().open_regular(
            self._workspace._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
        )
        try:
            state.runtime_start_attempted = True
            state.api = self._workspace.runtime.start(
                root_fd=lease.root_fd,
                market_fd=state.market_fd,
                lease_fd=lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
        finally:
            os.close(log_fd)
        smoke_started = time.monotonic()
        smoke_result = self._runtime_smoke.smoke(
            state.api,
            config,
            operation_id=report_id,
            market_root=retained_root / "market-timeseries",
            market_directory_fd=state.market_fd,
            guard_lease_fd=lease.fd,
        )
        phases = (
            {
                "name": "retained_market_smoke",
                "status": "passed",
                "durationSeconds": round(time.monotonic() - smoke_started, 6),
            },
        )
        state.completed_api_checks = smoke_result.api_paths
        state.completed_phases = phases
        state.completed_evidence = {
            "schemaVersion": smoke_result.schema_version,
            "stockPriceAdjustmentMode": smoke_result.adjustment_mode,
            "providerVintage": smoke_result.lineage,
        }
        self._workspace.runtime.stop(state.api)
        state.api = None
        identity_after = self._unchanged_market_tree_identity(
            lease.root_fd,
            state.source_market_identity_before,
        )
        state.source_market_identity_after = cast(dict[str, object], identity_after)
        self._validate_retained_after_smoke(
            retained_root,
            lease,
            target_root_fingerprint=target_root_fingerprint,
            source_retained_root_fingerprint=source_retained_root_fingerprint,
            code_version=code_version,
        )
        return self._publish_retained_rehearsal_report(
            report_id=report_id,
            started=started,
            smoke_result=smoke_result,
            evidence=state.completed_evidence,
            phases=phases,
            config=config,
            code_version=code_version,
            source_rehearsal_report_id=source_rehearsal_report_id,
            source_rehearsal_code_version=source_rehearsal_code_version,
            source_retained_root_fingerprint=source_retained_root_fingerprint,
            source_market_identity_before=state.source_market_identity_before,
            source_market_identity_after=state.source_market_identity_after,
            target_root_fingerprint=target_root_fingerprint,
            retained_root=retained_root,
            lease=lease,
        )

    def _validate_retained_before_runtime(
        self,
        retained_root: Path,
        lease: _market_operation_lease.MarketOperationLease,
        *,
        target_root_fingerprint: str,
        code_version: str,
    ) -> None:
        self._market_identity.assert_retained_root_identity(
            retained_root,
            lease.root_fd,
        )
        if (
            self._evidence.root_fingerprint(self._workspace.data_root)
            != target_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active configuration changed before retained runtime start"
            )
        if self._market_identity._configuration_fingerprint_at(
            lease.root_fd
        ) != self._evidence.configuration_fingerprint(self._workspace.data_root):
            raise _managed_root.CutoverSafetyError(
                "Retained rehearsal configuration changed before runtime start"
            )
        self._workspace._require_unchanged_code_identity(code_version)

    def _validate_retained_after_smoke(
        self,
        retained_root: Path,
        lease: _market_operation_lease.MarketOperationLease,
        *,
        target_root_fingerprint: str,
        source_retained_root_fingerprint: str,
        code_version: str,
    ) -> None:
        if (
            self._evidence.root_fingerprint(self._workspace.data_root)
            != target_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active configuration changed during retained rehearsal"
            )
        self._workspace._require_unchanged_code_identity(code_version)
        self._market_identity.assert_retained_root_identity(
            retained_root,
            lease.root_fd,
        )
        if (
            self._market_identity.root_fingerprint_at(lease.root_fd)
            != source_retained_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Retained configuration changed during semantic smoke"
            )

    def _retained_rehearsal_paths(self, report_id: str) -> tuple[str, Path, Path]:
        runtime_name = f".cutover-runtime-{report_id}"
        report_dir = self._workspace.operations_root / "reports" / report_id
        return runtime_name, report_dir, report_dir / "server.log"

    def _unchanged_market_tree_identity(
        self,
        root_fd: int,
        identity_before: dict[str, object] | None,
    ) -> dict[str, object]:
        identity_after = self._market_identity.market_tree_identity(root_fd)
        if identity_before != identity_after:
            raise RetainedMarketMutationError(
                "retained Market tree changed during smoke"
            )
        return identity_after

    def _operation_result(self, report_id: str, report_path: Path) -> OperationResult:
        return OperationResult(
            report_id,
            report_path.relative_to(self._workspace.data_root).as_posix(),
        )

    def _open_retained_market_for_rehearsal(
        self,
        report_dir: Path,
        lease: _market_operation_lease.MarketOperationLease,
    ) -> tuple[int, dict[str, object]]:
        try:
            self._workspace.managed().stat(
                self._workspace._managed_relative(report_dir)
            )
        except FileNotFoundError:
            pass
        else:
            raise _managed_root.CutoverSafetyError(
                "Retained report destination already exists"
            )
        market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=lease.root_fd,
        )
        try:
            metadata = self._workspace.duckdb.inspect(
                market_fd,
                "market.duckdb",
                guard_lease_fd=lease.fd,
            )
            if metadata.schema_version != 5:
                raise _managed_root.CutoverSafetyError(
                    "Retained Market schema v5 is required"
                )
            if metadata.adjustment_mode != "provider_adjusted_v1":
                raise _managed_root.CutoverSafetyError(
                    "Retained Market adjustment mode is incompatible"
                )
            if metadata.provider_vintage_ready is not True:
                raise _managed_root.CutoverSafetyError(
                    "Retained provider-vintage current-basis lineage is not ready"
                )
            return market_fd, self._market_identity.market_tree_identity(
                lease.root_fd
            )
        except Exception:
            os.close(market_fd)
            raise

    def _publish_retained_rehearsal_report(
        self,
        *,
        report_id: str,
        started: float,
        smoke_result: SmokeResult,
        evidence: dict[str, object],
        phases: tuple[dict[str, object], ...],
        config: SmokeConfig,
        code_version: str,
        source_rehearsal_report_id: str,
        source_rehearsal_code_version: str,
        source_retained_root_fingerprint: str,
        source_market_identity_before: dict[str, object],
        source_market_identity_after: dict[str, object],
        target_root_fingerprint: str,
        retained_root: Path,
        lease: _market_operation_lease.MarketOperationLease,
    ) -> Path:
        report = self._reports._operation_report(
            report_id=report_id,
            phase="rehearsal",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=smoke_result.api_paths,
            server_log=f"reports/{report_id}/server.log",
            evidence=evidence,
            phases=phases,
            config=config,
            code_version=code_version,
            rehearsal_mode="retained_market_smoke",
            source_rehearsal_report_id=source_rehearsal_report_id,
            source_rehearsal_code_version=source_rehearsal_code_version,
            source_retained_root_fingerprint=source_retained_root_fingerprint,
            source_market_identity_before=source_market_identity_before,
            source_market_identity_after=source_market_identity_after,
            server_process_joined=True,
            worker_process_joined=True,
            target_root_fingerprint=target_root_fingerprint,
        )

        def final_validator() -> None:
            self._workspace._require_unchanged_code_identity(code_version)
            if (
                self._evidence.root_fingerprint(self._workspace.data_root)
                != target_root_fingerprint
            ):
                raise _managed_root.CutoverSafetyError(
                    "Active configuration changed at report publication"
                )
            self._market_identity.assert_retained_root_identity(
                retained_root,
                lease.root_fd,
            )
            if (
                self._market_identity.root_fingerprint_at(lease.root_fd)
                != source_retained_root_fingerprint
            ):
                raise _managed_root.CutoverSafetyError(
                    "Retained root changed at report publication"
                )
            if (
                self._market_identity.market_tree_identity(lease.root_fd)
                != source_market_identity_after
            ):
                raise RetainedMarketMutationError(
                    "Retained Market changed at report publication"
                )

        return self._reports._write_report(
            report_id,
            report,
            expected_root_fingerprint=target_root_fingerprint,
            final_validator=final_validator,
        )

    def _handle_retained_rehearsal_failure(
        self,
        *,
        exc: Exception,
        report_id: str,
        report_dir: Path,
        runtime_name: str,
        runtime_start_attempted: bool,
        runtime_reserved: bool,
        report_reserved: bool,
        api: ApiAdapter | None,
        market_fd: int | None,
        lease: _market_operation_lease.MarketOperationLease,
        started: float,
        completed_api_checks: tuple[str, ...],
        completed_phases: tuple[dict[str, object], ...],
        completed_evidence: dict[str, object] | None,
        config: SmokeConfig,
        code_version: str,
        source_rehearsal_report_id: str,
        source_rehearsal_code_version: str,
        source_retained_root_fingerprint: str,
        source_market_identity_before: dict[str, object] | None,
        inherited_environment: dict[str, str],
        target_root_fingerprint: str,
    ) -> NoReturn:
        if not runtime_start_attempted:
            if market_fd is not None and runtime_reserved:
                try:
                    self._workspace._remove_market_runtime(market_fd, runtime_name)
                except FileNotFoundError:
                    pass
            if report_reserved:
                self._workspace.managed().remove_tree(
                    self._workspace._managed_relative(report_dir),
                    missing_ok=True,
                )
        cleanup_error: Exception | None = None
        stop_error: Exception | None = (
            exc if isinstance(exc, RuntimeStopError) else None
        )
        server_process_joined = (
            exc.process_joined if isinstance(exc, RuntimeStopError) else api is None
        )
        worker_process_joined = not (
            isinstance(exc, WorkerShutdownError) and not exc.process_joined
        )
        if api is not None:
            try:
                self._workspace.runtime.cancel_owned_work(api)
            except Exception as runtime_cleanup_error:
                cleanup_error = runtime_cleanup_error
            try:
                self._workspace.runtime.stop(api)
            except RuntimeStopError as runtime_stop_error:
                stop_error = runtime_stop_error
                server_process_joined = runtime_stop_error.process_joined
            except Exception as runtime_stop_error:
                stop_error = runtime_stop_error
                server_process_joined = False
            else:
                server_process_joined = True
        source_market_identity_after = None
        if source_market_identity_before is not None:
            try:
                source_market_identity_after = (
                    self._market_identity.market_tree_identity(lease.root_fd)
                )
            except Exception:
                pass
        if not server_process_joined or not worker_process_joined:
            lease.unlock_on_release = False
        failure_report = self._reports._operation_report(
            report_id=report_id,
            phase="rehearsal",
            status=(
                "failed"
                if server_process_joined and worker_process_joined
                else "stop_failed_cleanup_deferred"
            ),
            duration_seconds=time.monotonic() - started,
            api_checks=completed_api_checks,
            server_log=f"reports/{report_id}/server.log",
            evidence=completed_evidence,
            phases=completed_phases,
            config=config,
            code_version=code_version,
            rehearsal_mode="retained_market_smoke",
            source_rehearsal_report_id=source_rehearsal_report_id,
            source_rehearsal_code_version=source_rehearsal_code_version,
            source_retained_root_fingerprint=source_retained_root_fingerprint,
            source_market_identity_before=source_market_identity_before,
            source_market_identity_after=source_market_identity_after,
            error=type(exc).__name__,
            error_message=self._reports._redact_diagnostic(
                str(exc),
                inherited_environment,
            ),
            cleanup_error=(type(cleanup_error).__name__ if cleanup_error else None),
            stop_error=type(stop_error).__name__ if stop_error else None,
            server_process_joined=server_process_joined,
            worker_process_joined=worker_process_joined,
            target_root_fingerprint=target_root_fingerprint,
        )
        if report_reserved and report_dir.exists() and not report_dir.is_symlink():
            self._reports._try_write_report(report_id, failure_report)
        if isinstance(exc, RetainedMarketMutationError):
            raise _managed_root.CutoverSafetyError(str(exc)) from exc
        raise _managed_root.CutoverSafetyError(
            "Retained Market rehearsal failed"
        ) from exc
