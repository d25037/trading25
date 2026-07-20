"""Full-rebuild Market v5 rehearsal orchestration."""

from __future__ import annotations

import os
from pathlib import Path
import time

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import ApiAdapter, OperationResult, SmokeConfig
from .errors import RuntimeStopError, WorkerShutdownError
from .evidence import MarketEvidence
from .filesystem import _DIR_OPEN_FLAGS
from .reports import CutoverReportRepository
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


class FullRebuildRehearsalService:
    """Build and smoke an isolated Market v5 root."""

    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
        reports: CutoverReportRepository,
        runtime_smoke: RuntimeSmokeService,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence
        self._reports = reports
        self._runtime_smoke = runtime_smoke

    def rehearse(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._workspace.managed_root_scope():
            return self._rehearse_managed(
                report_id,
                config,
                inherited_environment=inherited_environment,
            )

    def _rehearse_managed(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        workspace = self._workspace
        report_id = workspace._validate_id(report_id, label="report")
        code_version = workspace._require_code_identity()
        workspace._validate_active_roots()
        target_root_fingerprint = self._evidence.root_fingerprint(
            workspace.data_root
        )
        source_configuration_fingerprint = (
            self._evidence.configuration_fingerprint(workspace.data_root)
        )
        rehearsal_dir = workspace.operations_root / "rehearsals" / report_id
        workspace._prepare_managed_directory(rehearsal_dir.parent, exist_ok=True)
        if rehearsal_dir.exists() or rehearsal_dir.is_symlink():
            raise _managed_root.CutoverSafetyError(
                "Rehearsal destination already exists"
            )
        workspace._prepare_managed_directory(rehearsal_dir, exist_ok=False)
        rehearsal_root = rehearsal_dir / "root"
        runtime_name = f".cutover-runtime-{report_id}"
        self._runtime_smoke.prepare_isolated_root(
            rehearsal_root,
            runtime_name=runtime_name,
        )
        if (
            self._evidence.configuration_fingerprint(rehearsal_root)
            != source_configuration_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Rehearsal configuration snapshot mismatch"
            )
        with _market_operation_lease.MarketOperationLease.acquire(
            rehearsal_root,
            exclusive=True,
        ) as lease:
            return self._rehearse_under_lease(
                report_id,
                config,
                inherited_environment=inherited_environment,
                rehearsal_dir=rehearsal_dir,
                rehearsal_root=rehearsal_root,
                lease=lease,
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
            )

    def _rehearse_under_lease(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
        rehearsal_dir: Path,
        rehearsal_root: Path,
        lease: _market_operation_lease.MarketOperationLease,
        target_root_fingerprint: str,
        code_version: str,
    ) -> OperationResult:
        workspace = self._workspace
        environment = self._runtime_smoke.isolated_environment(
            inherited_environment,
            lease_fd=lease.fd,
            root_fd=lease.root_fd,
            runtime_name=f".cutover-runtime-{report_id}",
        )
        started = time.monotonic()
        api: ApiAdapter | None = None
        market_fd: int | None = None
        log_path = rehearsal_dir / "server.log"
        try:
            market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=lease.root_fd,
            )
            log_fd = workspace.managed().open_regular(
                workspace._managed_relative(log_path),
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
            )
            try:
                api = workspace.runtime.start(
                    root_fd=lease.root_fd,
                    market_fd=market_fd,
                    lease_fd=lease.fd,
                    environment=environment,
                    log_path=log_path,
                    log_fd=log_fd,
                )
            finally:
                os.close(log_fd)
            checks, evidence, phases, _market_identity = (
                self._runtime_smoke.run_rebuild(
                    api,
                    config,
                    rehearsal_root,
                    report_id,
                    market_directory_fd=market_fd,
                    guard_lease_fd=lease.fd,
                )
            )
            workspace.runtime.stop(api)
            api = None
            os.close(market_fd)
            market_fd = None
            if (
                self._evidence.root_fingerprint(workspace.data_root)
                != target_root_fingerprint
            ):
                raise _managed_root.CutoverSafetyError(
                    "Active configuration changed during isolated rehearsal"
                )
            workspace._require_unchanged_code_identity(code_version)
        except Exception as exc:
            if market_fd is not None:
                os.close(market_fd)
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
                    workspace.runtime.cancel_owned_work(api)
                except Exception as runtime_cleanup_error:
                    cleanup_error = runtime_cleanup_error
                try:
                    workspace.runtime.stop(api)
                except RuntimeStopError as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = runtime_stop_error.process_joined
                except Exception as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = False
                else:
                    server_process_joined = True
            if not server_process_joined or not worker_process_joined:
                lease.unlock_on_release = False
            report = self._reports._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status=(
                    "failed"
                    if server_process_joined and worker_process_joined
                    else "stop_failed_cleanup_deferred"
                ),
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log=f"rehearsals/{report_id}/server.log",
                evidence=None,
                phases=(),
                config=config,
                error=type(exc).__name__,
                error_message=self._reports._redact_diagnostic(
                    str(exc),
                    inherited_environment,
                ),
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
                rehearsal_mode="full_rebuild",
                cleanup_error=(type(cleanup_error).__name__ if cleanup_error else None),
                stop_error=type(stop_error).__name__ if stop_error else None,
                server_process_joined=server_process_joined,
                worker_process_joined=worker_process_joined,
            )
            self._reports._write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Isolated Market v5 rehearsal failed"
            ) from exc
        report = self._reports._operation_report(
            report_id=report_id,
            phase="rehearsal",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=checks,
            server_log=f"rehearsals/{report_id}/server.log",
            evidence=evidence,
            phases=phases,
            config=config,
            target_root_fingerprint=target_root_fingerprint,
            code_version=code_version,
            rehearsal_mode="full_rebuild",
            server_process_joined=True,
            worker_process_joined=True,
        )
        try:
            report_path = self._reports._write_report(
                report_id,
                report,
                expected_root_fingerprint=target_root_fingerprint,
            )
        except Exception as exc:
            failure_report = self._reports._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status="failed",
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log=f"rehearsals/{report_id}/server.log",
                evidence=None,
                phases=(),
                config=config,
                error=type(exc).__name__,
                error_message=self._reports._redact_diagnostic(
                    str(exc),
                    inherited_environment,
                ),
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
                rehearsal_mode="full_rebuild",
                server_process_joined=True,
                worker_process_joined=True,
            )
            self._reports._try_write_report(report_id, failure_report)
            raise _managed_root.CutoverSafetyError(
                "Isolated Market v5 rehearsal failed"
            ) from exc
        return OperationResult(
            report_id,
            report_path.relative_to(workspace.data_root).as_posix(),
        )
