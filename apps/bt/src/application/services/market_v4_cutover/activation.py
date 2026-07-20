"""Focused Market v5 full-rebuild activation and rollback."""

from __future__ import annotations

import os
from pathlib import Path
import time

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import market_operation_lease as _market_operation_lease

from .contracts import (
    ApiAdapter,
    OperationResult,
    SmokeConfig,
)
from .backup import MarketBackupService
from .activation_contract import full_rebuild_report_contract_valid
from .errors import RuntimeStopError, WorkerShutdownError
from .evidence import MarketEvidence
from .filesystem import _DIR_OPEN_FLAGS
from .reports import CutoverReportRepository
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


class MarketActivationService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
        reports: CutoverReportRepository,
        runtime_smoke: RuntimeSmokeService,
        backups: MarketBackupService,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence
        self._reports = reports
        self._runtime_smoke = runtime_smoke
        self._backups = backups

    def cutover(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._workspace.exclusive_operation() as code_version:
            return self._cutover_under_lease(
                report_id,
                rehearsal_report_id=rehearsal_report_id,
                backup_id=backup_id,
                config=config,
                inherited_environment=inherited_environment,
                code_version=code_version,
            )

    def _cutover_under_lease(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
    ) -> OperationResult:
        report_id = self._workspace._validate_id(report_id, label="report")
        rehearsal_report_id = self._workspace._validate_id(
            rehearsal_report_id, label="rehearsal report"
        )
        backup_id = self._workspace._validate_id(backup_id, label="backup")
        expected_root_fingerprint = self._validate_cutover_rehearsal(
            rehearsal_report_id=rehearsal_report_id,
            config=config,
            code_version=code_version,
        )
        self._backups.verify_backup(backup_id)
        self._backups._preflight_under_lease()
        backup_market_tree_sha256 = (
            self._backups._assert_active_matches_backup_under_lease(backup_id)
        )
        assert self._workspace._active_lease is not None
        return self._execute_cutover(
            report_id=report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=config,
            inherited_environment=inherited_environment,
            code_version=code_version,
            expected_root_fingerprint=expected_root_fingerprint,
            backup_market_tree_sha256=backup_market_tree_sha256,
        )

    def _validate_cutover_rehearsal(
        self,
        *,
        rehearsal_report_id: str,
        config: SmokeConfig,
        code_version: str,
    ) -> str:
        rehearsal = self._reports._read_report(rehearsal_report_id)
        target_root_fingerprint = rehearsal.get("targetRootFingerprint")
        expected_root_fingerprint = (
            target_root_fingerprint
            if isinstance(target_root_fingerprint, str)
            else None
        )
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        common_valid = (
            rehearsal.get("phase") == "rehearsal"
            and rehearsal.get("status") == "passed"
            and rehearsal.get("reportId") == rehearsal_report_id
            and rehearsal.get("rehearsalMode") == "full_rebuild"
            and rehearsal.get("serverProcessJoined") is True
            and rehearsal.get("workerProcessJoined") is True
            and expected_root_fingerprint
            == self._evidence.root_fingerprint(self._workspace.data_root)
            and rehearsal.get("codeVersion") == code_version
            and rehearsal.get("smokeConfig") == expected_smoke_config
        )
        full_rebuild_valid = full_rebuild_report_contract_valid(
            rehearsal,
            config=config,
        )
        if not common_valid or not full_rebuild_valid:
            raise _managed_root.CutoverSafetyError(
                "An exact passing rehearsal report from a Market v5 full-rebuild is required"
            )
        assert expected_root_fingerprint is not None
        return expected_root_fingerprint

    def _execute_cutover(
        self,
        *,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
        expected_root_fingerprint: str,
        backup_market_tree_sha256: str,
    ) -> OperationResult:
        assert self._workspace._active_lease is not None
        started = time.monotonic()
        api: ApiAdapter | None = None
        activated = False
        activation_attempted = False
        staging_lease: _market_operation_lease.MarketOperationLease | None = None
        staged_market_fd: int | None = None
        active_market_fd: int | None = None
        report_dir, log_path = self._prepare_cutover_report_directory(report_id)
        try:
            staging = self._prepare_cutover_staging(
                report_id, expected_root_fingerprint=expected_root_fingerprint
            )
            staging_root, runtime_name, runtime_template = staging
            staging_lease = _market_operation_lease.MarketOperationLease.acquire(
                staging_root, exclusive=True
            )
            if staging_lease is not None:
                staging_root_identity = os.fstat(staging_lease.root_fd)
                staged_market_fd = self._open_staged_market(staging_lease)
                staged_market_identity = os.fstat(staged_market_fd)
                environment = self._runtime_smoke.isolated_environment(
                    inherited_environment,
                    lease_fd=staging_lease.fd,
                    root_fd=staging_lease.root_fd,
                    runtime_name=runtime_name,
                )
                log_fd = self._workspace.managed().open_regular(
                    self._workspace._managed_relative(log_path),
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                )
                try:
                    api = self._workspace.runtime.start(
                        root_fd=staging_lease.root_fd,
                        market_fd=staged_market_fd,
                        lease_fd=staging_lease.fd,
                        environment=environment,
                        log_path=log_path,
                        log_fd=log_fd,
                    )
                finally:
                    os.close(log_fd)
                self._assert_active_root_fingerprint(
                    expected_root_fingerprint,
                    message="Active inputs changed during staged server start",
                )
                (
                    checks,
                    evidence,
                    phases,
                    _verified_market_identity,
                ) = self._runtime_smoke.run_rebuild(
                    api,
                    config,
                    staging_root,
                    report_id,
                    market_directory_fd=staged_market_fd,
                    guard_lease_fd=staging_lease.fd,
                )
                if (
                    _verified_market_identity.st_dev,
                    _verified_market_identity.st_ino,
                ) != (staged_market_identity.st_dev, staged_market_identity.st_ino):
                    raise _managed_root.CutoverSafetyError(
                        "Staged Market identity changed during rebuild"
                    )
                self._workspace.runtime.stop(api)
                api = None
                os.close(staged_market_fd)
                staged_market_fd = None
                staging_lease.release()
                staging_lease = None
            self._validate_rebuilt_market(
                staging_root=staging_root,
                runtime_name=runtime_name,
                runtime_template=runtime_template,
                expected_root_fingerprint=expected_root_fingerprint,
                staging_root_identity=staging_root_identity,
                staged_market_identity=staged_market_identity,
            )
            self._assert_backup_still_matches_active(
                backup_id, backup_market_tree_sha256
            )
            activation_attempted = True
            self._activate_rebuilt_market(
                staging_root=staging_root,
                runtime_template=runtime_template,
                runtime_name=runtime_name,
                report_id=report_id,
            )
            activated = True
            active_environment = self._active_cutover_environment(
                inherited_environment, runtime_name=runtime_name
            )
            active_log_path = report_dir / "active-smoke.log"
            active_log_fd, active_market_fd = self._open_active_smoke_files(
                active_log_path
            )
            try:
                api = self._workspace.runtime.start(
                    root_fd=self._workspace._active_lease.root_fd,
                    market_fd=active_market_fd,
                    lease_fd=self._workspace._active_lease.fd,
                    environment=active_environment,
                    log_path=active_log_path,
                    log_fd=active_log_fd,
                )
            finally:
                os.close(active_log_fd)
            active_smoke_started = time.monotonic()
            active_smoke = self._runtime_smoke.smoke(
                api,
                config,
                operation_id=f"{report_id}.active",
                market_directory_fd=active_market_fd,
                guard_lease_fd=self._workspace._active_lease.fd,
            )
            active_smoke_duration = time.monotonic() - active_smoke_started
            self._assert_activated_lineage(evidence, active_smoke.lineage)
            self._workspace.runtime.stop(api)
            api = None
            self._workspace._remove_market_runtime(active_market_fd, runtime_name)
            os.close(active_market_fd)
            active_market_fd = None
            checks = (*checks, *active_smoke.api_paths)
            phases = (
                *phases,
                {
                    "name": "activated_market_smoke",
                    "status": "passed",
                    "durationSeconds": round(active_smoke_duration, 6),
                },
            )
            self._assert_active_root_fingerprint(
                expected_root_fingerprint,
                message="Active inputs changed before report persistence",
            )
            self._workspace._assert_current_data_root_identity()
            self._workspace._require_unchanged_code_identity(code_version)
            return self._publish_cutover_success(
                report_id=report_id,
                rehearsal_report_id=rehearsal_report_id,
                backup_id=backup_id,
                config=config,
                code_version=code_version, expected_root_fingerprint=expected_root_fingerprint,
                started=started, checks=checks, evidence=evidence, phases=phases,
                backup_market_tree_sha256=backup_market_tree_sha256,
                active_provider_vintage=active_smoke.lineage,
            )
        except Exception as exc:
            self._handle_cutover_failure(
                exc,
                report_id=report_id,
                rehearsal_report_id=rehearsal_report_id,
                backup_id=backup_id,
                config=config,
                inherited_environment=inherited_environment,
                code_version=code_version,
                expected_root_fingerprint=expected_root_fingerprint,
                started=started,
                api=api,
                staging_lease=staging_lease,
                staged_market_fd=staged_market_fd,
                active_market_fd=active_market_fd,
                activated=activated,
                activation_attempted=activation_attempted,
            )
            raise AssertionError("cutover failure handler must raise")

    def _assert_backup_still_matches_active(
        self, backup_id: str, expected_sha256: str
    ) -> None:
        # Deliberately the last active-tree read before atomic exchange.
        if (
            self._backups._assert_active_matches_backup_under_lease(backup_id)
            != expected_sha256
        ):
            raise _managed_root.CutoverSafetyError(
                "Selected backup identity changed before activation"
            )

    @staticmethod
    def _assert_activated_lineage(
        staged_evidence: dict[str, object], active_lineage: dict[str, object]
    ) -> None:
        staged = staged_evidence.get("providerVintage")
        if not isinstance(staged, dict) or active_lineage != staged:
            raise _managed_root.CutoverSafetyError(
                "Activated Market provider vintage differs from staged lineage"
            )

    def _assert_active_root_fingerprint(
        self,
        expected: str,
        *,
        message: str,
    ) -> None:
        if self._evidence.root_fingerprint(self._workspace.data_root) != expected:
            raise _managed_root.CutoverSafetyError(message)

    def _prepare_cutover_report_directory(self, report_id: str) -> tuple[Path, Path]:
        report_dir = self._workspace.operations_root / "reports" / report_id
        self._workspace._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._workspace._prepare_managed_directory(report_dir, exist_ok=False)
        log_path = report_dir / "server.log"
        self._workspace._assert_managed_target_absent(log_path)
        return report_dir, log_path

    @staticmethod
    def _open_staged_market(
        lease: _market_operation_lease.MarketOperationLease,
    ) -> int:
        return os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=lease.root_fd,
        )

    def _open_active_smoke_files(self, log_path: Path) -> tuple[int, int]:
        assert self._workspace._active_lease is not None
        log_fd = self._workspace.managed().open_regular(
            self._workspace._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
        )
        try:
            market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=self._workspace._active_lease.root_fd,
            )
        except Exception:
            os.close(log_fd)
            raise
        return log_fd, market_fd

    def _active_cutover_environment(
        self,
        inherited_environment: dict[str, str],
        *,
        runtime_name: str,
    ) -> dict[str, str]:
        assert self._workspace._active_lease is not None
        return self._runtime_smoke.isolated_environment(
            inherited_environment,
            lease_fd=self._workspace._active_lease.fd,
            root_fd=self._workspace._active_lease.root_fd,
            runtime_name=runtime_name,
        )

    def _prepare_cutover_staging(
        self,
        report_id: str,
        *,
        expected_root_fingerprint: str,
    ) -> tuple[Path, str, Path]:
        staging_dir = self._workspace.operations_root / "staging" / report_id
        self._workspace._prepare_managed_directory(staging_dir.parent, exist_ok=True)
        self._workspace._prepare_managed_directory(staging_dir, exist_ok=False)
        staging_root = staging_dir / "root"
        runtime_name = f".cutover-runtime-{report_id}"
        runtime_template = staging_root / f"runtime-template-{report_id}"
        self._runtime_smoke.prepare_isolated_root(
            staging_root, runtime_name=runtime_name
        )
        if self._evidence.configuration_fingerprint(
            staging_root
        ) != self._evidence.configuration_fingerprint(self._workspace.data_root):
            raise _managed_root.CutoverSafetyError(
                "Cutover staging configuration snapshot mismatch"
            )
        if (
            self._evidence.root_fingerprint(self._workspace.data_root)
            != expected_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active inputs changed before owned server start"
            )
        return staging_root, runtime_name, runtime_template

    def _validate_rebuilt_market(
        self,
        *,
        staging_root: Path,
        runtime_name: str,
        runtime_template: Path,
        expected_root_fingerprint: str,
        staging_root_identity: os.stat_result,
        staged_market_identity: os.stat_result,
    ) -> None:
        self._workspace._secure_rename(
            staging_root / "market-timeseries" / runtime_name,
            runtime_template,
        )
        if (
            self._evidence.root_fingerprint(self._workspace.data_root)
            != expected_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active inputs changed during staged rebuild"
            )
        self._workspace._assert_current_data_root_identity()
        self._workspace._validate_active_roots()
        self._workspace._assert_managed_directory_identity(
            staging_root, staging_root_identity
        )
        self._workspace._assert_managed_directory_identity(
            staging_root / "market-timeseries",
            staged_market_identity,
        )

    def _activate_rebuilt_market(
        self,
        *,
        staging_root: Path,
        runtime_template: Path,
        runtime_name: str,
        report_id: str,
    ) -> None:
        self._workspace._activate_staged_market(
            staging_root / "market-timeseries",
            report_id,
        )
        self._workspace._secure_rename(
            runtime_template, self._workspace.market_root / runtime_name
        )

    def _publish_cutover_success(
        self,
        *,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
        expected_root_fingerprint: str,
        started: float,
        checks: tuple[str, ...],
        evidence: dict[str, object],
        phases: tuple[dict[str, object], ...],
        backup_market_tree_sha256: str,
        active_provider_vintage: dict[str, object],
    ) -> OperationResult:
        report = self._reports._operation_report(
            report_id=report_id,
            phase="cutover",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=checks,
            server_log=f"reports/{report_id}/server.log",
            evidence=evidence,
            phases=phases,
            config=config,
            backup_id=backup_id,
            rehearsal_report_id=rehearsal_report_id,
            target_root_fingerprint=expected_root_fingerprint,
            code_version=code_version,
        )
        report["activeBackupTreeSha256"] = backup_market_tree_sha256
        report["stagedProviderVintage"] = evidence["providerVintage"]
        report["activeProviderVintage"] = active_provider_vintage
        report_path = self._reports._write_report(
            report_id,
            report,
            expected_root_fingerprint=expected_root_fingerprint,
        )
        return OperationResult(
            report_id,
            report_path.relative_to(self._workspace.data_root).as_posix(),
        )

    def _handle_cutover_failure(
        self,
        exc: Exception,
        *,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
        expected_root_fingerprint: str,
        started: float,
        api: ApiAdapter | None,
        staging_lease: _market_operation_lease.MarketOperationLease | None,
        staged_market_fd: int | None,
        active_market_fd: int | None,
        activated: bool,
        activation_attempted: bool,
    ) -> None:
        if staged_market_fd is not None:
            os.close(staged_market_fd)
        if active_market_fd is not None:
            os.close(active_market_fd)
        stop_error: Exception | None = (
            exc if isinstance(exc, RuntimeStopError) else None
        )
        server_stopped = api is None
        if isinstance(exc, RuntimeStopError):
            server_stopped = exc.process_joined
        worker_stopped = not (
            isinstance(exc, WorkerShutdownError) and not exc.process_joined
        )
        if api is not None:
            try:
                self._workspace.runtime.cancel_owned_work(api)
            except Exception:
                pass
            try:
                self._workspace.runtime.stop(api)
            except RuntimeStopError as runtime_stop_error:
                server_stopped = runtime_stop_error.process_joined
                stop_error = runtime_stop_error
            except Exception as runtime_stop_error:
                stop_error = runtime_stop_error
            else:
                server_stopped = True
        if staging_lease is not None:
            if not server_stopped or not worker_stopped:
                staging_lease.unlock_on_release = False
            staging_lease.release()
        report_arguments = {
            "report_id": report_id,
            "phase": "cutover",
            "duration_seconds": time.monotonic() - started,
            "api_checks": (),
            "server_log": f"reports/{report_id}/server.log",
            "evidence": None,
            "phases": (),
            "config": config,
            "backup_id": backup_id,
            "rehearsal_report_id": rehearsal_report_id,
            "error": type(exc).__name__,
            "error_message": self._reports._redact_diagnostic(
                str(exc), inherited_environment
            ),
            "target_root_fingerprint": expected_root_fingerprint,
            "code_version": code_version,
        }
        if not server_stopped or not worker_stopped:
            assert self._workspace._active_lease is not None
            self._workspace._active_lease.unlock_on_release = False
            report = self._reports._operation_report(
                **report_arguments,
                status="stop_failed_restore_deferred",
                stop_error=type(stop_error).__name__ if stop_error else None,
                server_process_joined=server_stopped,
                worker_process_joined=worker_stopped,
            )
            self._reports._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Owned process stop was not proven; restore is deferred"
            ) from (stop_error or exc)
        if not activated and not activation_attempted:
            report = self._reports._operation_report(
                **report_arguments,
                status="failed_active_untouched",
            )
            self._reports._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Staged cutover failed before activation; active market is unchanged"
            ) from exc
        try:
            self._backups.restore(backup_id)
        except Exception as restore_exc:
            report = self._reports._operation_report(
                **report_arguments,
                status="restore_failed",
                restore_error=type(restore_exc).__name__,
            )
            self._reports._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Active cutover failed and explicit restore also failed"
            ) from restore_exc
        report = self._reports._operation_report(
            **report_arguments,
            status="failed_restored",
        )
        self._reports._try_write_report(report_id, report)
        raise _managed_root.CutoverSafetyError(
            f"Active cutover failed; restored backup {backup_id}"
        ) from exc
