"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
import time

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    ApiAdapter,
    OperationResult,
    SmokeConfig,
)
from .errors import RuntimeStopError, WorkerShutdownError
from .filesystem import _DIR_OPEN_FLAGS


class ActivationMixin:
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
        report_id = self._validate_id(report_id, label="report")
        rehearsal_report_id = self._validate_id(
            rehearsal_report_id, label="rehearsal report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        expected_root_fingerprint = self._validate_cutover_rehearsal(
            rehearsal_report_id=rehearsal_report_id,
            config=config,
            code_version=code_version,
        )
        self.verify_backup(backup_id)
        self._preflight_under_lease()
        assert self._active_lease is not None
        return self._execute_cutover(
            report_id=report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=config,
            inherited_environment=inherited_environment,
            code_version=code_version,
            expected_root_fingerprint=expected_root_fingerprint,
        )

    def _validate_cutover_rehearsal(
        self,
        *,
        rehearsal_report_id: str,
        config: SmokeConfig,
        code_version: str,
    ) -> str:
        rehearsal = self._read_report(rehearsal_report_id)
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
        mode = rehearsal.get("rehearsalMode")
        common_valid = (
            rehearsal.get("phase") == "rehearsal"
            and rehearsal.get("status") == "passed"
            and rehearsal.get("reportId") == rehearsal_report_id
            and mode in {"full_rebuild", "retained_market_smoke"}
            and rehearsal.get("serverProcessJoined") is True
            and rehearsal.get("workerProcessJoined") is True
            and expected_root_fingerprint == self.root_fingerprint(self.data_root)
            and rehearsal.get("codeVersion") == code_version
            and rehearsal.get("smokeConfig") == expected_smoke_config
        )
        retained_valid = True
        if mode == "full_rebuild":
            retained_valid = self._full_rebuild_report_contract_valid(
                rehearsal,
                config=config,
            )
        elif mode == "retained_market_smoke":
            source_market_identity_before = rehearsal.get("sourceMarketIdentityBefore")
            retained_valid = (
                isinstance(rehearsal.get("sourceRehearsalReportId"), str)
                and bool(rehearsal["sourceRehearsalReportId"])
                and isinstance(rehearsal.get("sourceRehearsalCodeVersion"), str)
                and bool(rehearsal["sourceRehearsalCodeVersion"])
                and isinstance(rehearsal.get("sourceRetainedRootFingerprint"), str)
                and bool(rehearsal["sourceRetainedRootFingerprint"])
                and isinstance(source_market_identity_before, dict)
                and source_market_identity_before
                == rehearsal.get("sourceMarketIdentityAfter")
                and self._retained_report_contract_valid(
                    rehearsal,
                    report_id=rehearsal_report_id,
                    config=config,
                )
            )
            if retained_valid:
                try:
                    source_report_id_value = rehearsal.get("sourceRehearsalReportId")
                    if not isinstance(source_report_id_value, str):
                        raise _managed_root.CutoverSafetyError(
                            "Retained rehearsal source report ID is invalid"
                        )
                    source_report_id = self._validate_id(
                        source_report_id_value,
                        label="source rehearsal report",
                    )
                    source_report = self._read_report(source_report_id)
                    retained_root = self._retained_rehearsal_root(source_report_id)
                    with _market_operation_lease.MarketOperationLease.acquire(
                        retained_root,
                        exclusive=True,
                    ) as source_lease:
                        self._assert_retained_root_identity(
                            retained_root,
                            source_lease.root_fd,
                        )
                        source_report = self._read_report(source_report_id)
                        current_market_identity = self._market_tree_identity(
                            source_lease.root_fd
                        )
                        retained_valid = (
                            source_report.get("reportId") == source_report_id
                            and source_report.get("phase") == "rehearsal"
                            and source_report.get("status") in {"passed", "failed"}
                            and source_report.get("targetRootFingerprint")
                            == expected_root_fingerprint
                            and source_report.get("smokeConfig")
                            == rehearsal.get("smokeConfig")
                            and source_report.get("codeVersion")
                            == rehearsal.get("sourceRehearsalCodeVersion")
                            and source_report.get("serverProcessJoined") is True
                            and source_report.get("workerProcessJoined") is True
                            and self._root_fingerprint_at(source_lease.root_fd)
                            == rehearsal.get("sourceRetainedRootFingerprint")
                            and current_market_identity
                            == rehearsal.get("sourceMarketIdentityBefore")
                        )
                except (_managed_root.CutoverSafetyError, TypeError):
                    retained_valid = False
        if not common_valid or not retained_valid:
            raise _managed_root.CutoverSafetyError(
                "An exact passing rehearsal report is required"
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
    ) -> OperationResult:
        assert self._active_lease is not None
        started = time.monotonic()
        api: ApiAdapter | None = None
        activated = False
        activation_attempted = False
        staging_lease: _market_operation_lease.MarketOperationLease | None = None
        staged_market_fd: int | None = None
        active_market_fd: int | None = None
        report_dir, log_path = self._prepare_cutover_report_directory(report_id)
        try:
            staging_root, runtime_name, runtime_template = (
                self._prepare_cutover_staging(
                    report_id,
                    expected_root_fingerprint=expected_root_fingerprint,
                )
            )
            staging_lease = _market_operation_lease.MarketOperationLease.acquire(
                staging_root, exclusive=True
            )
            if staging_lease is not None:
                staging_root_identity = os.fstat(staging_lease.root_fd)
                staged_market_fd = self._open_staged_market(staging_lease)
                staged_market_identity = os.fstat(staged_market_fd)
                environment = self._isolated_environment(
                    inherited_environment,
                    lease_fd=staging_lease.fd,
                    root_fd=staging_lease.root_fd,
                    runtime_name=runtime_name,
                )
                log_fd = self._managed().open_regular(
                    self._managed_relative(log_path),
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                )
                try:
                    api = self.runtime.start(
                        root_fd=staging_lease.root_fd,
                        market_fd=staged_market_fd,
                        lease_fd=staging_lease.fd,
                        environment=environment,
                        log_path=log_path,
                        log_fd=log_fd,
                    )
                finally:
                    os.close(log_fd)
                if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                    raise _managed_root.CutoverSafetyError(
                        "Active inputs changed during staged server start"
                    )
                (
                    checks,
                    evidence,
                    phases,
                    _verified_market_identity,
                ) = self._run_rebuild(
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
                self.runtime.stop(api)
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
                api = self.runtime.start(
                    root_fd=self._active_lease.root_fd,
                    market_fd=active_market_fd,
                    lease_fd=self._active_lease.fd,
                    environment=active_environment,
                    log_path=active_log_path,
                    log_fd=active_log_fd,
                )
            finally:
                os.close(active_log_fd)
            active_smoke_started = time.monotonic()
            active_smoke = self.smoke(
                api,
                config,
                operation_id=f"{report_id}.active",
                market_directory_fd=active_market_fd,
                guard_lease_fd=self._active_lease.fd,
            )
            active_smoke_duration = time.monotonic() - active_smoke_started
            self.runtime.stop(api)
            api = None
            self._remove_market_runtime(active_market_fd, runtime_name)
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
            if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                raise _managed_root.CutoverSafetyError(
                    "Active inputs changed before report persistence"
                )
            self._assert_current_data_root_identity()
            self._require_unchanged_code_identity(code_version)
            return self._publish_cutover_success(
                report_id=report_id,
                rehearsal_report_id=rehearsal_report_id,
                backup_id=backup_id,
                config=config,
                code_version=code_version,
                expected_root_fingerprint=expected_root_fingerprint,
                started=started,
                checks=checks,
                evidence=evidence,
                phases=phases,
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

    def _prepare_cutover_report_directory(self, report_id: str) -> tuple[Path, Path]:
        report_dir = self.operations_root / "reports" / report_id
        self._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._prepare_managed_directory(report_dir, exist_ok=False)
        log_path = report_dir / "server.log"
        self._assert_managed_target_absent(log_path)
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
        assert self._active_lease is not None
        log_fd = self._managed().open_regular(
            self._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
        )
        try:
            market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=self._active_lease.root_fd,
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
        assert self._active_lease is not None
        return self._isolated_environment(
            inherited_environment,
            lease_fd=self._active_lease.fd,
            root_fd=self._active_lease.root_fd,
            runtime_name=runtime_name,
        )

    def _prepare_cutover_staging(
        self,
        report_id: str,
        *,
        expected_root_fingerprint: str,
    ) -> tuple[Path, str, Path]:
        staging_dir = self.operations_root / "staging" / report_id
        self._prepare_managed_directory(staging_dir.parent, exist_ok=True)
        self._prepare_managed_directory(staging_dir, exist_ok=False)
        staging_root = staging_dir / "root"
        runtime_name = f".cutover-runtime-{report_id}"
        runtime_template = staging_root / f"runtime-template-{report_id}"
        self._prepare_isolated_root(staging_root, runtime_name=runtime_name)
        if self.configuration_fingerprint(
            staging_root
        ) != self.configuration_fingerprint(self.data_root):
            raise _managed_root.CutoverSafetyError(
                "Cutover staging configuration snapshot mismatch"
            )
        if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
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
        self._secure_rename(
            staging_root / "market-timeseries" / runtime_name,
            runtime_template,
        )
        if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
            raise _managed_root.CutoverSafetyError(
                "Active inputs changed during staged rebuild"
            )
        self._assert_current_data_root_identity()
        self._validate_active_roots()
        self._assert_managed_directory_identity(staging_root, staging_root_identity)
        self._assert_managed_directory_identity(
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
        self._activate_staged_market(
            staging_root / "market-timeseries",
            report_id,
        )
        self._secure_rename(runtime_template, self.market_root / runtime_name)

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
    ) -> OperationResult:
        report = self._operation_report(
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
        report_path = self._write_report(
            report_id,
            report,
            expected_root_fingerprint=expected_root_fingerprint,
        )
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
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
                self.runtime.cancel_owned_work(api)
            except Exception:
                pass
            try:
                self.runtime.stop(api)
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
            "error_message": self._redact_diagnostic(str(exc), inherited_environment),
            "target_root_fingerprint": expected_root_fingerprint,
            "code_version": code_version,
        }
        if not server_stopped or not worker_stopped:
            assert self._active_lease is not None
            self._active_lease.unlock_on_release = False
            report = self._operation_report(
                **report_arguments,
                status="stop_failed_restore_deferred",
                stop_error=type(stop_error).__name__ if stop_error else None,
                server_process_joined=server_stopped,
                worker_process_joined=worker_stopped,
            )
            self._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Owned process stop was not proven; restore is deferred"
            ) from (stop_error or exc)
        if not activated and not activation_attempted:
            report = self._operation_report(
                **report_arguments,
                status="failed_active_untouched",
            )
            self._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Staged cutover failed before activation; active market is unchanged"
            ) from exc
        try:
            self.restore(backup_id)
        except Exception as restore_exc:
            report = self._operation_report(
                **report_arguments,
                status="restore_failed",
                restore_error=type(restore_exc).__name__,
            )
            self._try_write_report(report_id, report)
            raise _managed_root.CutoverSafetyError(
                "Active cutover failed and explicit restore also failed"
            ) from restore_exc
        report = self._operation_report(
            **report_arguments,
            status="failed_restored",
        )
        self._try_write_report(report_id, report)
        raise _managed_root.CutoverSafetyError(
            f"Active cutover failed; restored backup {backup_id}"
        ) from exc
