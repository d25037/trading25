"""Exact same-ID recovery for interrupted Market v5 activation."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
import json
import os
from pathlib import Path
import time
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .activation import MarketActivationService
from .activation_contract import (
    _exact_evidence,
    _mutable_evidence,
    activated_lineage_valid,
    activation_report_contract_valid,
    market_tree_identity_evidence,
)
from .activation_journal import ActivationJournalRepository
from .activation_runtime import (
    ActivationRuntimeOwnership,
    ActivationRuntimePlacement,
)
from .backup import MarketBackupService
from .contracts import (
    ActivationAttempt,
    ActivationJournalRecord,
    ActivationState,
    ApiAdapter,
    MarketTreeIdentity,
    OperationResult,
    SmokeConfig,
    SmokeResult,
)
from .errors import RuntimeStopError, WorkerShutdownError
from .evidence import MarketEvidence
from .filesystem import _DIR_OPEN_FLAGS
from .reports import CutoverReportRepository
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


class _ActivationLayout(StrEnum):
    NOT_EXCHANGED = "not_exchanged"
    EXCHANGED_NOT_QUARANTINED = "exchanged_not_quarantined"
    QUARANTINED = "quarantined"


class ActivationRecoveryService:
    """Resume only one exact journaled activation under the active writer lease."""

    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
        reports: CutoverReportRepository,
        runtime_smoke: RuntimeSmokeService,
        backups: MarketBackupService,
        activation: MarketActivationService,
        journal: ActivationJournalRepository,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence
        self._reports = reports
        self._runtime_smoke = runtime_smoke
        self._backups = backups
        self._activation = activation
        self._journal = journal
        self._runtime = ActivationRuntimeOwnership(workspace, backups)

    def recover_if_present(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
    ) -> OperationResult | None:
        report_id = self._workspace._validate_id(report_id, label="report")
        rehearsal_report_id = self._workspace._validate_id(
            rehearsal_report_id, label="rehearsal report"
        )
        backup_id = self._workspace._validate_id(backup_id, label="backup")
        records = self._load_existing_records(report_id)
        if records is None:
            return None
        attempt = records[0].attempt
        self._validate_attempt_context(
            attempt,
            report_id=report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=config,
            code_version=code_version,
        )
        expected_root_fingerprint = self._activation._validate_cutover_rehearsal(
            rehearsal_report_id=rehearsal_report_id,
            config=config,
            code_version=code_version,
        )
        self._validate_attempt_relationships(attempt)
        staging_root = (
            self._workspace.operations_root
            / "staging"
            / attempt.report_id
            / "root"
        )
        if self._evidence.configuration_fingerprint(
            staging_root
        ) != self._evidence.configuration_fingerprint(self._workspace.data_root):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery staging configuration does not exactly match"
            )
        self._backups._verify_backup_managed(backup_id)
        self._backups._assert_market_tree_identity(
            self._workspace.backups_root / backup_id / "payload",
            attempt.backup,
        )
        quarantine = self._quarantine_identity(attempt)
        latest_state = records[-1].state
        runtime_placement = self._runtime.placement(attempt)
        layout = self._resolve_layout(
            attempt,
            quarantine,
            exact_runtime_exists=(
                runtime_placement is ActivationRuntimePlacement.ACTIVE
            ),
        )
        if (
            runtime_placement is ActivationRuntimePlacement.ACTIVE
            and layout is not _ActivationLayout.QUARANTINED
        ):
            raise _managed_root.CutoverSafetyError(
                "Exact activation runtime is incompatible with filesystem layout"
            )
        frozen_evidence = self._frozen_evidence(attempt)

        if latest_state is ActivationState.REPORTED:
            self._require_layout(layout, _ActivationLayout.QUARANTINED)
            if runtime_placement is ActivationRuntimePlacement.ACTIVE:
                raise _managed_root.CutoverSafetyError(
                    "Reported activation unexpectedly retains a runtime"
                )
            return self._adopt_existing_report(
                attempt,
                quarantine,
                evidence=frozen_evidence,
                expected_root_fingerprint=expected_root_fingerprint,
            )

        if self._report_exists(report_id) and latest_state is not ActivationState.ACTIVATED:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery report exists before an activated state"
            )

        if latest_state is ActivationState.PREPARED:
            self._require_layout(layout, _ActivationLayout.NOT_EXCHANGED)
            self._journal.append(attempt, ActivationState.EXCHANGE_STARTED)
            latest_state = ActivationState.EXCHANGE_STARTED

        if latest_state is ActivationState.EXCHANGE_STARTED:
            self._resume_exchange(attempt, quarantine, layout)
        elif latest_state is ActivationState.ACTIVATED:
            self._require_layout(layout, _ActivationLayout.QUARANTINED)
        else:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery journal state is unsupported"
            )

        smoke = self._run_recovery_smoke(
            attempt,
            quarantine,
            config=config,
            inherited_environment=inherited_environment,
            code_version=code_version,
            expected_root_fingerprint=expected_root_fingerprint,
        )
        if latest_state is not ActivationState.ACTIVATED:
            self._journal.append(attempt, ActivationState.ACTIVATED)

        if self._report_exists(report_id):
            result = self._adopt_existing_report(
                attempt,
                quarantine,
                evidence=frozen_evidence,
                expected_root_fingerprint=expected_root_fingerprint,
            )
            self._journal.append(attempt, ActivationState.REPORTED)
            return result

        return self._activation._publish_cutover_success(
            report_id=attempt.report_id,
            rehearsal_report_id=attempt.rehearsal_report_id,
            backup_id=attempt.backup_id,
            config=attempt.config,
            code_version=attempt.code_version,
            expected_root_fingerprint=expected_root_fingerprint,
            started=time.monotonic(),
            checks=smoke.api_paths,
            evidence=frozen_evidence,
            phases=(
                {
                    "name": "recovered_activated_market_smoke",
                    "status": "passed",
                    "durationSeconds": 0.0,
                },
            ),
            backup_market_tree_sha256=cast(
                str, attempt.active_before.payload["marketTreeSha256"]
            ),
            active_provider_vintage=smoke.lineage,
            attempt=attempt,
            quarantine_identity=quarantine,
        )

    def _load_existing_records(
        self, report_id: str
    ) -> tuple[ActivationJournalRecord, ...] | None:
        return self._journal.load_existing(report_id)

    @staticmethod
    def _validate_attempt_context(
        attempt: ActivationAttempt,
        *,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
    ) -> None:
        if (
            attempt.report_id != report_id
            or attempt.rehearsal_report_id != rehearsal_report_id
            or attempt.backup_id != backup_id
            or attempt.code_version != code_version
            or attempt.config != config
        ):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery attempt arguments do not exactly match"
            )

    def _validate_attempt_relationships(self, attempt: ActivationAttempt) -> None:
        staging_path = (
            self._workspace.operations_root
            / "staging"
            / attempt.report_id
            / "root"
            / "market-timeseries"
        ).relative_to(self._workspace.data_root).as_posix()
        backup_path = (
            self._workspace.backups_root / attempt.backup_id / "payload"
        ).relative_to(self._workspace.data_root).as_posix()
        active_path = self._workspace.market_root.relative_to(
            self._workspace.data_root
        ).as_posix()
        if (
            attempt.source.path != staging_path
            or attempt.staged.path != staging_path
            or attempt.active_before.path != active_path
            or attempt.expected_active.path != active_path
            or attempt.backup.path != backup_path
            or not _exact_evidence(
                market_tree_identity_evidence(attempt.source),
                market_tree_identity_evidence(attempt.staged),
            )
            or not _exact_evidence(
                dict(attempt.expected_active.directory),
                dict(attempt.staged.directory),
            )
            or not _exact_evidence(
                attempt.expected_active.payload,
                attempt.staged.payload,
            )
            or not _exact_evidence(
                attempt.active_before.payload.get("marketTreeSha256"),
                attempt.backup.payload.get("marketTreeSha256"),
            )
        ):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery journal identity relationships are invalid"
            )

    @staticmethod
    def _frozen_evidence(attempt: ActivationAttempt) -> dict[str, object]:
        evidence = attempt.source.payload.get("schemaCoverage")
        if not isinstance(evidence, Mapping):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery journal has no schema evidence"
            )
        try:
            mutable = json.loads(
                json.dumps(
                    _mutable_evidence(evidence),
                    allow_nan=False,
                    sort_keys=True,
                )
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery schema evidence is noncanonical"
            ) from exc
        if not isinstance(mutable, dict):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery schema evidence is invalid"
            )
        return cast(dict[str, object], mutable)

    def _quarantine_identity(self, attempt: ActivationAttempt) -> MarketTreeIdentity:
        quarantine_path = (
            self._workspace.operations_root
            / "quarantine"
            / f"pre-cutover-{attempt.report_id}"
        )
        return MarketTreeIdentity(
            quarantine_path.relative_to(self._workspace.data_root).as_posix(),
            attempt.active_before.directory,
            attempt.active_before.payload,
        )

    def _path_exists(self, path: Path) -> bool:
        try:
            self._workspace.managed().stat(self._workspace._managed_relative(path))
        except FileNotFoundError:
            return False
        return True

    def _matches(self, path: Path, expected: MarketTreeIdentity) -> bool:
        if not self._path_exists(path):
            return False
        try:
            self._backups._assert_market_tree_identity(path, expected)
        except _managed_root.CutoverSafetyError:
            return False
        return True

    def _resolve_layout(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
        *,
        exact_runtime_exists: bool,
    ) -> _ActivationLayout:
        staged_path = self._workspace.data_root / attempt.staged.path
        quarantine_path = self._workspace.data_root / quarantine.path
        staged_old = MarketTreeIdentity(
            attempt.staged.path,
            attempt.active_before.directory,
            attempt.active_before.payload,
        )
        active_is_old = self._matches(
            self._workspace.market_root, attempt.active_before
        )
        active_is_new = (
            self._runtime.active_matches_expected(attempt)
            if exact_runtime_exists
            else self._matches(self._workspace.market_root, attempt.expected_active)
        )
        staged_is_new = self._matches(staged_path, attempt.staged)
        staged_is_old = self._matches(staged_path, staged_old)
        quarantine_is_old = self._matches(quarantine_path, quarantine)
        staged_exists = self._path_exists(staged_path)
        quarantine_exists = self._path_exists(quarantine_path)
        legal = []
        if active_is_old and staged_is_new and not quarantine_exists:
            legal.append(_ActivationLayout.NOT_EXCHANGED)
        if active_is_new and staged_is_old and not quarantine_exists:
            legal.append(_ActivationLayout.EXCHANGED_NOT_QUARANTINED)
        if active_is_new and not staged_exists and quarantine_is_old:
            legal.append(_ActivationLayout.QUARANTINED)
        if len(legal) != 1:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery filesystem layout is ambiguous or invalid"
            )
        return legal[0]

    @staticmethod
    def _require_layout(
        actual: _ActivationLayout, expected: _ActivationLayout
    ) -> None:
        if actual is not expected:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery filesystem layout does not match journal state"
            )

    def _resume_exchange(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
        layout: _ActivationLayout,
    ) -> None:
        staging_root = (
            self._workspace.operations_root / "staging" / attempt.report_id / "root"
        )
        staged_market = self._workspace.data_root / attempt.staged.path
        quarantine_path = self._workspace.data_root / quarantine.path
        runtime_name = self._runtime.runtime_name(attempt)
        runtime_template = self._runtime.runtime_template(attempt)
        if layout is _ActivationLayout.NOT_EXCHANGED:
            self._activation._activate_rebuilt_market(
                staging_root=staging_root,
                runtime_template=runtime_template,
                runtime_name=runtime_name,
                attempt=attempt,
                quarantine=quarantine_path,
                quarantine_identity=quarantine,
            )
            return
        if layout is _ActivationLayout.EXCHANGED_NOT_QUARANTINED:
            self._workspace._assert_managed_target_absent(quarantine_path)
            self._workspace._secure_rename(staged_market, quarantine_path)
            self._backups._assert_activation_identities(attempt, quarantine)
            self._workspace._secure_rename(
                runtime_template, self._workspace.market_root / runtime_name
            )
            return
        self._require_layout(layout, _ActivationLayout.QUARANTINED)
        runtime_placement = self._runtime.placement(attempt)
        template_exists = self._path_exists(runtime_template)
        if runtime_placement is ActivationRuntimePlacement.ACTIVE:
            self._runtime.assert_activation_identities(attempt, quarantine)
        else:
            self._backups._assert_activation_identities(attempt, quarantine)
        if (
            template_exists
            and runtime_placement is not ActivationRuntimePlacement.ABSENT
        ):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery found duplicate exact runtime ownership"
            )
        if template_exists:
            self._workspace._secure_rename(
                runtime_template, self._workspace.market_root / runtime_name
            )
        elif runtime_placement is ActivationRuntimePlacement.ABSENT:
            self._runtime.prepare(attempt)

    def _run_recovery_smoke(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
        *,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
        expected_root_fingerprint: str,
    ) -> SmokeResult:
        assert self._workspace._active_lease is not None
        runtime_name = self._runtime.runtime_name(attempt)
        self._activate_runtime_for_smoke(attempt, quarantine)
        market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=self._workspace._active_lease.root_fd,
        )
        report_dir = (
            self._workspace.operations_root / "reports" / attempt.report_id
        )
        log_path = report_dir / f"recovery-active-smoke-{time.time_ns()}.log"
        log_fd = self._workspace.managed().open_regular(
            self._workspace._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        api: ApiAdapter | None = None
        try:
            environment = self._runtime_smoke.isolated_environment(
                inherited_environment,
                lease_fd=self._workspace._active_lease.fd,
                root_fd=self._workspace._active_lease.root_fd,
                runtime_name=runtime_name,
            )
            api = self._workspace.start_runtime(
                root_fd=self._workspace._active_lease.root_fd,
                market_fd=market_fd,
                lease_fd=self._workspace._active_lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            os.close(log_fd)
            log_fd = -1
            smoke = self._runtime_smoke.smoke(
                api,
                config,
                operation_id=(
                    f"{attempt.report_id}.recovery-{time.time_ns()}"
                ),
                market_directory_fd=market_fd,
                guard_lease_fd=self._workspace._active_lease.fd,
            )
            if not activated_lineage_valid(
                self._frozen_evidence(attempt), smoke.lineage
            ):
                raise _managed_root.CutoverSafetyError(
                    "Recovered Market provider vintage differs from journal evidence"
            )
            self._workspace.runtime.stop(api)
            api = None
        except BaseException as exc:
            self._handle_recovery_smoke_failure(
                exc,
                api=api,
                attempt=attempt,
            )
            raise AssertionError("recovery smoke failure handler must raise")
        finally:
            if log_fd >= 0:
                os.close(log_fd)
            os.close(market_fd)
        self._retire_runtime(attempt, quarantine)
        if self._evidence.root_fingerprint(self._workspace.data_root) != (
            expected_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active inputs changed during activation recovery"
            )
        self._workspace._assert_current_data_root_identity()
        self._workspace._require_unchanged_code_identity(code_version)
        self._backups._assert_activation_identities(attempt, quarantine)
        return smoke

    def _activate_runtime_for_smoke(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
    ) -> None:
        placement = self._runtime.placement(attempt)
        if placement is ActivationRuntimePlacement.ACTIVE:
            self._runtime.assert_activation_identities(attempt, quarantine)
        else:
            self._backups._assert_activation_identities(attempt, quarantine)
        self._runtime.activate_for_smoke(attempt, placement)
        self._runtime.assert_activation_identities(attempt, quarantine)

    def _handle_recovery_smoke_failure(
        self,
        exc: BaseException,
        *,
        api: ApiAdapter | None,
        attempt: ActivationAttempt,
    ) -> None:
        assert self._workspace._active_lease is not None
        server_joined = api is None
        worker_joined = not (
            isinstance(exc, WorkerShutdownError) and not exc.process_joined
        )
        stop_error: BaseException | None = None
        if isinstance(exc, RuntimeStopError):
            server_joined = exc.process_joined
            stop_error = exc
        elif api is not None:
            try:
                self._workspace.runtime.cancel_owned_work(api)
            except BaseException:
                pass
            try:
                self._workspace.runtime.stop(api)
            except RuntimeStopError as runtime_stop_error:
                server_joined = runtime_stop_error.process_joined
                stop_error = runtime_stop_error
            except BaseException as runtime_stop_error:
                stop_error = runtime_stop_error
            else:
                server_joined = True
        if not server_joined or not worker_joined:
            self._workspace._active_lease.unlock_on_release = False
            raise _managed_root.CutoverSafetyError(
                "Recovery-owned process stop was not proven; recovery is deferred"
            ) from (stop_error or exc)
        try:
            self._runtime.assert_exact(attempt)
            self._runtime.assert_activation_identities(
                attempt,
                self._quarantine_identity(attempt),
            )
            self._runtime.retire(attempt)
        except BaseException as retirement_error:
            self._raise_runtime_retirement_failure(retirement_error)
        raise exc

    @staticmethod
    def _raise_runtime_retirement_failure(retirement_error: BaseException) -> None:
        raise _managed_root.CutoverSafetyError(
            f"Recovery runtime retirement failure: {retirement_error}"
        ) from retirement_error

    def _retire_runtime(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
    ) -> None:
        try:
            self._runtime.assert_exact(attempt)
            self._runtime.assert_activation_identities(attempt, quarantine)
            self._runtime.retire(attempt)
        except BaseException as retirement_error:
            self._raise_runtime_retirement_failure(retirement_error)

    def _report_exists(self, report_id: str) -> bool:
        report_path = (
            self._workspace.operations_root
            / "reports"
            / report_id
            / "report.json"
        )
        return self._path_exists(report_path)

    def _adopt_existing_report(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
        *,
        evidence: dict[str, object],
        expected_root_fingerprint: str,
    ) -> OperationResult:
        report = self._reports._read_report(attempt.report_id)
        if (
            report.get("targetRootFingerprint") != expected_root_fingerprint
            or report.get("backupManifest")
            != f"backups/{attempt.backup_id}/manifest.json"
            or not activation_report_contract_valid(
                report,
                attempt=attempt,
                quarantine=quarantine,
                evidence=evidence,
            )
        ):
            raise _managed_root.CutoverSafetyError(
                "Existing activation recovery report does not exactly match the attempt"
            )
        self._backups._assert_activation_identities(attempt, quarantine)
        report_path = self._reports._write_or_adopt_exact_report(
            attempt.report_id,
            report,
            expected_root_fingerprint=expected_root_fingerprint,
            final_validator=lambda: self._backups._assert_activation_identities(
                attempt, quarantine
            ),
        )
        return OperationResult(
            attempt.report_id,
            report_path.relative_to(self._workspace.data_root).as_posix(),
        )
