"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    DetachedArtifactEvidence,
    OperationResult,
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionEligibility,
    RetainedPromotionPreparation,
    SmokeConfig,
)
from .errors import RuntimeStopError, WorkerShutdownError
from .journal import PromotionJournal
from .promotion_contracts import RetainedPromotionContext


class PromotionMixin:
    def _recover_retained_promotion(
        self,
        report_id: str,
        *,
        retained_report_id: str,
        backup_id: str,
    ) -> OperationResult | None:
        """Authorize and recover only one exact same-ID promotion attempt."""

        report_id = self._validate_id(report_id, label="report")
        retained_report_id = self._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        with self._existing_exclusive_operation():
            retained_report, retained_sha256, _retained_stat = (
                self._promotion_report_snapshot(retained_report_id)
            )
            source_id_value = retained_report.get("sourceRehearsalReportId")
            if not isinstance(source_id_value, str):
                raise _managed_root.CutoverSafetyError(
                    "Retained report identity is invalid"
                )
            source_report_id = self._validate_id(
                source_id_value, label="source rehearsal report"
            )
            retained_root = self._retained_rehearsal_root(source_report_id)
            with _market_operation_lease.MarketOperationLease.acquire_existing(
                retained_root, exclusive=True
            ) as retained_lease:
                self._retained_lease = retained_lease
                try:
                    journal = PromotionJournal(self._managed(), report_id, now=self.now)
                    attempt_id = journal.recovery_attempt_id()
                    recovered = journal.recover(attempt_id)
                    if recovered.status is PromotionAppendStatus.INDETERMINATE:
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal same-attempt recovery is indeterminate"
                        )
                    records = journal.read_validated()
                    if not records:
                        raise _managed_root.CutoverSafetyError(
                            "Promotion recovery journal has no committed evidence"
                        )
                    last = records[-1]
                    base = last.identities
                    source_report, source_sha256, _source_stat = (
                        self._promotion_report_snapshot(source_report_id)
                    )
                    recorded_source = retained_report.get("sourceMarketIdentityBefore")
                    if (
                        retained_report.get("reportId") != retained_report_id
                        or retained_report.get("status") != "passed"
                        or retained_report.get("sourceRehearsalReportId")
                        != source_report_id
                        or recorded_source != base.retained_v4_payload
                        or source_report.get("reportId") != source_report_id
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion recovery retained report identity mismatch"
                        )
                    (
                        backup_manifest_sha256,
                        backup_file_set_sha256,
                        backup_payload_identity,
                    ) = self._verified_backup_evidence(
                        backup_id,
                        expected_payload=base.active_before_payload,
                    )
                    if (
                        backup_manifest_sha256 != base.backup_manifest_sha256
                        or backup_file_set_sha256 != base.backup_file_set_sha256
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion recovery backup identity mismatch"
                        )
                    if last.state is PromotionState.ROLLED_BACK:
                        self._validate_rolled_back_promotion_recovery(
                            report_id=report_id,
                            retained_root=retained_root,
                            base=base,
                        )
                        return None
                    preparation = self._recover_retained_promotion_preparation(
                        report_id=report_id,
                        retained_report_id=retained_report_id,
                        retained_report_sha256=retained_sha256,
                        source_report_id=source_report_id,
                        source_report_sha256=source_sha256,
                        retained_root=retained_root,
                        backup_id=backup_id,
                        backup_manifest_sha256=backup_manifest_sha256,
                        backup_file_set_sha256=backup_file_set_sha256,
                        backup_payload_identity=backup_payload_identity,
                        records=records,
                        last=last,
                        base=base,
                    )
                    if last.state is PromotionState.COMMITTED:
                        return self._validate_committed_promotion_recovery(
                            report_id=report_id,
                            retained_report_id=retained_report_id,
                            backup_id=backup_id,
                            base=base,
                            preparation=preparation,
                        )
                    self._rollback_retained_promotion(
                        RetainedPromotionContext(preparation, journal),
                        processes_joined=True,
                    )
                    return None
                finally:
                    self._retained_lease = None

    def _recover_retained_promotion_preparation(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        retained_report_sha256: str,
        source_report_id: str,
        source_report_sha256: str,
        retained_root: Path,
        backup_id: str,
        backup_manifest_sha256: str,
        backup_file_set_sha256: str,
        backup_payload_identity: dict[str, object],
        records: tuple[PromotionJournalRecord, ...],
        last: PromotionJournalRecord,
        base: PromotionIdentityEvidence,
    ) -> RetainedPromotionPreparation:
        holding_root = self.operations_root / "holding" / report_id
        detached_artifacts = tuple(
            DetachedArtifactEvidence(
                name=cast(str, artifact["name"]),
                kind=cast(str, artifact["kind"]),
                identity=cast(dict[str, object], artifact["identity"]),
                directories=cast(dict[str, dict[str, int]], artifact["directories"]),
                files=cast(dict[str, dict[str, object]], artifact["files"]),
            )
            for artifact in base.detached_artifacts
        )
        holding_directory = self._recover_holding_directory(
            report_id=report_id,
            holding_root=holding_root,
            holding_location=last.identities.holding_current,
            recorded_holding=next(
                (
                    record.identities.holding_current
                    for record in reversed(records)
                    if record.identities.holding_current is not None
                ),
                None,
            ),
            detached_artifacts=detached_artifacts,
            last_state=last.state,
        )
        eligibility = RetainedPromotionEligibility(
            retained_report_id=retained_report_id,
            retained_report_sha256=retained_report_sha256,
            source_report_id=source_report_id,
            source_report_sha256=source_report_sha256,
            retained_root=retained_root,
            source_market_identity=base.retained_v4_payload,
            active_market_identity=base.active_before_payload,
            target_root_fingerprint=self.root_fingerprint(self.data_root),
            configuration_fingerprint=self.configuration_fingerprint(self.data_root),
        )
        return RetainedPromotionPreparation(
            eligibility=eligibility,
            backup_id=backup_id,
            backup_manifest_sha256=backup_manifest_sha256,
            backup_file_set_sha256=backup_file_set_sha256,
            backup_payload_identity=backup_payload_identity,
            holding_root=holding_root,
            holding_directory_identity=holding_directory,
            detached_runtime_names=base.detached_runtime_names,
            detached_artifacts=detached_artifacts,
        )

    def _recover_holding_directory(
        self,
        *,
        report_id: str,
        holding_root: Path,
        holding_location: dict[str, object] | None,
        recorded_holding: dict[str, object] | None,
        detached_artifacts: tuple[DetachedArtifactEvidence, ...],
        last_state: PromotionState,
    ) -> dict[str, int]:
        if holding_location is None:
            try:
                holding_fd = self._managed().open_dir(
                    self._managed_relative(holding_root)
                )
            except FileNotFoundError:
                return (
                    cast(dict[str, int], recorded_holding["directory"])
                    if recorded_holding is not None
                    else {"device": 0, "inode": 0}
                )
            try:
                current_artifacts = self._held_artifacts_evidence(holding_fd)
                expected = {artifact.name: artifact for artifact in detached_artifacts}
                if any(
                    artifact.name not in expected or artifact != expected[artifact.name]
                    for artifact in current_artifacts
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion recovery held artifact identity mismatch"
                    )
                return self._directory_identity_evidence(holding_fd)
            finally:
                os.close(holding_fd)
        holding_directory = cast(dict[str, int], holding_location["directory"])
        if last_state is PromotionState.COMMITTED:
            return holding_directory
        opened: list[int] = []
        for candidate in (holding_root, self._cleanup_staging_root(report_id)):
            try:
                opened.append(
                    self._managed().open_dir(self._managed_relative(candidate))
                )
            except FileNotFoundError:
                continue
        partial_state = last_state in {
            PromotionState.RUNTIMES_DETACHED,
            PromotionState.PREPARED,
            PromotionState.EXCHANGED_BACK,
            PromotionState.ROLLBACK_DEFERRED,
        }
        if len(opened) > 1 or (not opened and not partial_state):
            for fd in opened:
                os.close(fd)
            raise _managed_root.CutoverSafetyError(
                "Promotion recovery artifact staging is missing or ambiguous"
            )
        if opened:
            self._validate_recovered_holding(
                opened[0],
                holding_directory=holding_directory,
                detached_artifacts=detached_artifacts,
                partial_state=partial_state,
            )
        return holding_directory

    def _validate_recovered_holding(
        self,
        holding_fd: int,
        *,
        holding_directory: dict[str, int],
        detached_artifacts: tuple[DetachedArtifactEvidence, ...],
        partial_state: bool,
    ) -> None:
        try:
            current_artifacts = self._held_artifacts_evidence(holding_fd)
            expected = {artifact.name: artifact for artifact in detached_artifacts}
            invalid = (
                self._directory_identity_evidence(holding_fd) != holding_directory
                or any(
                    artifact.name not in expected or artifact != expected[artifact.name]
                    for artifact in current_artifacts
                )
                or (not partial_state and current_artifacts != detached_artifacts)
            )
            if invalid:
                raise _managed_root.CutoverSafetyError(
                    "Promotion recovery held artifact identity mismatch"
                )
        finally:
            os.close(holding_fd)

    def _retained_promotion_attempt_exists(self, report_id: str) -> bool:
        """Return whether durable same-ID evidence requires recovery."""

        candidates = (
            Path("operations/market-v4-cutover/journals") / report_id,
            Path("operations/market-v4-cutover/journal-controls") / report_id,
            Path("operations/market-v4-cutover/reports") / report_id / "report.json",
        )
        with self._managed_root_scope():
            for candidate in candidates:
                try:
                    self._managed().stat(candidate)
                except FileNotFoundError:
                    continue
                return True
        return False

    def promote_retained(
        self,
        report_id: str,
        *,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str] | None = None,
    ) -> OperationResult:
        """Promote one exact retained rehearsal through the recovery-safe path."""

        report_id = self._validate_id(report_id, label="report")
        retained_report_id = self._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        if self._retained_promotion_attempt_exists(report_id):
            recovered = self._recover_retained_promotion(
                report_id,
                retained_report_id=retained_report_id,
                backup_id=backup_id,
            )
            if recovered is None:
                raise _managed_root.CutoverSafetyError(
                    "Existing promotion attempt was rolled back; use a new report ID"
                )
            return recovered

        with self._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id=retained_report_id,
            backup_id=backup_id,
            config=config,
        ) as eligibility:
            journal = PromotionJournal(self._managed(), report_id, now=self.now)
            preparation = self._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id=backup_id,
                journal=journal,
            )
            return self._promote_retained_under_leases(
                preparation,
                journal=journal,
                config=config,
                inherited_environment=inherited_environment or {},
            )

    def _promote_retained_under_leases(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        journal: PromotionJournal,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        try:
            return self._promote_retained_under_leases_unchecked(
                preparation,
                journal=journal,
                config=config,
                inherited_environment=inherited_environment,
            )
        except Exception as exc:
            try:
                current_state = journal.read_validated()[-1].state
            except (_managed_root.CutoverSafetyError, IndexError):
                current_state = None
            if current_state is PromotionState.COMMITTED:
                raise _managed_root.CutoverSafetyError(
                    "Committed promotion cleanup incomplete; same-ID recovery required"
                ) from exc
            joined = not (
                isinstance(exc, (RuntimeStopError, WorkerShutdownError))
                and not exc.process_joined
            )
            try:
                self._rollback_retained_promotion(
                    RetainedPromotionContext(preparation, journal),
                    processes_joined=joined,
                )
            except _managed_root.CutoverSafetyError as rollback_error:
                if not joined and "deferred" in str(rollback_error):
                    raise rollback_error from exc
                raise _managed_root.CutoverSafetyError(
                    "Retained promotion failed and rollback recovery failed"
                ) from rollback_error
            if isinstance(exc, _managed_root.CutoverSafetyError):
                raise exc
            raise _managed_root.CutoverSafetyError(
                "Retained promotion failed and was rolled back"
            ) from exc
