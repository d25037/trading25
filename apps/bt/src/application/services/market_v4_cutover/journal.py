"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Callable, cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    PromotionAppendResult,
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    _PromotionJournalAuthorization,
)
from .filesystem import (
    _FILE_NOFOLLOW,
    validate_operation_id,
    write_all,
)
from .journal_storage import JournalStorageMixin
from .journal_validation import JournalValidationMixin
from . import filesystem


class PromotionJournal(JournalValidationMixin, JournalStorageMixin):
    """Descriptor-confined, append-only promotion state journal."""

    _SCHEMA_VERSION = 1
    _RECORD_NAME = re.compile(r"[0-9]{8}\.json")
    _RECORD_KEYS = {
        "schema_version",
        "operation_id",
        "sequence",
        "state",
        "created_at",
        "identities",
        "previous_record_sha256",
    }
    _CONTROL_NAME = re.compile(r"[0-9]{8}\.(?:intent|resolution)\.json")
    _CONTROL_COMMON_KEYS = {
        "schema_version",
        "control_sequence",
        "kind",
        "operation_id",
        "attempt_id",
        "created_at",
        "previous_control_sha256",
    }
    _INTENT_KEYS = _CONTROL_COMMON_KEYS | {
        "target_sequence",
        "target_name",
        "payload_sha256",
        "previous_record_sha256",
        "state",
        "identities",
    }
    _RESOLUTION_KEYS = _CONTROL_COMMON_KEYS | {
        "target_sequence",
        "target_name",
        "payload_sha256",
        "outcome",
    }
    _IDENTITY_KEYS = {
        "active_before_directory",
        "active_before_payload",
        "retained_v4_directory",
        "retained_v4_payload",
        "backup_manifest_sha256",
        "backup_file_set_sha256",
        "active_current",
        "retained_current",
        "quarantine_current",
        "holding_current",
        "detached_runtime_names",
        "detached_artifacts",
        "rollback_mode",
        "promotion_report_sha256",
    }
    _TRANSITIONS: dict[PromotionState | None, frozenset[PromotionState]] = {
        None: frozenset({PromotionState.VALIDATED}),
        PromotionState.VALIDATED: frozenset(
            {
                PromotionState.RUNTIMES_DETACHED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.RUNTIMES_DETACHED: frozenset(
            {
                PromotionState.PREPARED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.PREPARED: frozenset(
            {
                PromotionState.EXCHANGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
                PromotionState.ROLLED_BACK,
            }
        ),
        PromotionState.EXCHANGED: frozenset(
            {
                PromotionState.QUARANTINED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.QUARANTINED: frozenset(
            {
                PromotionState.ACTIVE_SMOKE_PASSED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.ACTIVE_SMOKE_PASSED: frozenset(
            {
                PromotionState.CLEANUP_STAGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.CLEANUP_STAGED: frozenset(
            {
                PromotionState.REPORT_PERSISTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.REPORT_PERSISTED: frozenset(
            {
                PromotionState.COMMITTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.COMMITTED: frozenset(),
        PromotionState.EXCHANGED_BACK: frozenset({PromotionState.ROLLED_BACK}),
        PromotionState.ROLLED_BACK: frozenset(),
        PromotionState.ROLLBACK_DEFERRED: frozenset({PromotionState.EXCHANGED_BACK}),
    }
    _LOCATION_REQUIREMENTS: dict[
        PromotionState, tuple[bool, bool | None, bool | None, bool | None]
    ] = {
        PromotionState.VALIDATED: (True, True, False, False),
        PromotionState.RUNTIMES_DETACHED: (True, True, False, True),
        PromotionState.PREPARED: (True, True, False, True),
        PromotionState.EXCHANGED: (True, True, False, True),
        PromotionState.QUARANTINED: (True, False, True, True),
        PromotionState.ACTIVE_SMOKE_PASSED: (True, False, True, True),
        PromotionState.CLEANUP_STAGED: (True, False, True, True),
        PromotionState.REPORT_PERSISTED: (True, False, True, True),
        PromotionState.COMMITTED: (True, False, True, True),
        PromotionState.EXCHANGED_BACK: (True, True, None, None),
        PromotionState.ROLLED_BACK: (True, True, None, False),
        # A deferred rollback may still have v3 at either rollback location.
        PromotionState.ROLLBACK_DEFERRED: (True, None, None, None),
    }

    def __init__(
        self,
        managed_root: _managed_root.ManagedRootFd,
        operation_id: str,
        *,
        now: Callable[[], str],
        file_fsync: Callable[[int], None] = os.fsync,
        directory_fsync: Callable[[int], None] = os.fsync,
        boundary_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._managed_root = managed_root
        self.operation_id = validate_operation_id(operation_id, label="operation")
        self._now = now
        self._file_fsync = file_fsync
        self._directory_fsync = directory_fsync
        self._boundary_hook = boundary_hook or (lambda _stage: None)
        self._authorization_secret = object()
        self._authorization: _PromotionJournalAuthorization | None = None
        self._recovery_fence_attempt: str | None = None
        self._relative = (
            Path("operations") / "market-v4-cutover" / "journals" / self.operation_id
        )
        self._control_relative = (
            Path("operations")
            / "market-v4-cutover"
            / "journal-controls"
            / self.operation_id
        )
        self._staging_relative = self._control_relative / "staging"
        self._lock_relative = (
            Path("operations")
            / "market-v4-cutover"
            / "journal-locks"
            / f"{self.operation_id}.lock"
        )

    def recovery_attempt_id(self) -> str:
        """Return the sole exact live attempt which may authorize this instance."""

        with self._locked(exclusive=False):
            _controls, intents, resolutions = self._control_state()
            if not intents:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal recovery intent is missing"
                )
            unresolved = tuple(
                attempt for attempt in intents if attempt not in resolutions
            )
            if len(unresolved) > 1:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal recovery intent is ambiguous"
                )
            attempt_id = unresolved[0] if unresolved else next(reversed(intents))
            if (
                resolutions
                and not unresolved
                and next(reversed(resolutions)) != attempt_id
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal recovery attempt is not current"
                )
            return attempt_id

    def append(
        self,
        state: PromotionState,
        *,
        identities: PromotionIdentityEvidence,
    ) -> PromotionAppendResult:
        attempt_id = f"attempt-{secrets.token_hex(12)}"
        last_result: PromotionAppendResult | None = None
        candidate_published = False

        def finish(
            status: PromotionAppendStatus,
            record: PromotionJournalRecord | None = None,
        ) -> PromotionAppendResult:
            nonlocal last_result
            if status is PromotionAppendStatus.INDETERMINATE:
                self._authorization = None
                self._recovery_fence_attempt = attempt_id
            last_result = PromotionAppendResult(status, record, attempt_id)
            return last_result

        if type(state) is not PromotionState:
            raise _managed_root.CutoverSafetyError("Promotion journal state is unknown")
        try:
            with self._locked(exclusive=True):
                existing = self._read_validated_locked()
                previous_state = existing[-1].state if existing else None
                self._validate_transition(previous_state, state)
                identity_mapping = self._identity_to_mapping(identities)
                if not self._identity_mapping_valid(identity_mapping, state):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal identity schema is invalid"
                    )
                if existing and self._immutable_identity(
                    identities
                ) != self._immutable_identity(existing[0].identities):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal immutable identity mismatch"
                    )
                created_at = self._now()
                if not isinstance(created_at, str) or not created_at:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal timestamp is invalid"
                    )
                entries = self._read_entries()
                sequence = len(existing) + 1
                name = f"{sequence:08d}.json"
                _controls, prior_intents, _resolutions = self._control_state()
                if any(
                    intent["target_name"] == name for intent in prior_intents.values()
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal numbered target path cannot be reused"
                    )
                previous_sha256 = (
                    hashlib.sha256(entries[-1][1]).hexdigest() if entries else None
                )
                payload = self._canonical_json(
                    {
                        "schema_version": self._SCHEMA_VERSION,
                        "operation_id": self.operation_id,
                        "sequence": sequence,
                        "state": state.value,
                        "created_at": created_at,
                        "identities": identity_mapping,
                        "previous_record_sha256": previous_sha256,
                    }
                )
                payload_sha256 = hashlib.sha256(payload).hexdigest()
                intent = {
                    "kind": "intent",
                    "operation_id": self.operation_id,
                    "attempt_id": attempt_id,
                    "target_sequence": sequence,
                    "target_name": name,
                    "payload_sha256": payload_sha256,
                    "previous_record_sha256": previous_sha256,
                    "state": state.value,
                    "identities": identity_mapping,
                }
                try:
                    self._append_control(intent, stage="intent")
                except Exception:
                    return finish(PromotionAppendStatus.NOT_COMMITTED)

                resolution_base = {
                    "kind": "resolution",
                    "operation_id": self.operation_id,
                    "attempt_id": attempt_id,
                    "target_sequence": sequence,
                    "target_name": name,
                    "payload_sha256": payload_sha256,
                }
                staged_name = f"{attempt_id}.candidate"
                if not self._stage_candidate(
                    staged_name=staged_name,
                    payload=payload,
                    resolution_base=resolution_base,
                ):
                    return finish(PromotionAppendStatus.NOT_COMMITTED)
                publication_status, candidate_published = self._publish_candidate(
                    staged_name=staged_name,
                    target_name=name,
                    resolution_base=resolution_base,
                )
                if publication_status is not None:
                    return finish(publication_status)

                try:
                    self._append_control(
                        {**resolution_base, "outcome": "accepted"},
                        stage="resolution",
                    )
                    self._grant_authorization(
                        attempt_id=attempt_id,
                        sequence=sequence,
                        candidate_sha256=payload_sha256,
                    )
                except Exception:
                    return finish(PromotionAppendStatus.INDETERMINATE)
                record = PromotionJournalRecord(
                    sequence=sequence,
                    state=state,
                    operation_id=self.operation_id,
                    identities=identities,
                    created_at=created_at,
                )
                return finish(PromotionAppendStatus.COMMITTED, record)
        except OSError:
            if last_result is not None:
                return last_result
            return finish(
                PromotionAppendStatus.INDETERMINATE
                if candidate_published
                else PromotionAppendStatus.NOT_COMMITTED
            )

    def _stage_candidate(
        self,
        *,
        staged_name: str,
        payload: bytes,
        resolution_base: dict[str, object],
    ) -> bool:
        staging_fd = self._managed_root.open_dir(self._staging_relative)
        staged_created = False
        try:
            staged_fd = os.open(
                staged_name,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW,
                0o600,
                dir_fd=staging_fd,
            )
            staged_created = True
            try:
                write_all(staged_fd, payload)
                self._boundary_hook("candidate_file_fsync_before")
                self._file_fsync(staged_fd)
                self._boundary_hook("candidate_file_fsynced")
            finally:
                os.close(staged_fd)
            self._boundary_hook("candidate_parent_fsync_before")
            self._directory_fsync(staging_fd)
            self._boundary_hook("candidate_parent_fsynced")
            return True
        except Exception:
            if staged_created:
                try:
                    self._boundary_hook("cleanup_unlink_before")
                    os.unlink(staged_name, dir_fd=staging_fd)
                    self._boundary_hook("cleanup_parent_fsync_before")
                    self._directory_fsync(staging_fd)
                except OSError:
                    pass
            try:
                self._append_control(
                    {**resolution_base, "outcome": "rejected"},
                    stage="resolution",
                )
            except Exception:
                pass
            return False
        finally:
            os.close(staging_fd)

    def _publish_candidate(
        self,
        *,
        staged_name: str,
        target_name: str,
        resolution_base: dict[str, object],
    ) -> tuple[PromotionAppendStatus | None, bool]:
        journal_fd = self._managed_root.open_dir(self._relative)
        staging_fd = self._managed_root.open_dir(self._staging_relative)
        published = False
        try:
            self._boundary_hook("publication_before")
            filesystem._rename_exclusive_at(
                staging_fd,
                staged_name,
                journal_fd,
                target_name,
            )
            published = True
            self._boundary_hook("publication_after")
            self._directory_fsync(staging_fd)
            self._boundary_hook("journal_parent_fsync_before")
            self._directory_fsync(journal_fd)
            self._boundary_hook("journal_parent_fsynced")
            return None, True
        except Exception:
            cleanup_proven = False
            if published:
                try:
                    self._boundary_hook("cleanup_unlink_before")
                    os.unlink(target_name, dir_fd=journal_fd)
                    self._boundary_hook("cleanup_parent_fsync_before")
                    self._directory_fsync(journal_fd)
                    cleanup_proven = True
                except OSError:
                    cleanup_proven = False
            if cleanup_proven:
                try:
                    self._append_control(
                        {**resolution_base, "outcome": "rejected"},
                        stage="resolution",
                    )
                except Exception:
                    return PromotionAppendStatus.INDETERMINATE, True
                return PromotionAppendStatus.NOT_COMMITTED, True
            return PromotionAppendStatus.INDETERMINATE, published
        finally:
            os.close(staging_fd)
            os.close(journal_fd)

    def recover(self, attempt_id: str) -> PromotionAppendResult:
        attempt_id = validate_operation_id(attempt_id, label="attempt")
        last_result: PromotionAppendResult | None = None

        def finish(
            status: PromotionAppendStatus,
            record: PromotionJournalRecord | None = None,
        ) -> PromotionAppendResult:
            nonlocal last_result
            if status is PromotionAppendStatus.INDETERMINATE:
                self._authorization = None
                self._recovery_fence_attempt = attempt_id
            last_result = PromotionAppendResult(status, record, attempt_id)
            return last_result

        try:
            with self._locked(exclusive=True):
                _controls, intents, resolutions = self._control_state()
                intent = intents.get(attempt_id)
                if intent is None:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery intent is missing"
                    )
                resolution = resolutions.get(attempt_id)
                if resolution is not None and next(reversed(resolutions)) != attempt_id:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery attempt is not current"
                    )
                unresolved = set(intents) - set(resolutions)
                if resolution is None and unresolved != {attempt_id}:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery intent is not exact"
                    )
                if resolution is not None and unresolved:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery ledger has another unresolved intent"
                    )
                journal_fd = self._managed_root.open_dir(self._relative)
                name = cast(str, intent["target_name"])
                try:
                    try:
                        candidate_stat = os.stat(
                            name, dir_fd=journal_fd, follow_symlinks=False
                        )
                    except FileNotFoundError:
                        self._directory_fsync(journal_fd)
                        if (
                            resolution is not None
                            and resolution["outcome"] != "rejected"
                        ):
                            raise _managed_root.CutoverSafetyError(
                                "Promotion journal accepted candidate is missing"
                            )
                        if resolution is None:
                            try:
                                self._append_control(
                                    {
                                        "kind": "resolution",
                                        "operation_id": self.operation_id,
                                        "attempt_id": attempt_id,
                                        "target_sequence": intent["target_sequence"],
                                        "target_name": name,
                                        "payload_sha256": intent["payload_sha256"],
                                        "outcome": "rejected",
                                    },
                                    stage="resolution",
                                )
                            except OSError:
                                return finish(PromotionAppendStatus.INDETERMINATE)
                        control_fd = self._managed_root.open_dir(self._control_relative)
                        staging_fd = self._managed_root.open_dir(self._staging_relative)
                        try:
                            self._directory_fsync(staging_fd)
                            self._directory_fsync(control_fd)
                        finally:
                            os.close(staging_fd)
                            os.close(control_fd)
                        self._grant_authorization(
                            attempt_id=attempt_id,
                            sequence=cast(int, intent["target_sequence"]),
                            candidate_sha256=cast(str, intent["payload_sha256"]),
                        )
                        return finish(PromotionAppendStatus.NOT_COMMITTED)
                    if resolution is not None and resolution["outcome"] != "accepted":
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal rejected candidate is still visible"
                        )
                    if not stat.S_ISREG(candidate_stat.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal recovery candidate is suspicious"
                        )
                    raw = self._read_regular(
                        journal_fd,
                        name,
                        label="Promotion journal recovery candidate",
                    )
                    if hashlib.sha256(raw).hexdigest() != intent["payload_sha256"]:
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal recovery candidate payload mismatch"
                        )
                    candidate_fd = os.open(
                        name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=journal_fd
                    )
                    try:
                        self._file_fsync(candidate_fd)
                    finally:
                        os.close(candidate_fd)
                    self._directory_fsync(journal_fd)
                finally:
                    os.close(journal_fd)
                records = self._read_records_validated()
                sequence = cast(int, intent["target_sequence"])
                if len(records) != sequence:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery candidate sequence mismatch"
                    )
                record = records[-1]
                if (
                    record.state.value != intent["state"]
                    or self._identity_to_mapping(record.identities)
                    != intent["identities"]
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal recovery candidate identity mismatch"
                    )
                if resolution is None:
                    try:
                        self._append_control(
                            {
                                "kind": "resolution",
                                "operation_id": self.operation_id,
                                "attempt_id": attempt_id,
                                "target_sequence": sequence,
                                "target_name": name,
                                "payload_sha256": intent["payload_sha256"],
                                "outcome": "accepted",
                            },
                            stage="resolution",
                        )
                    except OSError:
                        return finish(PromotionAppendStatus.INDETERMINATE)
                control_fd = self._managed_root.open_dir(self._control_relative)
                staging_fd = self._managed_root.open_dir(self._staging_relative)
                try:
                    self._directory_fsync(staging_fd)
                    self._directory_fsync(control_fd)
                finally:
                    os.close(staging_fd)
                    os.close(control_fd)
                self._grant_authorization(
                    attempt_id=attempt_id,
                    sequence=sequence,
                    candidate_sha256=cast(str, intent["payload_sha256"]),
                )
                return finish(PromotionAppendStatus.COMMITTED, record)
        except OSError:
            return last_result or finish(PromotionAppendStatus.INDETERMINATE)
