"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import cast, Iterator

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    PromotionJournalRecord,
    PromotionState,
    _PromotionJournalAuthorization,
)
from .filesystem import (
    _FILE_NOFOLLOW,
    validate_operation_id,
    write_all,
)
from .journal_directories import JournalDirectoriesMixin
from . import filesystem


class JournalStorageMixin(JournalDirectoriesMixin):
    def _ensure_layout(self) -> None:
        for relative in (
            self._relative,
            self._control_relative,
            self._staging_relative,
            self._lock_relative.parent,
        ):
            fd = self._ensure_durable_directory(relative)
            os.close(fd)

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[None]:
        self._ensure_layout()
        parent = self._ensure_durable_directory(self._lock_relative.parent)
        name = self._lock_relative.name
        created = False
        try:
            try:
                lock_fd = os.open(
                    name,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | _FILE_NOFOLLOW,
                    0o600,
                    dir_fd=parent,
                )
                created = True
            except FileExistsError:
                lock_fd = os.open(
                    name,
                    os.O_RDWR | _FILE_NOFOLLOW,
                    dir_fd=parent,
                )
            if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                os.close(lock_fd)
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal lock must be regular"
                )
            if created:
                self._file_fsync(lock_fd)
                self._directory_fsync(parent)
            fcntl.flock(lock_fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
        finally:
            os.close(parent)

    @staticmethod
    def _read_regular(directory_fd: int, name: str, *, label: str) -> bytes:
        try:
            fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
        except OSError as exc:
            raise _managed_root.CutoverSafetyError(
                f"{label} is not a confined regular file"
            ) from exc
        chunks: list[bytes] = []
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise _managed_root.CutoverSafetyError(
                    f"{label} must be a regular file"
                )
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
        return b"".join(chunks)

    def _read_control_entries(self) -> list[tuple[str, bytes]]:
        directory_fd = self._managed_root.open_dir(self._control_relative)
        entries: list[tuple[str, bytes]] = []
        try:
            for name in sorted(os.listdir(directory_fd)):
                if name == "staging":
                    entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                    if not stat.S_ISDIR(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal staging entry must be a directory"
                        )
                    continue
                if self._CONTROL_NAME.fullmatch(name) is None:
                    raise _managed_root.CutoverSafetyError(
                        f"Promotion journal has unknown control entry: {name}"
                    )
                entries.append(
                    (
                        name,
                        self._read_regular(
                            directory_fd,
                            name,
                            label="Promotion journal control record",
                        ),
                    )
                )
        finally:
            os.close(directory_fd)
        return entries

    def _directory_snapshot(
        self,
        relative: Path,
        *,
        skip_staging: bool,
    ) -> tuple[tuple[int, int], tuple[tuple[str, int, int, int, str], ...]]:
        directory_fd = self._managed_root.open_dir(relative)
        files: list[tuple[str, int, int, int, str]] = []
        try:
            directory_stat = os.fstat(directory_fd)
            for name in sorted(os.listdir(directory_fd)):
                if skip_staging and name == "staging":
                    continue
                fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
                digest = hashlib.sha256()
                try:
                    before = os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal authorization identity is invalid"
                        )
                    while chunk := os.read(fd, 1024 * 1024):
                        digest.update(chunk)
                    after = os.fstat(fd)
                    current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                    identity = (before.st_dev, before.st_ino, before.st_size)
                    if identity != (
                        after.st_dev,
                        after.st_ino,
                        after.st_size,
                    ) or identity != (
                        current.st_dev,
                        current.st_ino,
                        current.st_size,
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal authorization identity changed"
                        )
                    files.append((name, *identity, digest.hexdigest()))
                finally:
                    os.close(fd)
        finally:
            os.close(directory_fd)
        return (directory_stat.st_dev, directory_stat.st_ino), tuple(files)

    def _resolution_sha256(self, attempt_id: str) -> str:
        for _name, raw in self._read_control_entries():
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if (
                isinstance(value, dict)
                and value.get("kind") == "resolution"
                and value.get("attempt_id") == attempt_id
            ):
                return hashlib.sha256(raw).hexdigest()
        raise _managed_root.CutoverSafetyError(
            "Promotion journal resolution is unavailable"
        )

    def _grant_authorization(
        self,
        *,
        attempt_id: str,
        sequence: int,
        candidate_sha256: str,
    ) -> None:
        record_directory, record_files = self._directory_snapshot(
            self._relative, skip_staging=False
        )
        control_directory, control_files = self._directory_snapshot(
            self._control_relative, skip_staging=True
        )
        self._authorization = _PromotionJournalAuthorization(
            secret=self._authorization_secret,
            operation_id=self.operation_id,
            attempt_id=attempt_id,
            sequence=sequence,
            candidate_sha256=candidate_sha256,
            resolution_sha256=self._resolution_sha256(attempt_id),
            record_directory=record_directory,
            control_directory=control_directory,
            record_files=record_files,
            control_files=control_files,
        )
        self._recovery_fence_attempt = None

    def _require_authorization(
        self,
        *,
        intents: dict[str, dict[str, object]],
        resolutions: dict[str, dict[str, object]],
    ) -> None:
        if self._recovery_fence_attempt is not None:
            raise _managed_root.CutoverSafetyError(
                "Promotion journal is fenced pending same-attempt recovery"
            )
        if not intents and not resolutions:
            return
        authorization = self._authorization
        if (
            authorization is None
            or authorization.secret is not self._authorization_secret
            or authorization.operation_id != self.operation_id
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion journal live recovery authorization is required"
            )
        if not resolutions or next(reversed(resolutions)) != authorization.attempt_id:
            self._authorization = None
            raise _managed_root.CutoverSafetyError(
                "Promotion journal authorization attempt is not current"
            )
        resolution = resolutions.get(authorization.attempt_id)
        intent = intents.get(authorization.attempt_id)
        if (
            resolution is None
            or intent is None
            or intent["target_sequence"] != authorization.sequence
            or intent["payload_sha256"] != authorization.candidate_sha256
            or self._resolution_sha256(authorization.attempt_id)
            != authorization.resolution_sha256
        ):
            self._authorization = None
            raise _managed_root.CutoverSafetyError(
                "Promotion journal authorization identity mismatch"
            )
        record_directory, record_files = self._directory_snapshot(
            self._relative, skip_staging=False
        )
        control_directory, control_files = self._directory_snapshot(
            self._control_relative, skip_staging=True
        )
        if (
            record_directory != authorization.record_directory
            or control_directory != authorization.control_directory
            or record_files != authorization.record_files
            or control_files != authorization.control_files
        ):
            self._authorization = None
            raise _managed_root.CutoverSafetyError(
                "Promotion journal authorization identity drifted"
            )

    def _control_state(
        self,
    ) -> tuple[
        list[dict[str, object]],
        dict[str, dict[str, object]],
        dict[str, dict[str, object]],
    ]:
        controls: list[dict[str, object]] = []
        intents: dict[str, dict[str, object]] = {}
        resolutions: dict[str, dict[str, object]] = {}
        target_names: set[str] = set()
        previous_raw: bytes | None = None
        expected_kind = "intent"
        for sequence, (name, raw) in enumerate(self._read_control_entries(), start=1):
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control record is invalid"
                ) from exc
            if not isinstance(value, dict):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control schema is invalid"
                )
            kind = value.get("kind")
            expected_keys = (
                self._INTENT_KEYS if kind == "intent" else self._RESOLUTION_KEYS
            )
            if kind not in {"intent", "resolution"} or set(value) != expected_keys:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control schema is invalid"
                )
            expected_name = f"{sequence:08d}.{kind}.json"
            if name != expected_name or kind != expected_kind:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control sequence is invalid"
                )
            if raw != self._canonical_json(value):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control is not canonical JSON"
                )
            if type(value["schema_version"]) is not int or value["schema_version"] != 1:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control schema version is unknown"
                )
            if (
                type(value["control_sequence"]) is not int
                or value["control_sequence"] != sequence
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control sequence is invalid"
                )
            if value["operation_id"] != self.operation_id:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control operation mismatch"
                )
            attempt_id = value["attempt_id"]
            if not isinstance(attempt_id, str):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control attempt is invalid"
                )
            validate_operation_id(attempt_id, label="attempt")
            expected_previous = (
                hashlib.sha256(previous_raw).hexdigest()
                if previous_raw is not None
                else None
            )
            if value["previous_control_sha256"] != expected_previous:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control SHA chain mismatch"
                )
            if (
                type(value["target_sequence"]) is not int
                or value["target_sequence"] <= 0
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control target is invalid"
                )
            if value["target_name"] != f"{value['target_sequence']:08d}.json":
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control target is invalid"
                )
            if not self._sha256_valid(value["payload_sha256"]):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control payload SHA is invalid"
                )
            if kind == "intent":
                if attempt_id in intents:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal attempt is duplicated"
                    )
                target_name = cast(str, value["target_name"])
                if target_name in target_names:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal numbered target path was reused"
                    )
                target_names.add(target_name)
                try:
                    state = PromotionState(value["state"])
                except (TypeError, ValueError) as exc:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal control state is invalid"
                    ) from exc
                identities = value["identities"]
                if not self._identity_mapping_valid(identities, state):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal identity schema is invalid"
                    )
                previous_record = value["previous_record_sha256"]
                if previous_record is not None and not self._sha256_valid(
                    previous_record
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal previous-record SHA is invalid"
                    )
                intents[attempt_id] = value
                expected_kind = "resolution"
            else:
                intent = intents.get(attempt_id)
                if intent is None or any(
                    value[key] != intent[key]
                    for key in ("target_sequence", "target_name", "payload_sha256")
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal resolution mismatch"
                    )
                if value["outcome"] not in {"accepted", "rejected"}:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal resolution outcome is invalid"
                    )
                resolutions[attempt_id] = value
                expected_kind = "intent"
            if not isinstance(value["created_at"], str) or not value["created_at"]:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control timestamp is invalid"
                )
            controls.append(value)
            previous_raw = raw
        return controls, intents, resolutions

    def _append_control(self, value: dict[str, object], *, stage: str) -> None:
        entries = self._read_control_entries()
        sequence = len(entries) + 1
        value = {
            **value,
            "schema_version": self._SCHEMA_VERSION,
            "control_sequence": sequence,
            "created_at": self._now(),
            "previous_control_sha256": (
                hashlib.sha256(entries[-1][1]).hexdigest() if entries else None
            ),
        }
        kind = value.get("kind")
        expected_keys = self._INTENT_KEYS if kind == "intent" else self._RESOLUTION_KEYS
        if kind not in {"intent", "resolution"} or set(value) != expected_keys:
            raise _managed_root.CutoverSafetyError(
                "Promotion journal control schema is invalid"
            )
        if (
            type(value["schema_version"]) is not int
            or value["schema_version"] != self._SCHEMA_VERSION
            or type(value["control_sequence"]) is not int
            or value["control_sequence"] != sequence
            or not isinstance(value["created_at"], str)
            or not value["created_at"]
            or value["operation_id"] != self.operation_id
            or not isinstance(value["attempt_id"], str)
            or type(value["target_sequence"]) is not int
            or value["target_sequence"] <= 0
            or value["target_name"] != f"{value['target_sequence']:08d}.json"
            or not self._sha256_valid(value["payload_sha256"])
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion journal control schema is invalid"
            )
        validate_operation_id(cast(str, value["attempt_id"]), label="attempt")
        previous_control = value["previous_control_sha256"]
        if previous_control is not None and not self._sha256_valid(previous_control):
            raise _managed_root.CutoverSafetyError(
                "Promotion journal control schema is invalid"
            )
        if kind == "intent":
            try:
                state = PromotionState(value["state"])
            except (TypeError, ValueError) as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control schema is invalid"
                ) from exc
            if not self._identity_mapping_valid(value["identities"], state) or (
                value["previous_record_sha256"] is not None
                and not self._sha256_valid(value["previous_record_sha256"])
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal control schema is invalid"
                )
        elif value["outcome"] not in {"accepted", "rejected"}:
            raise _managed_root.CutoverSafetyError(
                "Promotion journal control schema is invalid"
            )
        payload = self._canonical_json(value)
        directory_fd = self._managed_root.open_dir(self._control_relative)
        staging_fd = self._managed_root.open_dir(self._staging_relative)
        name = f"{sequence:08d}.{value['kind']}.json"
        staged_name = f"{value['attempt_id']}.{sequence:08d}.{value['kind']}.control"
        published = False
        try:
            fd = os.open(
                staged_name,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW,
                0o600,
                dir_fd=staging_fd,
            )
            try:
                write_all(fd, payload)
                self._boundary_hook(f"{stage}_file_fsync_before")
                self._file_fsync(fd)
                self._boundary_hook(f"{stage}_file_fsynced")
            finally:
                os.close(fd)
            self._boundary_hook(f"{stage}_parent_fsync_before")
            self._directory_fsync(staging_fd)
            self._boundary_hook(f"{stage}_parent_fsynced")
            self._boundary_hook(f"{stage}_control_publication_before")
            filesystem._rename_exclusive_at(staging_fd, staged_name, directory_fd, name)
            published = True
            self._boundary_hook(f"{stage}_control_publication_after")
            self._boundary_hook(f"{stage}_source_parent_fsync_before")
            self._directory_fsync(staging_fd)
            self._boundary_hook(f"{stage}_source_parent_fsynced")
            self._boundary_hook(f"{stage}_destination_parent_fsync_before")
            self._directory_fsync(directory_fd)
            self._boundary_hook(f"{stage}_destination_parent_fsynced")
        except Exception:
            if published:
                try:
                    os.unlink(name, dir_fd=directory_fd)
                    os.fsync(directory_fd)
                    os.fsync(staging_fd)
                except OSError:
                    pass
            raise
        finally:
            os.close(staging_fd)
            os.close(directory_fd)

    def _open_journal_dir(self, *, create: bool) -> int | None:
        try:
            return self._managed_root.open_dir(self._relative, create=create)
        except FileNotFoundError:
            if not create:
                return None
            raise

    def _read_entries(self) -> list[tuple[str, bytes]]:
        directory_fd = self._open_journal_dir(create=False)
        if directory_fd is None:
            return []
        entries: list[tuple[str, bytes]] = []
        try:
            names = sorted(os.listdir(directory_fd))
            for name in names:
                if self._RECORD_NAME.fullmatch(name) is None:
                    raise _managed_root.CutoverSafetyError(
                        f"Promotion journal has unknown journal entry: {name}"
                    )
                try:
                    record_fd = os.open(
                        name,
                        os.O_RDONLY | _FILE_NOFOLLOW,
                        dir_fd=directory_fd,
                    )
                except OSError as exc:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal record is not a confined regular file"
                    ) from exc
                chunks: list[bytes] = []
                try:
                    if not stat.S_ISREG(os.fstat(record_fd).st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion journal record must be a regular file"
                        )
                    while chunk := os.read(record_fd, 1024 * 1024):
                        chunks.append(chunk)
                finally:
                    os.close(record_fd)
                entries.append((name, b"".join(chunks)))
        finally:
            os.close(directory_fd)
        return entries

    def _read_records_validated(self) -> tuple[PromotionJournalRecord, ...]:
        records: list[PromotionJournalRecord] = []
        previous_bytes: bytes | None = None
        immutable_identity: tuple[object, ...] | None = None
        for expected_sequence, (name, raw) in enumerate(self._read_entries(), start=1):
            expected_name = f"{expected_sequence:08d}.json"
            if name != expected_name:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal sequence is not contiguous"
                )
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record is torn or invalid"
                ) from exc
            if not isinstance(value, dict) or set(value) != self._RECORD_KEYS:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record schema is invalid"
                )
            if raw != self._canonical_json(value):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record is not canonical JSON"
                )
            if (
                type(value["schema_version"]) is not int
                or value["schema_version"] != self._SCHEMA_VERSION
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal schema version is unknown"
                )
            if value["operation_id"] != self.operation_id:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal operation ID mismatch"
                )
            if (
                type(value["sequence"]) is not int
                or value["sequence"] != expected_sequence
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record sequence mismatch"
                )
            if not isinstance(value["created_at"], str) or not value["created_at"]:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record timestamp is invalid"
                )
            try:
                state = PromotionState(value["state"])
            except (TypeError, ValueError) as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal record state is unknown"
                ) from exc
            self._validate_transition(records[-1].state if records else None, state)
            expected_previous_sha = (
                None
                if previous_bytes is None
                else hashlib.sha256(previous_bytes).hexdigest()
            )
            if value["previous_record_sha256"] != expected_previous_sha:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal previous-record SHA mismatch"
                )
            identities_value = value["identities"]
            if not isinstance(identities_value, dict):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal identity schema is invalid"
                )
            identities = self._identity_from_mapping(identities_value, state)
            current_immutable_identity = self._immutable_identity(identities)
            if immutable_identity is None:
                immutable_identity = current_immutable_identity
            elif current_immutable_identity != immutable_identity:
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal immutable identity mismatch"
                )
            records.append(
                PromotionJournalRecord(
                    sequence=expected_sequence,
                    state=state,
                    operation_id=self.operation_id,
                    identities=identities,
                    created_at=value["created_at"],
                )
            )
            previous_bytes = raw
        return tuple(records)

    def _read_validated_locked(
        self,
        *,
        require_authorization: bool = True,
    ) -> tuple[PromotionJournalRecord, ...]:
        _controls, intents, resolutions = self._control_state()
        unresolved = set(intents) - set(resolutions)
        if unresolved:
            raise _managed_root.CutoverSafetyError(
                "Promotion journal has unresolved intent; dedicated recovery is required"
            )
        records = self._read_records_validated()
        accepted = [
            (intent, resolutions[attempt_id])
            for attempt_id, intent in intents.items()
            if resolutions[attempt_id]["outcome"] == "accepted"
        ]
        if len(records) != len(accepted):
            raise _managed_root.CutoverSafetyError(
                "Promotion journal accepted resolution mismatch"
            )
        raw_entries = self._read_entries()
        for index, ((intent, _resolution), (name, raw)) in enumerate(
            zip(accepted, raw_entries, strict=True), start=1
        ):
            if (
                intent["target_sequence"] != index
                or intent["target_name"] != name
                or intent["payload_sha256"] != hashlib.sha256(raw).hexdigest()
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion journal accepted candidate mismatch"
                )
        if require_authorization:
            self._require_authorization(intents=intents, resolutions=resolutions)
        return records

    def read_validated(self) -> tuple[PromotionJournalRecord, ...]:
        with self._locked(exclusive=False):
            return self._read_validated_locked()
