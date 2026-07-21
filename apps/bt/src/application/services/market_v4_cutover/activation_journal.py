"""Create-only, fail-closed journal primitives for Market v5 activation."""

from __future__ import annotations

from collections.abc import Mapping
import errno
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    ActivationAttempt,
    ActivationJournalRecord,
    ActivationState,
    ImmutableJsonValue,
    MarketTreeIdentity,
    SmokeConfig,
)


_STATE_ORDER = (
    ActivationState.PREPARED,
    ActivationState.EXCHANGE_STARTED,
    ActivationState.ACTIVATED,
    ActivationState.REPORTED,
)
_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_CODE_VERSION_PATTERN = re.compile(r"[0-9a-f]{7,64}")
_DIR_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def _error(message: str) -> _managed_root.CutoverSafetyError:
    return _managed_root.CutoverSafetyError(f"Activation journal {message}")


def _mutable_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _mutable_json_value(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable_json_value(child) for child in value]
    return value


def _canonical_json(value: object) -> bytes:
    try:
        encoded = json.dumps(
            _mutable_json_value(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise _error("contains a noncanonical JSON value") from exc
    return (encoded + "\n").encode("ascii")


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError(errno.EIO, "activation journal write made no progress")
        view = view[written:]


def _read_all(fd: int) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = os.read(fd, 64 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _validate_id(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _ID_PATTERN.fullmatch(value) is None:
        raise _error(f"has an invalid {label} ID")
    return value


def _validate_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise _error("identity path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise _error("identity path is invalid")
    return value


def _validate_directory(value: object) -> Mapping[str, int]:
    if not isinstance(value, Mapping) or set(value) != {"device", "inode"}:
        raise _error("directory identity is invalid")
    if any(type(value[key]) is not int or value[key] < 0 for key in value):
        raise _error("directory identity is invalid")
    return cast(Mapping[str, int], value)


def _validate_payload(value: object) -> Mapping[str, ImmutableJsonValue]:
    if not isinstance(value, Mapping) or not value:
        raise _error("payload identity is invalid")
    canonical = _canonical_json(value)
    try:
        round_trip = json.loads(canonical)
    except json.JSONDecodeError as exc:  # pragma: no cover - canonical encoder guards it
        raise _error("payload identity is invalid") from exc
    if _canonical_json(round_trip) != canonical:
        raise _error("payload identity is not canonical JSON")
    return cast(Mapping[str, ImmutableJsonValue], value)


def _validate_identity(identity: MarketTreeIdentity) -> MarketTreeIdentity:
    if not isinstance(identity, MarketTreeIdentity):
        raise _error("tree identity has an invalid type")
    _validate_path(identity.path)
    _validate_directory(identity.directory)
    _validate_payload(identity.payload)
    return identity


def _validate_attempt(attempt: ActivationAttempt) -> ActivationAttempt:
    if not isinstance(attempt, ActivationAttempt):
        raise _error("attempt has an invalid type")
    _validate_id(attempt.report_id, label="report")
    _validate_id(attempt.rehearsal_report_id, label="rehearsal report")
    _validate_id(attempt.backup_id, label="backup")
    if _CODE_VERSION_PATTERN.fullmatch(attempt.code_version) is None:
        raise _error("has an invalid code version")
    if not isinstance(attempt.config, SmokeConfig) or any(
        not isinstance(value, str) or not value
        for value in (
            attempt.config.symbol,
            attempt.config.strategy,
            attempt.config.dataset_preset,
        )
    ):
        raise _error("config is invalid")
    for identity in (
        attempt.source,
        attempt.staged,
        attempt.active_before,
        attempt.backup,
        attempt.expected_active,
    ):
        _validate_identity(identity)
    return attempt


def _identity_mapping(identity: MarketTreeIdentity) -> dict[str, object]:
    return {
        "directory": identity.directory,
        "path": identity.path,
        "payload": identity.payload,
    }


def _attempt_mapping(attempt: ActivationAttempt) -> dict[str, object]:
    return {
        "activeBefore": _identity_mapping(attempt.active_before),
        "backup": _identity_mapping(attempt.backup),
        "backupId": attempt.backup_id,
        "codeVersion": attempt.code_version,
        "config": {
            "datasetPreset": attempt.config.dataset_preset,
            "strategy": attempt.config.strategy,
            "symbol": attempt.config.symbol,
        },
        "expectedActive": _identity_mapping(attempt.expected_active),
        "rehearsalReportId": attempt.rehearsal_report_id,
        "reportId": attempt.report_id,
        "source": _identity_mapping(attempt.source),
        "staged": _identity_mapping(attempt.staged),
    }


def _record_mapping(record: ActivationJournalRecord) -> dict[str, object]:
    return {
        "attempt": _attempt_mapping(record.attempt),
        "sequence": record.sequence,
        "state": record.state.value,
    }


def _require_exact_mapping(
    value: object,
    keys: set[str],
    *,
    label: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise _error(f"{label} fields are noncanonical")
    return cast(dict[str, object], value)


def _identity_from_mapping(value: object) -> MarketTreeIdentity:
    mapping = _require_exact_mapping(
        value,
        {"directory", "path", "payload"},
        label="identity",
    )
    identity = MarketTreeIdentity(
        path=_validate_path(mapping["path"]),
        directory=_validate_directory(mapping["directory"]),
        payload=_validate_payload(mapping["payload"]),
    )
    return _validate_identity(identity)


def _attempt_from_mapping(value: object) -> ActivationAttempt:
    mapping = _require_exact_mapping(
        value,
        {
            "activeBefore",
            "backup",
            "backupId",
            "codeVersion",
            "config",
            "expectedActive",
            "rehearsalReportId",
            "reportId",
            "source",
            "staged",
        },
        label="attempt",
    )
    config = _require_exact_mapping(
        mapping["config"],
        {"datasetPreset", "strategy", "symbol"},
        label="config",
    )
    attempt = ActivationAttempt(
        report_id=_validate_id(mapping["reportId"], label="report"),
        rehearsal_report_id=_validate_id(
            mapping["rehearsalReportId"], label="rehearsal report"
        ),
        backup_id=_validate_id(mapping["backupId"], label="backup"),
        code_version=cast(str, mapping["codeVersion"]),
        config=SmokeConfig(
            symbol=cast(str, config["symbol"]),
            strategy=cast(str, config["strategy"]),
            dataset_preset=cast(str, config["datasetPreset"]),
        ),
        source=_identity_from_mapping(mapping["source"]),
        staged=_identity_from_mapping(mapping["staged"]),
        active_before=_identity_from_mapping(mapping["activeBefore"]),
        backup=_identity_from_mapping(mapping["backup"]),
        expected_active=_identity_from_mapping(mapping["expectedActive"]),
    )
    return _validate_attempt(attempt)


def _record_from_bytes(payload: bytes) -> ActivationJournalRecord:
    try:
        value = json.loads(payload.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("record is torn or invalid JSON") from exc
    if payload != _canonical_json(value):
        raise _error("record encoding is noncanonical")
    mapping = _require_exact_mapping(
        value,
        {"attempt", "sequence", "state"},
        label="record",
    )
    sequence = mapping["sequence"]
    if type(sequence) is not int or sequence < 1:
        raise _error("record sequence is invalid")
    try:
        state = ActivationState(mapping["state"])
    except (TypeError, ValueError) as exc:
        raise _error("record state is unknown") from exc
    return ActivationJournalRecord(
        sequence=sequence,
        state=state,
        attempt=_attempt_from_mapping(mapping["attempt"]),
    )


def _verified_open_directory(
    name: str | Path,
    *,
    dir_fd: int | None = None,
) -> int:
    try:
        before = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            raise _error("directory is not a real directory")
        fd = os.open(name, _DIR_FLAGS, dir_fd=dir_fd)
    except _managed_root.CutoverSafetyError:
        raise
    except OSError as exc:
        raise _error("directory is unavailable or symlinked") from exc
    after = os.fstat(fd)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        os.close(fd)
        raise _error("directory identity changed during open")
    return fd


def _open_child_directory(parent_fd: int, name: str, *, create: bool) -> int | None:
    if create:
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError as exc:
            raise _error("directory could not be created") from exc
    else:
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
    return _verified_open_directory(name, dir_fd=parent_fd)


class ActivationJournalRepository:
    """Load and append exact create-only activation journal records."""

    def __init__(self, operations_root: Path) -> None:
        self._operations_root = Path(os.path.abspath(operations_root))
        self._indeterminate_reports: set[str] = set()

    def _assert_determinate(self, report_id: str) -> None:
        if report_id in self._indeterminate_reports:
            raise _error("durability is indeterminate after a failed fsync")

    def _open_report_directory(
        self,
        report_id: str,
        *,
        create: bool,
    ) -> int | None:
        operations_fd = _verified_open_directory(self._operations_root)
        try:
            journals_fd = _open_child_directory(
                operations_fd,
                "activation-journals",
                create=create,
            )
        finally:
            os.close(operations_fd)
        if journals_fd is None:
            return None
        try:
            return _open_child_directory(journals_fd, report_id, create=create)
        finally:
            os.close(journals_fd)

    @staticmethod
    def _read_record(directory_fd: int, name: str) -> ActivationJournalRecord:
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
            ):
                raise _error("record is symlinked, hardlinked, or non-regular")
            fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
        except _managed_root.CutoverSafetyError:
            raise
        except OSError as exc:
            raise _error("record is unavailable or symlinked") from exc
        try:
            after = os.fstat(fd)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise _error("record identity changed during open")
            return _record_from_bytes(_read_all(fd))
        finally:
            os.close(fd)

    def load(self, attempt: ActivationAttempt) -> tuple[ActivationJournalRecord, ...]:
        attempt = _validate_attempt(attempt)
        self._assert_determinate(attempt.report_id)
        directory_fd = self._open_report_directory(attempt.report_id, create=False)
        if directory_fd is None:
            return ()
        try:
            names = sorted(os.listdir(directory_fd))
            if len(names) > len(_STATE_ORDER):
                raise _error("contains duplicate or excess records")
            records: list[ActivationJournalRecord] = []
            expected_attempt = _canonical_json(_attempt_mapping(attempt))
            for sequence, state in enumerate(_STATE_ORDER[: len(names)], start=1):
                expected_name = f"{sequence:08d}-{state.value}.json"
                name = names[sequence - 1]
                if name != expected_name:
                    raise _error("record name or sequence is noncanonical")
                record = self._read_record(directory_fd, name)
                if record.sequence != sequence or record.state is not state:
                    raise _error("record name, sequence, and state do not match")
                if _canonical_json(_attempt_mapping(record.attempt)) != expected_attempt:
                    raise _error("attempt ID or identity mismatch")
                records.append(record)
            return tuple(records)
        finally:
            os.close(directory_fd)

    def append(
        self,
        attempt: ActivationAttempt,
        state: ActivationState,
    ) -> ActivationJournalRecord:
        attempt = _validate_attempt(attempt)
        if not isinstance(state, ActivationState):
            raise _error("state is unknown")
        self._assert_determinate(attempt.report_id)
        records = self.load(attempt)
        sequence = len(records) + 1
        if sequence > len(_STATE_ORDER) or state is not _STATE_ORDER[sequence - 1]:
            raise _error("state transition is duplicate, skipped, or regressed")
        record = ActivationJournalRecord(sequence, state, attempt)
        directory_fd = self._open_report_directory(attempt.report_id, create=True)
        assert directory_fd is not None
        name = f"{sequence:08d}-{state.value}.json"
        file_created = False
        fd = -1
        try:
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW
            fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
            file_created = True
            _write_all(fd, _canonical_json(_record_mapping(record)))
            os.fsync(fd)
            os.fsync(directory_fd)
            return record
        except Exception:
            if file_created:
                self._indeterminate_reports.add(attempt.report_id)
            raise
        finally:
            if fd >= 0:
                os.close(fd)
            os.close(directory_fd)
