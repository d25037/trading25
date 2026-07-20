"""Market v5 cutover journal core tests."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import stat
import threading
import time

import pytest

from src.application.services.market_v4_cutover.contracts import (
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionState,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    _promotion_payload,
    _promotion_location,
    _promotion_identities,
    _promotion_journal,
)


def test_promotion_journal_appends_create_only_fsynced_records(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    def file_fsync(fd: int) -> None:
        assert stat.S_ISREG(os.fstat(fd).st_mode)
        events.append("file")
        os.fsync(fd)

    def directory_fsync(fd: int) -> None:
        assert stat.S_ISDIR(os.fstat(fd).st_mode)
        events.append("directory")
        os.fsync(fd)

    managed, journal = _promotion_journal(
        tmp_path / "xdg",
        file_fsync=file_fsync,
        directory_fsync=directory_fsync,
    )
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        assert result.record is not None
        record = result.record
        record_path = (
            tmp_path
            / "xdg/operations/market-v5-cutover/journals/promotion-001/00000001.json"
        )
        before = record_path.read_bytes()
        assert events.count("file") >= 3
        assert events.count("directory") >= 3
        assert record.sequence == 1
        assert journal.read_validated() == (record,)
        with pytest.raises(CutoverSafetyError, match="transition|create-only"):
            journal.append(
                PromotionState.VALIDATED,
                identities=_promotion_identities(PromotionState.VALIDATED),
            )
        assert record_path.read_bytes() == before
    finally:
        managed.close()

    calls = 0

    def failing_directory_fsync(_fd: int) -> None:
        nonlocal calls
        calls += 1
        raise OSError(errno.EIO, "injected directory fsync failure")

    failed_managed, failed = _promotion_journal(
        tmp_path / "failed-xdg",
        directory_fsync=failing_directory_fsync,
    )
    try:
        failed_result = failed.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert failed_result.status is PromotionAppendStatus.NOT_COMMITTED
        assert calls == 1
    finally:
        failed_managed.close()
    reload_managed, reload_failed = _promotion_journal(tmp_path / "failed-xdg")
    try:
        assert reload_failed.read_validated() == ()
    finally:
        reload_managed.close()


def test_promotion_journal_rejects_skipped_duplicate_or_regressed_state(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        with pytest.raises(CutoverSafetyError, match="transition"):
            journal.append(
                PromotionState.PREPARED,
                identities=_promotion_identities(PromotionState.PREPARED),
            )
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        for state in (PromotionState.VALIDATED, PromotionState.COMMITTED):
            with pytest.raises(CutoverSafetyError, match="transition|create-only"):
                journal.append(state, identities=_promotion_identities(state))
    finally:
        managed.close()


def test_promotion_journal_rejects_torn_or_unknown_record(tmp_path: Path) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        journal_dir = (
            tmp_path / "xdg/operations/market-v5-cutover/journals/promotion-001"
        )
        (journal_dir / "00000002.json").write_bytes(b'{"torn":')
        with pytest.raises(CutoverSafetyError, match="journal record"):
            journal.read_validated()
        (journal_dir / "00000002.json").unlink()
        (journal_dir / "README").write_text("unknown")
        with pytest.raises(CutoverSafetyError, match="unknown journal entry"):
            journal.read_validated()
    finally:
        managed.close()


def test_promotion_journal_rejects_operation_and_identity_mismatch(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        path = (
            tmp_path
            / "xdg/operations/market-v5-cutover/journals/promotion-001/00000001.json"
        )
        payload = json.loads(path.read_text())
        payload["operation_id"] = "promotion-elsewhere"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        with pytest.raises(CutoverSafetyError, match="operation"):
            journal.read_validated()
    finally:
        managed.close()

    identity_managed, identity_journal = _promotion_journal(tmp_path / "identity-xdg")
    try:
        identity_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        with pytest.raises(CutoverSafetyError, match="identity"):
            identity_journal.append(
                PromotionState.RUNTIMES_DETACHED,
                identities=_promotion_identities(
                    PromotionState.RUNTIMES_DETACHED,
                    backup_manifest_sha256="c" * 64,
                ),
            )
    finally:
        identity_managed.close()


def test_promotion_journal_reload_reconstructs_exact_state(tmp_path: Path) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        expected = []
        attempts = []
        for state in (
            PromotionState.VALIDATED,
            PromotionState.RUNTIMES_DETACHED,
            PromotionState.PREPARED,
        ):
            result = journal.append(state, identities=_promotion_identities(state))
            assert result.status is PromotionAppendStatus.COMMITTED
            assert result.record is not None
            expected.append(result.record)
            attempts.append(result.attempt_id)
    finally:
        managed.close()

    reloaded_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            reloaded.read_validated()
        recovered = reloaded.recover(attempts[-1])
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert reloaded.read_validated() == tuple(expected)
        raw = (
            data_root
            / "operations/market-v5-cutover/journals/promotion-001/00000003.json"
        ).read_bytes()
        assert raw == (
            json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
    finally:
        reloaded_managed.close()


def test_promotion_journal_requires_exact_state_identity_schema(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        valid = _promotion_identities(PromotionState.VALIDATED)
        invalid_nested = PromotionIdentityEvidence(
            **{
                **valid.__dict__,
                "active_before_directory": {"device": 1, "inode": 2, "extra": 3},
            }
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_nested)
        invalid_location = PromotionIdentityEvidence(
            **{**valid.__dict__, "holding_current": _promotion_location(40)}
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_location)
        invalid_path_payload = _promotion_payload(1)
        parquet = invalid_path_payload["parquetSha256"]
        assert isinstance(parquet, dict)
        parquet["stock_data//part.parquet"] = parquet.pop("stock_data/part.parquet")
        invalid_path = PromotionIdentityEvidence(
            **{**valid.__dict__, "active_before_payload": invalid_path_payload}
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_path)

        journal.append(PromotionState.VALIDATED, identities=valid)
        path = (
            tmp_path
            / "xdg/operations/market-v5-cutover/journals/promotion-001/00000001.json"
        )
        raw = json.loads(path.read_text())
        raw["identities"]["unknown"] = None
        path.write_text(json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n")
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.read_validated()
        raw["identities"].pop("unknown")
        raw["schema_version"] = True
        path.write_text(json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n")
        with pytest.raises(CutoverSafetyError, match="schema version"):
            journal.read_validated()
    finally:
        managed.close()


@pytest.mark.parametrize(
    ("artifact", "field_path"),
    [
        ("record", ("schema_version",)),
        ("record", ("sequence",)),
        ("record", ("identities", "active_before_directory", "device")),
        ("record", ("identities", "active_before_payload", "marketDuckdb", "size")),
        ("control", ("schema_version",)),
        ("control", ("control_sequence",)),
        ("control", ("target_sequence",)),
    ],
)
def test_promotion_journal_rejects_bool_for_every_integer_field(
    tmp_path: Path,
    artifact: str,
    field_path: tuple[str, ...],
) -> None:
    data_root = tmp_path / f"{artifact}-{'-'.join(field_path)}"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
    finally:
        managed.close()
    if artifact == "record":
        path = data_root / (
            "operations/market-v5-cutover/journals/promotion-001/00000001.json"
        )
    else:
        path = data_root / (
            "operations/market-v5-cutover/journal-controls/promotion-001/"
            "00000001.intent.json"
        )
    payload = json.loads(path.read_text())
    target = payload
    for key in field_path[:-1]:
        target = target[key]
    target[field_path[-1]] = True
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")

    reload_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="schema|sequence|identity|target"):
            reloaded.read_validated()
    finally:
        reload_managed.close()


@pytest.mark.parametrize(
    "paused_boundary",
    ["candidate_file_fsync_before", "journal_parent_fsync_before"],
)
def test_promotion_journal_never_publishes_to_ordinary_reader_during_append(
    tmp_path: Path,
    paused_boundary: str,
) -> None:
    reached = threading.Event()
    release = threading.Event()
    append_result: list[object] = []
    read_result: list[object] = []

    def boundary(stage: str) -> None:
        if stage == paused_boundary:
            reached.set()
            assert release.wait(5)

    managed, journal = _promotion_journal(tmp_path / "xdg", boundary_hook=boundary)
    try:
        append_thread = threading.Thread(
            target=lambda: append_result.append(
                journal.append(
                    PromotionState.VALIDATED,
                    identities=_promotion_identities(PromotionState.VALIDATED),
                )
            )
        )
        append_thread.start()
        assert reached.wait(5)
        reader = threading.Thread(
            target=lambda: read_result.append(journal.read_validated())
        )
        reader.start()
        time.sleep(0.05)
        assert reader.is_alive()
        assert read_result == []
        release.set()
        append_thread.join(5)
        reader.join(5)
        assert not append_thread.is_alive()
        assert not reader.is_alive()
        result = append_result[0]
        assert result.status is PromotionAppendStatus.COMMITTED
        assert read_result == [(result.record,)]
    finally:
        release.set()
        managed.close()


def test_promotion_journal_returns_indeterminate_when_cleanup_is_unprovable(
    tmp_path: Path,
) -> None:
    def fail_before_publication(stage: str) -> None:
        if stage in {"candidate_file_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    prepublish_root = tmp_path / "prepublish-xdg"
    pre_managed, prepublish = _promotion_journal(
        prepublish_root, boundary_hook=fail_before_publication
    )
    try:
        not_committed = prepublish.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert not_committed.status is PromotionAppendStatus.NOT_COMMITTED
        with pytest.raises(CutoverSafetyError, match="authorization"):
            prepublish.read_validated()
    finally:
        pre_managed.close()
    pre_reload_managed, pre_reload = _promotion_journal(prepublish_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            pre_reload.read_validated()
    finally:
        pre_reload_managed.close()

    def fail_boundaries(stage: str) -> None:
        if stage in {"journal_parent_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root, boundary_hook=fail_boundaries)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.INDETERMINATE
        assert result.record is None
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            journal.read_validated()
    finally:
        managed.close()

    reloaded_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            reloaded.read_validated()
    finally:
        reloaded_managed.close()

    def fail_resolution(stage: str) -> None:
        if stage == "resolution_parent_fsync_before":
            raise OSError(errno.EIO, "injected resolution fsync failure")

    resolution_root = tmp_path / "resolution-xdg"
    resolution_managed, resolution_journal = _promotion_journal(
        resolution_root, boundary_hook=fail_resolution
    )
    try:
        resolution_result = resolution_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert resolution_result.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            resolution_journal.read_validated()
    finally:
        resolution_managed.close()

    resolution_reload_managed, resolution_reload = _promotion_journal(resolution_root)
    try:
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            resolution_reload.read_validated()
    finally:
        resolution_reload_managed.close()


def test_promotion_journal_recovery_adopts_only_exact_durable_candidate(
    tmp_path: Path,
) -> None:
    failed_once = False

    def fail_publication_fsync(stage: str) -> None:
        nonlocal failed_once
        if stage == "journal_parent_fsync_before" and not failed_once:
            failed_once = True
            raise OSError(errno.EIO, "injected publication fsync")
        if stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup failure")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(
        data_root, boundary_hook=fail_publication_fsync
    )
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()

    recovery_managed, recovery = _promotion_journal(data_root)
    try:
        recovered = recovery.recover(attempt.attempt_id)
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert recovered.record is not None
        assert recovery.read_validated() == (recovered.record,)
    finally:
        recovery_managed.close()


def test_promotion_journal_recovery_keeps_mismatch_fail_stopped(
    tmp_path: Path,
) -> None:
    def fail_boundaries(stage: str) -> None:
        if stage in {"journal_parent_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root, boundary_hook=fail_boundaries)
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()
    candidate = data_root / (
        "operations/market-v5-cutover/journals/promotion-001/00000001.json"
    )
    candidate.write_bytes(candidate.read_bytes() + b" ")

    recovery_managed, recovery = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="candidate.*mismatch"):
            recovery.recover(attempt.attempt_id)
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            recovery.read_validated()
    finally:
        recovery_managed.close()
