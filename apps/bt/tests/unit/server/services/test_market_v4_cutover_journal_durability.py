"""Market v4 cutover journal durability tests."""

from __future__ import annotations

from contextlib import contextmanager
import errno
import inspect
import os
from pathlib import Path
import stat
import time

import pytest

import src.application.services.market_v4_cutover.journal as journal_module
import src.application.services.market_v4_cutover.journal_directories as journal_directories_module
import src.application.services.market_v4_cutover.journal_storage as cutover_module
import src.application.services.market_v4_cutover.journal_validation as journal_validation_module
from src.application.services.market_v4_cutover.contracts import (
    PromotionAppendStatus,
    PromotionState,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root
from tests.unit.server.services.market_v4_cutover_test_support import (
    _promotion_identities,
    _promotion_journal,
)


def test_promotion_journal_serializes_append_read_and_recovery_cross_process(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        with journal._storage.locked(exclusive=True):
            read_fd, write_fd = os.pipe()
            child = os.fork()
            if child == 0:
                os.close(read_fd)
                child_managed, child_journal = _promotion_journal(data_root)
                try:
                    child_journal.read_validated()
                    os.write(write_fd, b"done")
                finally:
                    child_managed.close()
                    os.close(write_fd)
                os._exit(0)
            os.close(write_fd)
            os.set_blocking(read_fd, False)
            time.sleep(0.05)
            with pytest.raises(BlockingIOError):
                os.read(read_fd, 4)
        os.set_blocking(read_fd, True)
        assert os.read(read_fd, 4) == b"done"
        os.close(read_fd)
        _pid, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == 0

        events: list[str] = []
        ancestor_managed, ancestor_journal = _promotion_journal(
            tmp_path / "ancestor-xdg", boundary_hook=events.append
        )
        result = ancestor_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        ancestor_events = {
            event for event in events if event.startswith("ancestor_parent_fsynced:")
        }
        assert {
            "ancestor_parent_fsynced:operations",
            "ancestor_parent_fsynced:operations/market-v4-cutover",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journals",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journals/promotion-001",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journal-controls",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journal-controls/promotion-001",
        } <= ancestor_events
        ancestor_managed.close()
    finally:
        managed.close()


def test_promotion_journal_fsyncs_both_control_parents_after_publication(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    managed, journal = _promotion_journal(tmp_path / "xdg", boundary_hook=events.append)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        for stage in ("intent", "resolution"):
            publication = events.index(f"{stage}_control_publication_after")
            source = events.index(f"{stage}_source_parent_fsynced")
            destination = events.index(f"{stage}_destination_parent_fsynced")
            assert publication < source < destination
    finally:
        managed.close()


@pytest.mark.parametrize(
    "failure_boundary",
    [
        "ancestor_child_fsync_before:operations",
        "ancestor_parent_fsync_before:operations",
        "ancestor_child_fsync_before:operations/market-v4-cutover",
        "ancestor_parent_fsync_before:operations/market-v4-cutover",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journals",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journals",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journals/promotion-001",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journals/promotion-001",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001/staging",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001/staging",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-locks",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-locks",
    ],
)
def test_promotion_journal_fails_closed_at_every_ancestor_fsync_boundary(
    tmp_path: Path,
    failure_boundary: str,
) -> None:
    def fail_boundary(stage: str) -> None:
        if stage == failure_boundary:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / failure_boundary.replace("/", "_")
    managed, journal = _promotion_journal(data_root, boundary_hook=fail_boundary)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.NOT_COMMITTED
    finally:
        managed.close()
    reload_managed, reloaded = _promotion_journal(data_root)
    try:
        assert reloaded.read_validated() == ()
    finally:
        reload_managed.close()


@pytest.mark.parametrize(
    "failure_boundary",
    [
        "resolution_file_fsync_before",
        "resolution_parent_fsync_before",
        "resolution_source_parent_fsync_before",
        "resolution_destination_parent_fsync_before",
    ],
)
def test_promotion_journal_recovery_resolution_failure_is_indeterminate(
    tmp_path: Path,
    failure_boundary: str,
) -> None:
    first_failure = False

    def make_indeterminate(stage: str) -> None:
        nonlocal first_failure
        if stage == "journal_parent_fsync_before" and not first_failure:
            first_failure = True
            raise OSError(errno.EIO, "injected candidate fsync")
        if stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup")

    data_root = tmp_path / failure_boundary
    managed, journal = _promotion_journal(data_root, boundary_hook=make_indeterminate)
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()

    def fail_recovery(stage: str) -> None:
        if stage == failure_boundary:
            raise OSError(errno.EIO, f"injected {stage}")

    recovery_managed, recovery = _promotion_journal(
        data_root, boundary_hook=fail_recovery
    )
    try:
        recovered = recovery.recover(attempt.attempt_id)
        assert recovered.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            recovery.read_validated()
    finally:
        recovery_managed.close()


@pytest.mark.parametrize(
    ("late_phase", "expected"),
    [
        ("committed", PromotionAppendStatus.COMMITTED),
        ("published", PromotionAppendStatus.INDETERMINATE),
    ],
)
def test_promotion_journal_late_lock_exit_error_never_downgrades_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    late_phase: str,
    expected: PromotionAppendStatus,
) -> None:
    boundary_failure = late_phase == "published"

    def boundary(stage: str) -> None:
        if boundary_failure and stage == "journal_parent_fsync_before":
            raise OSError(errno.EIO, "injected publication ambiguity")
        if boundary_failure and stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup ambiguity")

    managed, journal = _promotion_journal(tmp_path / late_phase, boundary_hook=boundary)
    original_locked = journal._storage.locked

    @contextmanager
    def late_failing_lock(*, exclusive: bool):
        with original_locked(exclusive=exclusive):
            yield
        raise OSError(errno.EIO, "injected late lock exit")

    monkeypatch.setattr(journal._storage, "locked", late_failing_lock)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is expected
    finally:
        managed.close()


def test_promotion_journal_validates_complete_control_before_staging(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    data_root = tmp_path / "xdg"
    data_root.mkdir()
    managed = managed_root.ManagedRootFd.open(data_root)
    journal = PromotionJournal(
        managed,
        "promotion-001",
        now=lambda: "",
        boundary_hook=events.append,
    )
    try:
        with pytest.raises(CutoverSafetyError, match="timestamp"):
            journal.append(
                PromotionState.VALIDATED,
                identities=_promotion_identities(PromotionState.VALIDATED),
            )
        assert "intent_file_fsync_before" not in events
        staging = data_root / (
            "operations/market-v4-cutover/journal-controls/promotion-001/staging"
        )
        assert list(staging.iterdir()) == []
    finally:
        managed.close()


@pytest.mark.parametrize(
    "resolution_failure",
    [
        "resolution_source_parent_fsync_before",
        "resolution_destination_parent_fsync_before",
    ],
)
def test_promotion_journal_resolution_cleanup_failure_never_authorizes_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resolution_failure: str,
) -> None:
    def fail_resolution(stage: str) -> None:
        if stage == resolution_failure:
            raise OSError(errno.EIO, f"injected {stage}")

    real_unlink = cutover_module.os.unlink
    real_fsync = cutover_module.os.fsync

    def fail_resolution_cleanup_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        if os.fspath(path).endswith(".resolution.json"):
            raise OSError(errno.EIO, "injected resolution cleanup unlink")
        real_unlink(path, dir_fd=dir_fd)

    def fail_resolution_cleanup_fsync(fd: int) -> None:
        names = os.listdir(fd) if stat.S_ISDIR(os.fstat(fd).st_mode) else []
        if any(name.endswith(".resolution.json") for name in names):
            raise OSError(errno.EIO, "injected resolution cleanup fsync")
        real_fsync(fd)

    data_root = tmp_path / resolution_failure
    managed, journal = _promotion_journal(data_root, boundary_hook=fail_resolution)
    monkeypatch.setattr(cutover_module.os, "unlink", fail_resolution_cleanup_unlink)
    monkeypatch.setattr(cutover_module.os, "fsync", fail_resolution_cleanup_fsync)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="authorization|fenced"):
            journal.read_validated()
    finally:
        managed.close()
        monkeypatch.undo()

    fresh_managed, fresh = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization|fenced"):
            fresh.read_validated()
    finally:
        fresh_managed.close()


def test_promotion_journal_clean_append_authorizes_only_its_instance(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        assert journal.read_validated() == (result.record,)
    finally:
        managed.close()

    fresh_managed, fresh = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            fresh.read_validated()
        recovered = fresh.recover(result.attempt_id)
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert fresh.read_validated() == (result.record,)
    finally:
        fresh_managed.close()


def test_promotion_journal_identity_drift_revokes_live_authorization(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        resolution = data_root / (
            "operations/market-v4-cutover/journal-controls/promotion-001/"
            "00000002.resolution.json"
        )
        replacement = resolution.with_suffix(".replacement")
        replacement.write_bytes(resolution.read_bytes())
        os.replace(replacement, resolution)
        with pytest.raises(CutoverSafetyError, match="authorization.*identity"):
            journal.read_validated()
    finally:
        managed.close()


def test_promotion_journal_uses_concrete_collaborators_without_mixin_inheritance() -> (
    None
):
    journal_source = inspect.getsource(journal_module)
    storage_source = inspect.getsource(cutover_module)
    validation_source = inspect.getsource(journal_validation_module)
    directories_source = inspect.getsource(journal_directories_module)

    assert journal_module.PromotionJournal.__bases__ == (object,)
    assert "JournalStorageMixin" not in storage_source
    assert "JournalValidationMixin" not in validation_source
    assert "JournalDirectoriesMixin" not in directories_source
    assert "JournalValidationMixin" not in journal_source
    assert "JournalStorageMixin" not in journal_source
    assert "__getattr__" not in journal_source
