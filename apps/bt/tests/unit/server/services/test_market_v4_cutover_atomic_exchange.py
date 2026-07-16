"""Market v4 cutover atomic exchange tests."""

from __future__ import annotations

import errno
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

import src.application.services.market_v4_cutover.filesystem as cutover_module
from src.application.services.market_v4_cutover.filesystem import (
    DarwinAtomicExchange,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root
from tests.unit.server.services.market_v4_cutover_test_support import (
    _forbid_non_atomic_exchange_fallbacks,
    _forbid_atomic_exchange_syscall,
    _exchange_identity,
)


@pytest.mark.darwin_capability
@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="requires Darwin renameatx_np(RENAME_SWAP)",
)
def test_atomic_exchange_swaps_real_directories_without_changing_inodes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    left_before = _exchange_identity(left)
    right_before = _exchange_identity(right)

    with managed_root.ManagedRootFd.open(root) as managed:
        DarwinAtomicExchange().exchange(
            managed,
            Path("left-parent/market"),
            Path("right-parent/market"),
        )

    assert _exchange_identity(left) == right_before
    assert _exchange_identity(right) == left_before


def test_atomic_exchange_rejects_cross_device_before_syscall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    before = (_exchange_identity(left), _exchange_identity(right))
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    _forbid_atomic_exchange_syscall(monkeypatch)
    monkeypatch.setattr(cutover_module.sys, "platform", "darwin")
    real_stat = cutover_module.os.stat
    real_open = cutover_module.os.open
    real_fstat = cutover_module.os.fstat
    right_leaf_fds: set[int] = set()

    def track_leaf_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if path == "right" and dir_fd is not None:
            right_leaf_fds.add(descriptor)
        return descriptor

    def cross_device_stat(
        path: str | bytes | int,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result | SimpleNamespace:
        result = real_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if path == "right" and dir_fd is not None and not follow_symlinks:
            values = {
                name: getattr(result, name)
                for name in dir(result)
                if name.startswith("st_")
            }
            values["st_dev"] = result.st_dev + 1
            return SimpleNamespace(**values)
        return result

    def cross_device_fstat(fd: int) -> os.stat_result | SimpleNamespace:
        result = real_fstat(fd)
        if fd in right_leaf_fds:
            values = {
                name: getattr(result, name)
                for name in dir(result)
                if name.startswith("st_")
            }
            values["st_dev"] = result.st_dev + 1
            return SimpleNamespace(**values)
        return result

    monkeypatch.setattr(cutover_module.os, "open", track_leaf_open)
    monkeypatch.setattr(cutover_module.os, "stat", cross_device_stat)
    monkeypatch.setattr(cutover_module.os, "fstat", cross_device_fstat)
    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="same device"):
            DarwinAtomicExchange().exchange(managed, Path("left"), Path("right"))

    assert (_exchange_identity(left), _exchange_identity(right)) == before


def test_atomic_exchange_rejects_unavailable_platform_without_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    before = (_exchange_identity(left), _exchange_identity(right))
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    monkeypatch.setattr(cutover_module.sys, "platform", "linux")

    def fail_libc_load(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("atomic exchange syscall binding loaded")

    monkeypatch.setattr(cutover_module.ctypes, "CDLL", fail_libc_load)

    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="unavailable"):
            DarwinAtomicExchange().exchange(managed, Path("left"), Path("right"))

    assert (_exchange_identity(left), _exchange_identity(right)) == before


def test_atomic_exchange_rejects_symlink_leaf_and_parent_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left_parent = root / "left-parent"
    right_parent = root / "right-parent"
    left_parent.mkdir(parents=True)
    right_parent.mkdir()
    (left_parent / "real-market").mkdir()
    (left_parent / "market").symlink_to("real-market")
    (right_parent / "market").mkdir()
    symlink_target_inode = (left_parent / "real-market").stat().st_ino
    right_inode = (right_parent / "market").stat().st_ino
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    _forbid_atomic_exchange_syscall(monkeypatch)
    monkeypatch.setattr(cutover_module.sys, "platform", "darwin")

    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="real directory"):
            DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert (left_parent / "market").is_symlink()
    assert (left_parent / "market").stat().st_ino == symlink_target_inode
    assert (right_parent / "market").stat().st_ino == right_inode

    (left_parent / "market").unlink()
    (left_parent / "market").mkdir()
    (left_parent / "market/payload").write_bytes(b"left")
    (right_parent / "market/payload").write_bytes(b"right")
    before = (
        _exchange_identity(left_parent / "market"),
        _exchange_identity(right_parent / "market"),
    )

    with managed_root.ManagedRootFd.open(root) as managed:
        real_open_parent = managed.open_parent
        calls = 0

        def replace_parent_before_identity_check(
            relative: Path, *, create: bool = False
        ) -> tuple[int, str]:
            nonlocal calls
            calls += 1
            if calls == 3:
                left_parent.rename(root / "left-parent.detached")
                left_parent.mkdir()
            return real_open_parent(relative, create=create)

        monkeypatch.setattr(
            managed, "open_parent", replace_parent_before_identity_check
        )
        with pytest.raises(CutoverSafetyError, match="parent identity changed"):
            DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert _exchange_identity(root / "left-parent.detached/market") == before[0]
    assert _exchange_identity(right_parent / "market") == before[1]


@pytest.mark.darwin_capability
@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="requires Darwin renameatx_np(RENAME_SWAP)",
)
def test_atomic_exchange_fsyncs_both_parents_after_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    parent_inodes = {left.parent.stat().st_ino, right.parent.stat().st_ino}
    fsynced_parent_inodes: list[int] = []
    real_fsync = cutover_module.os.fsync

    def record_fsync(fd: int) -> None:
        inode = os.fstat(fd).st_ino
        if inode in parent_inodes:
            fsynced_parent_inodes.append(inode)
        real_fsync(fd)

    monkeypatch.setattr(cutover_module.os, "fsync", record_fsync)
    with managed_root.ManagedRootFd.open(root) as managed:
        DarwinAtomicExchange().exchange(
            managed,
            Path("left-parent/market"),
            Path("right-parent/market"),
        )

    assert fsynced_parent_inodes == [
        left.parent.stat().st_ino,
        right.parent.stat().st_ino,
    ]


def test_atomic_exchange_rejects_leaf_replacement_at_syscall_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")

    class ReplaceLeafAtSyscall:
        argtypes: object = None
        restype: object = None

        def __call__(
            self,
            left_parent: int,
            left_name: bytes,
            _right_parent: int,
            _right_name: bytes,
            _flags: int,
        ) -> int:
            name = os.fsdecode(left_name)
            os.rename(
                name,
                f"{name}.detached",
                src_dir_fd=left_parent,
                dst_dir_fd=left_parent,
            )
            os.mkdir(name, dir_fd=left_parent)
            return 0

    monkeypatch.setattr(
        cutover_module.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: SimpleNamespace(renameatx_np=ReplaceLeafAtSyscall()),
    )
    monkeypatch.setattr(cutover_module.sys, "platform", "darwin")
    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="leaf identity changed"):
            DarwinAtomicExchange().exchange(managed, Path("left"), Path("right"))

    assert (root / "left.detached/payload").read_bytes() == b"left"
    assert (right / "payload").read_bytes() == b"right"


@pytest.mark.darwin_capability
@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="requires Darwin renameatx_np(RENAME_SWAP)",
)
def test_atomic_exchange_attempts_both_parent_fsyncs_when_first_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    left_parent_inode = left.parent.stat().st_ino
    right_parent_inode = right.parent.stat().st_ino
    attempts: list[int] = []

    def fail_first_fsync(fd: int) -> None:
        inode = os.fstat(fd).st_ino
        attempts.append(inode)
        if inode == left_parent_inode:
            raise OSError(errno.EIO, "injected first parent fsync failure")

    monkeypatch.setattr(cutover_module.os, "fsync", fail_first_fsync)
    with managed_root.ManagedRootFd.open(root) as managed:
        with pytest.raises(OSError, match="first parent fsync failure"):
            DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert attempts == [left_parent_inode, right_parent_inode]
    assert (left / "payload").read_bytes() == b"right"
    assert (right / "payload").read_bytes() == b"left"
