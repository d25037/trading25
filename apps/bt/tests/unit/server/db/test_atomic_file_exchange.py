from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.infrastructure.db.market.atomic_exchange import PlatformAtomicExchange
from src.infrastructure.db.market.managed_root import ManagedRootError, ManagedRootFd


def test_atomic_regular_file_exchange_swaps_inodes_and_contents(tmp_path: Path) -> None:
    (tmp_path / "left").write_bytes(b"left")
    (tmp_path / "right").write_bytes(b"right")
    left_inode = (tmp_path / "left").stat().st_ino
    right_inode = (tmp_path / "right").stat().st_ino

    with ManagedRootFd.open(tmp_path) as root:
        PlatformAtomicExchange().exchange_regular_files(root, Path("left"), Path("right"))

    assert (tmp_path / "left").read_bytes() == b"right"
    assert (tmp_path / "right").read_bytes() == b"left"
    assert (tmp_path / "left").stat().st_ino == right_inode
    assert (tmp_path / "right").stat().st_ino == left_inode


def test_atomic_regular_file_exchange_rejects_symlink(tmp_path: Path) -> None:
    (tmp_path / "target").write_bytes(b"target")
    (tmp_path / "right").write_bytes(b"right")
    os.symlink(tmp_path / "target", tmp_path / "left")

    with ManagedRootFd.open(tmp_path) as root:
        with pytest.raises(ManagedRootError, match="regular file"):
            PlatformAtomicExchange().exchange_regular_files(root, Path("left"), Path("right"))


def test_atomic_regular_file_exchange_fsync_failure_is_not_suppressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "left").write_bytes(b"left")
    (tmp_path / "right").write_bytes(b"right")
    real_fsync = os.fsync
    calls = 0

    def fail_parent_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls >= 1:
            raise OSError("fsync failed")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_parent_fsync)
    with ManagedRootFd.open(tmp_path) as root:
        with pytest.raises(OSError, match="fsync failed"):
            PlatformAtomicExchange().exchange_regular_files(root, Path("left"), Path("right"))


def test_linux_uses_renameat2_exchange_without_rename_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.infrastructure.db.market.atomic_exchange as exchange_module

    class FakeFunction:
        argtypes: object = None
        restype: object = None

        def __call__(self, *_args: object) -> int:
            return 0

    class FakeLibc:
        renameat2 = FakeFunction()

    monkeypatch.setattr(exchange_module.sys, "platform", "linux")
    monkeypatch.setattr(exchange_module.ctypes, "CDLL", lambda *_a, **_k: FakeLibc())

    function = PlatformAtomicExchange.require_capability()

    assert function is FakeLibc.renameat2


def test_unsupported_platform_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.infrastructure.db.market.atomic_exchange as exchange_module

    monkeypatch.setattr(exchange_module.sys, "platform", "win32")
    with pytest.raises(ManagedRootError, match="unavailable"):
        PlatformAtomicExchange.require_capability()


def test_regular_file_exchange_rejects_replaced_candidate_parent(tmp_path: Path) -> None:
    (tmp_path / "left").write_bytes(b"left")
    staging = tmp_path / ".market-maintenance-test"
    staging.mkdir(mode=0o700)
    (staging / "candidate.duckdb").write_bytes(b"candidate")
    expected = (staging.stat().st_dev, staging.stat().st_ino)
    staging.rename(tmp_path / "detached")
    staging.mkdir(mode=0o700)
    (staging / "candidate.duckdb").write_bytes(b"attacker")

    with ManagedRootFd.open(tmp_path) as root:
        with pytest.raises(ManagedRootError, match="parent identity changed"):
            PlatformAtomicExchange().exchange_regular_files(
                root,
                Path("left"),
                Path(".market-maintenance-test/candidate.duckdb"),
                expected_right_parent_identity=expected,
            )

    assert (tmp_path / "left").read_bytes() == b"left"
    assert (staging / "candidate.duckdb").read_bytes() == b"attacker"
