"""Cross-process shared/exclusive ownership for the active Market root."""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import os
from pathlib import Path
import stat
import time

from .managed_root import ManagedRootError, ManagedRootFd, lexical_absolute


class MarketOperationLeaseError(ManagedRootError):
    pass


_LOCK_NAME = ".market-timeseries.operation.lock"
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def _descriptor_path(fd: int) -> Path | None:
    get_path = getattr(fcntl, "F_GETPATH", None)
    if get_path is None:
        return None
    try:
        value = fcntl.fcntl(fd, get_path, b"\0" * 1024)
    except (OSError, ValueError):
        return None
    raw = value.split(b"\0", 1)[0]
    return Path(os.fsdecode(raw)) if raw else None


@dataclass
class MarketOperationLease:
    data_root: Path
    path: Path
    fd: int
    exclusive: bool
    root_fd: int = -1
    owns_fd: bool = True
    unlock_on_release: bool = True

    @classmethod
    def acquire(
        cls,
        data_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
        timeout: float | None = None,
    ) -> "MarketOperationLease":
        return cls._acquire(data_root, exclusive=exclusive, blocking=blocking, timeout=timeout, create=True)

    @classmethod
    def acquire_existing(
        cls,
        data_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
        timeout: float | None = None,
    ) -> "MarketOperationLease":
        return cls._acquire(data_root, exclusive=exclusive, blocking=blocking, timeout=timeout, create=False)

    @classmethod
    def _acquire(
        cls,
        data_root: Path,
        *,
        exclusive: bool,
        blocking: bool,
        timeout: float | None,
        create: bool,
    ) -> "MarketOperationLease":
        data_root = lexical_absolute(data_root)
        managed = ManagedRootFd.open(data_root)
        flags = (os.O_CREAT | os.O_RDWR) if create else os.O_RDONLY
        flags |= _NOFOLLOW
        try:
            fd = os.open(_LOCK_NAME, flags, 0o600, dir_fd=managed.fd)
        except OSError as exc:
            managed.close()
            raise MarketOperationLeaseError("Could not open Market operation lock") from exc
        try:
            cls._validate_identity(managed, fd)
            cls._flock(fd, exclusive=exclusive, blocking=blocking, timeout=timeout)
            try:
                cls._validate_identity(managed, fd)
            except MarketOperationLeaseError as exc:
                raise MarketOperationLeaseError(
                    "Existing Market operation lock identity changed"
                ) from exc
            if create:
                os.fchmod(fd, 0o600)
        except Exception:
            os.close(fd)
            managed.close()
            raise
        return cls(data_root, data_root / _LOCK_NAME, fd, exclusive, managed.fd)

    @staticmethod
    def _validate_identity(managed: ManagedRootFd, fd: int) -> None:
        descriptor = os.fstat(fd)
        path = os.stat(_LOCK_NAME, dir_fd=managed.fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(descriptor.st_mode)
            or stat.S_ISLNK(path.st_mode)
            or (descriptor.st_dev, descriptor.st_ino) != (path.st_dev, path.st_ino)
        ):
            raise MarketOperationLeaseError("Market operation lock must be a regular file")

    @staticmethod
    def _flock(fd: int, *, exclusive: bool, blocking: bool, timeout: float | None) -> None:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if blocking and timeout is None:
            fcntl.flock(fd, operation)
            return
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fd, operation | fcntl.LOCK_NB)
                return
            except BlockingIOError as exc:
                if not blocking or (deadline is not None and time.monotonic() >= deadline):
                    raise MarketOperationLeaseError(
                        "Market operation lease is held by another process"
                    ) from exc
                time.sleep(0.01)

    @classmethod
    def resolve_inherited_data_root(cls, root_fd: int) -> Path:
        try:
            descriptor = os.fstat(root_fd)
        except OSError as exc:
            raise MarketOperationLeaseError(
                "Inherited Market root descriptor is invalid"
            ) from exc
        path = _descriptor_path(root_fd)
        if not stat.S_ISDIR(descriptor.st_mode) or path is None:
            raise MarketOperationLeaseError(
                "Inherited Market root descriptor path is unavailable"
            )
        return lexical_absolute(path)

    def assert_live_exclusive(self) -> None:
        if self.fd < 0 or self.root_fd < 0 or not self.exclusive:
            raise MarketOperationLeaseError("Market writer lease is not live and exclusive")
        descriptor = os.fstat(self.root_fd)
        path_identity = os.stat(self.data_root, follow_symlinks=False)
        if (
            not stat.S_ISDIR(descriptor.st_mode)
            or stat.S_ISLNK(path_identity.st_mode)
            or (descriptor.st_dev, descriptor.st_ino)
            != (path_identity.st_dev, path_identity.st_ino)
        ):
            raise MarketOperationLeaseError("Market writer lease root identity changed")
        managed = ManagedRootFd(self.data_root, self.root_fd)
        self._validate_identity(managed, self.fd)
        probe = os.open(_LOCK_NAME, os.O_RDONLY | _NOFOLLOW, dir_fd=self.root_fd)
        try:
            try:
                fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            fcntl.flock(probe, fcntl.LOCK_UN)
            raise MarketOperationLeaseError(
                "Market writer lease did not establish exclusivity"
            )
        finally:
            os.close(probe)

    @classmethod
    def adopt_inherited(
        cls,
        data_root: Path,
        fd: int,
        *,
        root_fd: int | None = None,
    ) -> "MarketOperationLease":
        data_root = lexical_absolute(data_root)
        if root_fd is None:
            managed = ManagedRootFd.open(data_root)
        else:
            try:
                descriptor = os.fstat(root_fd)
            except OSError as exc:
                raise MarketOperationLeaseError(
                    "Inherited Market root descriptor is invalid"
                ) from exc
            if not stat.S_ISDIR(descriptor.st_mode):
                raise MarketOperationLeaseError(
                    "Inherited Market root descriptor does not match data root"
                )
            try:
                path_identity = os.stat(data_root, follow_symlinks=False)
            except OSError as exc:
                raise MarketOperationLeaseError(
                    "Inherited Market data root is unavailable"
                ) from exc
            if (
                stat.S_ISLNK(path_identity.st_mode)
                or not stat.S_ISDIR(path_identity.st_mode)
                or (descriptor.st_dev, descriptor.st_ino)
                != (path_identity.st_dev, path_identity.st_ino)
            ):
                raise MarketOperationLeaseError(
                    "Inherited Market root descriptor does not exactly match data root"
                )
            managed = ManagedRootFd(data_root, root_fd)
        try:
            try:
                cls._validate_identity(managed, fd)
            except OSError as exc:
                raise MarketOperationLeaseError(
                    "Inherited Market operation lease is invalid"
                ) from exc
            cls._flock(fd, exclusive=True, blocking=False, timeout=None)
            probe = os.open(_LOCK_NAME, os.O_RDONLY | _NOFOLLOW, dir_fd=managed.fd)
            try:
                try:
                    fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    fcntl.flock(probe, fcntl.LOCK_UN)
                    raise MarketOperationLeaseError(
                        "Inherited Market operation lease did not establish exclusivity"
                    )
            finally:
                os.close(probe)
        except Exception:
            managed.close()
            raise
        return cls(data_root, data_root / _LOCK_NAME, fd, True, managed.fd, unlock_on_release=False)

    def detach_for_inheritance(self) -> int:
        if self.fd < 0 or not self.exclusive:
            raise MarketOperationLeaseError("Only a live exclusive lease can be inherited")
        fd = self.fd
        self.fd = -1
        if self.root_fd >= 0:
            os.close(self.root_fd)
            self.root_fd = -1
        return fd

    def convert(
        self,
        *,
        exclusive: bool,
        blocking: bool = True,
        timeout: float | None = None,
    ) -> None:
        if self.fd < 0:
            raise MarketOperationLeaseError("Cannot convert a released Market lease")
        self._flock(
            self.fd,
            exclusive=exclusive,
            blocking=blocking,
            timeout=timeout,
        )
        self.exclusive = exclusive

    def release(self) -> None:
        if self.fd < 0:
            return
        try:
            if self.unlock_on_release:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            if self.owns_fd:
                os.close(self.fd)
            if self.root_fd >= 0:
                os.close(self.root_fd)
                self.root_fd = -1
            self.fd = -1

    def __enter__(self) -> "MarketOperationLease":
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()
