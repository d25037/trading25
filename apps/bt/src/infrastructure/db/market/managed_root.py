"""Descriptor-relative safety primitives for the managed Market data root."""

from __future__ import annotations

from dataclasses import dataclass
import errno
import hashlib
import os
from pathlib import Path
import stat


class CutoverSafetyError(RuntimeError):
    """A managed-root safety invariant was violated."""


ManagedRootError = CutoverSafetyError


_DIR_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)


def lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def _safe_relative_parts(relative: Path) -> tuple[str, ...]:
    if relative.is_absolute() or not relative.parts:
        raise ManagedRootError("Managed path must be a non-empty relative path")
    if any(part in {"", ".", ".."} or "/" in part for part in relative.parts):
        raise ManagedRootError("Managed path contains an unsafe component")
    return relative.parts


def assert_safe_directory_chain(path: Path, *, target_may_be_missing: bool = False) -> None:
    path = lexical_absolute(path)
    for component in reversed((path, *path.parents)):
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise ManagedRootError(
                f"Managed path component must not be a symlink: {component.name or '/'}"
            )
        if not stat.S_ISDIR(mode):
            raise ManagedRootError("Managed path component must be a directory")
    if not target_may_be_missing and not path.exists():
        raise ManagedRootError("Managed directory is missing")


def assert_real_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise ManagedRootError(f"{label} is missing") from exc
    if stat.S_ISLNK(mode):
        raise ManagedRootError(f"{label} must not be a symlink")
    if not stat.S_ISDIR(mode):
        raise ManagedRootError(f"{label} must be a real directory")


def mkdir_safe_directory_chain(path: Path) -> None:
    path = lexical_absolute(path)
    assert_safe_directory_chain(path, target_may_be_missing=True)
    current = os.open("/", _DIR_OPEN_FLAGS)
    try:
        for component in path.parts[1:]:
            try:
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=current)
            except FileNotFoundError:
                os.mkdir(component, 0o700, dir_fd=current)
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=current)
            os.close(current)
            current = child
    finally:
        os.close(current)
    assert_safe_directory_chain(path)


@dataclass
class ManagedRootFd:
    path: Path
    fd: int

    @classmethod
    def open(cls, path: Path) -> "ManagedRootFd":
        path = lexical_absolute(path)
        assert_safe_directory_chain(path)
        fd = os.open("/", _DIR_OPEN_FLAGS)
        try:
            for component in path.parts[1:]:
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = child
            descriptor_stat = os.fstat(fd)
            path_stat = path.lstat()
            if (
                not stat.S_ISDIR(descriptor_stat.st_mode)
                or stat.S_ISLNK(path_stat.st_mode)
                or (descriptor_stat.st_dev, descriptor_stat.st_ino)
                != (path_stat.st_dev, path_stat.st_ino)
            ):
                raise ManagedRootError("Managed data-root descriptor identity mismatch")
        except Exception:
            os.close(fd)
            raise
        return cls(path, fd)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self) -> "ManagedRootFd":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def open_dir(self, relative: Path, *, create: bool = False, exclusive_leaf: bool = False) -> int:
        parts = _safe_relative_parts(relative)
        current = os.dup(self.fd)
        try:
            for index, part in enumerate(parts):
                leaf = index == len(parts) - 1
                try:
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                    if leaf and exclusive_leaf:
                        os.close(child)
                        raise FileExistsError(errno.EEXIST, "directory exists", part)
                except FileNotFoundError:
                    if not create:
                        raise
                    os.mkdir(part, 0o700, dir_fd=current)
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                if not stat.S_ISDIR(os.fstat(child).st_mode):
                    os.close(child)
                    raise ManagedRootError("Managed component is not a directory")
                os.close(current)
                current = child
            return current
        except OSError as exc:
            os.close(current)
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ManagedRootError(
                    "Managed directory traversal encountered a symlink"
                ) from exc
            raise
        except Exception:
            os.close(current)
            raise

    def open_parent(self, relative: Path, *, create: bool = False) -> tuple[int, str]:
        parts = _safe_relative_parts(relative)
        if len(parts) == 1:
            return os.dup(self.fd), parts[0]
        return self.open_dir(Path(*parts[:-1]), create=create), parts[-1]

    def open_regular(self, relative: Path, flags: int, mode: int = 0o600) -> int:
        parent, name = self.open_parent(relative)
        try:
            fd = os.open(name, flags | getattr(os, "O_NOFOLLOW", 0), mode, dir_fd=parent)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                os.close(fd)
                raise ManagedRootError("Managed file is not regular")
            return fd
        finally:
            os.close(parent)

    def stat(self, relative: Path) -> os.stat_result:
        parent, name = self.open_parent(relative)
        try:
            return os.stat(name, dir_fd=parent, follow_symlinks=False)
        finally:
            os.close(parent)

    def unlink(self, relative: Path, *, missing_ok: bool = False) -> None:
        parent, name = self.open_parent(relative)
        try:
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                if not missing_ok:
                    raise
        finally:
            os.close(parent)

    def fsync_dir(self, relative: Path) -> None:
        fd = self.open_dir(relative)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def remove_tree(self, relative: Path, *, missing_ok: bool = False) -> None:
        parent, name = self.open_parent(relative)
        try:
            try:
                directory = os.open(name, _DIR_OPEN_FLAGS, dir_fd=parent)
            except FileNotFoundError:
                if missing_ok:
                    return
                raise

            def remove_contents(directory_fd: int) -> None:
                for child_name in os.listdir(directory_fd):
                    child_stat = os.stat(child_name, dir_fd=directory_fd, follow_symlinks=False)
                    if stat.S_ISDIR(child_stat.st_mode):
                        child_fd = os.open(child_name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                        try:
                            remove_contents(child_fd)
                        finally:
                            os.close(child_fd)
                        os.rmdir(child_name, dir_fd=directory_fd)
                    elif stat.S_ISREG(child_stat.st_mode):
                        os.unlink(child_name, dir_fd=directory_fd)
                    else:
                        raise ManagedRootError(
                            "Managed cleanup encountered a symlink or special file"
                        )

            try:
                remove_contents(directory)
            finally:
                os.close(directory)
            os.rmdir(name, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(parent)

    def chmod_tree(self, relative: Path, *, directory_mode: int, file_mode: int) -> None:
        root = self.open_dir(relative)

        def chmod_contents(directory_fd: int) -> None:
            for child_name in os.listdir(directory_fd):
                child_stat = os.stat(child_name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISDIR(child_stat.st_mode):
                    child_fd = os.open(child_name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        chmod_contents(child_fd)
                        os.fchmod(child_fd, directory_mode)
                    finally:
                        os.close(child_fd)
                elif stat.S_ISREG(child_stat.st_mode):
                    os.chmod(
                        child_name,
                        file_mode,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                else:
                    raise ManagedRootError(
                        "Managed chmod encountered a symlink or special file"
                    )

        try:
            chmod_contents(root)
            os.fchmod(root, directory_mode)
        finally:
            os.close(root)

    def regular_files(self, relative: Path) -> list[tuple[Path, os.stat_result]]:
        root = self.open_dir(relative)
        files: list[tuple[Path, os.stat_result]] = []

        def walk(directory_fd: int, prefix: Path) -> None:
            for name in sorted(os.listdir(directory_fd)):
                entry_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                child_relative = prefix / name
                if stat.S_ISDIR(entry_stat.st_mode):
                    child_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        walk(child_fd, child_relative)
                    finally:
                        os.close(child_fd)
                elif stat.S_ISREG(entry_stat.st_mode):
                    files.append((child_relative, entry_stat))
                else:
                    raise ManagedRootError(
                        "Managed tree contains a symlink or special file"
                    )

        try:
            walk(root, Path())
        finally:
            os.close(root)
        return files

    def sha256(self, relative: Path) -> str:
        fd = self.open_regular(relative, os.O_RDONLY)
        digest = hashlib.sha256()
        try:
            while chunk := os.read(fd, 1024 * 1024):
                digest.update(chunk)
        finally:
            os.close(fd)
        return digest.hexdigest()

    def read_bytes(self, relative: Path) -> bytes:
        fd = self.open_regular(relative, os.O_RDONLY)
        chunks: list[bytes] = []
        try:
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
        return b"".join(chunks)

    def copy_tree_create(self, source: Path, target: Path) -> None:
        source_fd = self.open_dir(source)
        try:
            target_fd = self.open_dir(target, create=True, exclusive_leaf=True)
        except Exception:
            os.close(source_fd)
            raise

        def copy_contents(source_dir_fd: int, target_dir_fd: int) -> None:
            for name in sorted(os.listdir(source_dir_fd)):
                entry_stat = os.stat(name, dir_fd=source_dir_fd, follow_symlinks=False)
                if stat.S_ISDIR(entry_stat.st_mode):
                    os.mkdir(name, 0o700, dir_fd=target_dir_fd)
                    source_child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=source_dir_fd)
                    target_child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=target_dir_fd)
                    try:
                        copy_contents(source_child, target_child)
                        os.fsync(target_child)
                    finally:
                        os.close(source_child)
                        os.close(target_child)
                elif stat.S_ISREG(entry_stat.st_mode):
                    source_file = os.open(
                        name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=source_dir_fd,
                    )
                    try:
                        target_file = os.open(
                            name,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                            entry_stat.st_mode & 0o777,
                            dir_fd=target_dir_fd,
                        )
                    except Exception:
                        os.close(source_file)
                        raise
                    try:
                        while chunk := os.read(source_file, 1024 * 1024):
                            view = memoryview(chunk)
                            while view:
                                written = os.write(target_file, view)
                                view = view[written:]
                        os.fsync(target_file)
                    finally:
                        os.close(source_file)
                        os.close(target_file)
                else:
                    raise ManagedRootError(
                        "Managed copy encountered a symlink or special file"
                    )

        try:
            copy_contents(source_fd, target_fd)
            os.fsync(target_fd)
        except Exception:
            os.close(source_fd)
            os.close(target_fd)
            self.remove_tree(target, missing_ok=True)
            raise
        os.close(source_fd)
        os.close(target_fd)

    def fsync_tree(self, relative: Path) -> None:
        root = self.open_dir(relative)

        def sync_contents(directory_fd: int) -> None:
            for name in os.listdir(directory_fd):
                entry_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISDIR(entry_stat.st_mode):
                    child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        sync_contents(child)
                        os.fsync(child)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(entry_stat.st_mode):
                    child = os.open(
                        name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=directory_fd,
                    )
                    try:
                        os.fsync(child)
                    finally:
                        os.close(child)
                else:
                    raise ManagedRootError(
                        "Managed fsync encountered a symlink or special file"
                    )

        try:
            sync_contents(root)
            os.fsync(root)
        finally:
            os.close(root)


def assert_market_managed_root_safe(data_root: Path, market_root: Path) -> None:
    data_root = lexical_absolute(data_root)
    market_root = lexical_absolute(market_root)
    assert_safe_directory_chain(data_root)
    if market_root.parent != data_root:
        raise ManagedRootError("Market root must be a direct child of the data root")
    assert_safe_directory_chain(market_root)
    mode = market_root.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise ManagedRootError("Market time-series root must be a real directory")


def prepare_market_managed_root(data_root: Path, market_root: Path) -> None:
    data_root = lexical_absolute(data_root)
    market_root = lexical_absolute(market_root)
    if market_root.parent != data_root:
        raise ManagedRootError("Market root must be a direct child of the data root")
    mkdir_safe_directory_chain(data_root)
    if market_root.exists() or market_root.is_symlink():
        assert_safe_directory_chain(market_root)
    else:
        with ManagedRootFd.open(data_root) as managed:
            os.mkdir(market_root.name, 0o700, dir_fd=managed.fd)
    assert_market_managed_root_safe(data_root, market_root)
