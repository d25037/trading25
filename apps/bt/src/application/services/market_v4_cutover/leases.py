"""Descriptor and cross-process lease scopes for cutover operations."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .errors import RuntimeStopError, WorkerShutdownError


class LeaseMixin:
    """Retain exact managed-root and writer lease capabilities."""

    @contextmanager
    def _managed_root_scope(self) -> Iterator[_managed_root.ManagedRootFd]:
        if self._managed_root_fd is not None:
            yield self._managed_root_fd
            return
        with _managed_root.ManagedRootFd.open(self.data_root) as managed:
            self._managed_root_fd = managed
            try:
                yield managed
            finally:
                self._managed_root_fd = None

    def _managed(self) -> _managed_root.ManagedRootFd:
        if self._managed_root_fd is None:
            raise _managed_root.CutoverSafetyError(
                "Managed data-root descriptor is not retained"
            )
        return self._managed_root_fd

    def _active_lease_fd(self) -> int:
        if self._active_lease is None:
            raise _managed_root.CutoverSafetyError(
                "An active Market operation lease is required"
            )
        return self._active_lease.fd

    @contextmanager
    def _exclusive_operation(self) -> Iterator[str]:
        if self._active_lease is not None:
            if self._active_code_version is None:
                raise _managed_root.CutoverSafetyError(
                    "Operation code identity is unavailable"
                )
            yield self._active_code_version
            return
        code_version = self._require_code_identity()
        self._validate_active_roots()
        with _market_operation_lease.MarketOperationLease.acquire(
            self.data_root, exclusive=True
        ) as lease:
            with self._managed_root_scope():
                self._active_lease = lease
                self._active_code_version = code_version
                try:
                    try:
                        yield code_version
                    except (RuntimeStopError, WorkerShutdownError) as exc:
                        if not exc.process_joined:
                            lease.unlock_on_release = False
                        raise
                finally:
                    self._active_code_version = None
                    self._active_lease = None

    @contextmanager
    def _existing_exclusive_operation(self) -> Iterator[str]:
        if self._active_lease is not None:
            raise _managed_root.CutoverSafetyError(
                "Promotion eligibility requires a newly acquired active lease"
            )
        code_version = self._require_code_identity()
        self._validate_active_roots()
        with _market_operation_lease.MarketOperationLease.acquire_existing(
            self.data_root, exclusive=True
        ) as lease:
            with self._managed_root_scope():
                self._active_lease = lease
                self._active_code_version = code_version
                try:
                    yield code_version
                finally:
                    self._active_code_version = None
                    self._active_lease = None
