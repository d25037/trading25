"""Market v5 cutover resource cleanup tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.runtime import (
    HttpApiAdapter,
    SubprocessRuntimeAdapter,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root


def test_copy_tree_create_closes_source_fd_when_target_open_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    (root / "source").mkdir(parents=True)
    with managed_root.ManagedRootFd.open(root) as managed:
        original_open_dir = managed.open_dir
        retained_source_fd = -1

        def fail_target_open(
            relative: Path,
            *,
            create: bool = False,
            exclusive_leaf: bool = False,
        ) -> int:
            nonlocal retained_source_fd
            if relative == Path("source"):
                retained_source_fd = original_open_dir(relative)
                return retained_source_fd
            assert relative == Path("target")
            assert create is True
            assert exclusive_leaf is True
            raise OSError("injected target open failure")

        monkeypatch.setattr(managed, "open_dir", fail_target_open)
        with pytest.raises(OSError, match="target open failure"):
            managed.copy_tree_create(Path("source"), Path("target"))

        assert retained_source_fd >= 0
        with pytest.raises(OSError):
            os.fstat(retained_source_fd)


def test_runtime_cancels_screening_and_dataset_jobs_before_polling_terminal() -> None:
    class RecordingApi(HttpApiAdapter):
        def __init__(self) -> None:
            super().__init__("http://unused")
            self.events: list[tuple[str, str]] = []

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del payload
            self.events.append((method, path))
            if method in {"POST", "DELETE"}:
                return {"status": "cancelled"}
            return {"status": "cancelled"}

    api = RecordingApi()
    api.owned_jobs = {"screening": "screen-1", "dataset": "data-1"}
    SubprocessRuntimeAdapter().cancel_owned_work(api)

    assert api.events == [
        ("POST", "/api/analytics/screening/jobs/screen-1/cancel"),
        ("GET", "/api/analytics/screening/jobs/screen-1"),
        ("DELETE", "/api/dataset/jobs/data-1"),
        ("GET", "/api/dataset/jobs/data-1"),
    ]


def test_runtime_treats_cancel_400_as_safe_when_followup_status_is_terminal() -> None:
    class TerminalRaceApi(HttpApiAdapter):
        def __init__(self) -> None:
            super().__init__("http://unused")
            self.events: list[tuple[str, str]] = []

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del payload
            self.events.append((method, path))
            if method == "DELETE":
                raise CutoverSafetyError("HTTP 400: job is already failed")
            return {"status": "failed"}

    api = TerminalRaceApi()
    api.owned_jobs = {"sync": "sync-1"}

    SubprocessRuntimeAdapter().cancel_owned_work(api)

    assert api.events == [
        ("DELETE", "/api/db/sync/jobs/sync-1"),
        ("GET", "/api/db/sync/jobs/sync-1"),
    ]
