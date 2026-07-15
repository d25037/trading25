from __future__ import annotations

from dataclasses import dataclass, field

from typer.testing import CliRunner
import subprocess

import pytest

from src.application.services.market_v4_cutover import CutoverSafetyError
from src.entrypoints.cli import app
from src.entrypoints.cli import market_cutover


def test_market_v4_cutover_cli_exposes_all_phases() -> None:
    assert hasattr(market_cutover, "market_v4_cutover_app")
    assert hasattr(market_cutover, "build_default_service")


def test_market_v4_cutover_help_lists_all_explicit_phases() -> None:
    result = CliRunner().invoke(app, ["market-cutover", "--help"])
    assert result.exit_code == 0
    for phase in ("preflight", "backup", "rehearse", "cutover", "restore", "smoke"):
        assert phase in result.stdout


@dataclass
class FakeService:
    calls: list[tuple[str, object]] = field(default_factory=list)

    def cutover(self, report_id: str, **kwargs: object) -> str:
        self.calls.append((report_id, kwargs))
        return "ok"


def test_cutover_cli_passes_exact_report_and_backup_ids(monkeypatch) -> None:
    service = FakeService()
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )
    result = CliRunner().invoke(
        app,
        [
            "market-cutover",
            "cutover",
            "active-001",
            "--rehearsal-report-id",
            "rehearsal-001",
            "--backup-id",
            "backup-001",
            "--symbol",
            "7203",
            "--strategy",
            "production/smoke",
        ],
    )
    assert result.exit_code == 0, result.stdout
    report_id, kwargs = service.calls[0]
    assert report_id == "active-001"
    assert kwargs["rehearsal_report_id"] == "rehearsal-001"
    assert kwargs["backup_id"] == "backup-001"


def test_restore_cli_requires_an_explicit_backup_id() -> None:
    result = CliRunner().invoke(app, ["market-cutover", "restore"])
    assert result.exit_code == 2


def test_code_identity_rejects_dirty_tree(monkeypatch) -> None:
    monkeypatch.setattr(
        market_cutover.subprocess,
        "check_output",
        lambda *_args, **_kwargs: " M tracked.py\n",
    )
    with pytest.raises(CutoverSafetyError, match="dirty"):
        market_cutover._code_version()


def test_code_identity_rejects_unavailable_git(monkeypatch) -> None:
    def unavailable(*_args: object, **_kwargs: object) -> str:
        raise subprocess.CalledProcessError(1, ["git"])

    monkeypatch.setattr(market_cutover.subprocess, "check_output", unavailable)
    with pytest.raises(CutoverSafetyError, match="immutable git"):
        market_cutover._code_version()
