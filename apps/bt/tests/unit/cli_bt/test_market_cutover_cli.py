from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from typer.testing import CliRunner
import subprocess

import pytest

from src.application.services.market_v4_cutover import CutoverSafetyError, SmokeConfig
from src.entrypoints.cli import app
from src.entrypoints.cli import market_cutover


def test_market_v4_cutover_cli_exposes_all_phases() -> None:
    assert hasattr(market_cutover, "market_v4_cutover_app")
    assert hasattr(market_cutover, "build_default_service")


def test_market_v4_cutover_help_lists_all_explicit_phases() -> None:
    result = CliRunner().invoke(app, ["market-cutover", "--help"])
    assert result.exit_code == 0
    for phase in (
        "preflight",
        "backup",
        "rehearse",
        "rehearse-retained",
        "promote-retained",
        "cutover",
        "restore",
        "smoke",
    ):
        assert phase in result.stdout


@dataclass
class FakeService:
    calls: list[tuple[str, object]] = field(default_factory=list)

    def rehearse(self, report_id: str, *_args: object, **kwargs: object) -> str:
        self.calls.append((report_id, kwargs))
        return "ok"

    def rehearse_retained(self, report_id: str, **kwargs: object) -> str:
        self.calls.append((report_id, kwargs))
        return "ok"

    def cutover(self, report_id: str, **kwargs: object) -> str:
        self.calls.append((report_id, kwargs))
        return "ok"

    def promote_retained(self, report_id: str, **kwargs: object) -> str:
        self.calls.append((report_id, kwargs))
        return "ok"


def test_promote_retained_help_exposes_only_canonical_inputs() -> None:
    result = CliRunner().invoke(
        app, ["market-cutover", "promote-retained", "--help"]
    )

    assert result.exit_code == 0, result.output
    for option in (
        "--retained-report-id",
        "--backup-id",
        "--symbol",
        "--strategy",
        "--dataset-preset",
        "--data-root",
    ):
        assert option in result.stdout
    for forbidden in (
        "--source-path",
        "--force",
        "--copy",
        "--jquants",
        "--rehearsal-report-id",
    ):
        assert forbidden not in result.stdout.lower()


def test_promote_retained_cli_maps_exact_contract_without_rebuild_credentials(
    monkeypatch,
) -> None:
    service = FakeService()
    monkeypatch.setenv("JQUANTS_API_KEY", "must-not-be-inherited")
    monkeypatch.setenv("JQUANTS_PLAN", "standard")
    monkeypatch.setattr(
        market_cutover,
        "_required_rebuild_environment",
        lambda: (_ for _ in ()).throw(AssertionError("must not load J-Quants env")),
    )
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )

    result = CliRunner().invoke(
        app,
        [
            "market-cutover",
            "promote-retained",
            "active-r14",
            "--retained-report-id",
            "retained-r13",
            "--backup-id",
            "backup-r14",
            "--symbol",
            "7203",
            "--strategy",
            "production/cutover_smoke",
            "--dataset-preset",
            "primeMarket",
        ],
    )

    assert result.exit_code == 0, result.output
    assert service.calls == [
        (
            "active-r14",
            {
                "retained_report_id": "retained-r13",
                "backup_id": "backup-r14",
                "config": SmokeConfig(
                    "7203", "production/cutover_smoke", "primeMarket"
                ),
                "inherited_environment": {},
            },
        )
    ]


def test_promote_retained_cli_retries_deferred_recovery_with_exact_same_ids(
    monkeypatch,
) -> None:
    service = FakeService()
    attempts = 0

    def deferred_then_recovered(report_id: str, **kwargs: object) -> str:
        nonlocal attempts
        service.calls.append((report_id, kwargs))
        attempts += 1
        if attempts == 1:
            raise CutoverSafetyError("Market operation lease is held by another process")
        return "recovered"

    monkeypatch.setattr(service, "promote_retained", deferred_then_recovered)
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )
    command = [
        "market-cutover",
        "promote-retained",
        "active-r14",
        "--retained-report-id",
        "retained-r13",
        "--backup-id",
        "backup-r14",
        "--symbol",
        "7203",
        "--strategy",
        "production/cutover_smoke",
    ]

    blocked = CliRunner().invoke(app, command)
    recovered = CliRunner().invoke(app, command)

    assert blocked.exit_code == 1
    assert "operation lease" in blocked.stderr
    assert recovered.exit_code == 0
    assert service.calls[0] == service.calls[1]


def test_rehearse_retained_cli_maps_contract_without_jquants_environment(
    monkeypatch,
) -> None:
    service = FakeService()
    monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
    monkeypatch.delenv("JQUANTS_PLAN", raising=False)
    monkeypatch.setattr(
        market_cutover,
        "_required_rebuild_environment",
        lambda: (_ for _ in ()).throw(AssertionError("must not load J-Quants env")),
    )
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )

    result = CliRunner().invoke(
        app,
        [
            "market-cutover",
            "rehearse-retained",
            "retained-r12",
            "--source-rehearsal-id",
            "market-v4-rehearsal-20260715-r10",
            "--symbol",
            "7203",
            "--strategy",
            "production/cutover_smoke",
            "--dataset-preset",
            "primeMarket",
        ],
    )

    assert result.exit_code == 0, result.output
    assert service.calls == [
        (
            "retained-r12",
            {
                "source_rehearsal_report_id": "market-v4-rehearsal-20260715-r10",
                "config": SmokeConfig(
                    "7203", "production/cutover_smoke", "primeMarket"
                ),
                "inherited_environment": {},
            },
        )
    ]


def test_cutover_cli_passes_exact_report_and_backup_ids(monkeypatch) -> None:
    service = FakeService()
    monkeypatch.setenv("JQUANTS_PLAN", "standard")
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
    assert kwargs["inherited_environment"]["JQUANTS_PLAN"] == "standard"


@pytest.mark.parametrize(
    "command",
    [
        [
            "market-cutover",
            "rehearse",
            "rehearsal-001",
            "--symbol",
            "7203",
            "--strategy",
            "production/smoke",
        ],
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
    ],
)
def test_rebuild_cli_rejects_missing_jquants_plan_before_service_call(
    monkeypatch, command: list[str]
) -> None:
    service = FakeService()
    monkeypatch.delenv("JQUANTS_PLAN", raising=False)
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )

    result = CliRunner().invoke(app, command)

    assert result.exit_code == 1
    assert "JQUANTS_PLAN" in result.stderr
    assert "~/.config/trading25/config.env" in result.stderr
    assert service.calls == []


@pytest.mark.parametrize("plan", ["", "starter", "STANDARD", " standard "])
def test_cutover_cli_rejects_invalid_jquants_plan_before_service_call(
    monkeypatch, plan: str
) -> None:
    service = FakeService()
    monkeypatch.setenv("JQUANTS_PLAN", plan)
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

    assert result.exit_code == 1
    assert "free, light, standard, premium" in result.stderr
    assert "~/.config/trading25/config.env" in result.stderr
    assert service.calls == []


@pytest.mark.parametrize("plan", ["free", "light", "standard", "premium"])
def test_rehearse_cli_accepts_current_jquants_plans(monkeypatch, plan: str) -> None:
    service = FakeService()
    monkeypatch.setenv("JQUANTS_PLAN", plan)
    monkeypatch.setattr(
        market_cutover,
        "build_default_service",
        lambda *_args, **_kwargs: service,
    )

    result = CliRunner().invoke(
        app,
        [
            "market-cutover",
            "rehearse",
            "rehearsal-001",
            "--symbol",
            "7203",
            "--strategy",
            "production/smoke",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert service.calls[0][1]["inherited_environment"]["JQUANTS_PLAN"] == plan


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


def test_default_service_preserves_lexical_symlink_root_for_validation(
    tmp_path: Path, monkeypatch
) -> None:
    external = tmp_path / "external"
    market = external / "market-timeseries"
    market.mkdir(parents=True)
    (market / "market.duckdb").touch()
    selected = tmp_path / "selected"
    selected.symlink_to(external, target_is_directory=True)
    monkeypatch.setattr(market_cutover, "_code_version", lambda: "deadbeef")

    service = market_cutover.build_default_service(selected)

    assert service.data_root == selected.absolute()
    with pytest.raises(CutoverSafetyError, match="symlink"):
        service.preflight()
    assert not (external / ".market-timeseries.operation.lock").exists()
