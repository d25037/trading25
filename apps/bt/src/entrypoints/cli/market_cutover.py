"""CLI entry points for the gated Market v4 cutover workflow."""

from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import shutil
import subprocess
from typing import Callable

import typer

from src.application.services.market_v4_cutover import (
    CutoverSafetyError,
    DefaultDuckDbAdapter,
    HttpApiAdapter,
    MarketV4CutoverService,
    SmokeConfig,
    SubprocessRuntimeAdapter,
)
from src.shared.paths.resolver import get_data_dir


market_v4_cutover_app = typer.Typer(
    help="Gated Market v4 backup, rehearsal, cutover, restore, and smoke workflow.",
    no_args_is_help=True,
)


def _code_version() -> str:
    repo_root = Path(__file__).resolve().parents[5]
    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if status:
            raise CutoverSafetyError("Tracked git tree is dirty; cutover identity is not immutable")
        identity = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CutoverSafetyError("Could not resolve immutable git code identity") from exc
    if not identity:
        raise CutoverSafetyError("Could not resolve immutable git code identity")
    return identity


def build_default_service(
    data_root: Path | None = None,
    *,
    health_url: str | None = None,
) -> MarketV4CutoverService:
    root = (data_root or get_data_dir()).expanduser().resolve()
    return MarketV4CutoverService(
        root,
        duckdb=DefaultDuckDbAdapter(),
        runtime=SubprocessRuntimeAdapter(health_url=health_url),
        disk_free_bytes=lambda path: shutil.disk_usage(path).free,
        now=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        code_version=_code_version,
    )


def _service(data_root: Path | None, health_url: str | None) -> MarketV4CutoverService:
    return build_default_service(data_root, health_url=health_url)


def _smoke_config(symbol: str, strategy: str, dataset_preset: str) -> SmokeConfig:
    return SmokeConfig(symbol, strategy, dataset_preset)


def _fail_closed(action: Callable[[], object]) -> None:
    try:
        result = action()
    except CutoverSafetyError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(result)


DataRootOption = typer.Option(None, "--data-root", help="Explicit trading25 data root.")
HealthUrlOption = typer.Option(
    None,
    "--health-url",
    help="FastAPI health URL that must be unreachable for offline phases.",
)


@market_v4_cutover_app.command("preflight")
def preflight_command(
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Prove stopped writers, exclusive checkpoint, empty WAL, and disk capacity."""
    _fail_closed(lambda: _service(data_root, health_url).preflight())


@market_v4_cutover_app.command("backup")
def backup_command(
    backup_id: str = typer.Argument(..., help="New immutable backup ID."),
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Create and verify an immutable Market DB + Parquet backup."""
    _fail_closed(lambda: _service(data_root, health_url).backup(backup_id))


@market_v4_cutover_app.command("rehearse")
def rehearse_command(
    report_id: str = typer.Argument(..., help="Unique rehearsal report ID."),
    symbol: str = typer.Option(..., "--symbol"),
    strategy: str = typer.Option(..., "--strategy"),
    dataset_preset: str = typer.Option("primeMarket", "--dataset-preset"),
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Rebuild and smoke an isolated XDG root using an owned server."""
    _fail_closed(
        lambda: _service(data_root, health_url).rehearse(
            report_id,
            _smoke_config(symbol, strategy, dataset_preset),
            inherited_environment=dict(os.environ),
        )
    )


@market_v4_cutover_app.command("cutover")
def cutover_command(
    report_id: str = typer.Argument(..., help="Unique active cutover report ID."),
    rehearsal_report_id: str = typer.Option(..., "--rehearsal-report-id"),
    backup_id: str = typer.Option(..., "--backup-id"),
    symbol: str = typer.Option(..., "--symbol"),
    strategy: str = typer.Option(..., "--strategy"),
    dataset_preset: str = typer.Option("primeMarket", "--dataset-preset"),
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Run the active reset only with exact rehearsal and backup IDs."""
    _fail_closed(
        lambda: _service(data_root, health_url).cutover(
            report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=_smoke_config(symbol, strategy, dataset_preset),
            inherited_environment=dict(os.environ),
        )
    )


@market_v4_cutover_app.command("restore")
def restore_command(
    backup_id: str = typer.Argument(..., help="Explicit verified backup ID."),
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Restore one explicit verified backup; never delete it."""
    _fail_closed(lambda: _service(data_root, health_url).restore(backup_id))


@market_v4_cutover_app.command("smoke")
def smoke_command(
    operation_id: str = typer.Option(..., "--operation-id"),
    symbol: str = typer.Option(..., "--symbol"),
    strategy: str = typer.Option(..., "--strategy"),
    dataset_preset: str = typer.Option("primeMarket", "--dataset-preset"),
    api_url: str = typer.Option("http://127.0.0.1:3002", "--api-url"),
    data_root: Path | None = DataRootOption,
    health_url: str | None = HealthUrlOption,
) -> None:
    """Run read/API semantic smoke against an already running server."""
    _fail_closed(
        lambda: _service(data_root, health_url).smoke(
            HttpApiAdapter(api_url),
            _smoke_config(symbol, strategy, dataset_preset),
            operation_id=operation_id,
        )
    )
