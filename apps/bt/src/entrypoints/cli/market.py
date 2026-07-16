"""Lease-bound Market DB maintenance CLI."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from src.infrastructure.db.market.market_compaction import (
    MarketCompactor,
    MarketMaintenanceEvidence,
)
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
)
from src.shared.config.settings import get_settings

console = Console()


def _format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GiB"


def _print_market_maintenance_evidence(evidence: MarketMaintenanceEvidence) -> None:
    table = Table(title="Market DuckDB Maintenance", show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white", overflow="fold")
    table.add_row("compacted", str(evidence.compacted).lower())
    table.add_row("trigger", evidence.trigger.value)
    table.add_row("before", _format_bytes(evidence.before_bytes))
    table.add_row("after", _format_bytes(evidence.after_bytes))
    table.add_row("validation", evidence.validation)
    table.add_row("elapsed", f"{evidence.duration_ms:.1f} ms")
    console.print(table)


def run_market_maintain_command() -> None:
    settings = get_settings()
    market_root = Path(settings.market_timeseries_dir).absolute()
    factory = MarketWriterResourceFactory(
        data_root=market_root.parent,
        market_root=market_root,
    )
    session = None
    token = None
    evidence = None
    operation_error: Exception | None = None
    try:
        session = factory.open_existing()
        token = session.close_writable_handles()
        authority = session.authorize_maintenance(token)
        evidence = MarketCompactor().maintain(authority)
    except Exception as exc:  # noqa: BLE001 - CLI maps maintenance failure to exit status
        operation_error = exc

    lifecycle_error: Exception | None = None
    ownership_fenced = False
    if session is not None:
        ownership_fenced = token is None or bool(getattr(session, "fenced", False))
        if token is not None and not ownership_fenced:
            try:
                read_only = session.reopen_read_only_and_release(token)
                read_only.close()
            except Exception as exc:  # noqa: BLE001 - lease/reopen failure is actionable
                lifecycle_error = exc
                ownership_fenced = bool(getattr(session, "fenced", False))

    if operation_error is not None or lifecycle_error is not None:
        if operation_error is not None:
            console.print(f"[red]{operation_error}[/red]")
        if lifecycle_error is not None:
            console.print(
                f"[red]Market maintenance cleanup failed: {lifecycle_error}[/red]"
            )
        if ownership_fenced:
            console.print(
                "[red]Market writer ownership remains fenced; resolve the ambiguous "
                "identity or close failure before retrying.[/red]"
            )
        raise typer.Exit(code=1) from None

    if evidence is None:
        raise RuntimeError("Market maintenance completed without evidence")
    _print_market_maintenance_evidence(evidence)
