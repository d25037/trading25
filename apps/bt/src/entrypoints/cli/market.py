"""Lease-bound Market DB maintenance CLI."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from src.application.contracts.market_maintenance import (
    MaintenanceOutcome,
    MarketMaintenanceRecord,
    MarketOperationOutcome,
)
from src.application.services.market_maintenance_finalizer import (
    MarketFinalizationDecision,
    MarketMaintenanceFinalizer,
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


def _print_market_maintenance_record(record: MarketMaintenanceRecord) -> None:
    table = Table(title="Market DuckDB Maintenance", show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white", overflow="fold")
    table.add_row("outcome", record.outcome.value)
    table.add_row("compacted", str(bool(record.compacted)).lower())
    table.add_row("trigger", record.trigger or "unknown")
    table.add_row("before", _format_bytes(record.beforeBytes or 0))
    table.add_row("after", _format_bytes(record.afterBytes or 0))
    table.add_row("validation", record.validation or "unknown")
    table.add_row("elapsed", f"{record.durationMs or 0:.1f} ms")
    console.print(table)


def run_market_maintain_command() -> None:
    settings = get_settings()
    market_root = Path(settings.market_timeseries_dir).absolute()
    factory = MarketWriterResourceFactory(
        data_root=market_root.parent,
        market_root=market_root,
    )
    session = None
    decisions: list[MarketFinalizationDecision] = []
    try:
        session = factory.open_existing()
        finalizer = MarketMaintenanceFinalizer(
            session=session,
            operation="market_maintain",
            attach=lambda resources, _evidence: resources.close(),
        )
        finalizer.finalize(
            operation_outcome=MarketOperationOutcome.SUCCEEDED,
            publish_terminal=decisions.append,
        )
    except Exception as exc:  # noqa: BLE001 - CLI maps maintenance failure to exit status
        console.print(f"[red]{exc}[/red]")
        if session is not None and bool(getattr(session, "fenced", False)):
            console.print(
                "[red]Market writer ownership remains fenced; resolve the ambiguous "
                "identity or close failure before retrying.[/red]"
            )
        raise typer.Exit(code=1) from None

    if not decisions:
        raise RuntimeError("Market maintenance completed without evidence")
    decision = decisions[0]
    if decision.maintenance.outcome is MaintenanceOutcome.FAILED:
        console.print(f"[red]{decision.error}[/red]")
        if session is not None and bool(getattr(session, "fenced", False)):
            console.print(
                "[red]Market writer ownership remains fenced; resolve the ambiguous "
                "identity or close failure before retrying.[/red]"
            )
        console.print(
            f"[red]Retry with {decision.maintenance.recoveryCommand} after resolving "
            "the reported condition.[/red]"
        )
        raise typer.Exit(code=1)
    _print_market_maintenance_record(decision.maintenance)
