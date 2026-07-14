"""Market DB maintenance CLI helpers."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from src.infrastructure.db.market.market_compaction import (
    MarketCompactionResult,
    compact_market_duckdb,
)
from src.shared.config.settings import get_settings

console = Console()


def _build_market_compact_paths(
    *,
    db_path: Path | None,
    output_path: Path | None,
) -> tuple[Path, Path]:
    if db_path is None:
        settings = get_settings()
        db_path = Path(settings.market_timeseries_dir) / "market.duckdb"
    if output_path is None:
        output_path = db_path.with_name("market.compact.duckdb")
    return db_path, output_path


def _format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1000 or unit == "GB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1000
    return f"{size:.1f} GB"


def _print_market_compaction_result(result: MarketCompactionResult) -> None:
    table = Table(title="Market DuckDB Compaction", show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white", overflow="fold")
    table.add_row("source", str(result.source_path))
    table.add_row("output", str(result.output_path))
    table.add_row("source bytes", _format_bytes(result.source_bytes))
    table.add_row("output bytes", _format_bytes(result.output_bytes))
    table.add_row("tables copied", str(result.table_count))
    table.add_row("elapsed", f"{result.elapsed_ms:.1f} ms")
    console.print(table)
    console.print(f"output: {result.output_path}")


def run_market_compact_command(
    *,
    db_path: Path | None,
    output_path: Path | None,
    overwrite: bool,
) -> None:
    try:
        source_path, resolved_output_path = _build_market_compact_paths(
            db_path=db_path,
            output_path=output_path,
        )
        result = compact_market_duckdb(
            source_path,
            resolved_output_path,
            overwrite=overwrite,
        )
    except Exception as exc:  # noqa: BLE001 - CLI should present maintenance errors compactly
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None

    _print_market_compaction_result(result)
