"""
Backtest Command Module

Backtest execution subcommand
"""

import time
import threading
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text
from loguru import logger

from src.shared.constants import THREAD_TIMEOUT_SECONDS
from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.domains.strategy.runtime.loader import ConfigLoader

console = Console()


def run_backtest(strategy: str) -> None:
    """
    Execute backtest

    Args:
        strategy: Strategy name (e.g., "range_break_v5", "production/range_break_v5")
    """
    try:
        # Initialize config loader
        config_loader = ConfigLoader()
        runner = BacktestRunner()

        # Load from strategy name
        try:
            strategy_config = config_loader.load_strategy_config(strategy)
        except ValueError as e:
            console.print(f"[bold red]Strategy Name Error:[/bold red] {e}")
            raise SystemExit(1)

        # Use parameters directly (complete with YAML config)
        # Base on default.yaml parameters, add entry_filter_params, exit_trigger_params
        parameters = config_loader.default_config.get("parameters", {}).copy()

        # Merge shared_config (default + strategy override)
        merged_shared_config = config_loader.merge_shared_config(strategy_config)
        parameters["shared_config"] = merged_shared_config

        # Add signal params
        if "entry_filter_params" in strategy_config:
            parameters["entry_filter_params"] = strategy_config["entry_filter_params"]
        if "exit_trigger_params" in strategy_config:
            parameters["exit_trigger_params"] = strategy_config["exit_trigger_params"]

        # Display execution info before running
        _display_execution_info(strategy_config, parameters)

        # Execute with progress spinner
        result, elapsed_time = _execute_runner_with_progress(
            runner=runner,
            strategy=strategy,
        )

        # Success message
        console.print("[bold green]Execution completed![/bold green]")
        console.print(
            f"[bold cyan]Metrics Artifact:[/bold cyan] "
            f"{result.metrics_path or 'N/A'}"
        )
        if result.html_path is not None:
            console.print(f"[bold cyan]HTML Output:[/bold cyan] {result.html_path}")
        elif result.render_error is not None:
            console.print(
                f"[bold yellow]Report Render Warning:[/bold yellow] {result.render_error}"
            )

        # Display summary
        summary = dict(result.summary)
        summary["execution_time"] = elapsed_time
        _display_execution_summary(summary)

    except Exception as e:
        console.print(f"[bold red]Execution Error:[/bold red] {e}")
        logger.error(f"CLI execution error: {e}")
        raise SystemExit(1)


def _execute_runner_with_progress(
    runner: BacktestRunner,
    strategy: str,
) -> Tuple[BacktestResult, float]:
    """Execute BacktestRunner with the same live progress display used by CLI."""

    result: Dict[str, Any] = {"value": None, "error": None}
    start_time = time.time()

    def run_execution() -> None:
        try:
            result["value"] = runner.execute(strategy=strategy, data_access_mode="direct")
        except Exception as e:
            result["error"] = e

    thread = threading.Thread(target=run_execution, daemon=True)
    thread.start()

    try:
        with Live(console=console, refresh_per_second=4) as live:
            while thread.is_alive():
                elapsed = time.time() - start_time
                minutes = int(elapsed // 60)
                seconds = elapsed % 60

                if minutes > 0:
                    time_str = f"{minutes}m {seconds:05.2f}s"
                else:
                    time_str = f"{seconds:05.2f}s"

                spinner = Spinner(
                    "dots",
                    text=Text.from_markup(
                        f"[bold blue]Executing backtest...[/bold blue] [cyan]{time_str}[/cyan]"
                    ),
                )
                live.update(spinner)
                time.sleep(0.1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user. Waiting for cleanup...[/yellow]")
        thread.join(timeout=THREAD_TIMEOUT_SECONDS)
        raise

    thread.join()
    elapsed_time = time.time() - start_time

    if result["error"]:
        raise result["error"]

    execution_result = result["value"]
    if not isinstance(execution_result, BacktestResult):
        raise RuntimeError("BacktestRunner did not return a BacktestResult")

    return execution_result, elapsed_time


def _display_execution_info(
    strategy_config: Dict[str, Any], parameters: Dict[str, Any]
) -> None:
    """Display execution info before running"""
    info_table = Table(title="Execution Settings", show_header=False)
    info_table.add_column("Item", style="bold cyan")
    info_table.add_column("Value", style="white")

    info_table.add_row("Strategy", strategy_config.get("display_name", ""))
    info_table.add_row("Description", strategy_config.get("description", ""))

    # Get info from shared_config
    shared_config = parameters.get("shared_config", {})
    stock_codes_display = shared_config.get("stock_codes", [])
    if isinstance(stock_codes_display, list):
        if len(stock_codes_display) == 1 and stock_codes_display[0] == "all":
            stock_codes_display = "all"
        elif len(stock_codes_display) > 5:
            stock_codes_display = f"{', '.join(stock_codes_display[:5])}... (total {len(stock_codes_display)} stocks)"
        else:
            stock_codes_display = ", ".join(stock_codes_display)
    info_table.add_row("Stocks", str(stock_codes_display))
    info_table.add_row("Initial Cash", f"{shared_config.get('initial_cash', 0):,}")
    info_table.add_row("Fees", f"{shared_config.get('fees', 0):.3f}")

    console.print(info_table)


def _display_execution_summary(summary: Dict[str, Any]) -> None:
    """Display execution summary for Marimo"""
    if "error" in summary:
        console.print(f"[red]Summary Error: {summary['error']}[/red]")
        return

    summary_table = Table(title="Execution Summary", show_header=False)
    summary_table.add_column("Item", style="bold cyan")
    summary_table.add_column("Value", style="white")

    # Execution time
    if summary.get("execution_time"):
        elapsed = summary["execution_time"]
        minutes = int(elapsed // 60)
        seconds = elapsed % 60
        if minutes > 0:
            time_str = f"{minutes}m {seconds:.2f}s"
        else:
            time_str = f"{seconds:.2f}s"
        summary_table.add_row("Execution Time", time_str)

    summary_table.add_row("Metrics Path", str(summary.get("metrics_path", "N/A")))
    summary_table.add_row("HTML Path", str(summary.get("html_path", "N/A")))

    file_size = summary.get("file_size", 0)
    if file_size > 0:
        file_size_kb = file_size / 1024
        summary_table.add_row("File Size", f"{file_size_kb:.1f} KB")

    summary_table.add_row("Generated At", str(summary.get("generated_at", "N/A")))
    if summary.get("render_status") == "failed":
        summary_table.add_row("Status", "Success (render warning)")
    else:
        summary_table.add_row("Status", "Success")

    console.print(summary_table)
