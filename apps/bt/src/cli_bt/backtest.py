"""
Backtest Command Module

Backtest execution subcommand
"""

import time
import threading
from pathlib import Path
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text
from loguru import logger

from src.constants import THREAD_TIMEOUT_SECONDS
from src.strategy_config.loader import ConfigLoader

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

        # Load from strategy name
        try:
            strategy_config = config_loader.load_strategy_config(strategy)
        except ValueError as e:
            console.print(f"[bold red]Strategy Name Error:[/bold red] {e}")
            raise SystemExit(1)

        # Use parameters directly (complete with YAML config)
        # Base on default.yaml parameters, add entry_filter_params, exit_trigger_params
        parameters = config_loader.default_config.get("parameters", {}).copy()

        # Extract strategy name from path (production/range_break_v5 -> range_break_v5)
        strategy_name_only = strategy.split("/")[-1]

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

        # Initialize executor based on mode
        executor_output_dir = config_loader.get_output_directory(strategy_config)

        # Marimo execution (HTML output)
        from src.backtest.marimo_executor import MarimoExecutor

        executor = MarimoExecutor(str(executor_output_dir))
        template_path = "notebooks/templates/marimo/strategy_analysis.py"

        # Execute with progress spinner
        html_path, elapsed_time = _execute_with_progress(
            executor=executor,
            template_path=template_path,
            parameters=parameters,
            strategy_name=strategy_name_only,
        )

        # Success message
        console.print("[bold green]Execution completed![/bold green]")
        console.print(f"[bold cyan]HTML Output:[/bold cyan] {html_path}")

        # Display summary
        summary = executor.get_execution_summary(html_path)
        summary["execution_time"] = elapsed_time
        _display_execution_summary(summary)

    except Exception as e:
        console.print(f"[bold red]Execution Error:[/bold red] {e}")
        logger.error(f"CLI execution error: {e}")
        raise SystemExit(1)


def _execute_with_progress(
    executor: Any,
    template_path: str,
    parameters: Dict[str, Any],
    strategy_name: str,
) -> Tuple[Path, float]:
    """
    Execute notebook with live progress display

    Args:
        executor: MarimoExecutor instance
        template_path: Path to template notebook
        parameters: Execution parameters
        strategy_name: Strategy name

    Returns:
        Tuple of (output_path, elapsed_time)

    Raises:
        Exception: Re-raises any exception from the execution thread
        KeyboardInterrupt: If user interrupts during execution
    """
    result: Dict[str, Any] = {"path": None, "error": None}
    start_time = time.time()

    def run_execution() -> None:
        try:
            result["path"] = executor.execute_notebook(
                template_path=template_path,
                parameters=parameters,
                strategy_name=strategy_name,
            )
        except Exception as e:
            result["error"] = e

    # Start execution in background thread (daemon=True for clean exit on interrupt)
    thread = threading.Thread(target=run_execution, daemon=True)
    thread.start()

    # Show progress with live display
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

                spinner = Spinner("dots", text=Text.from_markup(
                    f"[bold blue]Executing backtest...[/bold blue] [cyan]{time_str}[/cyan]"
                ))
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

    return result["path"], elapsed_time


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

    summary_table.add_row("HTML Path", str(summary.get("html_path", "N/A")))

    file_size = summary.get("file_size", 0)
    if file_size > 0:
        file_size_kb = file_size / 1024
        summary_table.add_row("File Size", f"{file_size_kb:.1f} KB")

    summary_table.add_row("Generated At", str(summary.get("generated_at", "N/A")))
    summary_table.add_row("Status", "Success")

    console.print(summary_table)
