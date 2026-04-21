"""
Backtest Command Module

Backtest execution subcommand
"""

import threading
import time
from typing import Any, Dict, Tuple, cast

from loguru import logger
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from src.shared.constants import THREAD_TIMEOUT_SECONDS
from src.domains.backtest.core import BacktestResult, BacktestRunner
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.production_requirements import (
    validate_production_strategy_dataset_requirement,
)

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

        category_resolver = getattr(config_loader, "resolve_strategy_category", None)
        resolved_category = category_resolver(strategy) if callable(category_resolver) else None
        validate_production_strategy_dataset_requirement(
            category=resolved_category if isinstance(resolved_category, str) else None,
            config=strategy_config,
            strategy_name=strategy,
        )

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
        runner = BacktestRunner()
        result, elapsed_time = _execute_with_progress(runner=runner, strategy=strategy)

        # Success message
        console.print("[bold green]Execution completed![/bold green]")
        html_path = result.html_path
        if html_path is None:
            console.print("[bold yellow]HTML Output:[/bold yellow] not generated")
        else:
            console.print(f"[bold cyan]HTML Output:[/bold cyan] {html_path}")

        # Display summary
        summary = dict(result.summary)
        summary["execution_time"] = elapsed_time
        if html_path is not None:
            summary["html_path"] = str(html_path)
            if html_path.exists():
                summary["file_size"] = html_path.stat().st_size
        _display_execution_summary(summary)

    except Exception as e:
        console.print(f"[bold red]Execution Error:[/bold red] {e}")
        logger.error(f"CLI execution error: {e}")
        raise SystemExit(1)


def _execute_with_progress(
    runner: BacktestRunner,
    strategy: str,
) -> Tuple[BacktestResult, float]:
    """
    Execute backtest with live progress display

    Args:
        runner: BacktestRunner instance
        strategy: Strategy name

    Returns:
        Tuple of (backtest result, elapsed_time)

    Raises:
        Exception: Re-raises any exception from the execution thread
        KeyboardInterrupt: If user interrupts during execution
    """
    result: Dict[str, Any] = {"backtest_result": None, "error": None}
    progress_state = {"status": "Executing backtest..."}
    start_time = time.time()

    def update_progress(status: str, _elapsed: float) -> None:
        progress_state["status"] = status

    def run_execution() -> None:
        try:
            result["backtest_result"] = runner.execute(
                strategy,
                progress_callback=update_progress,
                data_access_mode="direct",
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

                spinner = Spinner(
                    "dots",
                    text=Text.assemble(
                        (progress_state["status"], "bold blue"),
                        " ",
                        (time_str, "cyan"),
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

    backtest_result = result["backtest_result"]
    if backtest_result is None:
        raise RuntimeError("Backtest finished without a result")

    return cast(BacktestResult, backtest_result), elapsed_time


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
    """Display execution summary."""
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

    if summary.get("report_status"):
        summary_table.add_row("Report Status", str(summary["report_status"]))
    if summary.get("_render_error"):
        summary_table.add_row("Render Error", str(summary["_render_error"]))

    summary_table.add_row("Generated At", str(summary.get("generated_at", "N/A")))
    summary_table.add_row("Status", "Success")

    console.print(summary_table)
