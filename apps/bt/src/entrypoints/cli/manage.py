"""
Management Command Module

Strategy management subcommands (list/validate/cleanup)
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table
from loguru import logger

from src.domains.strategy.runtime.loader import ConfigLoader

console = Console()


def list_strategies() -> None:
    """
    List available strategies by category (formerly execute-notebook list-strategies)
    """
    config_loader = ConfigLoader()
    categorized_strategies = config_loader.get_available_strategies()

    if not categorized_strategies:
        console.print("[yellow]No strategies found[/yellow]")
        return

    # Display by category
    for category, strategies in sorted(categorized_strategies.items()):
        # Category display name (don't display root)
        if category == "root":
            category_display = "Other"
        elif category == "production":
            category_display = "Production"
        elif category == "experimental":
            category_display = "Experimental"
        elif category == "reference":
            category_display = "Reference"
        else:
            category_display = f"{category}"

        # Create table for each category
        table = Table(title=category_display)
        table.add_column("Strategy Name", style="cyan")
        table.add_column("Display Name", style="magenta")
        table.add_column("Description", style="green")

        for strategy_name in strategies:
            try:
                config = config_loader.load_strategy_config(strategy_name)
                display_name = config.get("display_name", strategy_name)
                description = config.get("description", "")

                table.add_row(strategy_name, display_name, description)
            except Exception as e:
                table.add_row(strategy_name, "Error", f"Config load failed: {e}")

        console.print(table)
        console.print()  # Empty line between categories


def validate_strategy(strategy: str) -> None:
    """
    Validate strategy configuration (formerly execute-notebook validate)

    Args:
        strategy: Strategy name to validate
    """
    try:
        config_loader = ConfigLoader()
        strategy_config = config_loader.load_strategy_config(strategy)

        is_valid = config_loader.validate_strategy_config(strategy_config)

        if is_valid:
            console.print(f"[bold green]{strategy} configuration is valid[/bold green]")
        else:
            console.print(f"[bold red]{strategy} configuration has issues[/bold red]")
            raise SystemExit(1)
    except Exception as e:
        console.print(f"[bold red]Validation Error:[/bold red] {e}")
        raise SystemExit(1)


def cleanup_notebooks(days: int = 7, output_dir: Optional[str] = None) -> None:
    """
    Cleanup old HTML/Notebook files (formerly execute-notebook cleanup)

    Args:
        days: Delete files older than N days
        output_dir: Target directory
    """
    try:
        if output_dir:
            target_dir = Path(output_dir)
        else:
            from src.shared.paths import get_backtest_results_dir
            target_dir = get_backtest_results_dir()

        if not target_dir.exists():
            console.print("[yellow]Output directory does not exist[/yellow]")
            return

        # Validate days parameter
        if days < 1 or days > 365:
            raise ValueError(
                f"削除対象日数が無効です: {days} (1-365の範囲で指定してください)"
            )

        cutoff_date = datetime.now() - timedelta(days=days)
        deleted_count = 0

        # Find and delete old HTML and notebook files
        for pattern in ["*.html", "*.ipynb"]:
            for file_path in target_dir.rglob(pattern):
                # Verify path is within output directory
                file_resolved = file_path.resolve()
                target_resolved = target_dir.resolve()

                if not str(file_resolved).startswith(str(target_resolved)):
                    logger.warning(f"出力ディレクトリ外のファイルをスキップ: {file_path.name}")
                    continue

                # Check modification time
                if file_path.stat().st_mtime < cutoff_date.timestamp():
                    try:
                        file_path.unlink()
                        deleted_count += 1
                        logger.debug(f"古いファイルを削除: {file_path.relative_to(target_dir)}")
                    except Exception as e:
                        logger.error(f"ファイル削除エラー ({file_path.name}): {e}")

        if deleted_count > 0:
            console.print(f"[bold green]Deleted {deleted_count} files[/bold green]")
        else:
            console.print("[yellow]No files to delete[/yellow]")

    except Exception as e:
        console.print(f"[bold red]Cleanup Error:[/bold red] {e}")
        raise SystemExit(1)
