"""
ポートフォリオ一覧表示サブコマンド
"""

from rich.console import Console
from rich.table import Table

from src.data.loaders.portfolio_loaders import load_portfolio_list

console = Console()


def run_list() -> None:
    """
    ポートフォリオ一覧を表示
    """
    console.print(
        "\n[bold cyan]📊 ポートフォリオ一覧[/bold cyan]\n", style="bold"
    )

    # ポートフォリオ一覧取得
    portfolios_df = load_portfolio_list()

    if portfolios_df.empty:
        console.print("[yellow]ポートフォリオが見つかりませんでした。[/yellow]")
        return

    # Rich Table作成
    table = Table(title="Portfolio List", show_header=True, header_style="bold magenta")
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Stocks", justify="right", style="white")
    table.add_column("Shares", justify="right", style="white")
    table.add_column("Created", style="blue")

    for _, row in portfolios_df.iterrows():
        table.add_row(
            str(row["id"]),
            str(row["name"]),
            str(row.get("stockCount", 0)),
            str(row.get("totalShares", 0)),
            str(row["createdAt"])[:10],  # 日付部分のみ
        )

    console.print(table)
    console.print(f"\n[bold]合計: {len(portfolios_df)} ポートフォリオ[/bold]\n")
