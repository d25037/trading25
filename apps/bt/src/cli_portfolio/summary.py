"""
ポートフォリオサマリー表示サブコマンド
"""

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.data.loaders.portfolio_loaders import load_portfolio_summary

console = Console()


def run_summary(portfolio_name: str) -> None:
    """
    ポートフォリオサマリーを表示

    Args:
        portfolio_name: ポートフォリオ名
    """
    console.print(
        f"\n[bold cyan]📊 ポートフォリオサマリー: {portfolio_name}[/bold cyan]\n",
        style="bold",
    )

    try:
        # サマリー取得
        summary = load_portfolio_summary(portfolio_name)

        # ポートフォリオ基本情報
        info_text = f"""
[bold]Name:[/bold] {summary.portfolio.name}
[bold]Description:[/bold] {summary.portfolio.description or "-"}
[bold]Created:[/bold] {summary.portfolio.created_at}
[bold]Updated:[/bold] {summary.portfolio.updated_at}
[bold]Total Stocks:[/bold] {summary.total_stocks}
[bold]Total Cost:[/bold] ¥{summary.total_cost:,.0f}
        """

        console.print(
            Panel(info_text.strip(), title="Portfolio Information", border_style="cyan")
        )

        # 保有銘柄一覧
        if summary.items:
            table = Table(
                title="Holdings", show_header=True, header_style="bold magenta"
            )
            table.add_column("Code", justify="right", style="cyan")
            table.add_column("Company", style="green")
            table.add_column("Quantity", justify="right", style="yellow")
            table.add_column("Purchase Price", justify="right", style="blue")
            table.add_column("Total Cost", justify="right", style="red")
            table.add_column("Purchase Date", style="white")

            for item in summary.items:
                table.add_row(
                    item.code,
                    item.company_name,
                    f"{item.quantity:,}",
                    f"¥{item.purchase_price:,.0f}",
                    f"¥{item.total_cost:,.0f}",
                    str(item.purchase_date),
                )

            console.print("\n")
            console.print(table)
            console.print()

    except ValueError as e:
        console.print(f"[bold red]エラー:[/bold red] {e}")
        raise
