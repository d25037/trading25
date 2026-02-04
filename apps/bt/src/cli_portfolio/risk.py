"""
ポートフォリオリスク分析サブコマンド
"""

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from loguru import logger

from src.data.loaders.portfolio_loaders import (
    load_portfolio_stock_data,
    create_portfolio_returns_matrix,
    load_portfolio_code_name_mapping,
)
from src.analysis.portfolio_risk import analyze_portfolio_risk

console = Console()


def run_risk_analysis(
    portfolio_name: str,
    lookback_days: int = 252,
    confidence_level: float = 0.95,
    verbose: bool = False,
) -> None:
    """
    ポートフォリオリスク分析を実行・表示

    Args:
        portfolio_name: ポートフォリオ名
        lookback_days: 分析期間（営業日数）
        confidence_level: VaR信頼区間
        verbose: 詳細ログ表示フラグ
    """
    # logger制御: verbose=Falseの場合はloggerを無効化
    if not verbose:
        logger.disable("src.data.loaders.portfolio_loaders")
        logger.disable("src.analysis.portfolio_risk")

    console.print(
        f"\n[bold cyan]📊 ポートフォリオリスク分析: {portfolio_name}[/bold cyan]\n",
        style="bold",
    )

    try:
        # データロード
        console.print("[yellow]データロード中...[/yellow]")
        stock_data = load_portfolio_stock_data(
            portfolio_name,
            lookback_days=lookback_days,
        )

        if not stock_data:
            console.print(
                "[bold red]エラー:[/bold red] 株価データが取得できませんでした。"
            )
            return

        # 銘柄コード→会社名マッピング取得
        code_name_mapping = load_portfolio_code_name_mapping(portfolio_name)

        # リターン行列作成
        returns_df = create_portfolio_returns_matrix(stock_data)

        # リスク分析実行
        console.print("[yellow]リスク分析実行中...[/yellow]")
        results = analyze_portfolio_risk(
            returns_df, confidence_level=confidence_level
        )

        # 結果表示
        _display_risk_results(results, confidence_level, code_name_mapping)

    except Exception as e:
        console.print(f"[bold red]エラー:[/bold red] {e}")
        raise
    finally:
        # logger再有効化（他の処理への影響を防ぐ）
        if not verbose:
            logger.enable("src.data.loaders.portfolio_loaders")
            logger.enable("src.analysis.portfolio_risk")


def _display_risk_results(
    results: dict, confidence_level: float, code_name_mapping: dict[str, str]
) -> None:
    """
    リスク分析結果を表示

    Args:
        results: analyze_portfolio_risk()の返り値
        confidence_level: VaR信頼区間
        code_name_mapping: {銘柄コード: 会社名} の辞書
    """
    # 基本統計
    basic_stats = f"""
[bold]銘柄数:[/bold] {results['num_stocks']}
[bold]分析期間:[/bold] {results['num_days']} 営業日
[bold]ポートフォリオボラティリティ（年率）:[/bold] {results['portfolio_volatility']:.2%}
[bold]シャープレシオ:[/bold] {results['sharpe_ratio']:.4f}
[bold]VaR ({confidence_level*100:.0f}%):[/bold] {results['var']:.2%}
[bold]CVaR ({confidence_level*100:.0f}%):[/bold] {results['cvar']:.2%}
    """
    console.print(
        Panel(basic_stats.strip(), title="基本統計", border_style="cyan", box=box.ROUNDED)
    )

    # 分散効果指標
    div_metrics = results["diversification_metrics"]
    div_text = f"""
[bold]平均相関係数:[/bold] {div_metrics['avg_correlation']:.3f}
[bold]最大相関係数:[/bold] {div_metrics['max_correlation']:.3f}
[bold]最小相関係数:[/bold] {div_metrics['min_correlation']:.3f}
[bold]分散比率:[/bold] {div_metrics['diversification_ratio']:.3f} (>1で分散効果あり)
    """
    console.print(
        "\n",
        Panel(
            div_text.strip(),
            title="分散効果指標",
            border_style="green",
            box=box.ROUNDED,
        ),
    )

    # リスク寄与度 Top 10
    console.print("\n[bold magenta]リスク寄与度 Top 10[/bold magenta]")
    risk_contrib = results["risk_contribution"].sort_values(ascending=False).head(10)

    table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
    table.add_column("Rank", justify="right", style="cyan")
    table.add_column("Code", style="green")
    table.add_column("Risk Contribution", justify="right", style="red")

    for rank, (code, value) in enumerate(risk_contrib.items(), start=1):
        table.add_row(str(rank), code, f"{value:.2%}")

    console.print(table)

    # 相関係数行列
    corr_matrix = results["correlation_matrix"]
    console.print("\n[bold magenta]相関係数行列[/bold magenta]")

    corr_table = Table(show_header=True, header_style="bold cyan", box=box.SIMPLE)
    corr_table.add_column("", style="green", justify="right")

    # ヘッダー追加（銘柄コード）
    for code in corr_matrix.columns:
        corr_table.add_column(code, justify="right", style="yellow")

    # 各行を追加
    for row_code in corr_matrix.index:
        row_values = [row_code]
        for col_code in corr_matrix.columns:
            value = corr_matrix.loc[row_code, col_code]
            # 対角成分（自己相関）は白、それ以外は相関の強さで色分け
            if row_code == col_code:
                row_values.append(f"[white]{value:.3f}[/white]")
            elif value > 0.5:
                row_values.append(f"[red]{value:.3f}[/red]")
            elif value > 0.3:
                row_values.append(f"[yellow]{value:.3f}[/yellow]")
            else:
                row_values.append(f"[green]{value:.3f}[/green]")
        corr_table.add_row(*row_values)

    console.print(corr_table)
    console.print(
        "[dim]※赤: 強い正相関(>0.5), 黄: 中程度の相関(0.3-0.5), 緑: 弱い相関(<0.3)[/dim]\n"
    )

    # 銘柄一覧（コード→会社名）
    console.print("\n[bold magenta]銘柄一覧[/bold magenta]")
    name_table = Table(show_header=True, header_style="bold cyan", box=box.SIMPLE)
    name_table.add_column("Code", style="green")
    name_table.add_column("Company Name", style="white")

    for code in sorted(code_name_mapping.keys()):
        name_table.add_row(code, code_name_mapping[code])

    console.print(name_table)
    console.print()
