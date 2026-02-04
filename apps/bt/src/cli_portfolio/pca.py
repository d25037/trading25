"""
主成分分析（PCA）サブコマンド
"""

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from loguru import logger

from src.constants import (
    P_VALUE_HIGHLY_SIGNIFICANT,
    P_VALUE_SIGNIFICANT,
    P_VALUE_VERY_SIGNIFICANT,
)
from src.data.loaders.portfolio_loaders import (
    load_portfolio_stock_data,
    create_portfolio_returns_matrix,
    load_portfolio_code_name_mapping,
)
from src.analysis.portfolio_pca import perform_full_pca_analysis

console = Console()


def run_pca_analysis(
    portfolio_name: str,
    lookback_days: int = 252,
    n_components: int | None = None,
    topix_regression: bool = False,
    verbose: bool = False,
) -> None:
    """
    主成分分析（PCA）を実行・表示

    Args:
        portfolio_name: ポートフォリオ名
        lookback_days: 分析期間（営業日数）
        n_components: 抽出する主成分数
        topix_regression: TOPIX回帰分析を実行（各PCのTOPIX感応度・R²値を計算）
        verbose: 詳細ログ表示フラグ
    """
    # logger制御: verbose=Falseの場合はloggerを無効化
    if not verbose:
        logger.disable("src.data.loaders.portfolio_loaders")
        logger.disable("src.analysis.portfolio_pca")
        logger.disable("src.analysis.portfolio_regression")
        logger.disable("src.data.loaders.index_loaders")

    console.print(
        f"\n[bold cyan]📊 主成分分析（PCA）: {portfolio_name}[/bold cyan]\n",
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

        # PCA実行
        console.print("[yellow]主成分分析実行中...[/yellow]")
        results = perform_full_pca_analysis(
            returns_df, n_components=n_components
        )

        # 結果表示
        _display_pca_results(results, code_name_mapping)

        # TOPIX回帰分析（オプション）
        if topix_regression:
            console.print("\n[bold cyan]📈 TOPIX回帰分析実行中...[/bold cyan]")
            try:
                from src.data.loaders.index_loaders import (
                    load_topix_data_from_market_db,
                )
                from src.analysis.portfolio_regression import (
                    calculate_benchmark_returns,
                    analyze_pcs_vs_benchmark,
                )

                # TOPIXデータロード
                topix_df = load_topix_data_from_market_db()
                topix_returns = calculate_benchmark_returns(
                    topix_df, price_column="Close"
                )

                # 回帰分析実行（上位5主成分のみ）
                regression_results = analyze_pcs_vs_benchmark(
                    results["principal_components"],
                    topix_returns,
                    max_components=5,
                )

                # 結果表示
                if regression_results:
                    _display_regression_results(regression_results)
                else:
                    console.print(
                        "[bold yellow]警告:[/bold yellow] 全ての主成分で回帰分析が失敗しました"
                    )

            except Exception as e:
                console.print(
                    f"[bold yellow]警告:[/bold yellow] TOPIX回帰分析がスキップされました: {e}"
                )
                if verbose:
                    raise

    except Exception as e:
        console.print(f"[bold red]エラー:[/bold red] {e}")
        raise
    finally:
        # logger再有効化（他の処理への影響を防ぐ）
        if not verbose:
            logger.enable("src.data.loaders.portfolio_loaders")
            logger.enable("src.analysis.portfolio_pca")
            logger.enable("src.analysis.portfolio_regression")
            logger.enable("src.data.loaders.index_loaders")


def _display_pca_results(results: dict, code_name_mapping: dict[str, str]) -> None:
    """
    PCA分析結果を表示

    Args:
        results: perform_full_pca_analysis()の返り値
        code_name_mapping: {銘柄コード: 会社名} の辞書
    """
    # 基本統計
    basic_stats = f"""
[bold]抽出主成分数:[/bold] {results['n_components']}
[bold]累積分散説明率（全成分）:[/bold] {results['cumulative_variance_ratio'].iloc[-1]:.2%}
[bold]第1主成分寄与率:[/bold] {results['explained_variance_ratio'].iloc[0]:.2%}
    """
    console.print(
        Panel(basic_stats.strip(), title="基本統計", border_style="cyan", box=box.ROUNDED)
    )

    # 分散説明率（上位10成分）
    console.print("\n[bold magenta]分散説明率（上位10主成分）[/bold magenta]")
    explained_var = results["explained_variance_ratio"].head(10)
    cumulative_var = results["cumulative_variance_ratio"].head(10)

    table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
    table.add_column("PC", justify="right", style="cyan")
    table.add_column("Explained Variance", justify="right", style="green")
    table.add_column("Cumulative", justify="right", style="blue")

    for pc, (exp_var, cum_var) in enumerate(
        zip(explained_var.values, cumulative_var.values), start=1
    ):
        table.add_row(f"PC{pc}", f"{exp_var:.2%}", f"{cum_var:.2%}")

    console.print(table)

    # 分散効果スコア
    div_score = results["diversification_score"]
    div_text = f"""
[bold]80%分散説明に必要な主成分数:[/bold] {div_score['n_components_for_threshold']}
[bold]分散スコア（第1主成分寄与率）:[/bold] {div_score['diversification_score']:.2%}
[dim]※分散スコアが低いほど分散が効いています（30%未満が理想的）[/dim]
    """
    console.print(
        "\n",
        Panel(
            div_text.strip(),
            title="分散効果評価",
            border_style="green",
            box=box.ROUNDED,
        ),
    )

    # 主成分への上位貢献銘柄
    console.print("\n[bold magenta]各主成分への上位貢献銘柄（Top 5）[/bold magenta]")

    for pc_name, top_stocks in results["top_contributors_per_pc"].items():
        table = Table(
            title=pc_name,
            show_header=True,
            header_style="bold cyan",
            box=box.SIMPLE,
        )
        table.add_column("Code", style="green")
        table.add_column("Loading", justify="right", style="yellow")

        for code, loading in top_stocks.items():
            # 正の寄与は赤、負の寄与は青で表示
            color = "red" if loading > 0 else "blue"
            table.add_row(code, f"[{color}]{loading:.4f}[/{color}]")

        console.print(table)
        console.print()

    console.print(
        "[dim]※詳細な主成分負荷量・時系列プロットは可視化Notebookで確認できます。[/dim]\n"
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


def _display_regression_results(results: dict) -> None:
    """
    TOPIX回帰分析結果を表示

    Args:
        results: analyze_pcs_vs_benchmark()の返り値（Dict[str, RegressionResult]）
    """
    from src.analysis.portfolio_regression import RegressionResult

    console.print("\n[bold magenta]TOPIX回帰分析結果[/bold magenta]")

    table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
    table.add_column("PC", justify="right", style="cyan")
    table.add_column("Correlation", justify="right", style="green")
    table.add_column("Beta (β)", justify="right", style="yellow")
    table.add_column("R²", justify="right", style="blue")
    table.add_column("Alpha (α)", justify="right", style="white")
    table.add_column("p-value", justify="right", style="dim")
    table.add_column("Significance", justify="center", style="bold")

    for pc_name in sorted(results.keys()):
        result: RegressionResult = results[pc_name]

        # 統計的有意性マーク
        if result.p_value < P_VALUE_HIGHLY_SIGNIFICANT:
            sig_mark = "***"
        elif result.p_value < P_VALUE_VERY_SIGNIFICANT:
            sig_mark = "**"
        elif result.p_value < P_VALUE_SIGNIFICANT:
            sig_mark = "*"
        else:
            sig_mark = ""

        # β係数の色分け（正: 赤、負: 青）
        beta_color = "red" if result.beta > 0 else "blue"

        table.add_row(
            pc_name,
            f"{result.correlation:+.4f}",
            f"[{beta_color}]{result.beta:+.4f}[/{beta_color}]",
            f"{result.r_squared:.4f}",
            f"{result.alpha:+.4e}",
            f"{result.p_value:.4e}",
            f"[green]{sig_mark}[/green]" if sig_mark else "[dim]n.s.[/dim]",
        )

    console.print(table)

    # 凡例
    console.print(
        "\n[dim]※ Beta (β): TOPIX感応度（TOPIX 1%変動時のPC変動率）[/dim]"
    )
    console.print("[dim]※ R²: TOPIXで説明できるPCの分散比率（0〜1）[/dim]")
    console.print(
        "[dim]※ Significance: *** p<0.001, ** p<0.01, * p<0.05[/dim]\n"
    )
