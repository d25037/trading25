"""
Portfolio Analysis CLI

ポートフォリオ分析コマンドラインインターフェース
"""

import typer

# Typerアプリ定義
app = typer.Typer(
    name="portfolio",
    help="💼 ポートフォリオ分析ツール",
    rich_markup_mode="rich",
    add_completion=False,
)


# list サブコマンドの登録
@app.command(name="list")
def list_command():
    """
    ポートフォリオ一覧表示

    Examples:
        uv run portfolio list
    """
    from src.cli_portfolio.list import run_list

    run_list()


# summary サブコマンドの登録
@app.command(name="summary")
def summary_command(
    portfolio_name: str = typer.Argument(..., help="ポートフォリオ名"),
):
    """
    ポートフォリオサマリー表示

    Examples:
        uv run portfolio summary RangeBreakSlow
    """
    from src.cli_portfolio.summary import run_summary

    run_summary(portfolio_name=portfolio_name)


# risk サブコマンドの登録
@app.command(name="risk")
def risk_command(
    portfolio_name: str = typer.Argument(..., help="ポートフォリオ名"),
    lookback_days: int = typer.Option(
        252,
        "--lookback-days",
        "-l",
        min=30,
        max=1000,
        help="分析期間（営業日数、30-1000の範囲）",
    ),
    confidence_level: float = typer.Option(
        0.95,
        "--confidence",
        "-c",
        min=0.9,
        max=0.99,
        help="VaR信頼区間（0.9-0.99の範囲）",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="詳細ログを表示",
    ),
):
    """
    ポートフォリオリスク分析

    相関係数・VaR・分散寄与度等のリスク指標を計算します。

    Examples:
        uv run portfolio risk RangeBreakSlow
        uv run portfolio risk RangeBreakSlow --lookback-days 500
        uv run portfolio risk RangeBreakSlow --confidence 0.99
    """
    from src.cli_portfolio.risk import run_risk_analysis

    run_risk_analysis(
        portfolio_name=portfolio_name,
        lookback_days=lookback_days,
        confidence_level=confidence_level,
        verbose=verbose,
    )


# pca サブコマンドの登録
@app.command(name="pca")
def pca_command(
    portfolio_name: str = typer.Argument(..., help="ポートフォリオ名"),
    lookback_days: int = typer.Option(
        252,
        "--lookback-days",
        "-l",
        min=30,
        max=1000,
        help="分析期間（営業日数、30-1000の範囲）",
    ),
    n_components: int = typer.Option(
        None,
        "--n-components",
        "-n",
        min=2,
        help="抽出する主成分数（指定しない場合は全成分）",
    ),
    topix_regression: bool = typer.Option(
        False,
        "--topix-regression",
        "-r",
        help="TOPIX回帰分析を実行（各PCのTOPIX感応度・R²値を計算）",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="詳細ログを表示",
    ),
):
    """
    主成分分析（PCA）

    ポートフォリオ銘柄の共変動構造を主成分分析で分解します。

    Examples:
        uv run portfolio pca RangeBreakSlow
        uv run portfolio pca RangeBreakSlow --n-components 5
        uv run portfolio pca RangeBreakSlow --topix-regression
    """
    from src.cli_portfolio.pca import run_pca_analysis

    run_pca_analysis(
        portfolio_name=portfolio_name,
        lookback_days=lookback_days,
        n_components=n_components,
        topix_regression=topix_regression,
        verbose=verbose,
    )


if __name__ == "__main__":
    app()
