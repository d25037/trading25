"""
パラメータ最適化CLIコマンド

戦略のパラメータ最適化を実行します。
"""

import sys

from rich.console import Console
from rich.table import Table

from src.domains.optimization.engine import ParameterOptimizationEngine

console = Console()


def run_optimization(
    strategy_name: str,
    verbose: bool = False,
) -> None:
    """
    パラメータ最適化実行

    指定された戦略のパラメータをグリッドサーチで最適化します。

    Args:
        strategy_name: 最適化する戦略名（例: range_break_v6）
        verbose: 詳細ログ出力（データローダー等のinfo/debugログを表示）
    """
    try:
        # ヘッダー表示
        console.print()
        console.print("=" * 60, style="bold blue")
        console.print(
            f"🚀 パラメータ最適化開始: [bold cyan]{strategy_name}[/bold cyan]",
            style="bold",
        )
        console.print("=" * 60, style="bold blue")
        console.print()

        # エンジン初期化（grid_config は常に自動推測）
        console.print("📋 設定読み込み中...", style="yellow")
        engine = ParameterOptimizationEngine(
            strategy_name, grid_config_path=None, verbose=verbose
        )

        console.print(f"  ✓ ベース戦略: {engine.base_config_path}", style="green")
        console.print(
            f"  ✓ 並列処理数: {engine.optimization_config['n_jobs']}", style="green"
        )
        console.print(f"  ✓ 組み合わせ総数: {engine.total_combinations:,}", style="green")
        console.print()

        # 最適化実行
        console.print("🔍 グリッドサーチ実行中...\n", style="yellow")
        result = engine.optimize()

        # 完了メッセージ
        console.print()
        console.print("=" * 60, style="bold green")
        console.print("✅ 最適化完了!", style="bold green")
        console.print("=" * 60, style="bold green")
        console.print()

        # Top 10 ランキング表示
        _display_ranking(result, top_n=10)

        # 最適パラメータ詳細表示
        _display_best_params(result)

        # HTMLパス表示
        _display_html_path(result)

        # 成功終了
        console.print()
        console.print("✨ 最適化が正常に完了しました。", style="bold green")
        console.print()

    except FileNotFoundError as e:
        console.print(f"❌ ファイルが見つかりません: {e}", style="bold red")
        console.print(
            "\n💡 ヒント: config/optimization/ ディレクトリに {strategy_name}_grid.yaml が存在することを確認してください。",
            style="yellow",
        )
        sys.exit(1)
    except ValueError as e:
        console.print(f"❌ 設定エラー: {e}", style="bold red")
        console.print(
            "\n💡 ヒント: グリッドYAMLファイルのparameter_ranges設定を確認してください。",
            style="yellow",
        )
        sys.exit(1)
    except RuntimeError as e:
        console.print(f"❌ 実行エラー: {e}", style="bold red")
        console.print(
            "\n💡 ヒント: 戦略設定YAMLとグリッドYAMLの整合性を確認してください。",
            style="yellow",
        )
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n⚠️  最適化が中断されました。", style="yellow")
        sys.exit(130)
    except Exception as e:
        console.print(f"❌ 予期しないエラーが発生しました: {e}", style="bold red")
        import traceback

        traceback.print_exc()
        console.print(
            "\n💡 詳細なログは上記のトレースバックを確認してください。", style="yellow"
        )
        sys.exit(1)


def _display_ranking(result, top_n: int = 10):
    """
    最適化結果ランキングを表示

    Args:
        result: OptimizationResult
        top_n: 表示する上位件数
    """
    # ランキング表
    table = Table(
        title=f"📊 最適化結果ランキング (Top {top_n})",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Rank", style="cyan", width=6, justify="right")
    table.add_column("Score", style="green", width=10, justify="right")
    table.add_column("Parameters", style="yellow")

    for i, r in enumerate(result.all_results[:top_n], 1):
        # ランク表示（1位はメダル）
        if i == 1:
            rank_str = "🥇 1"
        elif i == 2:
            rank_str = "🥈 2"
        elif i == 3:
            rank_str = "🥉 3"
        else:
            rank_str = str(i)

        # パラメータ文字列
        params_str = _format_params_table(r["params"])

        table.add_row(rank_str, f"{r['score']:.4f}", params_str)

    console.print(table)
    console.print()


def _display_best_params(result):
    """
    最適パラメータ詳細を表示

    Args:
        result: OptimizationResult
    """
    console.print("=" * 60, style="bold yellow")
    console.print("🥇 最適パラメータ詳細", style="bold yellow")
    console.print("=" * 60, style="bold yellow")
    console.print()

    # 最適スコア
    console.print(f"  複合スコア: [bold green]{result.best_score:.4f}[/bold green]")
    console.print()

    # スコアリング重み
    console.print("  スコアリング重み:", style="cyan")
    for metric, weight in result.scoring_weights.items():
        console.print(f"    • {metric}: {weight}", style="dim")
    console.print()

    # 最適パラメータ
    console.print("  最適パラメータ:", style="cyan")
    for key, value in result.best_params.items():
        # ネスト構造を読みやすく
        display_key = key.replace("entry_filter_params.", "").replace(
            "exit_trigger_params.", ""
        )
        console.print(f"    • {display_key}: [bold]{value}[/bold]", style="dim")
    console.print()

    # パフォーマンス指標
    console.print("  パフォーマンス指標:", style="cyan")
    try:
        portfolio = result.best_portfolio

        # Sharpe Ratio
        sharpe = portfolio.sharpe_ratio()
        console.print(f"    • Sharpe Ratio: [bold]{sharpe:.4f}[/bold]", style="dim")

        # Calmar Ratio
        calmar = portfolio.calmar_ratio()
        console.print(f"    • Calmar Ratio: [bold]{calmar:.4f}[/bold]", style="dim")

        # Total Return
        total_return = portfolio.total_return()
        console.print(
            f"    • Total Return: [bold]{total_return:.2%}[/bold]", style="dim"
        )

        # Max Drawdown
        max_dd = portfolio.max_drawdown()
        console.print(f"    • Max Drawdown: [bold]{max_dd:.2%}[/bold]", style="dim")

    except Exception as e:
        console.print(f"    （指標取得エラー: {e}）", style="dim red")

    console.print()


def _display_html_path(result):
    """
    可視化HTMLパスを表示

    Args:
        result: OptimizationResult
    """
    console.print()
    console.print("=" * 60, style="bold cyan")
    console.print("📊 可視化HTML生成完了", style="bold cyan")
    console.print("=" * 60, style="bold cyan")
    console.print()

    console.print(f"  📓 {result.html_path}", style="green")
    console.print()


def _format_params_table(params: dict) -> str:
    """
    パラメータを読みやすくフォーマット（テーブル表示用）

    Args:
        params: パラメータ辞書

    Returns:
        str: フォーマット済み文字列
    """
    formatted = []
    for key, value in params.items():
        # ネストを簡略化（entry_filter_params.period_breakout.period → period=100）
        short_key = key.split(".")[-1]
        formatted.append(f"{short_key}={value}")

    return ", ".join(formatted)
