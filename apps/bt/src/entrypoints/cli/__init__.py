"""
Unified CLI Interface for Backtesting Tool

バックテスト戦略管理の統一CLIインターフェース
"""

from copy import deepcopy
import sys
from typing import Any

import typer
from loguru import logger
from rich.console import Console

# サブコマンドの遅延インポート（循環参照回避）
console = Console()

app = typer.Typer(
    name="bt",
    help="📊 バックテスト戦略管理ツール",
    rich_markup_mode="rich",
    add_completion=False,
)


def configure_logging(verbose: bool) -> None:
    """
    グローバルログ設定

    Args:
        verbose: True=DEBUG以上、False=WARNING以上
    """
    logger.remove()
    if verbose:
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
            "<level>{message}</level>"
        )
        logger.add(sys.stderr, level="DEBUG", format=log_format)
    else:
        logger.add(
            sys.stderr,
            level="WARNING",
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<level>{message}</level>"
            ),
        )


@app.callback()
def main(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="詳細ログ出力"),
) -> None:
    """📊 バックテスト戦略管理ツール"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    configure_logging(verbose)


# バックテスト実行サブコマンド
@app.command(name="backtest")
def backtest_command(
    ctx: typer.Context,
    strategy: str = typer.Argument(..., help="戦略名 (例: production/range_break_v5)"),
    optimize: bool = typer.Option(False, "--optimize", "-O", help="パラメータ最適化モード"),
):
    """
    バックテスト実行

    Marimoテンプレートを実行してバックテストを行います。
    --optimize フラグでパラメータ最適化モードに切り替えます。

    Examples:
        uv run bt -v backtest production/range_break_v5
        uv run bt backtest range_break_v6 --optimize
        uv run bt -v backtest range_break_v6 -O
    """
    verbose = ctx.obj.get("verbose", False) if ctx.obj else False
    if optimize:
        # 最適化モード
        from src.entrypoints.cli.optimize import run_optimization

        run_optimization(strategy_name=strategy, verbose=verbose)
    else:
        # 通常バックテストモード
        from src.entrypoints.cli.backtest import run_backtest

        run_backtest(strategy=strategy)


# 戦略一覧表示
@app.command(name="list")
def list_command():
    """
    利用可能な戦略の一覧をカテゴリ別に表示

    Examples:
        uv run bt list
    """
    from src.entrypoints.cli.manage import list_strategies

    list_strategies()


# 設定検証
@app.command(name="validate")
def validate_command(
    strategy: str = typer.Argument(..., help="検証する戦略名"),
):
    """
    戦略設定の妥当性をチェック

    Examples:
        uv run bt validate production/range_break_v5
    """
    from src.entrypoints.cli.manage import validate_strategy

    validate_strategy(strategy)


# クリーンアップ
@app.command(name="cleanup")
def cleanup_command(
    days: int = typer.Option(7, "--days", "-d", help="削除対象の日数"),
    output_dir: str = typer.Option(None, "--output-dir", help="対象ディレクトリ"),
):
    """
    古いHTMLファイルをクリーンアップ

    Examples:
        uv run bt cleanup
        uv run bt cleanup --days 30
    """
    from src.entrypoints.cli.manage import cleanup_notebooks

    cleanup_notebooks(days=days, output_dir=output_dir)


def _kill_process_on_port(port: int) -> bool:
    """指定ポートを使用しているプロセスをkillする

    Args:
        port: 対象ポート番号

    Returns:
        プロセスをkillした場合True
    """
    import subprocess

    try:
        # ポートを使用しているプロセスのPIDを取得
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True,
        )
        pids = result.stdout.strip()

        if pids:
            for pid in pids.split("\n"):
                if pid:
                    subprocess.run(["kill", "-9", pid], check=False)
            return True
    except Exception:
        pass
    return False


def _build_uvicorn_log_config() -> dict[str, Any]:
    """Uvicorn ログ設定に時刻表示を追加する"""
    import uvicorn

    log_config: dict[str, Any] = deepcopy(uvicorn.config.LOGGING_CONFIG)
    timestamp_prefix = "%(asctime)s.%(msecs)03d | "

    log_config["formatters"]["default"]["fmt"] = (
        f"{timestamp_prefix}%(levelprefix)s %(message)s"
    )
    log_config["formatters"]["default"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    log_config["formatters"]["access"]["fmt"] = (
        f'{timestamp_prefix}%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
    )
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    return log_config


# API サーバー起動
@app.command(name="server")
def server_command(
    port: int = typer.Option(3002, "--port", "-p", help="サーバーポート"),
    host: str = typer.Option("0.0.0.0", "--host", "-H", help="ホストアドレス"),
    reload: bool = typer.Option(False, "--reload", "-r", help="開発モード（ホットリロード）"),
):
    """
    FastAPI サーバーを起動（バックテストAPI）

    trading25-ts からバックテストを実行するためのREST APIを提供します。

    Examples:
        uv run bt server
        uv run bt server --port 3002
        uv run bt server --reload
    """
    import time

    import uvicorn

    # ポートがすでに使用中の場合はkill
    if _kill_process_on_port(port):
        console.print(f"[yellow]ポート {port} を使用中のプロセスを終了しました[/yellow]")
        time.sleep(0.5)  # プロセス終了を待機

    console.print("[green]🚀 trading25-bt API サーバーを起動中...[/green]")
    console.print(f"[cyan]   URL: http://{host}:{port}[/cyan]")
    console.print(f"[cyan]   Docs: http://{host}:{port}/doc[/cyan]")

    uvicorn.run(
        "src.entrypoints.http.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
        log_config=_build_uvicorn_log_config(),
    )


# ラボサブコマンド登録
from src.entrypoints.cli.lab import lab_app  # noqa: E402

app.add_typer(lab_app, name="lab")


if __name__ == "__main__":
    app()
