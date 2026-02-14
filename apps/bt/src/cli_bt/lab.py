"""
戦略ラボCLIコマンド

戦略の自動生成・最適化・改善を行うCLIインターフェース
"""

from typing import cast

import typer
from rich.console import Console
from rich.table import Table

from src.agent.models import SignalCategory

console = Console()

lab_app = typer.Typer(
    name="lab",
    help="🧪 戦略自動生成・改善ラボ",
    rich_markup_mode="rich",
)

VALID_SIGNAL_CATEGORIES: tuple[SignalCategory, ...] = (
    "breakout",
    "trend",
    "oscillator",
    "volatility",
    "volume",
    "macro",
    "fundamental",
    "sector",
)


def _resolve_allowed_categories(allowed_category: list[str] | None) -> list[SignalCategory]:
    """カテゴリ制約を検証して正規化する。"""
    allowed_categories_raw = [c.lower() for c in (allowed_category or [])]
    invalid_categories = [
        c for c in allowed_categories_raw if c not in VALID_SIGNAL_CATEGORIES
    ]
    if invalid_categories:
        console.print(
            f"[red]エラー[/red]: 無効な --allowed-category {invalid_categories} "
            f"(有効値: {VALID_SIGNAL_CATEGORIES})"
        )
        raise typer.Exit(code=1)
    return cast(list[SignalCategory], allowed_categories_raw)


@lab_app.command(name="generate")
def generate_command(
    count: int = typer.Option(100, "--count", "-n", help="生成する戦略数"),
    top: int = typer.Option(10, "--top", "-t", help="評価する上位戦略数"),
    seed: int = typer.Option(None, "--seed", "-s", help="乱数シード（再現性用）"),
    save: bool = typer.Option(True, "--save/--no-save", help="結果をYAMLに保存"),
    direction: str = typer.Option(
        "longonly",
        "--direction",
        "-d",
        help="売買方向 (longonly/shortonly/both)",
    ),
    timeframe: str = typer.Option(
        "daily",
        "--timeframe",
        "-T",
        help="時間軸 (daily/weekly)",
    ),
    dataset: str = typer.Option(
        "primeExTopix500",
        "--dataset",
        "-D",
        help="データセット名 (primeExTopix500/topix500等)",
    ),
    entry_filter_only: bool = typer.Option(
        False,
        "--entry-filter-only",
        help="Entryフィルターのみ生成（Exitシグナルは生成しない）",
    ),
    allowed_category: list[str] | None = typer.Option(
        None,
        "--allowed-category",
        "-C",
        help="許可カテゴリ（複数指定可: --allowed-category fundamental）",
    ),
):
    """
    新規戦略を自動生成

    シグナルの組み合わせをランダムに生成し、バックテストで評価します。

    Examples:
        uv run bt -v lab generate
        uv run bt lab generate --count 200 --top 20
        uv run bt lab generate --direction shortonly --timeframe weekly
        uv run bt lab generate --dataset topix500
    """

    from src.agent import GeneratorConfig, StrategyEvaluator, StrategyGenerator
    from src.agent.yaml_updater import YamlUpdater

    # direction バリデーション
    valid_directions = ("longonly", "shortonly", "both")
    if direction not in valid_directions:
        console.print(f"[red]エラー[/red]: --direction は {valid_directions} のいずれか")
        raise typer.Exit(code=1)

    # timeframe バリデーション
    valid_timeframes = ("daily", "weekly")
    if timeframe not in valid_timeframes:
        console.print(f"[red]エラー[/red]: --timeframe は {valid_timeframes} のいずれか")
        raise typer.Exit(code=1)

    allowed_categories = _resolve_allowed_categories(allowed_category)

    console.print(
        f"[bold blue]戦略自動生成[/bold blue]: {count}個生成 → 上位{top}個評価"
    )
    console.print(f"  direction: {direction}, timeframe: {timeframe}, dataset: {dataset}")
    console.print(
        f"  entry_filter_only: {entry_filter_only}, "
        f"allowed_categories: {allowed_categories or ['all']}"
    )

    # 生成設定
    config = GeneratorConfig(
        n_strategies=count,
        seed=seed,
        entry_filter_only=entry_filter_only,
        allowed_categories=allowed_categories,
    )

    # 生成
    generator = StrategyGenerator(config=config)
    candidates = generator.generate()
    console.print(f"✅ {len(candidates)}個の戦略候補を生成")

    # 評価（CLI指定値で上書き、デフォルトはconfig/default.yamlから読み込み）
    console.print("📊 バックテスト評価中...")
    evaluator = StrategyEvaluator(
        shared_config_dict={
            "direction": direction,
            "timeframe": timeframe,
            "dataset": dataset,
        }
    )
    results = evaluator.evaluate_batch(candidates, top_k=top)

    # 結果表示
    table = Table(title="評価結果（上位10件）")
    table.add_column("順位", style="cyan")
    table.add_column("戦略ID", style="green")
    table.add_column("スコア", style="yellow")
    table.add_column("Sharpe", style="magenta")
    table.add_column("Calmar", style="magenta")
    table.add_column("Total Return", style="magenta")

    for i, result in enumerate(results[:10], 1):
        if result.success:
            table.add_row(
                str(i),
                result.candidate.strategy_id,
                f"{result.score:.4f}",
                f"{result.sharpe_ratio:.4f}",
                f"{result.calmar_ratio:.4f}",
                f"{result.total_return:.2%}",
            )
        else:
            table.add_row(
                str(i),
                result.candidate.strategy_id,
                "FAILED",
                "-",
                "-",
                "-",
            )

    console.print(table)

    # 保存（shared_config を候補に設定してから保存）
    if save and results and results[0].success:
        best_candidate = results[0].candidate
        # shared_config を更新
        best_candidate.shared_config = {
            **best_candidate.shared_config,
            "direction": direction,
            "timeframe": timeframe,
            "dataset": dataset,
        }
        yaml_updater = YamlUpdater()
        path = yaml_updater.save_candidate(best_candidate)
        console.print(f"💾 最良戦略を保存: {path}")


@lab_app.command(name="evolve")
def evolve_command(
    strategy: str = typer.Argument(..., help="ベース戦略名"),
    generations: int = typer.Option(20, "--generations", "-g", help="世代数"),
    population: int = typer.Option(50, "--population", "-p", help="個体数"),
    structure_mode: str = typer.Option(
        "params_only",
        "--structure-mode",
        help="探索パターン (params_only/random_add)",
    ),
    random_add_entry_signals: int = typer.Option(
        1,
        "--random-add-entry-signals",
        help="random_add時に追加するentryシグナル数（ベースに対する追加分）",
    ),
    random_add_exit_signals: int = typer.Option(
        1,
        "--random-add-exit-signals",
        help="random_add時に追加するexitシグナル数（ベースに対する追加分）",
    ),
    seed: int | None = typer.Option(
        None,
        "--seed",
        help="乱数シード（再現性用）",
    ),
    save: bool = typer.Option(True, "--save/--no-save", help="結果をYAMLに保存"),
    entry_filter_only: bool = typer.Option(
        False,
        "--entry-filter-only",
        help="Entryフィルターのみ最適化（Exitパラメータは変更しない）",
    ),
    allowed_category: list[str] | None = typer.Option(
        None,
        "--allowed-category",
        "-C",
        help="最適化対象カテゴリ（複数指定可）",
    ),
):
    """
    遺伝的アルゴリズムでパラメータ最適化

    既存戦略をベースに、パラメータを進化的に最適化します。

    Examples:
        uv run bt -v lab evolve range_break_v15
        uv run bt lab evolve range_break_v15 --generations 30 --population 100
    """

    from src.agent.models import EvolutionConfig
    from src.agent.parameter_evolver import ParameterEvolver
    from src.agent.yaml_updater import YamlUpdater
    allowed_categories = _resolve_allowed_categories(allowed_category)

    console.print(
        f"[bold blue]遺伝的アルゴリズム最適化[/bold blue]: {strategy}"
    )
    valid_structure_modes = ("params_only", "random_add")
    if structure_mode not in valid_structure_modes:
        console.print(
            f"[red]エラー[/red]: --structure-mode は {valid_structure_modes} のいずれか"
        )
        raise typer.Exit(code=1)

    console.print(
        f"設定: 世代数={generations}, 個体数={population}, "
        f"structure_mode={structure_mode}, "
        f"entry_filter_only={entry_filter_only}, "
        f"allowed_categories={allowed_categories or ['all']}"
    )
    if structure_mode == "random_add":
        console.print(
            f"  random_add_entry_signals={random_add_entry_signals}, "
            f"random_add_exit_signals={random_add_exit_signals}, seed={seed}"
        )

    # 進化設定
    config = EvolutionConfig(
        population_size=population,
        generations=generations,
        entry_filter_only=entry_filter_only,
        allowed_categories=allowed_categories,
        structure_mode=structure_mode,
        random_add_entry_signals=random_add_entry_signals,
        random_add_exit_signals=random_add_exit_signals,
        seed=seed,
    )

    # 進化実行
    evolver = ParameterEvolver(config=config)
    best_candidate, _ = evolver.evolve(strategy)

    # 進化履歴表示
    history = evolver.get_evolution_history()
    table = Table(title="進化履歴")
    table.add_column("世代", style="cyan")
    table.add_column("最良スコア", style="yellow")
    table.add_column("平均スコア", style="magenta")

    for h in history:
        table.add_row(
            str(h["generation"]),
            f"{h['best_score']:.4f}",
            f"{h['avg_score']:.4f}",
        )

    console.print(table)

    # 最良結果
    console.print(f"\n[bold green]最良戦略[/bold green]: {best_candidate.strategy_id}")

    # 保存
    if save:
        yaml_updater = YamlUpdater()
        strategy_path, history_path = yaml_updater.save_evolution_result(
            best_candidate, history, base_strategy_name=strategy
        )
        console.print(f"💾 戦略保存: {strategy_path}")
        console.print(f"📜 履歴保存: {history_path}")


@lab_app.command(name="optimize")
def optimize_command(
    strategy: str = typer.Argument(..., help="ベース戦略名"),
    trials: int = typer.Option(100, "--trials", "-n", help="試行回数"),
    sampler: str = typer.Option(
        "tpe", "--sampler", "-s", help="サンプラー (tpe/random/cmaes)"
    ),
    structure_mode: str = typer.Option(
        "params_only",
        "--structure-mode",
        help="探索パターン (params_only/random_add)",
    ),
    random_add_entry_signals: int = typer.Option(
        1,
        "--random-add-entry-signals",
        help="random_add時に追加するentryシグナル数（ベースに対する追加分）",
    ),
    random_add_exit_signals: int = typer.Option(
        1,
        "--random-add-exit-signals",
        help="random_add時に追加するexitシグナル数（ベースに対する追加分）",
    ),
    seed: int | None = typer.Option(
        None,
        "--seed",
        help="乱数シード（再現性用）",
    ),
    save: bool = typer.Option(True, "--save/--no-save", help="結果をYAMLに保存"),
    entry_filter_only: bool = typer.Option(
        False,
        "--entry-filter-only",
        help="Entryフィルターのみ最適化（Exitパラメータは変更しない）",
    ),
    allowed_category: list[str] | None = typer.Option(
        None,
        "--allowed-category",
        "-C",
        help="最適化対象カテゴリ（複数指定可）",
    ),
):
    """
    Optuna（ベイズ最適化）でパラメータ最適化

    TPEサンプラーを使用して効率的にパラメータ空間を探索します。

    Examples:
        uv run bt -v lab optimize range_break_v15
        uv run bt lab optimize range_break_v15 --trials 200
        uv run bt lab optimize range_break_v15 --sampler cmaes
    """

    try:
        from src.agent.models import OptunaConfig
        from src.agent.optuna_optimizer import OptunaOptimizer
        from src.agent.yaml_updater import YamlUpdater
    except ImportError as e:
        console.print(f"[bold red]エラー[/bold red]: {e}")
        console.print("Optunaをインストールしてください: uv add optuna")
        raise typer.Exit(code=1)
    allowed_categories = _resolve_allowed_categories(allowed_category)

    console.print(f"[bold blue]Optuna最適化[/bold blue]: {strategy}")
    valid_structure_modes = ("params_only", "random_add")
    if structure_mode not in valid_structure_modes:
        console.print(
            f"[red]エラー[/red]: --structure-mode は {valid_structure_modes} のいずれか"
        )
        raise typer.Exit(code=1)

    console.print(
        f"設定: 試行回数={trials}, サンプラー={sampler}, "
        f"structure_mode={structure_mode}, "
        f"entry_filter_only={entry_filter_only}, "
        f"allowed_categories={allowed_categories or ['all']}"
    )
    if structure_mode == "random_add":
        console.print(
            f"  random_add_entry_signals={random_add_entry_signals}, "
            f"random_add_exit_signals={random_add_exit_signals}, seed={seed}"
        )

    # 最適化設定
    config = OptunaConfig(
        n_trials=trials,
        sampler=sampler,
        structure_mode=structure_mode,
        random_add_entry_signals=random_add_entry_signals,
        random_add_exit_signals=random_add_exit_signals,
        seed=seed,
        entry_filter_only=entry_filter_only,
        allowed_categories=allowed_categories,
    )

    # 最適化実行
    optimizer = OptunaOptimizer(config=config)
    best_candidate, study = optimizer.optimize(strategy)

    # 結果表示
    console.print(f"\n[bold green]最良スコア[/bold green]: {study.best_value:.4f}")
    console.print("[bold green]最良パラメータ[/bold green]:")
    for key, value in study.best_params.items():
        console.print(f"  {key}: {value}")

    # 保存
    if save:
        yaml_updater = YamlUpdater()
        history = optimizer.get_optimization_history(study)
        strategy_path, history_path = yaml_updater.save_optuna_result(
            best_candidate, history, base_strategy_name=strategy
        )
        console.print(f"\n💾 戦略保存: {strategy_path}")
        console.print(f"📜 履歴保存: {history_path}")


@lab_app.command(name="improve")
def improve_command(
    strategy: str = typer.Argument(..., help="改善対象の戦略名"),
    auto_apply: bool = typer.Option(
        True, "--auto-apply/--no-apply", help="改善を自動適用"
    ),
    entry_filter_only: bool = typer.Option(
        False,
        "--entry-filter-only",
        help="Entryフィルターの改善のみを許可",
    ),
    allowed_category: list[str] | None = typer.Option(
        None,
        "--allowed-category",
        "-C",
        help="改善対象カテゴリ（複数指定可）",
    ),
):
    """
    既存戦略を分析して改善

    弱点を分析し、改善提案を生成します。--auto-applyで自動適用も可能。

    Examples:
        uv run bt -v lab improve range_break_v15
        uv run bt lab improve range_break_v15 --no-apply
    """

    from src.agent.strategy_improver import StrategyImprover
    from src.agent.yaml_updater import YamlUpdater
    from src.lib.strategy_runtime.loader import ConfigLoader

    console.print(f"[bold blue]戦略分析[/bold blue]: {strategy}")
    allowed_categories = _resolve_allowed_categories(allowed_category)

    # 分析
    improver = StrategyImprover()
    report = improver.analyze(
        strategy,
        entry_filter_only=entry_filter_only,
        allowed_categories=allowed_categories,
    )

    # 弱点表示
    console.print("\n[bold yellow]弱点分析結果[/bold yellow]:")
    console.print(f"  最大ドローダウン: {report.max_drawdown:.1%}")
    if report.max_drawdown_duration_days > 0:
        console.print(f"  ドローダウン期間: {report.max_drawdown_duration_days}日")

    if report.losing_trade_patterns:
        console.print("\n  負けパターン:")
        for pattern in report.losing_trade_patterns:
            console.print(f"    - {pattern}")

    if report.suggested_improvements:
        console.print("\n[bold cyan]改善提案[/bold cyan]:")
        for suggestion in report.suggested_improvements:
            console.print(f"  • {suggestion}")

    # 具体的な改善案生成
    config_loader = ConfigLoader()
    strategy_config = config_loader.load_strategy_config(strategy)
    improvements = improver.suggest_improvements(
        report,
        strategy_config,
        entry_filter_only=entry_filter_only,
        allowed_categories=allowed_categories,
    )

    if improvements:
        console.print("\n[bold green]具体的な改善案[/bold green]:")
        table = Table()
        table.add_column("タイプ", style="cyan")
        table.add_column("対象", style="yellow")
        table.add_column("シグナル", style="green")
        table.add_column("理由", style="magenta")

        for imp in improvements:
            table.add_row(
                imp.improvement_type,
                imp.target,
                imp.signal_name,
                imp.reason,
            )

        console.print(table)

        # 自動適用
        if auto_apply and improvements:
            yaml_updater = YamlUpdater()
            output_path = yaml_updater.apply_improvements(strategy, improvements)
            console.print(f"\n💾 改善済み戦略を保存: {output_path}")
    else:
        console.print("\n[dim]改善提案はありません[/dim]")
