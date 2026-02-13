"""lab CLI コマンドのテスト"""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli_bt import app

runner = CliRunner()


def _make_eval_result() -> MagicMock:
    candidate = MagicMock()
    candidate.strategy_id = "auto_test"
    candidate.shared_config = {}

    result = MagicMock()
    result.success = True
    result.candidate = candidate
    result.score = 1.2
    result.sharpe_ratio = 1.0
    result.calmar_ratio = 0.8
    result.total_return = 0.12
    result.max_drawdown = -0.05
    return result


def test_lab_generate_accepts_fundamental_constraints() -> None:
    with (
        patch("src.agent.StrategyGenerator") as MockGenerator,
        patch("src.agent.StrategyEvaluator") as MockEvaluator,
    ):
        MockGenerator.return_value.generate.return_value = [MagicMock()]
        MockEvaluator.return_value.evaluate_batch.return_value = [_make_eval_result()]

        result = runner.invoke(
            app,
            [
                "lab",
                "generate",
                "--count",
                "5",
                "--top",
                "1",
                "--entry-filter-only",
                "--allowed-category",
                "fundamental",
                "--no-save",
            ],
        )

    assert result.exit_code == 0
    config = MockGenerator.call_args.kwargs["config"]
    assert config.entry_filter_only is True
    assert config.allowed_categories == ["fundamental"]


def test_lab_generate_rejects_invalid_category() -> None:
    result = runner.invoke(
        app,
        ["lab", "generate", "--allowed-category", "invalid"],
    )

    assert result.exit_code == 1
    assert "無効な --allowed-category" in result.stdout


def test_lab_generate_rejects_invalid_direction() -> None:
    result = runner.invoke(
        app,
        ["lab", "generate", "--direction", "invalid"],
    )

    assert result.exit_code == 1
    assert "--direction は" in result.stdout


def test_lab_generate_rejects_invalid_timeframe() -> None:
    result = runner.invoke(
        app,
        ["lab", "generate", "--timeframe", "monthly"],
    )

    assert result.exit_code == 1
    assert "--timeframe は" in result.stdout


def test_lab_generate_help_includes_new_options() -> None:
    result = runner.invoke(app, ["lab", "generate", "--help"])
    assert result.exit_code == 0
    assert "--entry-filter-only" in result.stdout
    assert "--allowed-category" in result.stdout


def test_lab_evolve_command_runs() -> None:
    with (
        patch("src.agent.parameter_evolver.ParameterEvolver") as MockEvolver,
        patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
    ):
        candidate = MagicMock()
        candidate.strategy_id = "evolved_x"
        MockEvolver.return_value.evolve.return_value = (candidate, [])
        MockEvolver.return_value.get_evolution_history.return_value = [
            {"generation": 1, "best_score": 1.0, "avg_score": 0.5}
        ]
        MockYaml.return_value.save_evolution_result.return_value = (
            "/tmp/strategy.yaml",
            "/tmp/history.yaml",
        )

        result = runner.invoke(
            app,
            ["lab", "evolve", "experimental/base_strategy_01", "--generations", "2", "--population", "10"],
        )

    assert result.exit_code == 0


def test_lab_evolve_command_runs_without_save() -> None:
    with patch("src.agent.parameter_evolver.ParameterEvolver") as MockEvolver:
        candidate = MagicMock()
        candidate.strategy_id = "evolved_x"
        MockEvolver.return_value.evolve.return_value = (candidate, [])
        MockEvolver.return_value.get_evolution_history.return_value = [
            {"generation": 1, "best_score": 1.0, "avg_score": 0.5}
        ]

        result = runner.invoke(
            app,
            [
                "lab",
                "evolve",
                "experimental/base_strategy_01",
                "--generations",
                "2",
                "--population",
                "10",
                "--no-save",
            ],
        )

    assert result.exit_code == 0


def test_lab_optimize_command_runs() -> None:
    with (
        patch("src.agent.optuna_optimizer.OptunaOptimizer") as MockOptimizer,
        patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
    ):
        candidate = MagicMock()
        study = MagicMock()
        study.best_value = 1.5
        study.best_params = {"period": 20}
        MockOptimizer.return_value.optimize.return_value = (candidate, study)
        MockOptimizer.return_value.get_optimization_history.return_value = []
        MockYaml.return_value.save_optuna_result.return_value = (
            "/tmp/strategy.yaml",
            "/tmp/history.yaml",
        )

        result = runner.invoke(
            app,
            ["lab", "optimize", "experimental/base_strategy_01", "--trials", "10"],
        )

    assert result.exit_code == 0


def test_lab_optimize_command_runs_without_save() -> None:
    with patch("src.agent.optuna_optimizer.OptunaOptimizer") as MockOptimizer:
        candidate = MagicMock()
        study = MagicMock()
        study.best_value = 1.5
        study.best_params = {"period": 20}
        MockOptimizer.return_value.optimize.return_value = (candidate, study)

        result = runner.invoke(
            app,
            [
                "lab",
                "optimize",
                "experimental/base_strategy_01",
                "--trials",
                "10",
                "--no-save",
            ],
        )

    assert result.exit_code == 0


def test_lab_generate_handles_failed_results() -> None:
    with (
        patch("src.agent.StrategyGenerator") as MockGenerator,
        patch("src.agent.StrategyEvaluator") as MockEvaluator,
    ):
        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "auto_failed"
        MockGenerator.return_value.generate.return_value = [mock_candidate]

        failed_result = MagicMock()
        failed_result.success = False
        failed_result.candidate = mock_candidate
        MockEvaluator.return_value.evaluate_batch.return_value = [failed_result]

        result = runner.invoke(
            app,
            ["lab", "generate", "--count", "1", "--top", "1", "--no-save"],
        )

    assert result.exit_code == 0


def test_lab_generate_saves_best_strategy() -> None:
    with (
        patch("src.agent.StrategyGenerator") as MockGenerator,
        patch("src.agent.StrategyEvaluator") as MockEvaluator,
        patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
    ):
        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "auto_success"
        mock_candidate.shared_config = {}
        MockGenerator.return_value.generate.return_value = [mock_candidate]

        success_result = _make_eval_result()
        success_result.candidate = mock_candidate
        MockEvaluator.return_value.evaluate_batch.return_value = [success_result]
        MockYaml.return_value.save_candidate.return_value = "/tmp/auto.yaml"

        result = runner.invoke(
            app,
            ["lab", "generate", "--count", "1", "--top", "1"],
        )

    assert result.exit_code == 0
    MockYaml.return_value.save_candidate.assert_called_once()


def test_lab_improve_accepts_fundamental_constraints() -> None:
    with (
        patch("src.agent.strategy_improver.StrategyImprover") as MockImprover,
        patch("src.lib.strategy_runtime.loader.ConfigLoader") as MockLoader,
    ):
        report = MagicMock()
        report.max_drawdown = 0.1
        report.max_drawdown_duration_days = 0
        report.losing_trade_patterns = []
        report.suggested_improvements = []
        MockImprover.return_value.analyze.return_value = report
        MockImprover.return_value.suggest_improvements.return_value = []
        MockLoader.return_value.load_strategy_config.return_value = {
            "entry_filter_params": {},
            "exit_trigger_params": {},
        }

        result = runner.invoke(
            app,
            [
                "lab",
                "improve",
                "experimental/base_strategy_01",
                "--entry-filter-only",
                "--allowed-category",
                "fundamental",
                "--no-apply",
            ],
        )

    assert result.exit_code == 0
    MockImprover.return_value.analyze.assert_called_once_with(
        "experimental/base_strategy_01",
        entry_filter_only=True,
        allowed_categories=["fundamental"],
    )
    MockImprover.return_value.suggest_improvements.assert_called_once()


def test_lab_improve_rejects_invalid_category() -> None:
    result = runner.invoke(
        app,
        ["lab", "improve", "experimental/base_strategy_01", "--allowed-category", "invalid"],
    )

    assert result.exit_code == 1
    assert "無効な --allowed-category" in result.stdout


def test_lab_improve_renders_details_and_auto_applies() -> None:
    with (
        patch("src.agent.strategy_improver.StrategyImprover") as MockImprover,
        patch("src.lib.strategy_runtime.loader.ConfigLoader") as MockLoader,
        patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
    ):
        report = MagicMock()
        report.max_drawdown = 0.2
        report.max_drawdown_duration_days = 12
        report.losing_trade_patterns = [{"type": "worst_trade", "return": -0.1}]
        report.suggested_improvements = ["volume を追加"]
        MockImprover.return_value.analyze.return_value = report
        MockLoader.return_value.load_strategy_config.return_value = {
            "entry_filter_params": {},
            "exit_trigger_params": {},
        }

        improvement = MagicMock()
        improvement.improvement_type = "add_signal"
        improvement.target = "entry"
        improvement.signal_name = "fundamental"
        improvement.reason = "テスト理由"
        MockImprover.return_value.suggest_improvements.return_value = [improvement]
        MockYaml.return_value.apply_improvements.return_value = "/tmp/improved.yaml"

        result = runner.invoke(
            app,
            ["lab", "improve", "experimental/base_strategy_01"],
        )

    assert result.exit_code == 0
    MockYaml.return_value.apply_improvements.assert_called_once()


def test_lab_improve_help_includes_new_options() -> None:
    result = runner.invoke(app, ["lab", "improve", "--help"])
    assert result.exit_code == 0
    assert "--entry-filter-only" in result.stdout
    assert "--allowed-category" in result.stdout
