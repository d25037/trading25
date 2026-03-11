"""
BacktestRunner unit tests
"""

import json
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from src.domains.backtest.core.artifacts import BacktestArtifactWriter
from src.domains.backtest.core.simulation import BacktestSimulationResult
from src.infrastructure.external_api.client import BaseAPIClient
from src.domains.backtest.core.runner import BacktestRunner


class _FakeExecutor:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.executed_template_path = None
        self.executed_strategy_name = None
        self.executed_extra_env = None
        self.executed_parameters = None

    def resolve_output_paths(
        self,
        parameters: dict[str, Any],
        *,
        strategy_name: str | None = None,
        output_filename: str | None = None,
    ):
        _ = (parameters, strategy_name, output_filename)
        return BacktestArtifactWriter.artifact_paths_for_html(
            Path(self.output_dir) / "result.html"
        )

    def execute_notebook(
        self,
        template_path: str,
        parameters: dict,
        strategy_name: str,
        extra_env: dict[str, str] | None = None,
    ):
        self.executed_template_path = template_path
        self.executed_strategy_name = strategy_name
        self.executed_extra_env = extra_env
        self.executed_parameters = parameters
        execution_meta = parameters.get("_execution", {})
        if isinstance(execution_meta, dict):
            report_data_path = execution_meta.get("report_data_path")
            if isinstance(report_data_path, str):
                assert Path(report_data_path).exists()
        assert (Path(self.output_dir) / "result.metrics.json").exists()
        assert (Path(self.output_dir) / "result.manifest.json").exists()
        html_path = Path(self.output_dir) / "result.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        return html_path

    def get_execution_summary(self, html_path: Path) -> dict:
        return {"html_path": str(html_path)}


class _FakeSimulationExecutor:
    def __init__(self, metrics_payload: dict[str, Any] | None = None) -> None:
        self.metrics_payload = metrics_payload or {
            "total_return": 1.5,
            "sharpe_ratio": 0.8,
            "sortino_ratio": 0.9,
            "calmar_ratio": 0.7,
            "max_drawdown": -1.2,
            "win_rate": 52.0,
            "total_trades": 4,
            "optimal_allocation": 0.25,
        }
        self.last_parameters: dict[str, Any] | None = None

    def execute(self, parameters: dict[str, Any]) -> BacktestSimulationResult:
        self.last_parameters = parameters
        return BacktestSimulationResult(
            initial_portfolio=None,
            kelly_portfolio=None,
            allocation_info=None,
            all_entries=None,
            summary_metrics=None,
            metrics_payload=self.metrics_payload,
        )


def test_backtest_runner_uses_execution_config(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()

    fake_executor = _FakeExecutor(str(tmp_path))

    def _fake_executor_factory(output_dir: str):
        assert output_dir == str(tmp_path)
        return fake_executor

    # ConfigLoaderの呼び出しを差し替え
    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda strategy: {
            "shared_config": {"dataset": "sample"},
            "execution": {
                "template_notebook": "custom_template.py",
                "output_directory": str(tmp_path),
            },
        },
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("custom_template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        _fake_executor_factory,
    )

    result = runner.execute("experimental/test_strategy")

    assert result.html_path is not None
    assert result.html_path.exists()
    assert result.manifest_path.exists()
    assert Path(result.summary["_report_data_path"]).exists()
    assert fake_executor.executed_parameters is not None
    assert fake_executor.executed_parameters["_execution"]["report_data_path"].endswith(
        ".report.json"
    )
    assert fake_executor.executed_template_path == "custom_template.py"
    assert fake_executor.executed_strategy_name == "test_strategy"
    assert fake_executor.executed_extra_env == {"BT_DATA_ACCESS_MODE": "direct"}


def test_backtest_runner_allows_http_override(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()
    fake_executor = _FakeExecutor(str(tmp_path))

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: fake_executor,
    )

    runner.execute("production/test_strategy", data_access_mode="http")

    assert fake_executor.executed_extra_env is not None
    assert fake_executor.executed_extra_env.get("BT_DATA_ACCESS_MODE") == "http"


def test_backtest_runner_default_direct_mode_bypasses_http_requests(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()

    class _FakeDatasetDb:
        def get_stock_ohlcv(self, _code, start=None, end=None):  # noqa: ANN001, ANN202
            _ = (start, end)
            return [
                SimpleNamespace(
                    date="2024-01-04",
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=12345,
                )
            ]

    class _FakeExecutorWithLoad(_FakeExecutor):
        def execute_notebook(
            self,
            template_path: str,
            parameters: dict,
            strategy_name: str,
            extra_env: dict[str, str] | None = None,
        ):
            self.executed_template_path = template_path
            self.executed_strategy_name = strategy_name
            self.executed_extra_env = extra_env
            from src.infrastructure.data_access.loaders.stock_loaders import load_stock_data

            df = load_stock_data("sample", "7203")
            assert not df.empty
            html_path = Path(self.output_dir) / "result.html"
            html_path.write_text("<html></html>", encoding="utf-8")
            return html_path

    monkeypatch.setattr(
        "src.infrastructure.data_access.clients._resolve_dataset_db",
        lambda _dataset_name: _FakeDatasetDb(),
    )

    def _fail_http_request(*args, **kwargs):  # noqa: ANN001, ANN002, ARG001
        raise AssertionError("HTTP client must not be used during backtest")

    monkeypatch.setattr(BaseAPIClient, "_request", _fail_http_request)

    fake_executor = _FakeExecutorWithLoad(str(tmp_path))

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: fake_executor,
    )

    runner.execute("production/test_strategy")

    assert fake_executor.executed_extra_env == {"BT_DATA_ACCESS_MODE": "direct"}


def test_backtest_runner_progress_callback_and_walk_forward_manifest(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()
    fake_executor = _FakeExecutor(str(tmp_path))
    statuses: list[str] = []

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: fake_executor,
    )
    monkeypatch.setattr(
        runner,
        "_run_walk_forward",
        lambda _parameters: {"count": 1, "splits": [], "aggregate": {}},
    )

    result = runner.execute(
        "production/test_strategy",
        progress_callback=lambda status, _elapsed: statuses.append(status),
    )

    manifest_path = Path(result.summary["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert statuses == [
        "設定を読み込み中...",
        "シミュレーションを実行中...",
        "レポートを生成中...",
        "完了！",
    ]
    assert "walk_forward" in result.summary
    assert manifest["strategy_name"] == "test_strategy"
    assert manifest["dataset_name"] == "sample"
    assert "walk_forward" in manifest
    assert manifest["artifact_contract"]["canonical_result_source"] == "metrics.json"
    assert manifest["core_artifacts"]["metrics_json"]["path"] == str(result.metrics_path)
    assert manifest["core_artifacts"]["metrics_json"]["available"] is True
    assert manifest["core_artifacts"]["report_data_json"]["available"] is True
    assert manifest["presentation_artifacts"]["result_html"]["role"] == "presentation_only"
    assert manifest["presentation_artifacts"]["result_html"]["available"] is True
    assert manifest["presentation_artifacts"]["result_html"]["render_status"] == "completed"


def test_backtest_runner_emits_simulation_checkpoint_before_render(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()
    fake_executor = _FakeExecutor(str(tmp_path))
    checkpoints: list[dict[str, Any]] = []

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: fake_executor,
    )

    result = runner.execute(
        "production/test_strategy",
        simulation_artifacts_callback=lambda raw_result: checkpoints.append(raw_result),
    )

    assert result.html_path is not None
    assert len(checkpoints) == 1
    checkpoint = checkpoints[0]
    assert checkpoint["render_status"] == "pending"
    assert Path(checkpoint["_metrics_path"]).exists()
    assert Path(checkpoint["_manifest_path"]).exists()
    assert Path(checkpoint["_report_data_path"]).exists()
    assert checkpoint["_expected_html_path"].endswith(".html")


def test_backtest_runner_preserves_simulation_result_when_html_missing(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor()

    class _MissingHtmlExecutor(_FakeExecutor):
        def resolve_output_paths(
            self,
            parameters: dict[str, Any],
            *,
            strategy_name: str | None = None,
            output_filename: str | None = None,
        ):
            _ = (parameters, strategy_name, output_filename)
            return BacktestArtifactWriter.artifact_paths_for_html(
                Path(self.output_dir) / "missing.html"
            )

        def execute_notebook(
            self,
            template_path: str,
            parameters: dict[str, Any],
            strategy_name: str | None = None,
            output_filename: str | None = None,
            timeout: int = 600,
            extra_env: dict[str, str] | None = None,
        ) -> Path:
            _ = (template_path, strategy_name, output_filename, timeout, extra_env)
            execution_meta = parameters.get("_execution", {})
            if isinstance(execution_meta, dict):
                report_data_path = execution_meta.get("report_data_path")
                if isinstance(report_data_path, str):
                    assert Path(report_data_path).exists()
            return Path(self.output_dir) / "missing.html"

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: _MissingHtmlExecutor(str(tmp_path)),
    )

    result = runner.execute("production/test_strategy")

    assert result.html_path is None
    assert result.metrics_path is not None
    assert result.metrics_path.exists()
    assert result.render_error is not None
    assert result.summary["total_return"] == 1.5


def test_backtest_runner_preserves_metrics_when_html_missing(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()
    runner.simulation_executor = _FakeSimulationExecutor(
        metrics_payload={
            "total_return": 3.5,
            "sharpe_ratio": 1.2,
            "sortino_ratio": 1.4,
            "calmar_ratio": 0.9,
            "max_drawdown": -2.1,
            "win_rate": 55.0,
            "total_trades": 7,
        }
    )

    class _MetricsOnlyExecutor(_FakeExecutor):
        def resolve_output_paths(
            self,
            parameters: dict[str, Any],
            *,
            strategy_name: str | None = None,
            output_filename: str | None = None,
        ):
            _ = (parameters, strategy_name, output_filename)
            return BacktestArtifactWriter.artifact_paths_for_html(
                Path(self.output_dir) / "missing.html"
            )

        def execute_notebook(
            self,
            template_path: str,
            parameters: dict[str, Any],
            strategy_name: str | None = None,
            output_filename: str | None = None,
            timeout: int = 600,
            extra_env: dict[str, str] | None = None,
        ) -> Path:
            _ = (template_path, strategy_name, output_filename, timeout, extra_env)
            execution_meta = parameters.get("_execution", {})
            if isinstance(execution_meta, dict):
                report_data_path = execution_meta.get("report_data_path")
                if isinstance(report_data_path, str):
                    assert Path(report_data_path).exists()
            metrics_path = Path(self.output_dir) / "missing.metrics.json"
            assert metrics_path.exists()
            manifest_path = Path(self.output_dir) / "missing.manifest.json"
            assert manifest_path.exists()
            return Path(self.output_dir) / "missing.html"

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample"}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_template_notebook_path",
        lambda _: Path("template.py"),
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.MarimoExecutor",
        lambda _output_dir: _MetricsOnlyExecutor(str(tmp_path)),
    )

    result = runner.execute("production/test_strategy")

    assert result.html_path is None
    assert result.metrics_path is not None
    assert result.metrics_path.exists()
    assert result.manifest_path.exists()
    assert result.render_error is not None
    assert result.summary["total_return"] == 3.5
    assert result.summary["trade_count"] == 7
    assert result.summary["manifest_path"] == str(result.manifest_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["core_artifacts"]["metrics_json"]["available"] is True
    assert manifest["presentation_artifacts"]["result_html"]["available"] is False
    assert manifest["presentation_artifacts"]["result_html"]["render_status"] == "failed"
    assert manifest["presentation_artifacts"]["result_html"]["render_error"] == result.render_error


def test_package_version_and_git_commit_helpers(monkeypatch):
    runner = BacktestRunner()

    monkeypatch.setattr("importlib.metadata.version", lambda _name: "9.9.9")
    assert runner._get_package_version("dummy") == "9.9.9"

    def _raise_version(_name):  # noqa: ANN001
        raise RuntimeError("no version")

    monkeypatch.setattr("importlib.metadata.version", _raise_version)
    assert runner._get_package_version("dummy") is None

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="abc123\n"),  # noqa: ARG005
    )
    assert runner._get_git_commit() == "abc123"

    def _raise_subprocess(*args, **kwargs):  # noqa: ANN001, ANN002, ARG001
        raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr(subprocess, "run", _raise_subprocess)
    assert runner._get_git_commit() is None


def test_collect_and_aggregate_walk_forward_metrics():
    runner = BacktestRunner()

    class _MetricValue:
        def __init__(self, value: float) -> None:
            self._value = value

        def mean(self) -> float:
            return self._value

    class _Portfolio:
        def total_return(self):
            return _MetricValue(10.0)

        def sharpe_ratio(self):
            return _MetricValue(1.2)

        def calmar_ratio(self):
            return _MetricValue(0.8)

    metrics = runner._collect_portfolio_metrics(_Portfolio())
    assert metrics == {
        "total_return": 10.0,
        "sharpe_ratio": 1.2,
        "calmar_ratio": 0.8,
    }
    assert runner._collect_portfolio_metrics(None) == {}
    assert runner._coerce_metric("invalid") is None

    aggregate = runner._aggregate_walk_forward_metrics(
        [
            {"metrics": {"total_return": 10.0, "sharpe_ratio": 1.0}},
            {"metrics": {"total_return": 20.0, "calmar_ratio": 0.5}},
        ]
    )
    assert aggregate == {"total_return": 15.0, "sharpe_ratio": 1.0, "calmar_ratio": 0.5}


def test_run_walk_forward_guard_paths(monkeypatch):
    runner = BacktestRunner()

    assert runner._run_walk_forward({"shared_config": {"walk_forward": {"enabled": False}}}) is None
    assert runner._run_walk_forward({"shared_config": {"walk_forward": "invalid"}}) is None

    parameters = {
        "shared_config": {
            "walk_forward": {"enabled": True},
            "stock_codes": ["all"],
            "dataset": "sample",
        }
    }
    monkeypatch.setattr("src.infrastructure.data_access.loaders.get_stock_list", lambda _dataset: (_ for _ in ()).throw(RuntimeError("failed")))
    assert runner._run_walk_forward(parameters) is None

    monkeypatch.setattr("src.infrastructure.data_access.loaders.get_stock_list", lambda _dataset: [])
    assert runner._run_walk_forward(parameters) is None


def test_run_walk_forward_success_and_max_splits(monkeypatch):
    runner = BacktestRunner()

    class _Portfolio:
        def total_return(self):
            return 10.0

        def sharpe_ratio(self):
            return 2.0

        def calmar_ratio(self):
            return 1.0

    split1 = SimpleNamespace(
        train_start="2024-01-01",
        train_end="2024-02-01",
        test_start="2024-02-02",
        test_end="2024-03-01",
    )
    split2 = SimpleNamespace(
        train_start="2024-03-02",
        train_end="2024-04-01",
        test_start="2024-04-02",
        test_end="2024-05-01",
    )

    fake_data_module = types.SimpleNamespace(get_stock_list=lambda _dataset: ["7203"])
    fake_stock_loader_module = types.SimpleNamespace(
        load_stock_data=lambda **kwargs: pd.DataFrame(  # noqa: ARG005
            {"Close": [100, 101, 102]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
    )
    fake_walkforward_module = types.SimpleNamespace(
        generate_walkforward_splits=lambda *args, **kwargs: [split1, split2]  # noqa: ARG005
    )
    fake_factory_module = types.SimpleNamespace(
        StrategyFactory=types.SimpleNamespace(
            execute_strategy_with_config=lambda *args, **kwargs: {  # noqa: ARG005
                "kelly_portfolio": _Portfolio()
            }
        )
    )

    monkeypatch.setitem(sys.modules, "src.infrastructure.data_access.loaders", fake_data_module)
    monkeypatch.setitem(sys.modules, "src.infrastructure.data_access.loaders.stock_loaders", fake_stock_loader_module)
    monkeypatch.setitem(sys.modules, "src.domains.backtest.core.walkforward", fake_walkforward_module)
    monkeypatch.setitem(sys.modules, "src.domains.strategy.core.factory", fake_factory_module)

    result = runner._run_walk_forward(
        {
            "shared_config": {
                "dataset": "sample",
                "stock_codes": ["all"],
                "timeframe": "daily",
                "walk_forward": {
                    "enabled": True,
                    "train_window": 10,
                    "test_window": 5,
                    "step": 2,
                    "max_splits": 1,
                },
            },
            "entry_filter_params": {"a": 1},
            "exit_trigger_params": {"b": 2},
        }
    )

    assert result is not None
    assert result["count"] == 1
    assert result["aggregate"] == {
        "total_return": 10.0,
        "sharpe_ratio": 2.0,
        "calmar_ratio": 1.0,
    }


def test_get_execution_info_success_and_error(monkeypatch):
    runner = BacktestRunner()

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"display_name": "S", "description": "D"},
    )
    monkeypatch.setattr(
        runner,
        "_build_parameters",
        lambda _config: {
            "shared_config": {"dataset": "sample", "initial_cash": 1000, "fees": 0.1, "kelly_fraction": 0.8}
        },
    )
    info = runner.get_execution_info("prod/s")
    assert info["dataset"] == "sample"
    assert info["kelly_fraction"] == 0.8

    def _raise(_strategy):  # noqa: ANN001
        raise RuntimeError("boom")

    monkeypatch.setattr(runner.config_loader, "load_strategy_config", _raise)
    error_info = runner.get_execution_info("prod/s")
    assert error_info == {"error": "boom"}


class TestBuildParametersConfigOverride:
    """_build_parameters の config_override テスト"""

    def _make_runner_with_defaults(self, monkeypatch: Any) -> BacktestRunner:
        runner = BacktestRunner()
        monkeypatch.setattr(
            runner.config_loader,
            "default_config",
            {"parameters": {"shared_config": {"dataset": "default_ds", "initial_cash": 1000000}}},
        )
        monkeypatch.setattr(
            runner.config_loader,
            "merge_shared_config",
            lambda sc: {**{"dataset": "default_ds", "initial_cash": 1000000}, **sc.get("shared_config", {})},
        )
        return runner

    def test_no_override(self, monkeypatch: Any):
        """config_override なしの場合、既存設定が保持されること"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        params = runner._build_parameters(strategy_config, config_override=None)
        assert params["shared_config"]["dataset"] == "sample"
        assert params["entry_filter_params"]["volume_ratio_above"]["enabled"] is True

    def test_partial_override_shared_config(self, monkeypatch: Any):
        """shared_config の部分上書きで既存設定が保持されること"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "initial_cash": 500000},
        }
        config_override: dict[str, Any] = {
            "shared_config": {"initial_cash": 2000000},
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["shared_config"]["dataset"] == "sample"  # 保持
        assert params["shared_config"]["initial_cash"] == 2000000  # 上書き

    def test_override_entry_filter_params(self, monkeypatch: Any):
        """entry_filter_params の部分上書き"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {
                "volume_ratio_above": {"enabled": True, "ratio_threshold": 1.5},
                "fundamental": {"enabled": True},
            },
        }
        config_override: dict[str, Any] = {
            "entry_filter_params": {
                "volume_ratio_above": {"ratio_threshold": 2.0},
            },
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["entry_filter_params"]["volume_ratio_above"]["enabled"] is True  # 保持
        assert params["entry_filter_params"]["volume_ratio_above"]["ratio_threshold"] == 2.0  # 上書き
        assert params["entry_filter_params"]["fundamental"]["enabled"] is True  # 保持

    def test_override_adds_new_key(self, monkeypatch: Any):
        """config_override で新しいキーを追加"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
        }
        config_override: dict[str, Any] = {
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["exit_trigger_params"]["volume_ratio_below"]["enabled"] is True

    def test_override_non_dict_value_raises(self, monkeypatch: Any):
        """config_override に dict 以外の値を渡すと ValueError"""
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
        }
        config_override: dict[str, Any] = {
            "shared_config": "invalid_string",
        }
        with pytest.raises(ValueError, match="must be a dict"):
            runner._build_parameters(strategy_config, config_override)

    def test_override_invalid_signal_params_raises(self, monkeypatch: Any):
        """config_override で無効なシグナルパラメータを渡すと ValueError"""
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        # fundamental.period_type に無効な値
        config_override: dict[str, Any] = {
            "entry_filter_params": {
                "fundamental": {"period_type": "INVALID_PERIOD"},
            },
        }
        with pytest.raises(ValueError, match="Invalid entry_filter_params"):
            runner._build_parameters(strategy_config, config_override)

    def test_next_session_round_trip_rejects_non_empty_exit_trigger_params(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {"next_session_round_trip": True},
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }

        with pytest.raises(ValueError, match="exit_trigger_params must be empty"):
            runner._build_parameters(strategy_config, config_override)

    def test_next_session_round_trip_rejects_invalid_shared_config_after_merge(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "next_session_round_trip": True,
                "timeframe": "weekly",
            },
        }

        with pytest.raises(ValueError, match="Invalid shared_config after config merge"):
            runner._build_parameters(strategy_config, config_override)

    def test_current_session_round_trip_oracle_rejects_non_empty_exit_trigger_params(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {"current_session_round_trip_oracle": True},
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }

        with pytest.raises(ValueError, match="exit_trigger_params must be empty"):
            runner._build_parameters(strategy_config, config_override)

    def test_current_session_round_trip_oracle_rejects_invalid_shared_config_after_merge(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "current_session_round_trip_oracle": True,
                "timeframe": "weekly",
            },
        }

        with pytest.raises(ValueError, match="Invalid shared_config after config merge"):
            runner._build_parameters(strategy_config, config_override)

    def test_same_day_oracle_signal_keeps_exit_trigger_params_when_round_trip_flag_is_off(
        self,
        monkeypatch: Any,
    ):
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {
                "oracle_index_open_gap_regime": {"enabled": True}
            },
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }

        params = runner._build_parameters(strategy_config, config_override=None)

        assert params["exit_trigger_params"]["volume_ratio_below"]["enabled"] is True
