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

from src.infrastructure.external_api.client import BaseAPIClient
from src.domains.backtest.core.runner import BacktestRunner


class _FakeExecutor:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.executed_template_path = None
        self.executed_strategy_name = None
        self.executed_extra_env = None

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
        html_path = Path(self.output_dir) / "result.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        return html_path

    def get_execution_summary(self, html_path: Path) -> dict:
        return {"html_path": str(html_path)}


def test_backtest_runner_uses_execution_config(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()

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

    assert result.html_path.exists()
    assert fake_executor.executed_template_path == "custom_template.py"
    assert fake_executor.executed_strategy_name == "test_strategy"
    assert fake_executor.executed_extra_env == {"BT_DATA_ACCESS_MODE": "direct"}


def test_backtest_runner_allows_http_override(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()
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

    assert statuses == ["設定を読み込み中...", "バックテストを実行中...", "完了！"]
    assert "walk_forward" in result.summary
    assert manifest["strategy_name"] == "test_strategy"
    assert manifest["dataset_name"] == "sample"
    assert "walk_forward" in manifest


def test_backtest_runner_raises_when_html_missing(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()

    class _MissingHtmlExecutor(_FakeExecutor):
        def execute_notebook(  # type: ignore[override]
            self,
            template_path: str,
            parameters: dict,
            strategy_name: str,
            extra_env: dict[str, str] | None = None,
        ):
            _ = (template_path, parameters, strategy_name, extra_env)
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

    import pytest

    with pytest.raises(RuntimeError, match="HTML file not found"):
        runner.execute("production/test_strategy")


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
            "entry_filter_params": {"volume": {"enabled": True}},
        }
        params = runner._build_parameters(strategy_config, config_override=None)
        assert params["shared_config"]["dataset"] == "sample"
        assert params["entry_filter_params"]["volume"]["enabled"] is True

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
                "volume": {"enabled": True, "threshold": 1.5},
                "fundamental": {"enabled": True},
            },
        }
        config_override: dict[str, Any] = {
            "entry_filter_params": {
                "volume": {"threshold": 2.0},
            },
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["entry_filter_params"]["volume"]["enabled"] is True  # 保持
        assert params["entry_filter_params"]["volume"]["threshold"] == 2.0  # 上書き
        assert params["entry_filter_params"]["fundamental"]["enabled"] is True  # 保持

    def test_override_adds_new_key(self, monkeypatch: Any):
        """config_override で新しいキーを追加"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample"},
        }
        config_override: dict[str, Any] = {
            "exit_trigger_params": {"volume": {"enabled": True}},
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["exit_trigger_params"]["volume"]["enabled"] is True

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
            "entry_filter_params": {"volume": {"enabled": True}},
        }
        # fundamental.period_type に無効な値
        config_override: dict[str, Any] = {
            "entry_filter_params": {
                "fundamental": {"period_type": "INVALID_PERIOD"},
            },
        }
        with pytest.raises(ValueError, match="Invalid entry_filter_params"):
            runner._build_parameters(strategy_config, config_override)
