"""
BacktestRunner unit tests
"""

from pathlib import Path
from typing import Any

from src.backtest.runner import BacktestRunner


class _FakeExecutor:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.executed_template_path = None
        self.executed_strategy_name = None

    def execute_notebook(self, template_path: str, parameters: dict, strategy_name: str):
        self.executed_template_path = template_path
        self.executed_strategy_name = strategy_name
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
        "src.backtest.runner.MarimoExecutor",
        _fake_executor_factory,
    )

    result = runner.execute("experimental/test_strategy")

    assert result.html_path.exists()
    assert fake_executor.executed_template_path == "custom_template.py"
    assert fake_executor.executed_strategy_name == "test_strategy"


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
