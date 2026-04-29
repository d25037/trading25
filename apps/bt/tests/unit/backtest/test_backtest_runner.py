"""
BacktestRunner unit tests
"""

import json
import math
import pickle
import subprocess
import sys
import time
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from src.infrastructure.external_api.client import BaseAPIClient
from src.infrastructure.data_access.mode import get_data_access_mode
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.backtest.core.report_renderer import BacktestReportPaths


class _FakeMetricValue:
    def __init__(self, value: float) -> None:
        self._value = value

    def mean(self) -> float:
        return self._value


class _FakePortfolio:
    def stats(self):
        return {
            "Total Return [%]": 12.3,
            "Max Drawdown [%]": -4.5,
            "Sharpe Ratio": 1.6,
            "Sortino Ratio": 2.1,
            "Calmar Ratio": 1.8,
            "Win Rate [%]": 57.0,
            "Profit Factor": 1.9,
            "Total Trades": 11,
        }

    def total_return(self):
        return _FakeMetricValue(12.3)

    def sharpe_ratio(self):
        return _FakeMetricValue(1.6)

    def sortino_ratio(self):
        return _FakeMetricValue(2.1)

    def calmar_ratio(self):
        return _FakeMetricValue(1.8)

    def max_drawdown(self):
        return _FakeMetricValue(-4.5)

    @property
    def trades(self):
        return types.SimpleNamespace(
            count=lambda: 11,
            win_rate=lambda: _FakeMetricValue(57.0),
        )


class _UnpickleablePortfolio:
    def __getstate__(self):
        raise TypeError("cannot pickle directly")

    def dumps(self) -> bytes:
        return b"serialized-portfolio"


def _fake_simulation_result() -> dict[str, Any]:
    return {
        "initial_portfolio": _FakePortfolio(),
        "kelly_portfolio": _FakePortfolio(),
        "allocation_info": types.SimpleNamespace(allocation=0.42),
        "all_entries": None,
    }


class _FakeRenderer:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.rendered_strategy_name = None
        self.rendered_dataset_name = None
        self.rendered_parameters = None
        self.rendered_report_payload_path = None

    def plan_report_paths(
        self,
        parameters: dict,
        strategy_name: str | None = None,
        output_filename: str | None = None,
    ) -> BacktestReportPaths:
        _ = (parameters, strategy_name, output_filename)
        html_path = Path(self.output_dir) / "result.html"
        return BacktestReportPaths(
            html_path=html_path,
            metrics_path=html_path.with_suffix(".metrics.json"),
            manifest_path=html_path.with_suffix(".manifest.json"),
            simulation_payload_path=html_path.with_suffix(".simulation.pkl"),
            report_payload_path=html_path.with_suffix(".report.json"),
        )

    def render_report(
        self,
        *,
        report_payload_path: Path,
        html_path: Path,
        strategy_name: str,
        dataset_name: str,
        metrics_path: Path | None = None,
        manifest_path: Path | None = None,
        parameters: dict[str, Any] | None = None,
    ):
        _ = (metrics_path, manifest_path)
        self.rendered_strategy_name = strategy_name
        self.rendered_dataset_name = dataset_name
        self.rendered_parameters = parameters
        self.rendered_report_payload_path = report_payload_path
        html_path.write_text("<html></html>", encoding="utf-8")
        return html_path


def test_backtest_runner_uses_execution_config(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()

    fake_renderer = _FakeRenderer(str(tmp_path))

    def _fake_renderer_factory(output_dir: str):
        assert output_dir == str(tmp_path)
        return fake_renderer

    # ConfigLoaderの呼び出しを差し替え
    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda strategy: {
            "shared_config": {"dataset": "sample", "static_universe": True},
            "execution": {
                "output_directory": str(tmp_path),
            },
        },
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        _fake_renderer_factory,
    )
    monkeypatch.setattr(runner, "_execute_simulation", lambda _parameters: _fake_simulation_result())

    result = runner.execute("experimental/test_strategy")

    assert result.html_path.exists()
    assert result.metrics_path is not None and result.metrics_path.exists()
    assert result.report_payload_path is not None and result.report_payload_path.exists()
    assert fake_renderer.rendered_strategy_name == "test_strategy"
    assert fake_renderer.rendered_dataset_name == "sample"
    assert fake_renderer.rendered_parameters is not None
    assert fake_renderer.rendered_report_payload_path == result.report_payload_path


def test_backtest_runner_allows_http_override(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()
    fake_renderer = _FakeRenderer(str(tmp_path))

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: fake_renderer,
    )

    def _simulate(_parameters):
        assert get_data_access_mode() == "http"
        return _fake_simulation_result()

    monkeypatch.setattr(runner, "_execute_simulation", _simulate)

    runner.execute("production/test_strategy", data_access_mode="http")


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

    monkeypatch.setattr(
        "src.infrastructure.data_access.clients._resolve_dataset_db",
        lambda _dataset_name: _FakeDatasetDb(),
    )

    def _fail_http_request(*args, **kwargs):  # noqa: ANN001, ANN002, ARG001
        raise AssertionError("HTTP client must not be used during backtest")

    monkeypatch.setattr(BaseAPIClient, "_request", _fail_http_request)

    fake_renderer = _FakeRenderer(str(tmp_path))

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: fake_renderer,
    )
    def _simulate(_parameters):
        from src.infrastructure.data_access.loaders.stock_loaders import load_stock_data

        df = load_stock_data("sample", "7203")
        assert not df.empty
        return _fake_simulation_result()

    monkeypatch.setattr(runner, "_execute_simulation", _simulate)

    runner.execute("production/test_strategy")

    assert fake_renderer.rendered_strategy_name == "test_strategy"


def test_backtest_runner_progress_callback_and_walk_forward_manifest(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    fake_renderer = _FakeRenderer(str(tmp_path))
    statuses: list[str] = []

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: fake_renderer,
    )
    monkeypatch.setattr(runner, "_execute_simulation", lambda _parameters: _fake_simulation_result())
    monkeypatch.setattr(
        runner,
        "_run_walk_forward",
        lambda _parameters: {"count": 1, "splits": [], "aggregate": {}},
    )

    result = runner.execute(
        "production/test_strategy",
        progress_callback=lambda status, _elapsed: statuses.append(status),
    )

    assert result.manifest_path is not None
    manifest_path = result.manifest_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert statuses == ["設定を読み込み中...", "バックテストを実行中...", "レポートを描画中...", "完了！"]
    assert "walk_forward" in result.summary
    assert manifest["strategy_name"] == "test_strategy"
    assert manifest["dataset_name"] == "sample"
    assert "walk_forward" in manifest


def test_backtest_runner_preserves_core_artifacts_when_html_render_fails(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()

    class _MissingHtmlRenderer(_FakeRenderer):
        def render_report(
            self,
            *,
            report_payload_path: Path,
            html_path: Path,
            strategy_name: str,
            dataset_name: str,
            metrics_path: Path | None = None,
            manifest_path: Path | None = None,
            parameters: dict[str, Any] | None = None,
        ) -> Path:
            _ = (
                report_payload_path,
                html_path,
                strategy_name,
                dataset_name,
                metrics_path,
                manifest_path,
                parameters,
            )
            raise RuntimeError("HTML file was not created")

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: _MissingHtmlRenderer(str(tmp_path)),
    )
    monkeypatch.setattr(runner, "_execute_simulation", lambda _parameters: _fake_simulation_result())

    result = runner.execute("production/test_strategy")

    assert result.html_path is None
    assert result.metrics_path is not None and result.metrics_path.exists()
    assert result.manifest_path is not None and result.manifest_path.exists()
    assert result.render_error == "HTML file was not created"
    assert result.report_payload_path is not None and result.report_payload_path.exists()


def test_backtest_runner_preserves_core_artifacts_when_walk_forward_fails(
    monkeypatch,
    tmp_path: Path,
):
    runner = BacktestRunner()
    fake_renderer = _FakeRenderer(str(tmp_path))

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: fake_renderer,
    )
    monkeypatch.setattr(runner, "_execute_simulation", lambda _parameters: _fake_simulation_result())
    monkeypatch.setattr(
        runner,
        "_run_walk_forward",
        lambda _parameters: (_ for _ in ()).throw(RuntimeError("walk forward failed")),
    )

    result = runner.execute("production/test_strategy")

    assert result.metrics_path is not None and result.metrics_path.exists()
    assert result.manifest_path is not None and result.manifest_path.exists()
    assert result.simulation_payload_path is not None and result.simulation_payload_path.exists()
    assert result.report_payload_path is not None and result.report_payload_path.exists()
    assert "walk_forward" not in result.summary


def test_write_simulation_payload_serializes_vectorbt_style_portfolios(tmp_path: Path):
    runner = BacktestRunner()
    payload_path = tmp_path / "result.simulation.pkl"

    runner._write_simulation_payload(
        payload_path,
        {
            "initial_portfolio": _UnpickleablePortfolio(),
            "kelly_portfolio": _UnpickleablePortfolio(),
            "allocation_info": 0.5,
            "all_entries": None,
        },
    )

    with payload_path.open("rb") as file:
        payload = pickle.load(file)

    assert payload["initial_portfolio"] == {
        "__serialization__": "vectorbt.dumps",
        "payload": b"serialized-portfolio",
    }
    assert payload["kelly_portfolio"] == {
        "__serialization__": "vectorbt.dumps",
        "payload": b"serialized-portfolio",
    }


def test_backtest_runner_elapsed_time_includes_report_render_time(monkeypatch, tmp_path: Path):
    runner = BacktestRunner()

    monkeypatch.setattr(
        runner.config_loader,
        "load_strategy_config",
        lambda _strategy: {"shared_config": {"dataset": "sample", "static_universe": True}},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "get_output_directory",
        lambda _: tmp_path,
    )
    monkeypatch.setattr(
        "src.domains.backtest.core.runner.StaticHtmlReportRenderer",
        lambda _output_dir: _FakeRenderer(str(tmp_path)),
    )
    monkeypatch.setattr(runner, "_execute_simulation", lambda _parameters: _fake_simulation_result())

    def _slow_render(**kwargs):
        _ = kwargs
        time.sleep(0.02)
        html_path = tmp_path / "result.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        return html_path, "completed", None, 0.02

    monkeypatch.setattr(runner, "_render_report", _slow_render)

    result = runner.execute("production/test_strategy")

    assert result.simulation_elapsed_time is not None
    assert result.elapsed_time >= result.simulation_elapsed_time
    assert result.summary["execution_time"] == result.elapsed_time

    assert result.manifest_path is not None
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["execution_time"] == result.elapsed_time
    assert manifest["simulation_elapsed_time"] == result.simulation_elapsed_time
    assert manifest["simulation"]["execution_time"] == result.simulation_elapsed_time


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


def test_build_metrics_payload_preserves_zero_values():
    runner = BacktestRunner()

    class _MetricValue:
        def __init__(self, value: float) -> None:
            self._value = value

        def mean(self) -> float:
            return self._value

    class _Portfolio:
        def stats(self):
            return {
                "Total Return [%]": 0.0,
                "Max Drawdown [%]": 0.0,
                "Sharpe Ratio": 0.0,
                "Sortino Ratio": 0.0,
                "Calmar Ratio": 0.0,
                "Win Rate [%]": 0.0,
                "Total Trades": 0.0,
            }

        def total_return(self):
            return _MetricValue(5.0)

        def sharpe_ratio(self):
            return _MetricValue(5.0)

        def sortino_ratio(self):
            return _MetricValue(5.0)

        def calmar_ratio(self):
            return _MetricValue(5.0)

        def max_drawdown(self):
            return _MetricValue(5.0)

        @property
        def trades(self):
            return types.SimpleNamespace(
                count=lambda: 7,
                win_rate=lambda: _MetricValue(5.0),
            )

    payload = runner._build_metrics_payload(
        kelly_portfolio=_Portfolio(),
        allocation_info=0.0,
    )

    assert payload["total_return"] == 0.0
    assert payload["sharpe_ratio"] == 0.0
    assert payload["trade_count"] == 0
    assert payload["optimal_allocation"] == 0.0


def test_write_json_artifact_normalizes_non_finite_values(tmp_path: Path):
    runner = BacktestRunner()
    artifact_path = tmp_path / "result.metrics.json"

    runner._write_json_artifact(
        artifact_path,
        {
            "total_return": math.nan,
            "nested": {"sharpe_ratio": math.inf},
            "items": [1.0, -math.inf],
        },
    )

    payload = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert payload == {
        "total_return": None,
        "nested": {"sharpe_ratio": None},
        "items": [1.0, None],
    }


def test_collect_portfolio_metrics_drops_non_finite_values():
    runner = BacktestRunner()

    class _Portfolio:
        def total_return(self):
            return math.nan

        def sharpe_ratio(self):
            return math.inf

        def calmar_ratio(self):
            return -math.inf

    assert runner._collect_portfolio_metrics(_Portfolio()) == {
        "total_return": None,
        "sharpe_ratio": None,
        "calmar_ratio": None,
    }


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
                "static_universe": True,
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
        lambda _config, config_override=None, strategy_name=None: {
            "shared_config": {"dataset": "sample", "static_universe": True, "initial_cash": 1000, "fees": 0.1, "kelly_fraction": 0.8}
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


def test_build_parameters_rejects_production_default_dataset_inheritance(monkeypatch):
    runner = BacktestRunner()
    monkeypatch.setattr(runner.config_loader, "default_config", {"parameters": {"shared_config": {"dataset": "default_ds", "static_universe": True}}})
    monkeypatch.setattr(
        runner.config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "default_ds", "initial_cash": 1000000, "static_universe": True},
    )
    monkeypatch.setattr(
        runner.config_loader,
        "resolve_strategy_category",
        lambda strategy_name: "production" if strategy_name == "production/demo" else None,
    )

    with pytest.raises(ValueError, match="shared_config.dataset explicitly"):
        runner._build_parameters(
            {"entry_filter_params": {"volume_ratio_above": {"enabled": True}}},
            strategy_name="production/demo",
        )


class TestBuildParametersConfigOverride:
    """_build_parameters の config_override テスト"""

    def _make_runner_with_defaults(self, monkeypatch: Any) -> BacktestRunner:
        runner = BacktestRunner()
        monkeypatch.setattr(
            runner.config_loader,
            "default_config",
            {"parameters": {"shared_config": {"dataset": "default_ds", "initial_cash": 1000000, "static_universe": True}}},
        )
        monkeypatch.setattr(
            runner.config_loader,
            "merge_shared_config",
            lambda sc: {**{"dataset": "default_ds", "initial_cash": 1000000, "static_universe": True}, **sc.get("shared_config", {})},
        )
        return runner

    def test_no_override(self, monkeypatch: Any):
        """config_override なしの場合、既存設定が保持されること"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        params = runner._build_parameters(strategy_config, config_override=None)
        assert params["shared_config"]["dataset"] == "sample"
        assert params["entry_filter_params"]["volume_ratio_above"]["enabled"] is True

    def test_partial_override_shared_config(self, monkeypatch: Any):
        """shared_config の部分上書きで既存設定が保持されること"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True, "initial_cash": 500000},
        }
        config_override: dict[str, Any] = {
            "shared_config": {"initial_cash": 2000000},
        }
        params = runner._build_parameters(strategy_config, config_override)
        assert params["shared_config"]["dataset"] == "sample"  # 保持
        assert params["shared_config"]["initial_cash"] == 2000000  # 上書き

    def test_market_universe_preset_resolves_stock_codes(self, monkeypatch: Any):
        """market.duckdb universe preset は dataset snapshot ではなく resolver で stock_codes を決める。"""
        runner = self._make_runner_with_defaults(monkeypatch)

        from src.domains.backtest.core import market_universe

        def fake_resolve(shared_config: dict[str, Any]) -> list[str] | None:
            assert shared_config["dataset"] == "prime"
            assert shared_config["start_date"] == "2024-01-05"
            shared_config["universe_preset"] = "prime"
            return ["1301", "7203"]

        monkeypatch.setattr(market_universe, "resolve_backtest_universe_codes", fake_resolve)

        params = runner._build_parameters(
            {
                "shared_config": {
                    "dataset": "prime",
                    "start_date": "2024-01-05",
                    "stock_codes": ["all"],
                },
                "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            },
            config_override=None,
        )

        assert params["shared_config"]["stock_codes"] == ["1301", "7203"]
        assert params["shared_config"]["universe_preset"] == "prime"

    def test_physical_dataset_snapshot_requires_static_universe_opt_in(self, monkeypatch: Any):
        """normal run では物理 dataset snapshot を universe SoT にしない。"""
        runner = self._make_runner_with_defaults(monkeypatch)

        with pytest.raises(ValueError, match="Physical dataset snapshots are no longer supported"):
            runner._build_parameters(
                {
                    "shared_config": {
                        "dataset": "archived_20240101",
                        "start_date": "2024-01-05",
                        "stock_codes": ["all"],
                        "static_universe": False,
                    },
                    "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
                },
                config_override=None,
            )

    def test_static_universe_opt_in_keeps_archived_dataset_path(self, monkeypatch: Any):
        """repro 明示時だけ legacy dataset snapshot universe を許可する。"""
        runner = self._make_runner_with_defaults(monkeypatch)

        params = runner._build_parameters(
            {
                "shared_config": {
                    "dataset": "archived_20240101",
                    "start_date": "2024-01-05",
                    "stock_codes": ["all"],
                    "static_universe": True,
                },
                "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            },
            config_override=None,
        )

        assert params["shared_config"]["stock_codes"] == ["all"]
        assert params["shared_config"]["static_universe"] is True

    def test_override_entry_filter_params(self, monkeypatch: Any):
        """entry_filter_params の部分上書き"""
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True},
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
            "shared_config": {"dataset": "sample", "static_universe": True},
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
            "shared_config": {"dataset": "sample", "static_universe": True},
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
            "shared_config": {"dataset": "sample", "static_universe": True},
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
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "execution_policy": {"mode": "next_session_round_trip"}
            },
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
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "execution_policy": {"mode": "next_session_round_trip"},
                "timeframe": "weekly",
            },
        }

        with pytest.raises(ValueError, match="Invalid shared_config after config merge"):
            runner._build_parameters(strategy_config, config_override)

    def test_current_session_round_trip_rejects_non_empty_exit_trigger_params(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "execution_policy": {"mode": "current_session_round_trip"}
            },
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }

        with pytest.raises(ValueError, match="exit_trigger_params must be empty"):
            runner._build_parameters(strategy_config, config_override)

    def test_current_session_round_trip_rejects_invalid_shared_config_after_merge(
        self,
        monkeypatch: Any,
    ):
        import pytest

        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        config_override: dict[str, Any] = {
            "shared_config": {
                "execution_policy": {"mode": "current_session_round_trip"},
                "timeframe": "weekly",
            },
        }

        with pytest.raises(ValueError, match="Invalid shared_config after config merge"):
            runner._build_parameters(strategy_config, config_override)

    def test_same_day_signal_keeps_exit_trigger_params_when_round_trip_flag_is_off(
        self,
        monkeypatch: Any,
    ):
        runner = self._make_runner_with_defaults(monkeypatch)
        strategy_config: dict[str, Any] = {
            "shared_config": {"dataset": "sample", "static_universe": True},
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True}
            },
            "exit_trigger_params": {"volume_ratio_below": {"enabled": True}},
        }

        params = runner._build_parameters(strategy_config, config_override=None)

        assert params["exit_trigger_params"]["volume_ratio_below"]["enabled"] is True
