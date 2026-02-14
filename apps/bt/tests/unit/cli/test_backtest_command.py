"""Unit tests for src.cli_bt.backtest."""

from __future__ import annotations

import sys
import types
import time
from pathlib import Path

import pytest

from src.cli_bt import backtest as backtest_module
from src.data.access.mode import DATA_ACCESS_MODE_ENV


class _NoOpLive:
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002
        _ = (args, kwargs)

    def __enter__(self) -> _NoOpLive:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001, ANN201
        _ = (exc_type, exc, tb)
        return None

    def update(self, _value) -> None:  # noqa: ANN001
        return None


def test_run_backtest_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: dict[str, object] = {}

    class _FakeConfigLoader:
        def __init__(self) -> None:
            self.default_config = {"parameters": {}}

        def load_strategy_config(self, _strategy: str) -> dict[str, object]:
            return {
                "shared_config": {"dataset": "sample", "stock_codes": ["all"]},
                "display_name": "test",
                "description": "desc",
                "entry_filter_params": {"volume": {"enabled": True}},
                "exit_trigger_params": {"volume": {"enabled": False}},
            }

        def merge_shared_config(self, strategy_config: dict[str, object]) -> dict[str, object]:
            return strategy_config["shared_config"]  # type: ignore[index]

        def get_output_directory(self, _strategy_config: dict[str, object]) -> Path:
            return tmp_path

    class _FakeMarimoExecutor:
        def __init__(self, output_dir: str) -> None:
            self.output_dir = output_dir

        def get_execution_summary(self, _html_path: Path) -> dict[str, object]:
            return {"html_path": "x", "generated_at": "now"}

    monkeypatch.setattr(backtest_module, "ConfigLoader", _FakeConfigLoader)
    monkeypatch.setattr(
        backtest_module,
        "_execute_with_progress",
        lambda executor, template_path, parameters, strategy_name: (  # noqa: ARG005
            tmp_path / "result.html",
            1.2,
        ),
    )
    monkeypatch.setattr(backtest_module, "_display_execution_info", lambda *_args: None)
    monkeypatch.setattr(backtest_module, "_display_execution_summary", lambda *_args: None)
    monkeypatch.setattr(
        backtest_module.console,
        "print",
        lambda *args, **kwargs: calls.setdefault("printed", True),  # noqa: ARG005
    )
    monkeypatch.setitem(
        sys.modules,
        "src.lib.backtest_core.marimo_executor",
        types.SimpleNamespace(MarimoExecutor=_FakeMarimoExecutor),
    )

    backtest_module.run_backtest("production/test_strategy")

    assert calls.get("printed") is True


def test_run_backtest_execution_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _FakeConfigLoader:
        def __init__(self) -> None:
            self.default_config = {"parameters": {}}

        def load_strategy_config(self, _strategy: str) -> dict[str, object]:
            return {"shared_config": {"dataset": "sample"}}

        def merge_shared_config(self, strategy_config: dict[str, object]) -> dict[str, object]:
            return strategy_config["shared_config"]  # type: ignore[index]

        def get_output_directory(self, _strategy_config: dict[str, object]) -> Path:
            return tmp_path

    class _FakeMarimoExecutor:
        def __init__(self, output_dir: str) -> None:
            self.output_dir = output_dir

    monkeypatch.setattr(backtest_module, "ConfigLoader", _FakeConfigLoader)
    monkeypatch.setattr(
        backtest_module,
        "_execute_with_progress",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("exec failed")),  # noqa: ARG005
    )
    monkeypatch.setattr(backtest_module, "_display_execution_info", lambda *_args: None)
    monkeypatch.setattr(backtest_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setitem(
        sys.modules,
        "src.lib.backtest_core.marimo_executor",
        types.SimpleNamespace(MarimoExecutor=_FakeMarimoExecutor),
    )

    with pytest.raises(SystemExit, match="1"):
        backtest_module.run_backtest("production/test_strategy")


def test_run_backtest_strategy_name_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeConfigLoader:
        def load_strategy_config(self, _strategy: str) -> dict[str, object]:
            raise ValueError("invalid strategy")

    monkeypatch.setattr(backtest_module, "ConfigLoader", _FakeConfigLoader)
    monkeypatch.setattr(backtest_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005

    with pytest.raises(SystemExit, match="1"):
        backtest_module.run_backtest("bad")


def test_execute_with_progress_sets_direct_data_access_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeExecutor:
        def __init__(self) -> None:
            self.extra_env: dict[str, str] | None = None

        def execute_notebook(
            self,
            template_path: str,
            parameters: dict,
            strategy_name: str,
            extra_env: dict[str, str] | None = None,
        ) -> Path:
            _ = (template_path, parameters, strategy_name)
            self.extra_env = extra_env
            return tmp_path / "result.html"

    fake_executor = _FakeExecutor()
    monkeypatch.setattr(backtest_module, "Live", _NoOpLive)
    monkeypatch.setattr(backtest_module.time, "sleep", lambda _s: None)

    html_path, _elapsed = backtest_module._execute_with_progress(
        executor=fake_executor,
        template_path="template.py",
        parameters={},
        strategy_name="test",
    )

    assert html_path == tmp_path / "result.html"
    assert fake_executor.extra_env == {DATA_ACCESS_MODE_ENV: "direct"}


def test_execute_with_progress_updates_live_spinner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[object] = []

    class _RecordingLive(_NoOpLive):
        def update(self, value) -> None:  # noqa: ANN001
            updates.append(value)

    class _SlowExecutor:
        def execute_notebook(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            time.sleep(0.2)
            return tmp_path / "result.html"

    monkeypatch.setattr(backtest_module, "Live", _RecordingLive)
    monkeypatch.setattr(backtest_module.time, "time", lambda: 120.0)

    html_path, _elapsed = backtest_module._execute_with_progress(
        executor=_SlowExecutor(),
        template_path="template.py",
        parameters={},
        strategy_name="test",
    )

    assert html_path == tmp_path / "result.html"
    assert updates


def test_execute_with_progress_raises_on_executor_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FailExecutor:
        def execute_notebook(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            raise RuntimeError("boom")

    monkeypatch.setattr(backtest_module, "Live", _NoOpLive)
    monkeypatch.setattr(backtest_module.time, "sleep", lambda _s: None)

    with pytest.raises(RuntimeError, match="boom"):
        backtest_module._execute_with_progress(
            executor=_FailExecutor(),
            template_path="template.py",
            parameters={},
            strategy_name="test",
        )


def test_execute_with_progress_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    class _InterruptLive:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002
            _ = (args, kwargs)

        def __enter__(self):  # noqa: ANN201
            raise KeyboardInterrupt()

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001, ANN201
            _ = (exc_type, exc, tb)
            return None

    class _SlowExecutor:
        def execute_notebook(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            time.sleep(0.2)
            return Path("/tmp/unused.html")

    monkeypatch.setattr(backtest_module, "Live", _InterruptLive)
    monkeypatch.setattr(backtest_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005

    with pytest.raises(KeyboardInterrupt):
        backtest_module._execute_with_progress(
            executor=_SlowExecutor(),
            template_path="template.py",
            parameters={},
            strategy_name="test",
        )


def test_display_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    printed: list[object] = []
    monkeypatch.setattr(
        backtest_module.console,
        "print",
        lambda value, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )

    backtest_module._display_execution_info(
        {"display_name": "A", "description": "B"},
        {"shared_config": {"stock_codes": ["all"], "initial_cash": 1000, "fees": 0.01}},
    )
    backtest_module._display_execution_info(
        {"display_name": "A", "description": "B"},
        {
            "shared_config": {
                "stock_codes": ["1", "2", "3", "4", "5", "6"],
                "initial_cash": 1000,
                "fees": 0.01,
            }
        },
    )
    backtest_module._display_execution_info(
        {"display_name": "A", "description": "B"},
        {"shared_config": {"stock_codes": "N/A", "initial_cash": 1000, "fees": 0.01}},
    )
    backtest_module._display_execution_summary({"error": "failed"})
    backtest_module._display_execution_summary(
        {
            "execution_time": 1.5,
            "html_path": "/tmp/a.html",
            "file_size": 2048,
            "generated_at": "now",
        }
    )
    backtest_module._display_execution_summary(
        {
            "html_path": "/tmp/b.html",
            "file_size": 0,
            "generated_at": "now",
        }
    )

    assert len(printed) >= 6
