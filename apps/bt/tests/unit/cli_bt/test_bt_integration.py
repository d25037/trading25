"""
CLI Integration Tests

Basic integration tests for unified CLI system
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from src.entrypoints import cli as cli_module
from src.entrypoints.cli import app

runner = CliRunner()


def test_bt_help():
    """Test 'bt --help' command works correctly"""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "bt" in result.stdout.lower() or "help" in result.stdout.lower()


def test_bt_backtest_help():
    """Test 'bt backtest --help' command works correctly"""
    result = runner.invoke(app, ["backtest", "--help"])
    assert result.exit_code == 0
    assert "backtest" in result.stdout.lower() or "strategy" in result.stdout.lower()


def test_bt_optimize_help():
    """Test 'bt backtest --help' shows optimize flag"""
    result = runner.invoke(app, ["backtest", "--help"])
    assert result.exit_code == 0
    assert "optimize" in result.stdout.lower() or "--optimize" in result.stdout.lower()


def test_bt_list_help():
    """Test 'bt list --help' command works correctly"""
    result = runner.invoke(app, ["list", "--help"])
    assert result.exit_code == 0


def test_bt_validate_help():
    """Test 'bt validate --help' command works correctly"""
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "validate" in result.stdout.lower() or "strategy" in result.stdout.lower()


def test_bt_cleanup_help():
    """Test 'bt cleanup --help' command works correctly"""
    result = runner.invoke(app, ["cleanup", "--help"])
    assert result.exit_code == 0
    assert "cleanup" in result.stdout.lower() or "notebook" in result.stdout.lower()


def test_bt_intraday_sync_help():
    """Test 'bt intraday-sync --help' command works correctly"""
    result = runner.invoke(app, ["intraday-sync", "--help"])
    assert result.exit_code == 0
    assert "intraday" in result.stdout.lower()


@pytest.mark.integration
def test_bt_list_execution():
    """Test 'bt list' command executes (may fail if no strategies found)"""
    result = runner.invoke(app, ["list"])
    # Allow both success (0) and "no strategies found" (1)
    assert result.exit_code in [0, 1]


def test_configure_logging_sets_debug_and_warning_levels() -> None:
    with (
        patch.object(cli_module.logger, "remove") as mock_remove,
        patch.object(cli_module.logger, "add") as mock_add,
    ):
        cli_module.configure_logging(True)
        cli_module.configure_logging(False)

    assert mock_remove.call_count == 2
    assert mock_add.call_args_list[0].kwargs["level"] == "DEBUG"
    assert mock_add.call_args_list[1].kwargs["level"] == "WARNING"


def test_main_callback_sets_context_object() -> None:
    class DummyContext:
        def __init__(self) -> None:
            self.obj = None

        def ensure_object(self, _value_type):  # noqa: ANN001
            if self.obj is None:
                self.obj = {}

    ctx = DummyContext()
    with patch.object(cli_module, "configure_logging") as mock_configure:
        cli_module.main(ctx, verbose=True)

    assert ctx.obj == {"verbose": True}
    mock_configure.assert_called_once_with(True)


def test_backtest_command_routes_normal_and_optimize_modes() -> None:
    ctx = SimpleNamespace(obj={"verbose": True})
    with (
        patch("src.entrypoints.cli.backtest.run_backtest") as mock_run_backtest,
        patch("src.entrypoints.cli.optimize.run_optimization") as mock_run_optimization,
    ):
        cli_module.backtest_command(ctx, "production/demo", optimize=False)
        cli_module.backtest_command(ctx, "production/demo", optimize=True)

    mock_run_backtest.assert_called_once_with(strategy="production/demo")
    mock_run_optimization.assert_called_once_with(
        strategy_name="production/demo",
        verbose=True,
    )


def test_manage_commands_delegate_to_manage_module() -> None:
    with (
        patch("src.entrypoints.cli.manage.list_strategies") as mock_list,
        patch("src.entrypoints.cli.manage.validate_strategy") as mock_validate,
        patch("src.entrypoints.cli.manage.cleanup_notebooks") as mock_cleanup,
    ):
        cli_module.list_command()
        cli_module.validate_command("production/demo")
        cli_module.cleanup_command(days=30, output_dir="/tmp/out")

    mock_list.assert_called_once_with()
    mock_validate.assert_called_once_with("production/demo")
    mock_cleanup.assert_called_once_with(days=30, output_dir="/tmp/out")


def test_migrate_optimization_specs_command_delegates() -> None:
    with patch(
        "src.entrypoints.cli.optimize.migrate_legacy_optimization_specs"
    ) as mock_migrate:
        cli_module.migrate_optimization_specs_command()

    mock_migrate.assert_called_once_with()


def test_server_helpers_cover_pid_detection_and_log_config() -> None:
    completed = MagicMock(stdout="123\n456\n")
    with patch("subprocess.run", return_value=completed) as mock_run:
        assert cli_module._kill_process_on_port(3002) is True

    assert mock_run.call_count == 3
    config = cli_module._build_uvicorn_log_config()
    assert "default" in config["formatters"]
    assert "access" in config["formatters"]


def test_server_command_runs_uvicorn_and_reports_url() -> None:
    with (
        patch.object(cli_module, "_kill_process_on_port", return_value=True),
        patch.object(cli_module.console, "print") as mock_print,
        patch("time.sleep") as mock_sleep,
        patch("uvicorn.run") as mock_run,
    ):
        cli_module.server_command(port=3100, host="127.0.0.1", reload=True)

    mock_sleep.assert_called_once_with(0.5)
    mock_run.assert_called_once()
    kwargs = mock_run.call_args.kwargs
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["port"] == 3100
    assert kwargs["reload"] is True
    assert mock_print.call_count >= 3
