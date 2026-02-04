"""
CLI Integration Tests

Basic integration tests for unified CLI system
"""

import pytest
from typer.testing import CliRunner

from src.cli_bt import app

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


@pytest.mark.integration
def test_bt_list_execution():
    """Test 'bt list' command executes (may fail if no strategies found)"""
    result = runner.invoke(app, ["list"])
    # Allow both success (0) and "no strategies found" (1)
    assert result.exit_code in [0, 1]
