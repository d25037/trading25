"""
Settings unit tests
"""

import pytest

from src.shared.config.settings import get_settings, reload_settings


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("API_BASE_URL", raising=False)
    monkeypatch.delenv("BT_API_URL", raising=False)
    monkeypatch.delenv("API_TIMEOUT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("BT_BACKTEST_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BT_OPTIMIZATION_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BT_LAB_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MARKET_SYNC_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("MARKET_SYNC_SCHEDULER_TIME_JST", raising=False)
    monkeypatch.delenv("MARKET_SYNC_SCHEDULER_ENFORCE_BULK_FOR_STOCK_DATA", raising=False)

    settings = reload_settings()

    assert settings.bt_api_url == "http://localhost:3002"
    assert settings.api_timeout == 30.0
    assert settings.log_level == "WARNING"
    assert settings.backtest_job_timeout_seconds == 3600
    assert settings.optimization_job_timeout_seconds == 3600
    assert settings.lab_job_timeout_seconds == 3600
    assert settings.market_sync_scheduler_enabled is False
    assert settings.market_sync_scheduler_time_jst == "16:30"
    assert settings.market_sync_scheduler_enforce_bulk_for_stock_data is False


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("BT_API_URL", "http://example:3002")
    monkeypatch.setenv("API_TIMEOUT", "12.5")
    monkeypatch.setenv("LOG_LEVEL", "info")
    monkeypatch.setenv("BT_BACKTEST_JOB_TIMEOUT_SECONDS", "1800")
    monkeypatch.setenv("BT_OPTIMIZATION_JOB_TIMEOUT_SECONDS", "2400")
    monkeypatch.setenv("BT_LAB_JOB_TIMEOUT_SECONDS", "3000")
    monkeypatch.setenv("MARKET_SYNC_SCHEDULER_ENABLED", "true")
    monkeypatch.setenv("MARKET_SYNC_SCHEDULER_TIME_JST", "17:05")
    monkeypatch.setenv("MARKET_SYNC_SCHEDULER_ENFORCE_BULK_FOR_STOCK_DATA", "true")

    settings = reload_settings()

    assert settings.bt_api_url == "http://example:3002"
    assert settings.api_timeout == 12.5
    assert settings.log_level == "info"
    assert settings.backtest_job_timeout_seconds == 1800
    assert settings.optimization_job_timeout_seconds == 2400
    assert settings.lab_job_timeout_seconds == 3000
    assert settings.market_sync_scheduler_enabled is True
    assert settings.market_sync_scheduler_time_jst == "17:05"
    assert settings.market_sync_scheduler_enforce_bulk_for_stock_data is True


def test_settings_cache(monkeypatch):
    monkeypatch.setenv("BT_API_URL", "http://cache-test")
    settings = reload_settings()
    assert settings.bt_api_url == "http://cache-test"

    monkeypatch.setenv("BT_API_URL", "http://cache-updated")
    assert get_settings().bt_api_url == "http://cache-test"


def test_settings_ignores_legacy_api_base_url(monkeypatch):
    monkeypatch.delenv("BT_API_URL", raising=False)
    monkeypatch.setenv("API_BASE_URL", "http://legacy.example:3002")

    settings = reload_settings()

    assert settings.bt_api_url == "http://localhost:3002"


def test_settings_module_loads_dotenv():
    """settings モジュールが load_dotenv をモジュールレベルで呼び出していること"""
    import src.shared.config.settings as settings_mod

    # モジュールレベルで _dotenv_path が定義されていることを確認
    assert hasattr(settings_mod, "_dotenv_path")
    expected = settings_mod._repo_root / ".env"
    assert settings_mod._dotenv_path == expected


def test_find_repo_root_raises_when_git_not_found():
    from pathlib import Path

    from src.shared.config.settings import _find_repo_root

    start = Path("/__trading25_missing_git_root__/nested/path")

    with pytest.raises(RuntimeError):
        _find_repo_root(start)


def test_settings_respects_explicit_db_paths(monkeypatch):
    monkeypatch.setenv("TRADING25_DATA_DIR", "/tmp/custom-data-dir")
    monkeypatch.setenv("MARKET_DB_PATH", "/tmp/market-explicit.db")
    monkeypatch.setenv("PORTFOLIO_DB_PATH", "/tmp/portfolio-explicit.db")
    monkeypatch.setenv("DATASET_BASE_PATH", "/tmp/datasets-explicit")

    settings = reload_settings()

    assert settings.market_db_path == "/tmp/market-explicit.db"
    assert settings.portfolio_db_path == "/tmp/portfolio-explicit.db"
    assert settings.dataset_base_path == "/tmp/datasets-explicit"
