"""
Settings unit tests
"""

import pytest

from src.config.settings import get_settings, reload_settings


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("API_BASE_URL", raising=False)
    monkeypatch.delenv("API_TIMEOUT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    settings = reload_settings()

    assert settings.api_base_url == "http://localhost:3002"
    assert settings.api_timeout == 30.0
    assert settings.log_level == "WARNING"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://example:3002")
    monkeypatch.setenv("API_TIMEOUT", "12.5")
    monkeypatch.setenv("LOG_LEVEL", "info")

    settings = reload_settings()

    assert settings.api_base_url == "http://example:3002"
    assert settings.api_timeout == 12.5
    assert settings.log_level == "info"


def test_settings_cache(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://cache-test")
    settings = reload_settings()
    assert settings.api_base_url == "http://cache-test"

    monkeypatch.setenv("API_BASE_URL", "http://cache-updated")
    assert get_settings().api_base_url == "http://cache-test"


def test_settings_module_loads_dotenv():
    """settings モジュールが load_dotenv をモジュールレベルで呼び出していること"""
    import src.config.settings as settings_mod

    # モジュールレベルで _dotenv_path が定義されていることを確認
    assert hasattr(settings_mod, "_dotenv_path")
    expected = settings_mod._repo_root / ".env"
    assert settings_mod._dotenv_path == expected


def test_find_repo_root_raises_when_git_not_found(tmp_path):
    from src.config.settings import _find_repo_root

    start = tmp_path / "nested" / "path"
    start.mkdir(parents=True)

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
