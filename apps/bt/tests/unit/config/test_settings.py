"""
Settings unit tests
"""

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
    assert settings_mod._dotenv_path.name == ".env"
