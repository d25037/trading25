"""Settings unit tests."""

from src.shared.config.settings import get_settings, reload_settings


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("API_BASE_URL", raising=False)
    monkeypatch.delenv("BT_API_URL", raising=False)
    monkeypatch.delenv("API_TIMEOUT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("BT_BACKTEST_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BT_OPTIMIZATION_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BT_LAB_JOB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MOOMOO_OPEND_ENABLED", raising=False)
    monkeypatch.delenv("MOOMOO_OPEND_HOST", raising=False)
    monkeypatch.delenv("MOOMOO_OPEND_PORT", raising=False)
    monkeypatch.delenv("MOOMOO_OPEND_IS_ENCRYPT", raising=False)
    monkeypatch.delenv("MOOMOO_OPEND_MAX_HISTORY_ROWS", raising=False)

    settings = reload_settings()

    assert settings.bt_api_url == "http://localhost:3002"
    assert settings.api_timeout == 30.0
    assert settings.log_level == "WARNING"
    assert settings.backtest_job_timeout_seconds == 3600
    assert settings.optimization_job_timeout_seconds == 3600
    assert settings.lab_job_timeout_seconds == 3600
    assert settings.moomoo_opend_enabled is True
    assert settings.moomoo_opend_host == "127.0.0.1"
    assert settings.moomoo_opend_port == 11111
    assert settings.moomoo_opend_is_encrypt is False
    assert settings.moomoo_opend_max_history_rows == 5000


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("BT_API_URL", "http://example:3002")
    monkeypatch.setenv("API_TIMEOUT", "12.5")
    monkeypatch.setenv("LOG_LEVEL", "info")
    monkeypatch.setenv("BT_BACKTEST_JOB_TIMEOUT_SECONDS", "1800")
    monkeypatch.setenv("BT_OPTIMIZATION_JOB_TIMEOUT_SECONDS", "2400")
    monkeypatch.setenv("BT_LAB_JOB_TIMEOUT_SECONDS", "3000")
    monkeypatch.setenv("MOOMOO_OPEND_ENABLED", "false")
    monkeypatch.setenv("MOOMOO_OPEND_HOST", "localhost")
    monkeypatch.setenv("MOOMOO_OPEND_PORT", "22222")
    monkeypatch.setenv("MOOMOO_OPEND_IS_ENCRYPT", "true")
    monkeypatch.setenv("MOOMOO_OPEND_MAX_HISTORY_ROWS", "2500")

    settings = reload_settings()

    assert settings.bt_api_url == "http://example:3002"
    assert settings.api_timeout == 12.5
    assert settings.log_level == "info"
    assert settings.backtest_job_timeout_seconds == 1800
    assert settings.optimization_job_timeout_seconds == 2400
    assert settings.lab_job_timeout_seconds == 3000
    assert settings.moomoo_opend_enabled is False
    assert settings.moomoo_opend_host == "localhost"
    assert settings.moomoo_opend_port == 22222
    assert settings.moomoo_opend_is_encrypt is True
    assert settings.moomoo_opend_max_history_rows == 2500


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


def test_settings_module_does_not_define_repo_dotenv_source():
    import src.shared.config.settings as settings_mod

    assert not hasattr(settings_mod, "_dotenv_path")


def test_settings_ignores_runtime_env_file_hint(monkeypatch, tmp_path):
    env_file = tmp_path / "trading25.env"
    env_file.write_text("BT_API_URL=http://runtime-config.example:3002\n", encoding="utf-8")
    monkeypatch.delenv("BT_API_URL", raising=False)
    monkeypatch.setenv("TRADING25_ENV_FILE", str(env_file))

    settings = reload_settings()

    assert settings.bt_api_url == "http://localhost:3002"


def test_settings_ignores_missing_runtime_env_file_hint(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing.env"
    monkeypatch.setenv("TRADING25_ENV_FILE", str(missing_path))

    settings = reload_settings()

    assert settings.bt_api_url == "http://localhost:3002"


def test_settings_respects_explicit_db_paths(monkeypatch):
    monkeypatch.setenv("TRADING25_DATA_DIR", "/tmp/custom-data-dir")
    monkeypatch.setenv("MARKET_DB_PATH", "/tmp/market-explicit.db")
    monkeypatch.setenv("PORTFOLIO_DB_PATH", "/tmp/portfolio-explicit.db")
    monkeypatch.setenv("DATASET_BASE_PATH", "/tmp/datasets-explicit")

    settings = reload_settings()

    assert settings.market_db_path == "/tmp/market-explicit.db"
    assert settings.portfolio_db_path == "/tmp/portfolio-explicit.db"
    assert settings.dataset_base_path == "/tmp/datasets-explicit"
