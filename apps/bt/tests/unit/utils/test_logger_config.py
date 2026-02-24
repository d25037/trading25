"""logger_config.py のテスト"""


from src.utils.logger_config import (
    Logger,
    get_logger,
    log_debug,
    log_error,
    log_info,
    log_warning,
    sanitize_sensitive_info,
    setup_logger,
    setup_production_logger,
    setup_quiet_logger,
    setup_verbose_logger,
)


class TestSanitizeSensitiveInfo:
    def test_masks_unix_user_path(self):
        result = sanitize_sensitive_info("/Users/alice/projects/myapp")
        assert "alice" not in result
        assert ".../myapp" in result

    def test_masks_windows_user_path(self):
        result = sanitize_sensitive_info("C:\\Users\\alice\\projects\\myapp")
        assert "alice" not in result

    def test_masks_system_path(self):
        result = sanitize_sensitive_info("File at /home/user/data.txt")
        assert "[SYSTEM_PATH]" in result

    def test_masks_password(self):
        result = sanitize_sensitive_info("password=mysecret123")
        assert "mysecret123" not in result
        assert "password=***" in result

    def test_masks_token(self):
        result = sanitize_sensitive_info("token=dummytoken")
        assert "dummytoken" not in result

    def test_masks_key(self):
        result = sanitize_sensitive_info("key=secretkey")
        assert "secretkey" not in result

    def test_masks_sqlite_connection(self):
        result = sanitize_sensitive_info("sqlite:///path/to/database.db")
        assert "sqlite:///" in result
        assert "database.db" in result

    def test_preserves_normal_text(self):
        msg = "Processing 100 records successfully"
        assert sanitize_sensitive_info(msg) == msg


class TestLogger:
    def test_creation(self):
        log = Logger(name="test", printlog=True)
        assert log.name == "test"
        assert log.printlog is True

    def test_quiet_mode(self):
        log = Logger(name="test", printlog=False)
        assert log.printlog is False

    def test_error_always_logs(self):
        log = Logger(name="test", printlog=False)
        log.error("test error")

    def test_info_suppressed_when_no_printlog(self):
        log = Logger(name="test", printlog=False)
        log.info("should be suppressed")

    def test_log_alias(self):
        log = Logger(name="test", printlog=True)
        log.log("test message")


class TestGetLogger:
    def test_returns_logger(self):
        log = get_logger("test_module")
        assert log is not None


class TestSetupLogger:
    def test_quiet_mode(self):
        setup_logger(quiet=True)

    def test_verbose_mode(self):
        setup_logger(verbose=True)

    def test_level_override(self):
        setup_logger(level_override="DEBUG")

    def test_default_mode(self):
        setup_logger()


class TestSetupHelpers:
    def test_quiet_logger(self):
        setup_quiet_logger()

    def test_verbose_logger(self):
        setup_verbose_logger()

    def test_production_logger(self):
        setup_production_logger()


class TestLoggerMethods:
    def test_debug_with_printlog(self):
        log = Logger(name="test", printlog=True)
        log.debug("debug message")

    def test_info_with_printlog(self):
        log = Logger(name="test", printlog=True)
        log.info("info message")

    def test_success_with_printlog(self):
        log = Logger(name="test", printlog=True)
        log.success("success message")

    def test_warning_with_printlog(self):
        log = Logger(name="test", printlog=True)
        log.warning("warning message")

    def test_critical(self):
        log = Logger(name="test", printlog=False)
        log.critical("critical message")


class TestGlobalLogFunctions:
    def test_log_info(self):
        log_info("info")

    def test_log_debug(self):
        log_debug("debug")

    def test_log_error(self):
        log_error("error")

    def test_log_warning(self):
        log_warning("warning")
