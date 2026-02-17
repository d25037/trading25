"""Tests for CLI logging configuration."""

from unittest.mock import patch

import uvicorn

from src.cli_bt import _build_uvicorn_log_config, configure_logging


TIMESTAMP_TOKEN = "{time:YYYY-MM-DD HH:mm:ss.SSS}"
UVICORN_TIMESTAMP_TOKEN = "%(asctime)s.%(msecs)03d"


def test_configure_logging_verbose_has_timestamp_and_debug_level() -> None:
    with patch("src.cli_bt.logger") as mock_logger:
        configure_logging(True)

    assert mock_logger.remove.call_count == 1
    assert mock_logger.add.call_count == 1
    assert [entry[0] for entry in mock_logger.method_calls[:2]] == ["remove", "add"]

    add_kwargs = mock_logger.add.call_args.kwargs
    assert add_kwargs["level"] == "DEBUG"
    assert TIMESTAMP_TOKEN in add_kwargs["format"]


def test_configure_logging_non_verbose_has_timestamp_and_warning_level() -> None:
    with patch("src.cli_bt.logger") as mock_logger:
        configure_logging(False)

    assert mock_logger.remove.call_count == 1
    assert mock_logger.add.call_count == 1
    assert [entry[0] for entry in mock_logger.method_calls[:2]] == ["remove", "add"]

    add_kwargs = mock_logger.add.call_args.kwargs
    assert add_kwargs["level"] == "WARNING"
    assert TIMESTAMP_TOKEN in add_kwargs["format"]


def test_build_uvicorn_log_config_has_timestamp_for_default_and_access() -> None:
    original_default_fmt = uvicorn.config.LOGGING_CONFIG["formatters"]["default"]["fmt"]
    original_access_fmt = uvicorn.config.LOGGING_CONFIG["formatters"]["access"]["fmt"]

    log_config = _build_uvicorn_log_config()

    assert UVICORN_TIMESTAMP_TOKEN in log_config["formatters"]["default"]["fmt"]
    assert UVICORN_TIMESTAMP_TOKEN in log_config["formatters"]["access"]["fmt"]
    assert log_config["formatters"]["default"]["datefmt"] == "%Y-%m-%d %H:%M:%S"
    assert log_config["formatters"]["access"]["datefmt"] == "%Y-%m-%d %H:%M:%S"

    # uvicorn のグローバル定数を破壊しないこと
    assert uvicorn.config.LOGGING_CONFIG["formatters"]["default"]["fmt"] == original_default_fmt
    assert uvicorn.config.LOGGING_CONFIG["formatters"]["access"]["fmt"] == original_access_fmt
