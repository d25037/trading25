"""Tests for CLI logging configuration."""

from unittest.mock import patch

from src.cli_bt import configure_logging


TIMESTAMP_TOKEN = "{time:YYYY-MM-DD HH:mm:ss.SSS}"


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
