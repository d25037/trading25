from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from typer.testing import CliRunner

from src.entrypoints.cli import app

runner = CliRunner()


def test_bt_market_maintain_uses_writer_session_and_reports_evidence(tmp_path) -> None:
    session = SimpleNamespace()
    session.close_writable_handles = lambda: "closed-token"
    session.authorize_maintenance = lambda token: "authority" if token == "closed-token" else None
    session.reopen_read_only_and_release = lambda token: SimpleNamespace(close=lambda: None)
    factory = SimpleNamespace(open_existing=lambda: session)
    evidence = SimpleNamespace(
        compacted=True,
        trigger=SimpleNamespace(value="hard"),
        before_bytes=1024,
        after_bytes=512,
        duration_ms=12.5,
        validation="passed",
    )
    settings = SimpleNamespace(market_timeseries_dir=str(tmp_path / "market-timeseries"))

    with (
        patch("src.entrypoints.cli.market.get_settings", return_value=settings),
        patch("src.entrypoints.cli.market.MarketWriterResourceFactory", return_value=factory),
        patch("src.entrypoints.cli.market.MarketCompactor") as compactor_type,
    ):
        compactor_type.return_value.maintain.return_value = evidence
        result = runner.invoke(app, ["market-maintain"])

    assert result.exit_code == 0
    compactor_type.return_value.maintain.assert_called_once_with("authority")
    assert "hard" in result.stdout
    assert "passed" in result.stdout


def test_legacy_market_compact_command_is_not_registered() -> None:
    result = runner.invoke(app, ["market-compact"])

    assert result.exit_code != 0
    assert "No such command" in result.stderr
