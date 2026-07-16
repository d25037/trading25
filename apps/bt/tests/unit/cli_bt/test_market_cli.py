from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from src.entrypoints.cli import app
from src.infrastructure.db.market.market_compaction import MarketCompactionError
from src.infrastructure.db.market.market_operation_lease import MarketOperationLease
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
)

runner = CliRunner()


def test_bt_market_maintain_uses_writer_session_and_reports_evidence(tmp_path) -> None:
    session = SimpleNamespace()
    session.close_writable_handles = lambda: "closed-token"
    session.authorize_maintenance = lambda token: (
        "authority" if token == "closed-token" else None
    )
    session.reopen_read_only_and_release = lambda token: SimpleNamespace(
        close=lambda: None
    )
    factory = SimpleNamespace(open_existing=lambda: session)
    evidence = SimpleNamespace(
        compacted=True,
        trigger=SimpleNamespace(value="hard_cap"),
        before_bytes=1024,
        after_bytes=512,
        duration_ms=12.5,
        validation="passed",
    )
    settings = SimpleNamespace(
        market_timeseries_dir=str(tmp_path / "market-timeseries")
    )

    with (
        patch("src.entrypoints.cli.market.get_settings", return_value=settings),
        patch(
            "src.entrypoints.cli.market.MarketWriterResourceFactory",
            return_value=factory,
        ),
        patch("src.entrypoints.cli.market.MarketCompactor") as compactor_type,
    ):
        compactor_type.return_value.maintain.return_value = evidence
        result = runner.invoke(app, ["market-maintain"])

    assert result.exit_code == 0
    compactor_type.return_value.maintain.assert_called_once_with("authority")
    assert "hard_cap" in result.stdout
    assert "passed" in result.stdout


def test_legacy_market_compact_command_is_not_registered() -> None:
    result = runner.invoke(app, ["market-compact"])

    assert result.exit_code != 0
    assert "No such command" in result.stderr


@pytest.mark.parametrize(
    "failure",
    [
        "Insufficient filesystem capacity for compaction",
        "Market v4/PIT lineage validation failed",
        "Market compaction exchange failed",
    ],
)
def test_bt_market_maintain_releases_retryable_failure_before_second_invocation(
    tmp_path, failure: str
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    initial = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=market_root,
    ).reset_and_open_v4()
    initial_token = initial.close_writable_handles()
    initial_read_only = initial.reopen_read_only_and_release(initial_token)
    initial_read_only.close()
    evidence = SimpleNamespace(
        compacted=False,
        trigger=SimpleNamespace(value="none"),
        before_bytes=1024,
        after_bytes=1024,
        duration_ms=1.0,
        validation="passed",
    )
    settings = SimpleNamespace(market_timeseries_dir=str(market_root))

    with (
        patch("src.entrypoints.cli.market.get_settings", return_value=settings),
        patch("src.entrypoints.cli.market.MarketCompactor") as compactor_type,
    ):
        compactor_type.return_value.maintain.side_effect = [
            MarketCompactionError(failure),
            evidence,
        ]
        failed = runner.invoke(app, ["market-maintain"])

        process_lock = MarketWriterResourceFactory._PROCESS_WRITER_LOCK
        assert process_lock.acquire(blocking=False)
        process_lock.release()
        lease = MarketOperationLease.acquire(
            data_root,
            exclusive=True,
            blocking=False,
        )
        lease.release()

        retried = runner.invoke(app, ["market-maintain"])

    assert failed.exit_code == 1
    assert failure in failed.stdout
    assert retried.exit_code == 0
    assert compactor_type.return_value.maintain.call_count == 2


def test_bt_market_maintain_keeps_ambiguous_identity_failure_explicitly_fenced(
    tmp_path,
) -> None:
    session = MagicMock(fenced=True)
    session.close_writable_handles.return_value = "closed-token"
    session.authorize_maintenance.return_value = "authority"
    factory = MagicMock()
    factory.open_existing.return_value = session
    settings = SimpleNamespace(
        market_timeseries_dir=str(tmp_path / "market-timeseries")
    )

    with (
        patch("src.entrypoints.cli.market.get_settings", return_value=settings),
        patch(
            "src.entrypoints.cli.market.MarketWriterResourceFactory",
            return_value=factory,
        ),
        patch("src.entrypoints.cli.market.MarketCompactor") as compactor_type,
    ):
        compactor_type.return_value.maintain.side_effect = MarketCompactionError(
            "Maintenance recovery identity mismatch"
        )
        result = runner.invoke(app, ["market-maintain"])

    assert result.exit_code == 1
    assert "ownership remains fenced" in result.stdout
    session.reopen_read_only_and_release.assert_not_called()


def test_bt_market_maintain_keeps_handle_close_failure_explicitly_fenced(
    tmp_path,
) -> None:
    session = MagicMock(fenced=True)
    session.close_writable_handles.side_effect = OSError("handle close failed")
    factory = MagicMock()
    factory.open_existing.return_value = session
    settings = SimpleNamespace(
        market_timeseries_dir=str(tmp_path / "market-timeseries")
    )

    with (
        patch("src.entrypoints.cli.market.get_settings", return_value=settings),
        patch(
            "src.entrypoints.cli.market.MarketWriterResourceFactory",
            return_value=factory,
        ),
    ):
        result = runner.invoke(app, ["market-maintain"])

    assert result.exit_code == 1
    assert "handle close failed" in result.stdout
    assert "ownership remains fenced" in result.stdout
    session.reopen_read_only_and_release.assert_not_called()
