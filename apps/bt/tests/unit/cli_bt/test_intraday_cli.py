from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from src.entrypoints.cli import app
from src.entrypoints.cli import intraday as intraday_module
from src.entrypoints.http.schemas.db import IntradaySyncRequest, IntradaySyncResponse

runner = CliRunner()


def test_build_intraday_request_defaults_to_latest_ready_date() -> None:
    with patch(
        "src.entrypoints.cli.intraday.resolve_latest_ready_intraday_date",
        return_value="2026-04-15",
    ):
        request, resolved_date = intraday_module._build_intraday_request(
            mode="auto",
            date_value=None,
            date_from=None,
            date_to=None,
            codes=[],
        )

    assert resolved_date == "2026-04-15"
    assert request.date == "2026-04-15"
    assert request.mode == "auto"


def test_bt_intraday_sync_command_uses_resolved_default_date() -> None:
    response = IntradaySyncResponse(
        success=True,
        mode="bulk",
        datesProcessed=1,
        recordsFetched=10,
        recordsStored=10,
        apiCalls=3,
        lastUpdated="2026-04-15T16:50:00+09:00",
    )

    with (
        patch(
            "src.entrypoints.cli.intraday.resolve_latest_ready_intraday_date",
            return_value="2026-04-15",
        ),
        patch(
            "src.entrypoints.cli.intraday.execute_intraday_sync",
            return_value=response,
        ) as mock_execute,
    ):
        result = runner.invoke(app, ["intraday-sync"])

    assert result.exit_code == 0
    request = mock_execute.call_args.args[0]
    assert isinstance(request, IntradaySyncRequest)
    assert request.date == "2026-04-15"
    assert "resolved date" in result.stdout


def test_bt_intraday_sync_command_accepts_explicit_rest_request() -> None:
    response = IntradaySyncResponse(
        success=True,
        mode="rest",
        requestedCodes=1,
        storedCodes=1,
        datesProcessed=1,
        recordsFetched=2,
        recordsStored=2,
        apiCalls=1,
        lastUpdated="2026-04-15T16:50:00+09:00",
    )

    with patch(
        "src.entrypoints.cli.intraday.execute_intraday_sync",
        return_value=response,
    ) as mock_execute:
        result = runner.invoke(
            app,
            ["intraday-sync", "--mode", "rest", "--date", "2026-04-15", "--code", "9984"],
        )

    assert result.exit_code == 0
    request = mock_execute.call_args.args[0]
    assert request.mode == "rest"
    assert request.date == "2026-04-15"
    assert request.codes == ["9984"]


@pytest.mark.asyncio
async def test_execute_intraday_sync_uses_market_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    market_db = MagicMock()
    store = MagicMock()
    store.close = MagicMock()
    client = MagicMock()
    client.close = AsyncMock()
    response = IntradaySyncResponse(
        success=True,
        mode="bulk",
        datesProcessed=1,
        recordsFetched=1,
        recordsStored=1,
        apiCalls=1,
        lastUpdated="2026-04-15T16:50:00+09:00",
    )
    sync_mock = AsyncMock(return_value=response)

    monkeypatch.setattr(
        intraday_module,
        "get_settings",
        lambda: SimpleNamespace(
            jquants_api_key="test-key",
            jquants_plan="standard",
            market_timeseries_dir="/tmp/trading25-market-timeseries",
        ),
    )
    monkeypatch.setattr(intraday_module, "MarketDb", lambda *_args, **_kwargs: market_db)
    monkeypatch.setattr(
        intraday_module,
        "create_time_series_store",
        lambda **_kwargs: store,
    )
    monkeypatch.setattr(
        intraday_module,
        "JQuantsAsyncClient",
        lambda **_kwargs: client,
    )
    monkeypatch.setattr(intraday_module, "sync_intraday_data", sync_mock)

    request = IntradaySyncRequest(date="2026-04-15")
    result = await intraday_module._execute_intraday_sync(request)

    assert result is response
    sync_mock.assert_awaited_once_with(
        request,
        market_db=market_db,
        time_series_store=store,
        jquants_client=client,
    )
    client.close.assert_awaited_once()
    store.close.assert_called_once()
    market_db.close.assert_called_once()
