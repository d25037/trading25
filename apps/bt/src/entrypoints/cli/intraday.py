"""Intraday minute sync CLI helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

import typer
from rich.console import Console
from rich.table import Table

from src.application.services.intraday_schedule import resolve_latest_ready_intraday_date
from src.application.services.intraday_sync_service import sync_intraday_data
from src.entrypoints.http.schemas.db import (
    IntradaySyncModeLiteral,
    IntradaySyncRequest,
    IntradaySyncResponse,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.time_series_store import create_time_series_store
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.shared.config.settings import get_settings

console = Console()


def _build_intraday_request(
    *,
    mode: str,
    date_value: str | None,
    date_from: str | None,
    date_to: str | None,
    codes: list[str],
) -> tuple[IntradaySyncRequest, str | None]:
    resolved_date: str | None = None
    if date_value is None and date_from is None and date_to is None:
        resolved_date = resolve_latest_ready_intraday_date()
        date_value = resolved_date

    request = IntradaySyncRequest(
        mode=cast(IntradaySyncModeLiteral, mode),
        date=date_value,
        dateFrom=date_from,
        dateTo=date_to,
        codes=codes,
    )
    return request, resolved_date


async def _execute_intraday_sync(request: IntradaySyncRequest) -> IntradaySyncResponse:
    settings = get_settings()
    if not settings.jquants_api_key:
        raise RuntimeError("JQUANTS_API_KEY is not configured")

    timeseries_dir = Path(settings.market_timeseries_dir)
    duckdb_path = timeseries_dir / "market.duckdb"
    parquet_dir = timeseries_dir / "parquet"
    market_db = MarketDb(str(duckdb_path), read_only=False)
    time_series_store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
    )
    if time_series_store is None:
        market_db.close()
        raise RuntimeError("DuckDB market time-series store is unavailable")

    client = JQuantsAsyncClient(
        api_key=settings.jquants_api_key,
        plan=settings.jquants_plan,
    )

    try:
        return await sync_intraday_data(
            request,
            market_db=market_db,
            time_series_store=time_series_store,
            jquants_client=client,
        )
    finally:
        await client.close()
        time_series_store.close()
        market_db.close()


def execute_intraday_sync(request: IntradaySyncRequest) -> IntradaySyncResponse:
    return asyncio.run(_execute_intraday_sync(request))


def _print_intraday_sync_result(
    request: IntradaySyncRequest,
    result: IntradaySyncResponse,
    *,
    resolved_date: str | None,
) -> None:
    table = Table(title="Intraday Sync", show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white")
    if resolved_date is not None:
        table.add_row("resolved date", resolved_date)
    table.add_row("mode", result.mode)
    table.add_row("request date", request.date or "(range)")
    table.add_row("request codes", str(len(request.codes)))
    table.add_row("records fetched", str(result.recordsFetched))
    table.add_row("records stored", str(result.recordsStored))
    table.add_row("stored codes", str(result.storedCodes))
    table.add_row("dates processed", str(result.datesProcessed))
    table.add_row("api calls", str(result.apiCalls))
    table.add_row("selected files", str(result.selectedFiles))
    table.add_row("cache hits", str(result.cacheHits))
    table.add_row("cache misses", str(result.cacheMisses))
    table.add_row("skipped rows", str(result.skippedRows))
    table.add_row("last updated", result.lastUpdated)
    console.print(table)


def run_intraday_sync_command(
    *,
    mode: str,
    date_value: str | None,
    date_from: str | None,
    date_to: str | None,
    codes: list[str],
) -> None:
    try:
        request, resolved_date = _build_intraday_request(
            mode=mode,
            date_value=date_value,
            date_from=date_from,
            date_to=date_to,
            codes=codes,
        )
        result = execute_intraday_sync(request)
    except Exception as exc:  # noqa: BLE001 - CLI should collapse validation/runtime errors
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None

    _print_intraday_sync_result(
        request,
        result,
        resolved_date=resolved_date,
    )
