"""Unit tests for JQuantsProxyService caching behavior."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.application.services.jquants_proxy_service import JQuantsProxyService


def _daily_quote_item(code: str = "72030") -> dict[str, object]:
    return {
        "Date": "2024-01-04",
        "Code": code,
        "O": 100.0,
        "H": 110.0,
        "L": 95.0,
        "C": 105.0,
        "AdjFactor": 1.0,
    }


@pytest.mark.asyncio
async def test_get_auth_status_reflects_api_key_presence() -> None:
    client = AsyncMock()
    client.has_api_key = True
    service = JQuantsProxyService(client)

    status = service.get_auth_status()

    assert status.authenticated is True
    assert status.hasApiKey is True


@pytest.mark.asyncio
async def test_get_daily_quotes_with_optional_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={"data": [_daily_quote_item()], "pagination_key": "next"})
    service = JQuantsProxyService(client)

    response = await service.get_daily_quotes(
        "7203",
        date_from="2024-01-01",
        date_to="2024-01-31",
        date="2024-01-04",
    )

    assert len(response.data) == 1
    assert response.pagination_key == "next"
    client.get.assert_awaited_once_with(
        "/equities/bars/daily",
        {
            "code": "7203",
            "from": "2024-01-01",
            "to": "2024-01-31",
            "date": "2024-01-04",
        },
    )


@pytest.mark.asyncio
async def test_get_daily_quotes_without_optional_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={"data": [_daily_quote_item()]})
    service = JQuantsProxyService(client)

    response = await service.get_daily_quotes("7203")

    assert len(response.data) == 1
    client.get.assert_awaited_once_with("/equities/bars/daily", {"code": "7203"})


@pytest.mark.asyncio
async def test_get_indices_with_filters_and_mapping() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [{"Date": "2024-01-05", "Code": "0000", "O": 1.0, "H": 2.0, "L": 0.5, "C": 1.5}]
    })
    service = JQuantsProxyService(client)

    response = await service.get_indices(
        code="0000",
        date_from="2024-01-01",
        date_to="2024-01-31",
        date="2024-01-05",
    )

    assert len(response.indices) == 1
    assert response.indices[0].date == "2024-01-05"
    assert response.indices[0].close == 1.5
    client.get.assert_awaited_once_with(
        "/indices/bars/daily",
        {
            "code": "0000",
            "from": "2024-01-01",
            "to": "2024-01-31",
            "date": "2024-01-05",
        },
    )


@pytest.mark.asyncio
async def test_get_indices_without_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={"data": []})
    service = JQuantsProxyService(client)

    response = await service.get_indices()

    assert response.indices == []
    client.get.assert_awaited_once_with("/indices/bars/daily", {})


@pytest.mark.asyncio
async def test_get_listed_info_with_filters_and_mapping() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [
            {
                "Code": "72030",
                "CoName": "Toyota Motor",
                "CoNameEn": "TOYOTA MOTOR CORPORATION",
                "Mkt": "0111",
                "MktNm": "Prime",
                "S33": "3700",
                "S33Nm": "Transport Equipment",
                "ScaleCat": "TOPIX Core30",
            }
        ]
    })
    service = JQuantsProxyService(client)

    response = await service.get_listed_info(code="7203", date="2024-01-05")

    assert len(response.info) == 1
    assert response.info[0].code == "7203"
    assert response.info[0].companyName == "Toyota Motor"
    client.get.assert_awaited_once_with("/equities/master", {"code": "7203", "date": "2024-01-05"})


@pytest.mark.asyncio
async def test_get_listed_info_without_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={"data": []})
    service = JQuantsProxyService(client)

    response = await service.get_listed_info()

    assert response.info == []
    client.get.assert_awaited_once_with("/equities/master", {})


@pytest.mark.asyncio
async def test_margin_interest_cache_hit() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [
            {
                "Date": "2024-01-01",
                "Code": "72030",
                "ShrtStdVol": 100,
                "LongStdVol": 200,
                "ShrtVol": 300,
                "LongVol": 400,
            }
        ]
    })
    service = JQuantsProxyService(client)

    first = await service.get_margin_interest("7203")
    second = await service.get_margin_interest("7203")

    assert len(first.marginInterest) == 1
    assert len(second.marginInterest) == 1
    assert client.get.await_count == 1


@pytest.mark.asyncio
async def test_margin_interest_includes_optional_filters_in_cache_key() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [
            {
                "Date": "2024-01-01",
                "Code": "72030",
                "ShrtStdVol": 100,
                "LongStdVol": 200,
            }
        ]
    })
    service = JQuantsProxyService(client)

    await service.get_margin_interest("7203", date_from="2024-01-01", date_to="2024-01-31", date="2024-01-08")

    client.get.assert_awaited_once_with(
        "/markets/margin-interest",
        {
            "code": "72030",
            "from": "2024-01-01",
            "to": "2024-01-31",
            "date": "2024-01-08",
        },
    )


@pytest.mark.asyncio
async def test_statements_and_statements_raw_share_cached_fins_summary() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [
            {
                "DiscDate": "2024-05-10",
                "Code": "72030",
                "CurPerType": "FY",
                "CurPerSt": "2023-04-01",
                "CurPerEn": "2024-03-31",
                "EPS": 100.0,
            }
        ]
    })
    service = JQuantsProxyService(client)

    statements = await service.get_statements("7203")
    statements_raw = await service.get_statements_raw("7203")

    assert len(statements.data) == 1
    assert len(statements_raw.data) == 1
    assert client.get.await_count == 1


@pytest.mark.asyncio
async def test_errors_are_not_cached() -> None:
    client = AsyncMock()
    client.get = AsyncMock(side_effect=[RuntimeError("boom"), {
        "data": [
            {
                "Date": "2024-01-01",
                "Code": "72030",
                "ShrtStdVol": 100,
                "LongStdVol": 200,
            }
        ]
    }])
    service = JQuantsProxyService(client)

    with pytest.raises(RuntimeError, match="boom"):
        await service.get_margin_interest("7203")

    result = await service.get_margin_interest("7203")

    assert len(result.marginInterest) == 1
    assert client.get.await_count == 2


@pytest.mark.asyncio
async def test_singleflight_coalesces_same_in_flight_request() -> None:
    started = asyncio.Event()
    unblock = asyncio.Event()

    async def delayed_response(_path: str, _params: dict[str, str]) -> dict[str, object]:
        started.set()
        await unblock.wait()
        return {
            "data": [
                {
                    "DiscDate": "2024-05-10",
                    "Code": "72030",
                    "CurPerType": "FY",
                    "CurPerSt": "2023-04-01",
                    "CurPerEn": "2024-03-31",
                    "EPS": 100.0,
                }
            ]
        }

    client = AsyncMock()
    client.get = AsyncMock(side_effect=delayed_response)
    service = JQuantsProxyService(client)

    task1 = asyncio.create_task(service.get_statements_raw("7203"))
    await started.wait()
    task2 = asyncio.create_task(service.get_statements_raw("7203"))

    unblock.set()
    result1 = await task1
    result2 = await task2

    assert len(result1.data) == 1
    assert len(result2.data) == 1
    assert client.get.await_count == 1


@pytest.mark.asyncio
async def test_get_topix_with_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={
        "data": [{"Date": "2024-01-05", "O": 1.0, "H": 2.0, "L": 0.5, "C": 1.5}]
    })
    service = JQuantsProxyService(client)

    response = await service.get_topix(
        date_from="2024-01-01",
        date_to="2024-01-31",
        date="2024-01-05",
    )

    assert len(response.topix) == 1
    assert response.topix[0].Date == "2024-01-05"
    client.get.assert_awaited_once_with(
        "/indices/bars/daily",
        {
            "code": "0000",
            "from": "2024-01-01",
            "to": "2024-01-31",
            "date": "2024-01-05",
        },
    )


@pytest.mark.asyncio
async def test_get_topix_without_filters() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value={"data": []})
    service = JQuantsProxyService(client)

    response = await service.get_topix()

    assert response.topix == []
    client.get.assert_awaited_once_with("/indices/bars/daily", {"code": "0000"})
