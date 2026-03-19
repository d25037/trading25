"""Unit tests for JQuantsProxyService caching behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.application.services.jquants_proxy_service import JQuantsProxyService


class _FrozenJstDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ANN206
        fixed = cls(2026, 3, 18, 9, 0, 0, tzinfo=UTC)
        if tz is None:
            return fixed.replace(tzinfo=None)
        return fixed.astimezone(tz)


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


@pytest.mark.asyncio
async def test_get_options_225_maps_payload_and_aggregates_calls() -> None:
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        return_value=(
            [
                {
                    "Date": "2026-03-18",
                    "Code": "130060018",
                    "WholeDayClose": 12.0,
                    "Volume": 10.0,
                    "OpenInterest": 30.0,
                    "ContractMonth": "2026-04",
                    "StrikePrice": 34000.0,
                    "Volume(OnlyAuction)": 1.0,
                    "PutCallDivision": "1",
                    "EmergencyMarginTriggerDivision": "002",
                    "SettlementPrice": 11.5,
                    "UnderlyingPrice": 37450.12,
                    "ImpliedVolatility": 18.4,
                },
                {
                    "Date": "2026-03-18",
                    "Code": "130060019",
                    "WholeDayClose": 14.0,
                    "Volume": 20.0,
                    "OpenInterest": 40.0,
                    "ContractMonth": "2026-05",
                    "StrikePrice": 35000.0,
                    "PutCallDivision": "2",
                    "EmergencyMarginTriggerDivision": "001",
                    "SettlementPrice": 13.5,
                    "UnderlyingPrice": 37480.55,
                },
            ],
            3,
        )
    )
    service = JQuantsProxyService(client)

    response = await service.get_options_225("2026-03-18")

    assert response.requestedDate == "2026-03-18"
    assert response.resolvedDate == "2026-03-18"
    assert response.sourceCallCount == 3
    assert response.availableContractMonths == ["2026-04", "2026-05"]
    assert response.summary.totalCount == 2
    assert response.summary.putCount == 1
    assert response.summary.callCount == 1
    assert response.summary.totalVolume == 30.0
    assert response.summary.totalOpenInterest == 70.0
    assert response.summary.strikePriceRange.min == 34000.0
    assert response.summary.strikePriceRange.max == 35000.0
    assert response.items[0].putCallLabel == "put"
    assert response.items[1].emergencyMarginTriggerLabel == "emergency_margin_triggered"
    client.get_paginated_with_meta.assert_awaited_once_with(
        "/derivatives/bars/daily/options/225",
        params={"date": "2026-03-18"},
    )


@pytest.mark.asyncio
async def test_get_options_225_converts_empty_numeric_strings_to_none() -> None:
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        return_value=(
            [
                {
                    "Date": "2026-03-18",
                    "Code": "130060018",
                    "NightSessionOpen": "",
                    "Volume(OnlyAuction)": "",
                    "SettlementPrice": "",
                }
            ],
            1,
        )
    )
    service = JQuantsProxyService(client)

    response = await service.get_options_225("2026-03-18")

    assert response.items[0].nightSessionOpen is None
    assert response.items[0].onlyAuctionVolume is None
    assert response.items[0].settlementPrice is None


@pytest.mark.asyncio
async def test_get_options_225_cache_hit() -> None:
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        return_value=(
            [{"Date": "2026-03-18", "Code": "130060018", "ContractMonth": "2026-04", "PutCallDivision": "1"}],
            2,
        )
    )
    service = JQuantsProxyService(client)

    first = await service.get_options_225("2026-03-18")
    second = await service.get_options_225("2026-03-18")

    assert first.resolvedDate == second.resolvedDate == "2026-03-18"
    assert client.get_paginated_with_meta.await_count == 1


@pytest.mark.asyncio
async def test_get_options_225_errors_are_not_cached() -> None:
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        side_effect=[
            RuntimeError("boom"),
            (
                [{"Date": "2026-03-18", "Code": "130060018", "ContractMonth": "2026-04", "PutCallDivision": "1"}],
                1,
            ),
        ]
    )
    service = JQuantsProxyService(client)

    with pytest.raises(RuntimeError, match="boom"):
        await service.get_options_225("2026-03-18")

    result = await service.get_options_225("2026-03-18")
    assert result.summary.totalCount == 1
    assert client.get_paginated_with_meta.await_count == 2


@pytest.mark.asyncio
async def test_get_options_225_singleflight_coalesces_in_flight_requests() -> None:
    started = asyncio.Event()
    unblock = asyncio.Event()

    async def delayed_response(_path: str, params: dict[str, str]) -> tuple[list[dict[str, object]], int]:
        assert params == {"date": "2026-03-18"}
        started.set()
        await unblock.wait()
        return ([{"Date": "2026-03-18", "Code": "130060018", "ContractMonth": "2026-04", "PutCallDivision": "1"}], 1)

    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(side_effect=delayed_response)
    service = JQuantsProxyService(client)

    task1 = asyncio.create_task(service.get_options_225("2026-03-18"))
    await started.wait()
    task2 = asyncio.create_task(service.get_options_225("2026-03-18"))

    unblock.set()
    result1 = await task1
    result2 = await task2

    assert result1.summary.totalCount == 1
    assert result2.summary.totalCount == 1
    assert client.get_paginated_with_meta.await_count == 1


@pytest.mark.asyncio
async def test_get_options_225_resolves_recent_available_date(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.application.services.jquants_proxy_service.datetime", _FrozenJstDateTime)
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        side_effect=[
            ([], 1),
            ([], 1),
            ([{"Date": "2026-03-16", "Code": "130060018", "ContractMonth": "2026-04", "PutCallDivision": "1"}], 2),
        ]
    )
    service = JQuantsProxyService(client)

    response = await service.get_options_225()

    assert response.requestedDate is None
    assert response.resolvedDate == "2026-03-16"
    assert response.sourceCallCount == 2
    assert client.get_paginated_with_meta.await_count == 3


@pytest.mark.asyncio
async def test_get_options_225_without_date_reuses_latest_resolution_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.application.services.jquants_proxy_service.datetime", _FrozenJstDateTime)
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(
        side_effect=[
            ([], 1),
            ([{"Date": "2026-03-17", "Code": "130060018", "ContractMonth": "2026-04", "PutCallDivision": "1"}], 2),
        ]
    )
    service = JQuantsProxyService(client)

    first = await service.get_options_225()
    second = await service.get_options_225()

    assert first.requestedDate is None
    assert first.resolvedDate == "2026-03-17"
    assert second.resolvedDate == "2026-03-17"
    assert client.get_paginated_with_meta.await_count == 2


@pytest.mark.asyncio
async def test_get_options_225_raises_not_found_when_lookback_is_exhausted() -> None:
    client = AsyncMock()
    client.get_paginated_with_meta = AsyncMock(return_value=([], 1))
    service = JQuantsProxyService(client)

    with pytest.raises(HTTPException) as exc_info:
        await service.get_options_225()

    assert exc_info.value.status_code == 404
