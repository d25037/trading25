from src.application.contracts.watchlist_prices import (
    WatchlistPricesResponse,
    WatchlistStockPrice,
)


def test_watchlist_prices_support_complete_and_optional_prices() -> None:
    response = WatchlistPricesResponse(
        prices=[
            WatchlistStockPrice(
                code="7203",
                close=2500.0,
                prevClose=2450.0,
                changePercent=2.04,
                volume=1_000_000,
                date="2026-07-14",
            ),
            WatchlistStockPrice(
                code="6758",
                close=13000.0,
                volume=500_000,
                date="2026-07-14",
            ),
        ]
    )

    assert response.prices[0].prevClose == 2450.0
    assert response.prices[0].changePercent == 2.04
    assert response.prices[1].prevClose is None
    assert response.prices[1].changePercent is None


def test_watchlist_model_property_and_required_orders() -> None:
    assert list(WatchlistStockPrice.model_json_schema()["properties"]) == [
        "code",
        "close",
        "prevClose",
        "changePercent",
        "volume",
        "date",
    ]
    assert list(WatchlistPricesResponse.model_json_schema()["properties"]) == ["prices"]
    assert WatchlistStockPrice.model_json_schema()["required"] == [
        "code",
        "close",
        "volume",
        "date",
    ]
    assert WatchlistPricesResponse.model_json_schema()["required"] == ["prices"]


def test_watchlist_responses_do_not_share_list_values() -> None:
    first = WatchlistPricesResponse(prices=[])
    second = WatchlistPricesResponse(prices=[])

    first.prices.append(
        WatchlistStockPrice(
            code="7203",
            close=2500.0,
            volume=1_000_000,
            date="2026-07-14",
        )
    )

    assert second.prices == []
