"""market_data_errors helpers."""

from src.application.services.market_data_errors import MarketDataError, to_http_error_detail


def test_market_data_error_initializes_value_error_message() -> None:
    error = MarketDataError(
        "Local stock data is missing",
        reason="local_stock_data_missing",
        recovery="stock_refresh",
    )

    assert str(error) == "Local stock data is missing"
    assert error.reason == "local_stock_data_missing"
    assert error.recovery == "stock_refresh"


def test_to_http_error_detail_includes_reason_and_recovery() -> None:
    error = MarketDataError(
        "TOPIX data is missing",
        reason="topix_data_missing",
        recovery="market_db_sync",
    )

    assert to_http_error_detail(error) == {
        "message": "TOPIX data is missing",
        "details": [
            {"field": "reason", "message": "topix_data_missing"},
            {"field": "recovery", "message": "market_db_sync"},
        ],
    }
