"""market_data_errors helpers."""

from src.application.services.market_data_errors import MarketDataError, to_http_error_detail
from src.entrypoints.http.error_utils import classify_market_data_http_exception


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


def test_classify_market_data_http_exception_does_not_force_topix_on_dataset_not_found() -> None:
    error = classify_market_data_http_exception(
        stock_code="9999",
        source="primeExTopix500",
        raw_message="Resource not found: Stock not found",
        market_reader=None,
        benchmark_code="topix",
        force_lookup=True,
    )

    assert error is None


def test_classify_market_data_http_exception_recognizes_explicit_topix_message() -> None:
    error = classify_market_data_http_exception(
        stock_code="7203",
        source="market",
        raw_message="ベンチマーク 'topix' のデータが取得できません",
        market_reader=None,
        benchmark_code="topix",
        force_lookup=False,
    )

    assert error is not None
    assert error.status_code == 404
    assert error.detail == {
        "message": "ベンチマーク 'topix' のデータが取得できません",
        "details": [
            {"field": "reason", "message": "topix_data_missing"},
            {"field": "recovery", "message": "market_db_sync"},
        ],
    }
