from src.domains.fundamentals.valuation_primitives import (
    market_cap_from_price_and_shares,
    valuation_ratio,
    valuation_ratio_series,
)
import pandas as pd


def test_valuation_ratio_returns_positive_ratio() -> None:
    assert valuation_ratio(500, 100) == 5


def test_valuation_ratio_returns_none_for_undefined_cases() -> None:
    assert valuation_ratio(500, 0) is None
    assert valuation_ratio(500, -100) is None
    assert valuation_ratio(None, 100) is None
    assert valuation_ratio(500, None) is None
    assert valuation_ratio(-500, 100) is None


def test_market_cap_from_price_and_shares() -> None:
    assert market_cap_from_price_and_shares(500, 1_000_000) == 500_000_000


def test_market_cap_returns_none_for_non_positive_inputs() -> None:
    assert market_cap_from_price_and_shares(0, 1_000_000) is None
    assert market_cap_from_price_and_shares(-500, 1_000_000) is None
    assert market_cap_from_price_and_shares(500, 0) is None
    assert market_cap_from_price_and_shares(500, -1_000_000) is None


def test_valuation_ratio_series_uses_same_null_semantics() -> None:
    result = valuation_ratio_series(
        pd.Series([500.0, 500.0, -500.0]),
        pd.Series([100.0, 0.0, 100.0]),
    )

    assert result.iloc[0] == 5.0
    assert pd.isna(result.iloc[1])
    assert pd.isna(result.iloc[2])
