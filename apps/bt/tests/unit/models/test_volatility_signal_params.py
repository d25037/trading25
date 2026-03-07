"""volatility signal param models の focused tests"""

import pytest
from pydantic import ValidationError

from src.shared.models.signals.volatility import (
    ATRSupportPositionParams,
    BollingerPositionSignalParams,
    VolatilityPercentileSignalParams,
)


class TestVolatilityPercentileSignalParams:
    def test_defaults(self) -> None:
        params = VolatilityPercentileSignalParams()
        assert params.window == 20
        assert params.lookback == 252
        assert params.percentile == 50.0

    def test_invalid_percentile_raises(self) -> None:
        with pytest.raises(ValidationError):
            VolatilityPercentileSignalParams(percentile=120.0)


class TestATRSupportPositionParams:
    def test_invalid_direction_raises(self) -> None:
        with pytest.raises(ValidationError, match="direction"):
            ATRSupportPositionParams(direction="sideways")

    def test_invalid_price_column_raises(self) -> None:
        with pytest.raises(ValidationError, match="price_column"):
            ATRSupportPositionParams(price_column="open")


class TestBollingerPositionSignalParams:
    def test_invalid_level_raises(self) -> None:
        with pytest.raises(ValidationError, match="level"):
            BollingerPositionSignalParams(level="outer")
