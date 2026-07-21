from __future__ import annotations

import numpy as np
import pytest

from src.domains.analytics.trend_slope_features import rolling_log_slope_features


def test_rolling_log_slope_features_recovers_known_fitted_move() -> None:
    beta = 0.01
    values = np.log(100.0) + beta * np.arange(20, dtype=float)

    slopes, r2 = rolling_log_slope_features(values, window=20)

    assert slopes[-1] == pytest.approx((np.exp(beta * 19) - 1) * 100)
    assert r2[-1] == pytest.approx(1.0)


def test_rolling_log_slope_features_returns_zero_for_flat_window() -> None:
    slopes, r2 = rolling_log_slope_features(np.ones(20), window=20)

    assert slopes[-1] == 0.0
    assert r2[-1] == 0.0
