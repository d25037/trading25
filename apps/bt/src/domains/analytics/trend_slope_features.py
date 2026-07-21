"""Shared rolling OLS features for log-price research."""

from __future__ import annotations

import numpy as np


def rolling_log_slope_features(
    values: np.ndarray,
    *,
    window: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return rolling fitted log-price moves and OLS R-squared values."""
    if window <= 1:
        raise ValueError("window must be greater than 1")

    slopes = np.full(len(values), np.nan, dtype=float)
    r2_values = np.full(len(values), np.nan, dtype=float)
    if len(values) < window:
        return slopes, r2_values

    x = np.arange(window, dtype=float)
    x_centered = x - x.mean()
    x_var = float(np.dot(x_centered, x_centered))
    for end in range(window - 1, len(values)):
        y = values[end - window + 1 : end + 1]
        if not np.isfinite(y).all():
            continue
        y_centered = y - y.mean()
        y_var = float(np.dot(y_centered, y_centered))
        if y_var <= 0.0:
            slopes[end] = 0.0
            r2_values[end] = 0.0
            continue
        slope_per_session = float(np.dot(x_centered, y_centered) / x_var)
        corr = float(np.dot(x_centered, y_centered) / np.sqrt(x_var * y_var))
        slopes[end] = (np.exp(slope_per_session * (window - 1)) - 1.0) * 100.0
        r2_values[end] = corr * corr
    return slopes, r2_values
