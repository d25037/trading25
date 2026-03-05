"""Shared parameter dependency constraints for Lab optimize/evolve."""

from __future__ import annotations

from typing import Any

import numpy as np

from .signal_search_space import ParamType


def apply_param_dependency_constraints(
    *,
    key: str,
    min_val: float,
    max_val: float,
    param_type: ParamType,
    sibling_params: dict[str, Any],
) -> tuple[float, float]:
    """Apply sibling-parameter ordering constraints to a numeric range."""
    lower_bound = min_val
    upper_bound = max_val

    if key == "long_period" and "short_period" in sibling_params:
        lower_bound = max(lower_bound, float(sibling_params["short_period"]) + 1.0)
    elif key == "short_period" and "long_period" in sibling_params:
        upper_bound = min(upper_bound, float(sibling_params["long_period"]) - 1.0)
    elif key == "slow_period" and "fast_period" in sibling_params:
        lower_bound = max(lower_bound, float(sibling_params["fast_period"]) + 1.0)
    elif key == "fast_period" and "slow_period" in sibling_params:
        upper_bound = min(upper_bound, float(sibling_params["slow_period"]) - 1.0)
    elif key == "max_threshold" and "min_threshold" in sibling_params:
        lower_bound = max(lower_bound, float(sibling_params["min_threshold"]) + 1e-6)
    elif key == "min_threshold" and "max_threshold" in sibling_params:
        upper_bound = min(upper_bound, float(sibling_params["max_threshold"]) - 1e-6)
    elif key == "max_beta" and "min_beta" in sibling_params:
        lower_bound = max(lower_bound, float(sibling_params["min_beta"]) + 1e-6)
    elif key == "min_beta" and "max_beta" in sibling_params:
        upper_bound = min(upper_bound, float(sibling_params["max_beta"]) - 1e-6)

    if param_type == "int":
        lower_bound = float(int(np.ceil(lower_bound)))
        upper_bound = float(int(np.floor(upper_bound)))
        if lower_bound > upper_bound:
            lower_bound = upper_bound
        return lower_bound, upper_bound

    if lower_bound >= upper_bound:
        upper_bound = float(np.nextafter(lower_bound, float("inf")))

    return lower_bound, upper_bound
