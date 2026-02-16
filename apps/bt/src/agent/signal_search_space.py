"""
Signal search space definitions for Lab (optimize/evolve/generate).

This module centralizes:
- Numeric parameter ranges (used by GA mutation and Optuna sampling)
- Parameters treated as categorical (excluded from numeric mutation/sampling)
"""

from __future__ import annotations

from typing import Literal

from .signal_catalog import PARAM_RANGES as CATALOG_PARAM_RANGES

ParamType = Literal["int", "float"]

PARAM_RANGES: dict[str, dict[str, tuple[float, float, ParamType]]] = (
    CATALOG_PARAM_RANGES
)


# Parameters that should be treated as categorical knobs and excluded from
# numeric mutation/sampling.
CATEGORICAL_PARAMS: set[str] = {
    "enabled",
    "direction",
    "condition",
    "type",
    "ma_type",
    "position",
    "baseline_type",
    "recovery_price",
    "recovery_direction",
    "deviation_direction",
    "price_column",
}
