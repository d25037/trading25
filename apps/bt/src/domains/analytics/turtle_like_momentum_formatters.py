"""Formatting and numeric coercion helpers for turtle-like momentum research."""

from __future__ import annotations

import math
from typing import cast

import pandas as pd


def float_or_nan(value: object) -> float:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


def format_int(value: object) -> str:
    try:
        number = int(float(cast(float, value)))
    except (TypeError, ValueError):
        return "-"
    return f"{number:,}"


def format_number(value: object, *, digits: int = 2, suffix: str = "") -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if pd.isna(number):
        return "-"
    return f"{number:.{digits}f}{suffix}"
