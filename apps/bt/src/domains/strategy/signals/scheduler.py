"""Project evaluated signals into executable decision windows."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from src.domains.strategy.runtime.compiler import CompiledSignalAvailability


class SignalDecisionScheduler:
    """Schedule evaluated signal series from compiled availability metadata."""

    def project(
        self,
        signal: pd.Series,
        *,
        availability: CompiledSignalAvailability,
    ) -> pd.Series:
        should_shift_to_current_session = (
            availability.execution_session == "current_session"
            and availability.available_at == "prior_session_close"
        )

        if should_shift_to_current_session:
            return signal.shift(1, fill_value=False)
        return signal
