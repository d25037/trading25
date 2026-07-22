"""Layer-neutral errors for canonical Fundamentals PIT reads."""

from typing import Literal


FundamentalsPitReason = Literal[
    "stock_not_listed_as_of",
    "provider_window_required",
    "current_adjusted_metrics_required",
    "stock_master_snapshot_required",
    "pit_snapshot_inconsistent",
]


class FundamentalsPitSnapshotError(RuntimeError):
    def __init__(self, reason: FundamentalsPitReason, message: str) -> None:
        self.reason = reason
        super().__init__(message)
