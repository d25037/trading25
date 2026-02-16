"""
API Services
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.server.services.job_manager import JobManager, job_manager

if TYPE_CHECKING:
    from src.server.services.backtest_attribution_service import BacktestAttributionService
    from src.server.services.backtest_service import BacktestService

__all__ = [
    "BacktestService",
    "BacktestAttributionService",
    "JobManager",
    "job_manager",
]


def __getattr__(name: str) -> Any:
    if name == "BacktestService":
        from src.server.services.backtest_service import BacktestService

        return BacktestService
    if name == "BacktestAttributionService":
        from src.server.services.backtest_attribution_service import BacktestAttributionService

        return BacktestAttributionService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
