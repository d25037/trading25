"""
API Schemas (Pydantic models)
"""

from src.entrypoints.http.schemas.common import BaseJobResponse
from src.entrypoints.http.schemas.backtest import (
    AttributionArtifactContentResponse,
    AttributionArtifactInfo,
    AttributionArtifactListResponse,
    BacktestJobResponse,
    BacktestRequest,
    BacktestResultResponse,
    BacktestResultSummary,
    JobStatus,
    SignalAttributionJobResponse,
    SignalAttributionRequest,
    SignalAttributionResult,
    SignalAttributionResultResponse,
)
from src.entrypoints.http.schemas.optimize import (
    OptimizationGridConfig,
    OptimizationGridListResponse,
    OptimizationGridSaveRequest,
    OptimizationGridSaveResponse,
    OptimizationJobResponse,
    OptimizationRequest,
)
from src.entrypoints.http.schemas.strategy import (
    StrategyDetailResponse,
    StrategyListResponse,
    StrategyMetadataResponse,
    StrategyValidationRequest,
    StrategyValidationResponse,
)
from src.entrypoints.http.schemas.signals import (
    SignalComputeRequest,
    SignalComputeResponse,
    SignalResult,
    SignalSpec,
)

__all__ = [
    "BaseJobResponse",
    "AttributionArtifactInfo",
    "AttributionArtifactListResponse",
    "AttributionArtifactContentResponse",
    "BacktestRequest",
    "BacktestJobResponse",
    "BacktestResultSummary",
    "BacktestResultResponse",
    "SignalAttributionRequest",
    "SignalAttributionJobResponse",
    "SignalAttributionResult",
    "SignalAttributionResultResponse",
    "JobStatus",
    "OptimizationRequest",
    "OptimizationJobResponse",
    "OptimizationGridConfig",
    "OptimizationGridListResponse",
    "OptimizationGridSaveRequest",
    "OptimizationGridSaveResponse",
    "StrategyListResponse",
    "StrategyMetadataResponse",
    "StrategyDetailResponse",
    "StrategyValidationRequest",
    "StrategyValidationResponse",
    "SignalComputeRequest",
    "SignalComputeResponse",
    "SignalResult",
    "SignalSpec",
]
