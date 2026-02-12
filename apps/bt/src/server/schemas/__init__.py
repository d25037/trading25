"""
API Schemas (Pydantic models)
"""

from src.server.schemas.common import BaseJobResponse
from src.server.schemas.backtest import (
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
from src.server.schemas.optimize import (
    OptimizationGridConfig,
    OptimizationGridListResponse,
    OptimizationGridSaveRequest,
    OptimizationGridSaveResponse,
    OptimizationJobResponse,
    OptimizationRequest,
)
from src.server.schemas.strategy import (
    StrategyDetailResponse,
    StrategyListResponse,
    StrategyMetadataResponse,
    StrategyValidationRequest,
    StrategyValidationResponse,
)
from src.server.schemas.signals import (
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
