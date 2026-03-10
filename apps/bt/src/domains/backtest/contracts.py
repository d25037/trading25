"""
Engine-neutral execution contracts for backtest-family runs.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EngineFamily(str, Enum):
    """Execution engine family."""

    VECTORBT = "vectorbt"
    NAUTILUS = "nautilus"
    UNKNOWN = "unknown"


class RunType(str, Enum):
    """Canonical run category."""

    BACKTEST = "backtest"
    OPTIMIZATION = "optimization"
    ATTRIBUTION = "attribution"
    SCREENING = "screening"
    LAB_GENERATE = "lab_generate"
    LAB_EVOLVE = "lab_evolve"
    LAB_OPTIMIZE = "lab_optimize"
    LAB_IMPROVE = "lab_improve"
    UNKNOWN = "unknown"


class ArtifactKind(str, Enum):
    """Artifact role within a run."""

    HTML = "html"
    METRICS_JSON = "metrics_json"
    MANIFEST_JSON = "manifest_json"
    RESULT_SUMMARY = "result_summary"
    RAW_RESULT_JSON = "raw_result_json"
    ATTRIBUTION_JSON = "attribution_json"
    STRATEGY_YAML = "strategy_yaml"
    HISTORY_YAML = "history_yaml"


class ArtifactStorage(str, Enum):
    """Physical storage backend for an artifact record."""

    FILESYSTEM = "filesystem"
    PORTFOLIO_DB = "portfolio_db"


class CompiledStrategyInputRequirements(BaseModel):
    """Declared inputs required by a compiled strategy."""

    schema_version: int = Field(default=1, description="Schema version")
    required_data_domains: list[str] = Field(
        default_factory=list,
        description="Required data domains such as market/statements/margin",
    )
    required_features: list[str] = Field(
        default_factory=list,
        description="Required feature identifiers",
    )
    required_fundamental_fields: list[str] = Field(
        default_factory=list,
        description="Required fundamental fields",
    )
    signal_ids: list[str] = Field(
        default_factory=list,
        description="Compiled signal identifiers",
    )


class RunSpec(BaseModel):
    """Engine-neutral execution input contract."""

    schema_version: int = Field(default=1, description="Schema version")
    run_type: RunType = Field(description="Canonical run category")
    strategy_name: str = Field(description="Resolved strategy name")
    strategy_source_ref: str | None = Field(
        default=None,
        description="Source reference for strategy definition",
    )
    strategy_fingerprint: str | None = Field(
        default=None,
        description="Stable strategy fingerprint when available",
    )
    dataset_name: str | None = Field(
        default=None,
        description="Legacy dataset name for compatibility",
    )
    dataset_snapshot_id: str | None = Field(
        default=None,
        description="Pinned dataset snapshot identifier",
    )
    market_snapshot_id: str | None = Field(
        default=None,
        description="Pinned market snapshot identifier",
    )
    engine_family: EngineFamily = Field(
        default=EngineFamily.UNKNOWN,
        description="Execution engine family",
    )
    execution_policy_version: str | None = Field(
        default=None,
        description="Execution semantics/policy version",
    )
    parent_run_id: str | None = Field(
        default=None,
        description="Parent run identifier for lineage",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Resolved run parameters",
    )
    compiled_strategy_requirements: CompiledStrategyInputRequirements | None = Field(
        default=None,
        description="Input requirements expected from compiled strategy IR",
    )


class RunMetadata(BaseModel):
    """Resolved run metadata persisted in the experiment registry."""

    schema_version: int = Field(default=1, description="Schema version")
    run_id: str = Field(description="Run identifier")
    run_type: RunType = Field(description="Canonical run category")
    strategy_name: str = Field(description="Resolved strategy name")
    dataset_name: str | None = Field(
        default=None,
        description="Legacy dataset name for compatibility",
    )
    dataset_snapshot_id: str | None = Field(
        default=None,
        description="Pinned dataset snapshot identifier",
    )
    market_snapshot_id: str | None = Field(
        default=None,
        description="Pinned market snapshot identifier",
    )
    engine_family: EngineFamily = Field(
        default=EngineFamily.UNKNOWN,
        description="Execution engine family",
    )
    execution_policy_version: str | None = Field(
        default=None,
        description="Execution semantics/policy version",
    )
    parent_run_id: str | None = Field(
        default=None,
        description="Parent run identifier for lineage",
    )


class ArtifactRecord(BaseModel):
    """Artifact registry entry for a run output."""

    kind: ArtifactKind = Field(description="Artifact role")
    storage: ArtifactStorage = Field(description="Storage backend")
    path: str | None = Field(
        default=None,
        description="Filesystem path when stored on disk",
    )
    location: str | None = Field(
        default=None,
        description="Logical storage location when not stored on disk",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional artifact metadata",
    )


class ArtifactIndex(BaseModel):
    """List of artifacts associated with a run."""

    schema_version: int = Field(default=1, description="Schema version")
    artifacts: list[ArtifactRecord] = Field(
        default_factory=list,
        description="Artifact records",
    )


class CanonicalExecutionMetrics(BaseModel):
    """Common scalar metrics available across execution engines."""

    total_return: float | None = Field(default=None, description="Total return")
    sharpe_ratio: float | None = Field(default=None, description="Sharpe ratio")
    sortino_ratio: float | None = Field(default=None, description="Sortino ratio")
    calmar_ratio: float | None = Field(default=None, description="Calmar ratio")
    max_drawdown: float | None = Field(default=None, description="Max drawdown")
    win_rate: float | None = Field(default=None, description="Win rate")
    trade_count: int | None = Field(default=None, description="Closed trade count")


class CanonicalExecutionResult(BaseModel):
    """Engine-neutral result envelope for run outputs."""

    schema_version: int = Field(default=1, description="Schema version")
    run_id: str = Field(description="Run identifier")
    run_type: RunType = Field(description="Canonical run category")
    strategy_name: str = Field(description="Resolved strategy name")
    engine_family: EngineFamily = Field(description="Execution engine family")
    status: str = Field(description="Run status")
    dataset_name: str | None = Field(
        default=None,
        description="Legacy dataset name for compatibility",
    )
    dataset_snapshot_id: str | None = Field(
        default=None,
        description="Pinned dataset snapshot identifier",
    )
    market_snapshot_id: str | None = Field(
        default=None,
        description="Pinned market snapshot identifier",
    )
    execution_policy_version: str | None = Field(
        default=None,
        description="Execution semantics/policy version",
    )
    execution_time: float | None = Field(
        default=None,
        description="Execution time in seconds",
    )
    summary_metrics: CanonicalExecutionMetrics | None = Field(
        default=None,
        description="Common scalar metrics when available",
    )
    payload: dict[str, Any] | None = Field(
        default=None,
        description="Original engine-specific payload",
    )
