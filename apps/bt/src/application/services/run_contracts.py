"""
Helpers for engine-neutral run contracts and artifact indexing.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from src.application.services.snapshot_resolver import (
    resolve_dataset_snapshot_id,
    resolve_market_snapshot_id,
)
from src.domains.backtest.contracts import (
    ArtifactIndex,
    ArtifactKind,
    ArtifactRecord,
    ArtifactStorage,
    CanonicalExecutionMetrics,
    CanonicalExecutionResult,
    EngineFamily,
    RunMetadata,
    RunSpec,
    RunType,
)
from src.domains.strategy.runtime.compiler import (
    compile_strategy_config,
    compile_strategy_requirements,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.entrypoints.http.schemas.backtest import JobStatus

if TYPE_CHECKING:
    from src.application.services.job_manager import JobInfo


_INTERNAL_RAW_RESULT_KEYS = {
    "_artifact_path",
    "_metrics_path",
    "_manifest_path",
    "_simulation_payload_path",
    "_report_payload_path",
    "_render_error",
}
_RAW_RESULT_ARTIFACT_PATHS: tuple[tuple[str, ArtifactKind], ...] = (
    ("_artifact_path", ArtifactKind.ATTRIBUTION_JSON),
    ("_metrics_path", ArtifactKind.METRICS_JSON),
    ("_manifest_path", ArtifactKind.MANIFEST_JSON),
    ("_simulation_payload_path", ArtifactKind.SIMULATION_PAYLOAD),
    ("saved_strategy_path", ArtifactKind.STRATEGY_YAML),
    ("saved_history_path", ArtifactKind.HISTORY_YAML),
)

_LEGACY_VECTORBT_POLICY_VERSION = "vectorbt-legacy-v1"
_JOB_TYPE_TO_RUN_TYPE: dict[str, RunType] = {
    "backtest": RunType.BACKTEST,
    "optimization": RunType.OPTIMIZATION,
    "backtest_attribution": RunType.ATTRIBUTION,
    "screening": RunType.SCREENING,
    "lab_generate": RunType.LAB_GENERATE,
    "lab_evolve": RunType.LAB_EVOLVE,
    "lab_optimize": RunType.LAB_OPTIMIZE,
    "lab_improve": RunType.LAB_IMPROVE,
}
_VECTORBT_JOB_TYPES = {
    "backtest",
    "optimization",
    "backtest_attribution",
    "lab_generate",
    "lab_evolve",
    "lab_optimize",
    "lab_improve",
}


def infer_run_type(job_type: str) -> RunType:
    return _JOB_TYPE_TO_RUN_TYPE.get(job_type, RunType.UNKNOWN)


def infer_engine_family(job_type: str) -> EngineFamily:
    if job_type in _VECTORBT_JOB_TYPES:
        return EngineFamily.VECTORBT
    return EngineFamily.UNKNOWN


def build_default_run_spec(job_type: str, strategy_name: str) -> RunSpec:
    engine_family = infer_engine_family(job_type)
    return RunSpec(
        run_type=infer_run_type(job_type),
        strategy_name=strategy_name,
        strategy_source_ref=strategy_name,
        market_snapshot_id=resolve_market_snapshot_id(),
        engine_family=engine_family,
        execution_policy_version=(
            _LEGACY_VECTORBT_POLICY_VERSION
            if engine_family == EngineFamily.VECTORBT
            else None
        ),
    )


def build_parameterized_run_spec(
    job_type: str,
    strategy_name: str,
    *,
    dataset_name: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> RunSpec:
    run_spec = build_default_run_spec(job_type, strategy_name)
    if dataset_name is not None:
        run_spec.dataset_name = dataset_name
        run_spec.dataset_snapshot_id = resolve_dataset_snapshot_id(dataset_name)
    if parameters:
        run_spec.parameters = deepcopy(parameters)
    return run_spec


def _normalize_dataset_name(dataset_name: Any) -> str | None:
    if not isinstance(dataset_name, str):
        return None

    normalized = dataset_name.strip()
    return normalized or None


def normalize_config_override(
    config_override: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if config_override is None:
        return None

    normalized_override = deepcopy(config_override)
    shared_config = normalized_override.get("shared_config")
    if not isinstance(shared_config, dict):
        return normalized_override

    if "dataset" not in shared_config:
        return normalized_override

    normalized_dataset_name = _normalize_dataset_name(shared_config.get("dataset"))
    if normalized_dataset_name is None:
        shared_config.pop("dataset", None)
    else:
        shared_config["dataset"] = normalized_dataset_name

    return normalized_override


def extract_dataset_name_from_shared_config(shared_config: dict[str, Any] | None) -> str | None:
    if not isinstance(shared_config, dict):
        return None
    return _normalize_dataset_name(shared_config.get("dataset"))


def extract_dataset_name_from_config_override(
    config_override: dict[str, Any] | None,
) -> str | None:
    if not isinstance(config_override, dict):
        return None

    shared_config = config_override.get("shared_config")
    return extract_dataset_name_from_shared_config(shared_config)


def resolve_strategy_dataset_name(
    strategy_name: str,
    *,
    config_override: dict[str, Any] | None = None,
    config_loader: ConfigLoader | None = None,
) -> str | None:
    override_dataset_name = extract_dataset_name_from_config_override(config_override)
    if override_dataset_name is not None:
        return override_dataset_name

    loader = config_loader or ConfigLoader()
    try:
        strategy_config = loader.load_strategy_config(strategy_name)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.debug(f"run_spec dataset 解決で戦略読み込みに失敗: {strategy_name}: {e}")
        return None

    try:
        shared_config = loader.merge_shared_config(strategy_config)
    except Exception as e:
        logger.debug(f"run_spec dataset 解決で shared_config merge に失敗: {strategy_name}: {e}")
        return None

    return extract_dataset_name_from_shared_config(shared_config)


def build_strategy_run_spec(
    job_type: str,
    strategy_name: str,
    *,
    config_override: dict[str, Any] | None = None,
    parameters: dict[str, Any] | None = None,
    config_loader: ConfigLoader | None = None,
) -> RunSpec:
    loader = config_loader or ConfigLoader()
    normalized_config_override = normalize_config_override(config_override)
    payload: dict[str, Any] = {}
    if normalized_config_override is not None:
        payload["config_override"] = normalized_config_override
    if parameters:
        payload.update(deepcopy(parameters))

    run_spec = build_parameterized_run_spec(
        job_type,
        strategy_name,
        dataset_name=resolve_strategy_dataset_name(
            strategy_name,
            config_override=normalized_config_override,
            config_loader=loader,
        ),
        parameters=payload or None,
    )
    try:
        strategy_config = loader.load_strategy_config(strategy_name)
        compiled_strategy = compile_strategy_config(
            strategy_name,
            strategy_config,
            config_loader=loader,
            config_override=normalized_config_override,
        )
        run_spec.compiled_strategy_requirements = compile_strategy_requirements(
            compiled_strategy
        )
    except Exception as exc:
        logger.debug(
            "run_spec compiled_strategy_requirements 解決に失敗: "
            f"{strategy_name}: {exc}"
        )

    return run_spec


def build_config_override_run_spec(
    job_type: str,
    strategy_name: str,
    *,
    config_override: dict[str, Any] | None = None,
    parameters: dict[str, Any] | None = None,
) -> RunSpec:
    return build_strategy_run_spec(
        job_type,
        strategy_name,
        config_override=config_override,
        parameters=parameters,
    )


def build_run_metadata_from_spec(job_id: str, run_spec: RunSpec) -> RunMetadata:
    return RunMetadata(
        run_id=job_id,
        run_type=run_spec.run_type,
        strategy_name=run_spec.strategy_name,
        dataset_name=run_spec.dataset_name,
        dataset_snapshot_id=run_spec.dataset_snapshot_id,
        market_snapshot_id=run_spec.market_snapshot_id,
        engine_family=run_spec.engine_family,
        execution_policy_version=run_spec.execution_policy_version,
        parent_run_id=run_spec.parent_run_id,
    )


def build_artifact_index(job: JobInfo) -> ArtifactIndex | None:
    artifacts: list[ArtifactRecord] = []
    seen: set[tuple[ArtifactKind, str | None, str | None]] = set()

    def _append_artifact(record: ArtifactRecord) -> None:
        key = (record.kind, record.path, record.location)
        if key in seen:
            return
        seen.add(key)
        artifacts.append(record)

    if job.html_path:
        html_path = Path(job.html_path)
        _append_artifact(
            ArtifactRecord(
                kind=ArtifactKind.HTML,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(html_path),
            )
        )
        metrics_path = html_path.with_suffix(".metrics.json")
        if metrics_path.exists():
            _append_artifact(
                ArtifactRecord(
                    kind=ArtifactKind.METRICS_JSON,
                    storage=ArtifactStorage.FILESYSTEM,
                    path=str(metrics_path),
                )
            )
        manifest_path = html_path.with_suffix(".manifest.json")
        if manifest_path.exists():
            _append_artifact(
                ArtifactRecord(
                    kind=ArtifactKind.MANIFEST_JSON,
                    storage=ArtifactStorage.FILESYSTEM,
                    path=str(manifest_path),
                )
            )
        simulation_payload_path = html_path.with_suffix(".simulation.pkl")
        if simulation_payload_path.exists():
            _append_artifact(
                ArtifactRecord(
                    kind=ArtifactKind.SIMULATION_PAYLOAD,
                    storage=ArtifactStorage.FILESYSTEM,
                    path=str(simulation_payload_path),
                )
            )

    if job.result is not None:
        _append_artifact(
            ArtifactRecord(
                kind=ArtifactKind.RESULT_SUMMARY,
                storage=ArtifactStorage.PORTFOLIO_DB,
                location="jobs.result_json",
            )
        )

    if job.raw_result is not None:
        artifacts.append(
            ArtifactRecord(
                kind=ArtifactKind.RAW_RESULT_JSON,
                storage=ArtifactStorage.PORTFOLIO_DB,
                location="jobs.raw_result_json",
            )
        )
        for field_name, kind in _RAW_RESULT_ARTIFACT_PATHS:
            artifact_path = job.raw_result.get(field_name)
            if not isinstance(artifact_path, str) or not artifact_path.strip():
                continue
            path = Path(artifact_path)
            if not path.exists():
                continue
            _append_artifact(
                ArtifactRecord(
                    kind=kind,
                    storage=ArtifactStorage.FILESYSTEM,
                    path=str(path),
                    metadata={"source_field": field_name},
                )
            )

    if not artifacts:
        return None

    return ArtifactIndex(artifacts=artifacts)


def build_default_payload(job: JobInfo) -> dict[str, Any] | None:
    if job.raw_result is not None:
        payload = {
            key: value
            for key, value in job.raw_result.items()
            if key not in _INTERNAL_RAW_RESULT_KEYS
        }
        return payload or None

    if job.job_type != "optimization":
        return None

    payload: dict[str, Any] = {}
    if job.best_score is not None:
        payload["best_score"] = job.best_score
    if job.best_params is not None:
        payload["best_params"] = job.best_params
    if job.worst_score is not None:
        payload["worst_score"] = job.worst_score
    if job.worst_params is not None:
        payload["worst_params"] = job.worst_params
    if job.total_combinations is not None:
        payload["total_combinations"] = job.total_combinations
    return payload or None


def build_summary_metrics(job: JobInfo) -> CanonicalExecutionMetrics | None:
    if job.result is not None:
        summary = job.result
        return CanonicalExecutionMetrics(
            total_return=summary.total_return,
            sharpe_ratio=summary.sharpe_ratio,
            sortino_ratio=summary.sortino_ratio,
            calmar_ratio=summary.calmar_ratio,
            max_drawdown=summary.max_drawdown,
            win_rate=summary.win_rate,
            trade_count=summary.trade_count,
        )

    if job.job_type == "backtest_attribution" and isinstance(job.raw_result, dict):
        baseline_metrics = job.raw_result.get("baseline_metrics")
        if isinstance(baseline_metrics, dict):
            return CanonicalExecutionMetrics(
                total_return=baseline_metrics.get("total_return"),
                sharpe_ratio=baseline_metrics.get("sharpe_ratio"),
            )

    return None


def refresh_job_execution_contracts(job: JobInfo) -> None:
    if job.run_spec is None:
        job.run_spec = build_default_run_spec(job.job_type, job.strategy_name)
    if job.run_metadata is None:
        job.run_metadata = build_run_metadata_from_spec(job.job_id, job.run_spec)

    if job.run_spec.market_snapshot_id is None:
        job.run_spec.market_snapshot_id = resolve_market_snapshot_id()
    if job.run_metadata.market_snapshot_id is None:
        job.run_metadata.market_snapshot_id = job.run_spec.market_snapshot_id

    if job.dataset_name is not None:
        if job.run_spec.dataset_name is None:
            job.run_spec.dataset_name = job.dataset_name
        if job.run_spec.dataset_snapshot_id is None:
            job.run_spec.dataset_snapshot_id = resolve_dataset_snapshot_id(job.dataset_name)
        if job.run_metadata.dataset_name is None:
            job.run_metadata.dataset_name = job.dataset_name
        if job.run_metadata.dataset_snapshot_id is None:
            job.run_metadata.dataset_snapshot_id = resolve_dataset_snapshot_id(job.dataset_name)

    job.artifact_index = build_artifact_index(job)
    job.canonical_result = CanonicalExecutionResult(
        run_id=job.job_id,
        run_type=job.run_metadata.run_type,
        strategy_name=job.strategy_name,
        engine_family=job.run_metadata.engine_family,
        status=job.status.value if isinstance(job.status, JobStatus) else str(job.status),
        dataset_name=job.run_metadata.dataset_name,
        dataset_snapshot_id=job.run_metadata.dataset_snapshot_id,
        market_snapshot_id=job.run_metadata.market_snapshot_id,
        execution_policy_version=job.run_metadata.execution_policy_version,
        execution_time=job.execution_time,
        summary_metrics=build_summary_metrics(job),
        payload=build_default_payload(job),
    )
