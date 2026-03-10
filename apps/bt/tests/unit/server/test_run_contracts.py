"""Tests for engine-neutral run contract helpers."""

from __future__ import annotations

from pathlib import Path

from src.application.services.job_manager import JobInfo
from src.application.services.run_contracts import (
    build_config_override_run_spec,
    build_default_run_spec,
    build_parameterized_run_spec,
    build_strategy_run_spec,
    build_run_metadata_from_spec,
    extract_dataset_name_from_config_override,
    normalize_config_override,
    resolve_strategy_dataset_name,
    refresh_job_execution_contracts,
)
from src.domains.backtest.contracts import ArtifactKind, EngineFamily, RunType
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus


class TestBuildDefaultRunSpec:
    def test_backtest_defaults_to_vectorbt_legacy_policy(self) -> None:
        run_spec = build_default_run_spec("backtest", "demo-strategy")

        assert run_spec.run_type == RunType.BACKTEST
        assert run_spec.market_snapshot_id == "market:latest"
        assert run_spec.engine_family == EngineFamily.VECTORBT
        assert run_spec.execution_policy_version == "vectorbt-legacy-v1"
        assert run_spec.strategy_source_ref == "demo-strategy"

    def test_screening_defaults_to_unknown_engine(self) -> None:
        run_spec = build_default_run_spec("screening", "analytics/screening")

        assert run_spec.run_type == RunType.SCREENING
        assert run_spec.engine_family == EngineFamily.UNKNOWN
        assert run_spec.execution_policy_version is None


class TestBuildRunMetadata:
    def test_metadata_keeps_parent_and_snapshot(self) -> None:
        run_spec = build_default_run_spec("lab_optimize", "strategy-a")
        run_spec.parent_run_id = "parent-1"
        run_spec.dataset_snapshot_id = "snapshot-1"
        run_spec.market_snapshot_id = "market:latest"

        metadata = build_run_metadata_from_spec("run-1", run_spec)

        assert metadata.run_id == "run-1"
        assert metadata.run_type == RunType.LAB_OPTIMIZE
        assert metadata.parent_run_id == "parent-1"
        assert metadata.dataset_snapshot_id == "snapshot-1"
        assert metadata.market_snapshot_id == "market:latest"


class TestRunSpecBuilders:
    def test_extract_dataset_name_from_config_override(self) -> None:
        dataset_name = extract_dataset_name_from_config_override(
            {"shared_config": {"dataset": " primeExTopix500 "}}
        )

        assert dataset_name == "primeExTopix500"

    def test_normalize_config_override_strips_or_drops_blank_dataset(self) -> None:
        normalized = normalize_config_override(
            {
                "shared_config": {
                    "dataset": "   ",
                    "direction": "longonly",
                },
                "entry_filter_params": {"foo": 1},
            }
        )

        assert normalized == {
            "shared_config": {"direction": "longonly"},
            "entry_filter_params": {"foo": 1},
        }

    def test_normalize_config_override_trims_dataset(self) -> None:
        normalized = normalize_config_override(
            {"shared_config": {"dataset": " primeExTopix500 "}}
        )

        assert normalized == {"shared_config": {"dataset": "primeExTopix500"}}

    def test_build_parameterized_run_spec_sets_dataset_snapshot_and_parameters(self) -> None:
        run_spec = build_parameterized_run_spec(
            "lab_generate",
            "generate(n=10,top=3)",
            dataset_name="primeExTopix500",
            parameters={"count": 10, "top": 3},
        )

        assert run_spec.dataset_name == "primeExTopix500"
        assert run_spec.dataset_snapshot_id == "primeExTopix500"
        assert run_spec.market_snapshot_id == "market:latest"
        assert run_spec.parameters == {"count": 10, "top": 3}

    def test_build_parameterized_run_spec_normalizes_snapshot_id_separately(self) -> None:
        run_spec = build_parameterized_run_spec(
            "screening",
            "analytics/screening",
            dataset_name=" sample.db ",
        )

        assert run_spec.dataset_name == " sample.db "
        assert run_spec.dataset_snapshot_id == "sample"
        assert run_spec.market_snapshot_id == "market:latest"

    def test_build_parameterized_run_spec_canonicalizes_legacy_path_snapshot_id(self) -> None:
        run_spec = build_parameterized_run_spec(
            "screening",
            "analytics/screening",
            dataset_name="dataset/primeExTopix500.db",
        )

        assert run_spec.dataset_name == "dataset/primeExTopix500.db"
        assert run_spec.dataset_snapshot_id == "primeExTopix500"
        assert run_spec.market_snapshot_id == "market:latest"

    def test_build_parameterized_run_spec_does_not_fallback_to_invalid_raw_snapshot_id(self) -> None:
        run_spec = build_parameterized_run_spec(
            "screening",
            "analytics/screening",
            dataset_name="../primeExTopix500.db",
        )

        assert run_spec.dataset_name == "../primeExTopix500.db"
        assert run_spec.dataset_snapshot_id is None
        assert run_spec.market_snapshot_id == "market:latest"

    def test_build_config_override_run_spec_keeps_config_and_extra_parameters(self) -> None:
        run_spec = build_config_override_run_spec(
            "backtest_attribution",
            "demo-strategy",
            config_override={"shared_config": {"dataset": "sample"}, "entry_filter_params": {}},
            parameters={"shapley_top_n": 5},
        )

        assert run_spec.dataset_name == "sample"
        assert run_spec.dataset_snapshot_id == "sample"
        assert run_spec.market_snapshot_id == "market:latest"
        assert run_spec.parameters["config_override"] == {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {},
        }
        assert run_spec.parameters["shapley_top_n"] == 5

    def test_resolve_strategy_dataset_name_uses_base_strategy_when_override_missing(self) -> None:
        class _StubConfigLoader:
            def load_strategy_config(self, strategy_name: str) -> dict[str, object]:
                assert strategy_name == "demo-strategy"
                return {"shared_config": {"dataset": "primeExTopix500"}}

            def merge_shared_config(self, _strategy_config: dict[str, object]) -> dict[str, object]:
                return {"dataset": "primeExTopix500", "direction": "longonly"}

        dataset_name = resolve_strategy_dataset_name(
            "demo-strategy",
            config_loader=_StubConfigLoader(),
        )

        assert dataset_name == "primeExTopix500"

    def test_build_strategy_run_spec_uses_base_strategy_dataset_when_override_missing(self) -> None:
        class _StubConfigLoader:
            def load_strategy_config(self, strategy_name: str) -> dict[str, object]:
                assert strategy_name == "demo-strategy"
                return {"shared_config": {"dataset": "primeExTopix500"}}

            def merge_shared_config(self, _strategy_config: dict[str, object]) -> dict[str, object]:
                return {"dataset": "primeExTopix500"}

        run_spec = build_strategy_run_spec(
            "optimization",
            "demo-strategy",
            parameters={"optimization_mode": "grid_search"},
            config_loader=_StubConfigLoader(),
        )

        assert run_spec.dataset_name == "primeExTopix500"
        assert run_spec.dataset_snapshot_id == "primeExTopix500"
        assert run_spec.market_snapshot_id == "market:latest"
        assert run_spec.parameters["optimization_mode"] == "grid_search"
        assert run_spec.compiled_strategy_requirements is not None
        assert run_spec.compiled_strategy_requirements.signal_ids == []

    def test_build_strategy_run_spec_drops_blank_dataset_override_and_falls_back_to_base_dataset(
        self,
    ) -> None:
        class _StubConfigLoader:
            def load_strategy_config(self, strategy_name: str) -> dict[str, object]:
                assert strategy_name == "demo-strategy"
                return {"shared_config": {"dataset": "primeExTopix500"}}

            def merge_shared_config(self, _strategy_config: dict[str, object]) -> dict[str, object]:
                return {"dataset": "primeExTopix500", "direction": "longonly"}

        run_spec = build_strategy_run_spec(
            "backtest",
            "demo-strategy",
            config_override={"shared_config": {"dataset": "   ", "direction": "shortonly"}},
            config_loader=_StubConfigLoader(),
        )

        assert run_spec.dataset_name == "primeExTopix500"
        assert run_spec.dataset_snapshot_id == "primeExTopix500"
        assert run_spec.market_snapshot_id == "market:latest"
        assert run_spec.parameters["config_override"] == {
            "shared_config": {"direction": "shortonly"},
        }

    def test_build_strategy_run_spec_attaches_compiled_strategy_requirements(self) -> None:
        class _StubConfigLoader:
            def load_strategy_config(self, strategy_name: str) -> dict[str, object]:
                assert strategy_name == "demo-strategy"
                return {
                    "shared_config": {"dataset": "primeExTopix500"},
                    "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
                }

            def merge_shared_config(self, strategy_config: dict[str, object]) -> dict[str, object]:
                shared_config = strategy_config.get("shared_config", {})
                merged: dict[str, object] = {
                    "dataset": "primeExTopix500",
                    "direction": "longonly",
                    "timeframe": "daily",
                }
                if isinstance(shared_config, dict):
                    merged.update(shared_config)
                return merged

        run_spec = build_strategy_run_spec(
            "backtest",
            "demo-strategy",
            config_loader=_StubConfigLoader(),
        )

        assert run_spec.compiled_strategy_requirements is not None
        assert run_spec.compiled_strategy_requirements.required_data_domains == ["market"]
        assert run_spec.compiled_strategy_requirements.required_features == ["volume"]
        assert run_spec.compiled_strategy_requirements.signal_ids == [
            "entry.volume_ratio_above"
        ]


class TestRefreshJobExecutionContracts:
    def test_backtest_job_builds_artifact_index_and_canonical_result(self, tmp_path: Path) -> None:
        html_path = tmp_path / "result.html"
        html_path.write_text("<html>ok</html>", encoding="utf-8")
        html_path.with_suffix(".metrics.json").write_text("{}", encoding="utf-8")
        html_path.with_suffix(".manifest.json").write_text("{}", encoding="utf-8")

        job = JobInfo("job-1", "demo-strategy", job_type="backtest")
        job.status = JobStatus.COMPLETED
        job.dataset_name = "snapshot-20260309"
        job.execution_time = 12.5
        job.result = BacktestResultSummary(
            total_return=15.0,
            sharpe_ratio=1.6,
            sortino_ratio=1.8,
            calmar_ratio=2.1,
            max_drawdown=-8.0,
            win_rate=58.0,
            trade_count=42,
            html_path=str(html_path),
        )
        job.raw_result = {"source": "vectorbt"}
        job.html_path = str(html_path)

        refresh_job_execution_contracts(job)

        assert job.run_metadata is not None
        assert job.run_metadata.dataset_snapshot_id == "snapshot-20260309"
        assert job.run_metadata.market_snapshot_id == "market:latest"
        assert job.canonical_result is not None
        assert job.canonical_result.market_snapshot_id == "market:latest"
        assert job.canonical_result.execution_time == 12.5
        assert job.canonical_result.summary_metrics is not None
        assert job.canonical_result.summary_metrics.trade_count == 42
        assert job.artifact_index is not None
        kinds = {artifact.kind for artifact in job.artifact_index.artifacts}
        assert ArtifactKind.HTML in kinds
        assert ArtifactKind.METRICS_JSON in kinds
        assert ArtifactKind.MANIFEST_JSON in kinds
        assert ArtifactKind.RESULT_SUMMARY in kinds
        assert ArtifactKind.RAW_RESULT_JSON in kinds

    def test_refresh_job_execution_contracts_canonicalizes_legacy_dataset_snapshot_id(
        self, tmp_path: Path
    ) -> None:
        html_path = tmp_path / "result.html"
        html_path.write_text("<html>ok</html>", encoding="utf-8")

        job = JobInfo("job-path", "demo-strategy", job_type="backtest")
        job.status = JobStatus.COMPLETED
        job.dataset_name = "dataset/primeExTopix500.db"
        job.html_path = str(html_path)

        refresh_job_execution_contracts(job)

        assert job.run_spec is not None
        assert job.run_spec.dataset_snapshot_id == "primeExTopix500"
        assert job.run_metadata is not None
        assert job.run_metadata.dataset_snapshot_id == "primeExTopix500"
        assert job.canonical_result is not None
        assert job.canonical_result.dataset_snapshot_id == "primeExTopix500"

    def test_attribution_job_uses_internal_artifact_path_for_index_and_strips_payload(self, tmp_path: Path) -> None:
        artifact_path = tmp_path / "attribution.json"
        artifact_path.write_text("{}", encoding="utf-8")

        job = JobInfo("job-attr", "demo-strategy", job_type="backtest_attribution")
        job.status = JobStatus.COMPLETED
        job.raw_result = {
            "baseline_metrics": {"total_return": 3.0, "sharpe_ratio": 1.4},
            "_artifact_path": str(artifact_path),
        }

        refresh_job_execution_contracts(job)

        assert job.canonical_result is not None
        assert job.canonical_result.summary_metrics is not None
        assert job.canonical_result.summary_metrics.total_return == 3.0
        assert job.canonical_result.payload == {
            "baseline_metrics": {"total_return": 3.0, "sharpe_ratio": 1.4},
        }
        assert job.artifact_index is not None
        kinds = {artifact.kind for artifact in job.artifact_index.artifacts}
        assert ArtifactKind.ATTRIBUTION_JSON in kinds

    def test_lab_job_indexes_saved_yaml_artifacts(self, tmp_path: Path) -> None:
        strategy_path = tmp_path / "candidate.yaml"
        strategy_path.write_text("shared_config: {}\n", encoding="utf-8")
        history_path = tmp_path / "history.yaml"
        history_path.write_text("history: []\n", encoding="utf-8")

        job = JobInfo("job-lab", "demo-strategy", job_type="lab_optimize")
        job.status = JobStatus.COMPLETED
        job.raw_result = {
            "saved_strategy_path": str(strategy_path),
            "saved_history_path": str(history_path),
        }

        refresh_job_execution_contracts(job)

        assert job.artifact_index is not None
        kinds = {artifact.kind for artifact in job.artifact_index.artifacts}
        assert ArtifactKind.STRATEGY_YAML in kinds
        assert ArtifactKind.HISTORY_YAML in kinds

    def test_optimization_job_uses_fallback_payload_without_raw_result(self) -> None:
        job = JobInfo("job-2", "demo-strategy", job_type="optimization")
        job.status = JobStatus.COMPLETED
        job.best_score = 1.23
        job.best_params = {"fast": 5}
        job.worst_score = -0.4
        job.worst_params = {"fast": 30}
        job.total_combinations = 128

        refresh_job_execution_contracts(job)

        assert job.canonical_result is not None
        assert job.canonical_result.payload == {
            "best_score": 1.23,
            "best_params": {"fast": 5},
            "worst_score": -0.4,
            "worst_params": {"fast": 30},
            "total_combinations": 128,
        }
