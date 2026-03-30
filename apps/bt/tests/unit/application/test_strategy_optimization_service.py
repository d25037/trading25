from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import src.application.services.strategy_optimization_service as service_module
from src.application.services.strategy_optimization_service import (
    StrategyOptimizationAnalysis,
    StrategyOptimizationService,
)
from src.domains.optimization.grid_validation import GridValidationIssue


@dataclass
class _Metadata:
    name: str


class _StubLoader:
    def __init__(
        self,
        configs: dict[str, dict[str, Any]],
        metadata: list[str] | None = None,
    ) -> None:
        self.configs = configs
        self.saved: list[tuple[str, dict[str, Any], bool, bool]] = []
        self._metadata = [_Metadata(name=name) for name in (metadata or list(configs.keys()))]

    def load_strategy_config(self, strategy_name: str) -> dict[str, Any]:
        if strategy_name not in self.configs:
            raise FileNotFoundError(strategy_name)
        return dict(self.configs[strategy_name])

    def save_strategy_config(
        self,
        strategy_name: str,
        config: dict[str, Any],
        *,
        force: bool,
        allow_production: bool,
    ) -> None:
        self.saved.append((strategy_name, dict(config), force, allow_production))
        self.configs[strategy_name] = dict(config)

    def get_strategy_metadata(self) -> list[_Metadata]:
        return list(self._metadata)


def _strategy_config() -> dict[str, Any]:
    return {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": True,
                "period": 20,
            }
        },
        "exit_trigger_params": {},
    }


def _yaml_content() -> str:
    return """description: migrated
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [10, 20, 30]
"""


def test_save_persists_valid_optimization_block() -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)

    analysis = service.save("production/demo", _yaml_content())

    assert analysis.valid is True
    assert loader.saved
    strategy_name, saved_config, force, allow_production = loader.saved[0]
    assert strategy_name == "production/demo"
    assert saved_config["optimization"]["description"] == "migrated"
    assert force is True
    assert allow_production is True


def test_save_rejects_invalid_yaml() -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)

    with pytest.raises(ValueError, match="YAML parse error"):
        service.save("production/demo", "parameter_ranges: [broken")

    assert loader.saved == []


def test_save_rejects_invalid_analysis() -> None:
    loader = _StubLoader(
        {
            "production/demo": {
                "entry_filter_params": {
                    "period_extrema_break": {
                        "enabled": False,
                        "period": 20,
                    }
                },
                "exit_trigger_params": {},
            }
        }
    )
    service = StrategyOptimizationService(config_loader=loader)

    with pytest.raises(ValueError, match="disabled"):
        service.save("production/demo", _yaml_content())

    assert loader.saved == []


def test_get_state_uses_saved_analysis(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    expected = StrategyOptimizationAnalysis(
        optimization={"parameter_ranges": {}},
        yaml_content="description: demo\nparameter_ranges: {}\n",
        valid=True,
        ready_to_run=False,
        param_count=0,
        combinations=0,
    )
    monkeypatch.setattr(service_module, "analyze_saved_strategy_optimization", lambda _config: expected)

    assert service.get_state("production/demo") is expected


def test_generate_draft_uses_strategy_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    expected = StrategyOptimizationAnalysis(
        optimization={"parameter_ranges": {}},
        yaml_content="description: draft\nparameter_ranges: {}\n",
        valid=True,
        ready_to_run=False,
        param_count=0,
        combinations=0,
    )
    monkeypatch.setattr(service_module, "generate_strategy_optimization_draft", lambda _config: expected)

    assert service.generate_draft("production/demo") is expected


def test_delete_skips_missing_optimization_block() -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)

    service.delete("production/demo")

    assert loader.saved == []


def test_delete_removes_existing_optimization_block() -> None:
    loader = _StubLoader(
        {
            "production/demo": {
                **_strategy_config(),
                "optimization": {
                    "description": "saved",
                    "parameter_ranges": {},
                },
            }
        }
    )
    service = StrategyOptimizationService(config_loader=loader)

    service.delete("production/demo")

    assert loader.saved
    assert "optimization" not in loader.saved[0][1]


def test_migrate_legacy_specs_moves_sidecar_into_strategy_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_dir = tmp_path / "optimization"
    legacy_dir.mkdir()
    legacy_file = legacy_dir / "demo_grid.yaml"
    legacy_file.write_text(_yaml_content(), encoding="utf-8")

    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [legacy_dir])

    report = service.migrate_legacy_specs()

    assert [entry.status for entry in report.migrated] == ["migrated"]
    assert report.migrated[0].strategy_name == "production/demo"
    assert not legacy_file.exists()
    assert loader.configs["production/demo"]["optimization"]["parameter_ranges"]["entry_filter_params"][
        "period_extrema_break"
    ]["period"] == [10, 20, 30]


def test_migrate_legacy_specs_skips_basename_collisions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_dir = tmp_path / "optimization"
    legacy_dir.mkdir()
    legacy_file = legacy_dir / "demo_grid.yaml"
    legacy_file.write_text(_yaml_content(), encoding="utf-8")

    loader = _StubLoader(
        {
            "production/demo": _strategy_config(),
            "experimental/demo": _strategy_config(),
        },
        metadata=["production/demo", "experimental/demo"],
    )
    service = StrategyOptimizationService(config_loader=loader)
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [legacy_dir])

    report = service.migrate_legacy_specs()

    assert [entry.status for entry in report.skipped] == ["skipped"]
    assert "collision" in (report.skipped[0].message or "")
    assert legacy_file.exists()
    assert loader.saved == []


def test_migrate_legacy_specs_ignores_missing_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    missing_dir = tmp_path / "missing"
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [missing_dir])

    report = service.migrate_legacy_specs()

    assert report == service_module.StrategyOptimizationMigrationReport()


def test_migrate_legacy_specs_reports_analysis_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_dir = tmp_path / "optimization"
    legacy_dir.mkdir()
    legacy_file = legacy_dir / "demo_grid.yaml"
    legacy_file.write_text(_yaml_content(), encoding="utf-8")

    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [legacy_dir])
    monkeypatch.setattr(
        service_module,
        "analyze_strategy_optimization",
        lambda *_args, **_kwargs: StrategyOptimizationAnalysis(
            optimization={"parameter_ranges": {}},
            yaml_content="",
            valid=False,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            errors=[GridValidationIssue(path="optimization", message="invalid analysis")],
        ),
    )

    report = service.migrate_legacy_specs()

    assert [entry.status for entry in report.failed] == ["failed"]
    assert "invalid analysis" in (report.failed[0].message or "")
    assert legacy_file.exists()


def test_migrate_legacy_specs_keeps_legacy_file_when_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_dir = tmp_path / "optimization"
    legacy_dir.mkdir()
    legacy_file = legacy_dir / "demo_grid.yaml"
    legacy_file.write_text("parameter_ranges: [broken", encoding="utf-8")

    loader = _StubLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [legacy_dir])

    report = service.migrate_legacy_specs()

    assert [entry.status for entry in report.failed] == ["failed"]
    assert "YAML parse error" in (report.failed[0].message or "")
    assert legacy_file.exists()
    assert loader.saved == []


def test_migrate_legacy_specs_reports_unexpected_save_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_dir = tmp_path / "optimization"
    legacy_dir.mkdir()
    legacy_file = legacy_dir / "demo_grid.yaml"
    legacy_file.write_text(_yaml_content(), encoding="utf-8")

    class _BrokenLoader(_StubLoader):
        def save_strategy_config(
            self,
            strategy_name: str,
            config: dict[str, Any],
            *,
            force: bool,
            allow_production: bool,
        ) -> None:
            raise RuntimeError("save failed")

    loader = _BrokenLoader({"production/demo": _strategy_config()})
    service = StrategyOptimizationService(config_loader=loader)
    monkeypatch.setattr(service_module, "get_all_optimization_grid_dirs", lambda: [legacy_dir])

    report = service.migrate_legacy_specs()

    assert [entry.status for entry in report.failed] == ["failed"]
    assert "save failed" in (report.failed[0].message or "")
    assert legacy_file.exists()


def test_index_strategies_by_basename_groups_matching_names() -> None:
    loader = _StubLoader(
        {
            "production/demo": _strategy_config(),
            "experimental/nested/demo": _strategy_config(),
        },
        metadata=["production/demo", "experimental/nested/demo"],
    )
    service = StrategyOptimizationService(config_loader=loader)

    assert service._index_strategies_by_basename() == {
        "demo": ["production/demo", "experimental/nested/demo"]
    }
