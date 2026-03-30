"""
Strategy-linked optimization service.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.domains.optimization.grid_validation import format_grid_validation_issues
from src.domains.optimization.strategy_spec import (
    StrategyOptimizationAnalysis,
    analyze_saved_strategy_optimization,
    analyze_strategy_optimization,
    generate_strategy_optimization_draft,
    parse_optimization_yaml,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.shared.paths.resolver import get_all_optimization_grid_dirs


@dataclass
class StrategyOptimizationMigrationEntry:
    legacy_path: str
    status: str
    strategy_name: str | None = None
    message: str | None = None


@dataclass
class StrategyOptimizationMigrationReport:
    migrated: list[StrategyOptimizationMigrationEntry] = field(default_factory=list)
    skipped: list[StrategyOptimizationMigrationEntry] = field(default_factory=list)
    failed: list[StrategyOptimizationMigrationEntry] = field(default_factory=list)


class StrategyOptimizationService:
    def __init__(self, config_loader: ConfigLoader | None = None) -> None:
        self._config_loader = config_loader or ConfigLoader()

    def _save_strategy_config(
        self,
        strategy_name: str,
        strategy_config: dict[str, Any],
    ) -> None:
        self._config_loader.save_strategy_config(
            strategy_name,
            strategy_config,
            force=True,
            allow_production=True,
        )

    def get_state(self, strategy_name: str) -> StrategyOptimizationAnalysis:
        strategy_config = self._config_loader.load_strategy_config(strategy_name)
        return analyze_saved_strategy_optimization(strategy_config)

    def generate_draft(self, strategy_name: str) -> StrategyOptimizationAnalysis:
        strategy_config = self._config_loader.load_strategy_config(strategy_name)
        return generate_strategy_optimization_draft(strategy_config)

    def save(self, strategy_name: str, yaml_content: str) -> StrategyOptimizationAnalysis:
        parsed, parse_errors = parse_optimization_yaml(yaml_content)
        if parse_errors:
            raise ValueError(format_grid_validation_issues(parse_errors))

        strategy_config = self._config_loader.load_strategy_config(strategy_name)
        analysis = analyze_strategy_optimization(strategy_config, parsed)
        if not analysis.valid:
            raise ValueError(format_grid_validation_issues(analysis.errors))

        updated_config = dict(strategy_config)
        updated_config["optimization"] = analysis.optimization
        self._save_strategy_config(strategy_name, updated_config)
        return self.get_state(strategy_name)

    def delete(self, strategy_name: str) -> None:
        strategy_config = self._config_loader.load_strategy_config(strategy_name)
        if "optimization" not in strategy_config:
            return

        updated_config = dict(strategy_config)
        updated_config.pop("optimization", None)
        self._save_strategy_config(strategy_name, updated_config)

    def migrate_legacy_specs(self) -> StrategyOptimizationMigrationReport:
        report = StrategyOptimizationMigrationReport()
        strategy_names_by_basename = self._index_strategies_by_basename()

        for search_dir in get_all_optimization_grid_dirs():
            if not search_dir.exists():
                continue
            for legacy_file in sorted(search_dir.glob("*_grid.yaml")):
                basename = legacy_file.stem.removesuffix("_grid")
                matches = strategy_names_by_basename.get(basename, [])
                if len(matches) != 1:
                    report.skipped.append(
                        StrategyOptimizationMigrationEntry(
                            legacy_path=str(legacy_file),
                            status="skipped",
                            message="basename collision or strategy not found",
                        )
                    )
                    continue

                strategy_name = matches[0]
                try:
                    yaml_content = legacy_file.read_text(encoding="utf-8")
                    parsed, parse_errors = parse_optimization_yaml(yaml_content)
                    if parse_errors:
                        report.failed.append(
                            StrategyOptimizationMigrationEntry(
                                legacy_path=str(legacy_file),
                                strategy_name=strategy_name,
                                status="failed",
                                message=format_grid_validation_issues(parse_errors),
                            )
                        )
                        continue

                    strategy_config = self._config_loader.load_strategy_config(strategy_name)
                    analysis = analyze_strategy_optimization(strategy_config, parsed)
                    if not analysis.valid:
                        report.failed.append(
                            StrategyOptimizationMigrationEntry(
                                legacy_path=str(legacy_file),
                                strategy_name=strategy_name,
                                status="failed",
                                message=format_grid_validation_issues(analysis.errors),
                            )
                        )
                        continue

                    updated_config = dict(strategy_config)
                    updated_config["optimization"] = analysis.optimization
                    self._save_strategy_config(strategy_name, updated_config)
                    legacy_file.unlink()
                    report.migrated.append(
                        StrategyOptimizationMigrationEntry(
                            legacy_path=str(legacy_file),
                            strategy_name=strategy_name,
                            status="migrated",
                        )
                    )
                except Exception as exc:
                    report.failed.append(
                        StrategyOptimizationMigrationEntry(
                            legacy_path=str(legacy_file),
                            strategy_name=strategy_name,
                            status="failed",
                            message=str(exc),
                        )
                    )

        return report

    def _index_strategies_by_basename(self) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        for metadata in self._config_loader.get_strategy_metadata():
            basename = metadata.name.split("/")[-1]
            index.setdefault(basename, []).append(metadata.name)
        return index


strategy_optimization_service = StrategyOptimizationService()
