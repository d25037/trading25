"""path_resolver.py のテスト"""

from pathlib import Path
from unittest.mock import patch

import pytest

from src.domains.strategy.runtime.path_resolver import (
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)


class TestInferStrategyPath:
    def test_default_config_uses_shared_resolver(self, monkeypatch) -> None:
        expected = Path("/tmp/external/production/demo.yaml")
        monkeypatch.setattr(
            "src.domains.strategy.runtime.path_resolver.paths_find_strategy_path",
            lambda _name: expected,
        )

        result = infer_strategy_path(Path("config"), "production/demo")
        assert result == expected

    def test_category_path_found(self, tmp_path: Path) -> None:
        strategy_file = tmp_path / "strategies" / "production" / "test_strat.yaml"
        strategy_file.parent.mkdir(parents=True)
        strategy_file.write_text("key: value\n")
        result = infer_strategy_path(tmp_path, "production/test_strat")
        assert result == strategy_file

    def test_category_path_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Strategy config not found"):
            infer_strategy_path(tmp_path, "production/nonexistent")

    def test_name_only_found(self, tmp_path: Path) -> None:
        strategy_file = tmp_path / "strategies" / "production" / "my_strat.yaml"
        strategy_file.parent.mkdir(parents=True)
        strategy_file.write_text("key: value\n")
        result = infer_strategy_path(tmp_path, "my_strat")
        assert result == strategy_file

    def test_name_only_not_found(self, tmp_path: Path) -> None:
        (tmp_path / "strategies").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="Strategy config not found"):
            infer_strategy_path(tmp_path, "nonexistent")


class TestValidatePathWithinStrategies:
    def test_valid_path(self, tmp_path: Path) -> None:
        strategies_dir = tmp_path / "strategies" / "production"
        strategies_dir.mkdir(parents=True)
        strategy_file = strategies_dir / "test.yaml"
        strategy_file.write_text("key: value\n")
        # Should not raise
        validate_path_within_strategies(strategy_file, tmp_path)

    def test_invalid_path(self, tmp_path: Path) -> None:
        with patch("src.shared.paths.get_data_dir", return_value=tmp_path / "data"):
            with pytest.raises(ValueError, match="許可されたディレクトリ外"):
                validate_path_within_strategies(Path("/etc/passwd"), tmp_path)

    def test_valid_external_path(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        strategy_file = data_dir / "strategies" / "production" / "test.yaml"
        strategy_file.parent.mkdir(parents=True)
        strategy_file.write_text("key: value\n")

        with patch("src.shared.paths.get_data_dir", return_value=data_dir):
            validate_path_within_strategies(strategy_file, tmp_path)

    def test_resolve_error_is_wrapped(self, tmp_path: Path, monkeypatch) -> None:
        target = tmp_path / "x.yaml"
        original_resolve = Path.resolve

        def _resolve(self: Path) -> Path:
            if self == target:
                raise RuntimeError("boom")
            return original_resolve(self)

        monkeypatch.setattr(
            Path,
            "resolve",
            _resolve,
        )
        with pytest.raises(ValueError, match="不正なファイルパス"):
            validate_path_within_strategies(target, tmp_path)


class TestGetAvailableStrategies:
    def test_returns_dict(self, tmp_path: Path) -> None:
        (tmp_path / "strategies" / "production").mkdir(parents=True)
        result = get_available_strategies(tmp_path)
        assert isinstance(result, dict)

    def test_delegates_project_dir(self, tmp_path: Path, monkeypatch) -> None:
        captured: dict[str, Path] = {}

        def _fake_get(project_strategies_dir: Path) -> dict[str, list[str]]:
            captured["project"] = project_strategies_dir
            return {"production": ["production/demo"]}

        monkeypatch.setattr(
            "src.domains.strategy.runtime.path_resolver.get_categorized_strategies",
            _fake_get,
        )

        result = get_available_strategies(tmp_path)
        assert result == {"production": ["production/demo"]}
        assert captured["project"] == tmp_path / "strategies"


class TestGetStrategyMetadata:
    def test_returns_list(self, tmp_path: Path) -> None:
        (tmp_path / "strategies" / "production").mkdir(parents=True)
        result = get_strategy_metadata(tmp_path)
        assert isinstance(result, list)

    def test_default_config_includes_external(self, monkeypatch) -> None:
        captured: dict[str, object] = {}

        def _fake_metadata(
            project_strategies_dir: Path, include_external: bool
        ) -> list[object]:
            captured["project"] = project_strategies_dir
            captured["include_external"] = include_external
            return []

        monkeypatch.setattr(
            "src.domains.strategy.runtime.path_resolver.get_strategy_metadata_list",
            _fake_metadata,
        )

        _ = get_strategy_metadata(Path("config"))
        assert captured["project"] == Path("config/strategies")
        assert captured["include_external"] is True
