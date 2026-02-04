"""path_resolver.py のテスト"""

from pathlib import Path
from unittest.mock import patch

import pytest

from src.strategy_config.path_resolver import (
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)


class TestInferStrategyPath:
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
        with patch("src.paths.get_data_dir", return_value=tmp_path / "data"):
            with pytest.raises(ValueError, match="許可されたディレクトリ外"):
                validate_path_within_strategies(Path("/etc/passwd"), tmp_path)


class TestGetAvailableStrategies:
    def test_returns_dict(self, tmp_path: Path) -> None:
        (tmp_path / "strategies" / "production").mkdir(parents=True)
        result = get_available_strategies(tmp_path)
        assert isinstance(result, dict)


class TestGetStrategyMetadata:
    def test_returns_list(self, tmp_path: Path) -> None:
        (tmp_path / "strategies" / "production").mkdir(parents=True)
        result = get_strategy_metadata(tmp_path)
        assert isinstance(result, list)
