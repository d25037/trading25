"""file_operations.py のテスト"""

from pathlib import Path

import pytest

from src.domains.strategy.runtime.file_operations import (
    delete_strategy_file,
    duplicate_to_experimental,
    load_yaml_file,
    save_yaml_file,
)


class TestLoadYamlFile:
    def test_load_valid(self, tmp_path: Path) -> None:
        f = tmp_path / "test.yaml"
        f.write_text("key: value\n")
        result = load_yaml_file(f)
        assert result == {"key": "value"}

    def test_load_empty(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.yaml"
        f.write_text("")
        result = load_yaml_file(f)
        assert result == {}

    def test_load_nonexistent_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_yaml_file(tmp_path / "nonexistent.yaml")


class TestSaveYamlFile:
    def test_save_and_reload(self, tmp_path: Path) -> None:
        f = tmp_path / "out.yaml"
        save_yaml_file(f, {"a": 1, "b": "hello"})
        result = load_yaml_file(f)
        assert result["a"] == 1
        assert result["b"] == "hello"

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        f = tmp_path / "sub" / "dir" / "out.yaml"
        save_yaml_file(f, {"x": 42})
        assert f.exists()


class TestDeleteStrategyFile:
    def test_delete_experimental(self, tmp_path: Path) -> None:
        f = tmp_path / "test.yaml"
        f.write_text("key: value\n")
        result = delete_strategy_file(f, "experimental")
        assert result is True
        assert not f.exists()

    def test_delete_production_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "test.yaml"
        f.write_text("key: value\n")
        with pytest.raises(PermissionError, match="削除不可"):
            delete_strategy_file(f, "production")


class TestDuplicateToExperimental:
    def test_duplicate_success(self, tmp_path: Path) -> None:
        source = tmp_path / "source.yaml"
        source.write_text("entry_filter_params:\n  volume:\n    enabled: true\n")
        target = tmp_path / "target.yaml"
        result = duplicate_to_experimental(source, target, "new_strat")
        assert result == target
        assert target.exists()
        loaded = load_yaml_file(target)
        assert "entry_filter_params" in loaded

    def test_duplicate_existing_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "source.yaml"
        source.write_text("key: value\n")
        target = tmp_path / "target.yaml"
        target.write_text("existing\n")
        with pytest.raises(FileExistsError, match="既に存在"):
            duplicate_to_experimental(source, target, "existing_strat")
