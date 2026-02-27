"""parameter_extractor.py のテスト"""

from pathlib import Path
from typing import Any

import pytest

from src.domains.strategy.runtime.parameter_extractor import (
    _deep_merge_dict,
    _get_dict_value,
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)


class TestGetDictValue:
    def test_returns_dict(self) -> None:
        assert _get_dict_value({"a": {"x": 1}}, "a") == {"x": 1}

    def test_missing_key_returns_empty(self) -> None:
        assert _get_dict_value({}, "a") == {}

    def test_non_dict_value_returns_empty(self) -> None:
        assert _get_dict_value({"a": "string"}, "a") == {}

    def test_none_value_returns_empty(self) -> None:
        assert _get_dict_value({"a": None}, "a") == {}

    def test_list_value_returns_empty(self) -> None:
        assert _get_dict_value({"a": [1, 2]}, "a") == {}


class TestDeepMergeDict:
    def test_simple_merge(self) -> None:
        result = _deep_merge_dict({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_override(self) -> None:
        result = _deep_merge_dict({"a": 1}, {"a": 2})
        assert result == {"a": 2}

    def test_nested_merge(self) -> None:
        base: dict[str, Any] = {"a": {"x": 1, "y": 2}}
        override: dict[str, Any] = {"a": {"y": 3, "z": 4}}
        result = _deep_merge_dict(base, override)
        assert result == {"a": {"x": 1, "y": 3, "z": 4}}

    def test_override_dict_with_scalar(self) -> None:
        result = _deep_merge_dict({"a": {"x": 1}}, {"a": 42})
        assert result == {"a": 42}

    def test_empty_dicts(self) -> None:
        assert _deep_merge_dict({}, {}) == {}

    def test_does_not_modify_base(self) -> None:
        base: dict[str, Any] = {"a": 1}
        _deep_merge_dict(base, {"b": 2})
        assert "b" not in base


class TestGetExecutionConfig:
    def test_merges_configs(self) -> None:
        strategy = {"execution": {"output_directory": "/custom"}}
        default = {"execution": {"template_notebook": "default.py"}}
        result = get_execution_config(strategy, default)
        assert result["template_notebook"] == "default.py"
        assert result["output_directory"] == "/custom"

    def test_empty_configs(self) -> None:
        result = get_execution_config({}, {})
        assert result == {}

    def test_strategy_overrides_default(self) -> None:
        strategy = {"execution": {"template_notebook": "custom.py"}}
        default = {"execution": {"template_notebook": "default.py"}}
        result = get_execution_config(strategy, default)
        assert result["template_notebook"] == "custom.py"


class TestGetTemplateNotebookPath:
    def test_custom_path(self) -> None:
        result = get_template_notebook_path({"template_notebook": "custom/path.py"})
        assert result == Path("custom/path.py")

    def test_default_path(self) -> None:
        result = get_template_notebook_path({})
        assert result == Path("notebooks/templates/strategy_analysis.py")


class TestGetOutputDirectory:
    def test_custom_directory(self) -> None:
        result = get_output_directory({"output_directory": "/tmp/output"})
        assert result == Path("/tmp/output")

    def test_default_directory(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "src.shared.paths.get_backtest_results_dir",
            lambda: Path("/default/results"),
        )
        result = get_output_directory({})
        assert result == Path("/default/results")


class TestExtractEntryFilterParams:
    def test_extracts_params(self) -> None:
        config = {"entry_filter_params": {"volume": {"enabled": True}}}
        assert extract_entry_filter_params(config) == {"volume": {"enabled": True}}

    def test_missing_key(self) -> None:
        assert extract_entry_filter_params({}) == {}

    def test_non_dict_returns_empty(self) -> None:
        assert extract_entry_filter_params({"entry_filter_params": "bad"}) == {}


class TestExtractExitTriggerParams:
    def test_extracts_params(self) -> None:
        config = {"exit_trigger_params": {"rsi": {"period": 14}}}
        assert extract_exit_trigger_params(config) == {"rsi": {"period": 14}}

    def test_missing_key(self) -> None:
        assert extract_exit_trigger_params({}) == {}


class TestMergeSharedConfig:
    def test_uses_default_when_no_strategy_override(self) -> None:
        default: dict[str, Any] = {"parameters": {"shared_config": {"initial_cash": 1000}}}
        result = merge_shared_config({}, default)
        assert result == {"initial_cash": 1000}

    def test_strategy_overrides_default(self) -> None:
        default: dict[str, Any] = {"parameters": {"shared_config": {"initial_cash": 1000, "fee": 0.01}}}
        strategy: dict[str, Any] = {"shared_config": {"initial_cash": 5000}}
        result = merge_shared_config(strategy, default)
        assert result["initial_cash"] == 5000
        assert result["fee"] == 0.01

    def test_empty_default(self) -> None:
        result = merge_shared_config({"shared_config": {"a": 1}}, {})
        assert result == {"a": 1}

    def test_both_empty(self) -> None:
        result = merge_shared_config({}, {})
        assert result == {}

    def test_warns_on_non_dict_parameters(self) -> None:
        default: dict[str, Any] = {"parameters": "invalid"}
        result = merge_shared_config({}, default)
        assert result == {}
