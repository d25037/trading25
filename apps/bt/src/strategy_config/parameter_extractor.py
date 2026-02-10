"""Compatibility facade for Phase 4C."""

from src.lib.strategy_runtime.parameter_extractor import (
    _deep_merge_dict,
    _get_dict_value,
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)

__all__ = [
    "_deep_merge_dict",
    "_get_dict_value",
    "extract_entry_filter_params",
    "extract_exit_trigger_params",
    "get_execution_config",
    "get_output_directory",
    "get_template_notebook_path",
    "merge_shared_config",
]
