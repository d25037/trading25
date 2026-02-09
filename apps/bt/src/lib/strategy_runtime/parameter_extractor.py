"""Compatibility wrapper for strategy parameter extraction."""

from src.strategy_config.parameter_extractor import (
    _deep_merge_dict,
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)

__all__ = [
    "_deep_merge_dict",
    "extract_entry_filter_params",
    "extract_exit_trigger_params",
    "get_execution_config",
    "get_output_directory",
    "get_template_notebook_path",
    "merge_shared_config",
]

