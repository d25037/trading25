"""
Strategy Configuration Package

戦略YAML設定の読み込みと管理
"""

from .file_operations import (
    delete_strategy_file,
    duplicate_to_experimental,
    load_yaml_file,
    save_yaml_file,
)
from .loader import ConfigLoader
from .parameter_extractor import (
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)
from .path_resolver import (
    StrategyMetadata,
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)
from .validator import (
    DANGEROUS_PATH_PATTERNS,
    MAX_STRATEGY_NAME_LENGTH,
    is_editable_category,
    validate_strategy_config,
    validate_strategy_name,
)

__all__ = [
    # Main class
    "ConfigLoader",
    # Metadata
    "StrategyMetadata",
    # Constants
    "DANGEROUS_PATH_PATTERNS",
    "MAX_STRATEGY_NAME_LENGTH",
    # Validator functions
    "is_editable_category",
    "validate_strategy_config",
    "validate_strategy_name",
    # Path resolver functions
    "get_available_strategies",
    "get_strategy_metadata",
    "infer_strategy_path",
    "validate_path_within_strategies",
    # Parameter extractor functions
    "extract_entry_filter_params",
    "extract_exit_trigger_params",
    "get_execution_config",
    "get_output_directory",
    "get_template_notebook_path",
    "merge_shared_config",
    # File operations
    "delete_strategy_file",
    "duplicate_to_experimental",
    "load_yaml_file",
    "save_yaml_file",
]
