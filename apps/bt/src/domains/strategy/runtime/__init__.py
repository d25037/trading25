"""Strategy runtime/config boundary for Phase 4C."""

from src.domains.strategy.runtime.file_operations import (
    delete_strategy_file,
    duplicate_to_experimental,
    load_yaml_file,
    save_yaml_file,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.models import (
    ExecutionConfig,
    StrategyConfig,
    StrategyConfigStrictValidationError,
    try_validate_strategy_config_dict,
    try_validate_strategy_config_dict_strict,
    validate_strategy_config_dict,
    validate_strategy_config_dict_strict,
)
from src.domains.strategy.runtime.parameter_extractor import (
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)
from src.domains.strategy.runtime.path_resolver import (
    StrategyMetadata,
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)
from src.domains.strategy.runtime.validator import (
    DANGEROUS_PATH_PATTERNS,
    MAX_STRATEGY_NAME_LENGTH,
    is_editable_category,
    validate_strategy_config,
    validate_strategy_name,
)

__all__ = [
    "ConfigLoader",
    "ExecutionConfig",
    "StrategyConfig",
    "StrategyMetadata",
    "DANGEROUS_PATH_PATTERNS",
    "MAX_STRATEGY_NAME_LENGTH",
    "is_editable_category",
    "validate_strategy_config",
    "validate_strategy_name",
    "get_available_strategies",
    "get_strategy_metadata",
    "infer_strategy_path",
    "validate_path_within_strategies",
    "extract_entry_filter_params",
    "extract_exit_trigger_params",
    "get_execution_config",
    "get_output_directory",
    "get_template_notebook_path",
    "merge_shared_config",
    "delete_strategy_file",
    "duplicate_to_experimental",
    "load_yaml_file",
    "save_yaml_file",
    "validate_strategy_config_dict",
    "try_validate_strategy_config_dict",
    "validate_strategy_config_dict_strict",
    "try_validate_strategy_config_dict_strict",
    "StrategyConfigStrictValidationError",
]
