"""strategy_config/models.py のテスト"""

from src.domains.strategy.runtime.models import (
    StrategyConfigStrictValidationError,
    try_validate_strategy_config_dict,
    try_validate_strategy_config_dict_strict,
    validate_strategy_config_dict,
    validate_strategy_config_dict_strict,
)

import pytest


class TestValidateStrategyConfigDict:
    def test_valid_config(self) -> None:
        config = {
            "entry_filter_params": {"volume": {"enabled": True}},
        }
        result = validate_strategy_config_dict(config)
        assert result.entry_filter_params is not None

    def test_missing_entry_filter_params_raises(self) -> None:
        with pytest.raises(Exception):
            validate_strategy_config_dict({})


class TestTryValidateStrategyConfigDict:
    def test_valid_returns_true(self) -> None:
        config = {"entry_filter_params": {"volume": {"enabled": True}}}
        is_valid, error = try_validate_strategy_config_dict(config)
        assert is_valid is True
        assert error is None

    def test_invalid_returns_false_with_error(self) -> None:
        is_valid, error = try_validate_strategy_config_dict({})
        assert is_valid is False
        assert error is not None
        assert len(error) > 0


class TestStrictValidation:
    def test_strict_valid_config(self) -> None:
        config = {"entry_filter_params": {"volume": {"enabled": True}}}
        result = validate_strategy_config_dict_strict(config)
        assert result.entry_filter_params is not None

    def test_strict_nested_typo_rejected(self) -> None:
        config = {
            "entry_filter_params": {
                "fundamental": {
                    "foward_eps_growth": {  # typo
                        "enabled": True,
                        "threshold": 0.2,
                        "condition": "above",
                    }
                }
            }
        }

        with pytest.raises(StrategyConfigStrictValidationError):
            validate_strategy_config_dict_strict(config)

    def test_try_strict_returns_all_errors(self) -> None:
        config = {
            "entry_filter_params": {
                "fundamental": {
                    "foward_eps_growth": {  # typo
                        "enabled": True,
                    }
                }
            }
        }
        is_valid, errors = try_validate_strategy_config_dict_strict(config)
        assert is_valid is False
        assert any("entry_filter_params.fundamental.foward_eps_growth" in e for e in errors)
