"""strategy_config/models.py のテスト"""

from src.strategy_config.models import (
    try_validate_strategy_config_dict,
    validate_strategy_config_dict,
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
