"""
Strategy config schema tests
"""

import pytest

from pydantic import ValidationError

from src.strategy_config.models import validate_strategy_config_dict


def test_strategy_config_schema_valid_minimal():
    config = {
        "entry_filter_params": {},
    }

    result = validate_strategy_config_dict(config)
    assert result.entry_filter_params is not None


def test_strategy_config_schema_missing_entry_params():
    config = {
        "shared_config": {"dataset": "primeExTopix500"},
    }

    with pytest.raises(ValidationError):
        validate_strategy_config_dict(config)


def test_strategy_config_schema_no_stock_code_resolution(monkeypatch):
    def _raise(*args, **kwargs):  # pragma: no cover - should not be called
        raise RuntimeError("should not be called")

    monkeypatch.setattr("src.data.get_stock_list", _raise)

    config = {
        "shared_config": {
            "dataset": "primeExTopix500",
            "stock_codes": ["all"],
        },
        "entry_filter_params": {},
    }

    result = validate_strategy_config_dict(config)
    assert result.shared_config is not None
