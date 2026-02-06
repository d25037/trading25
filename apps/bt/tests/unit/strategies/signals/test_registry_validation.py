"""Tests for SIGNAL_REGISTRY param_key uniqueness validation."""

from unittest.mock import patch

import pytest

from src.strategies.signals.registry import SIGNAL_REGISTRY, SignalDefinition, _validate_registry


class TestRegistryParamKeyUniqueness:
    """param_key の重複がないことを検証するテスト"""

    def test_no_duplicate_param_keys(self) -> None:
        """現在のレジストリに重複 param_key がないことを確認"""
        param_keys = [sig.param_key for sig in SIGNAL_REGISTRY]
        assert len(param_keys) == len(set(param_keys)), (
            f"Duplicate param_keys found: "
            f"{[k for k in param_keys if param_keys.count(k) > 1]}"
        )

    def test_validate_registry_passes(self) -> None:
        """_validate_registry() が正常に完了することを確認"""
        _validate_registry()

    def test_validate_registry_raises_on_duplicate(self) -> None:
        """重複 param_key がある場合に ValueError が発生することを確認"""
        duplicate_registry = [
            SignalDefinition(
                name="Signal A",
                signal_func=lambda: None,
                enabled_checker=lambda p: True,
                param_builder=lambda p, d: {},
                entry_purpose="test",
                exit_purpose="test",
                category="test",
                description="test",
                param_key="duplicate_key",
            ),
            SignalDefinition(
                name="Signal B",
                signal_func=lambda: None,
                enabled_checker=lambda p: True,
                param_builder=lambda p, d: {},
                entry_purpose="test",
                exit_purpose="test",
                category="test",
                description="test",
                param_key="duplicate_key",
            ),
        ]

        with patch(
            "src.strategies.signals.registry.SIGNAL_REGISTRY", duplicate_registry
        ):
            with pytest.raises(ValueError, match="Duplicate param_key.*duplicate_key"):
                _validate_registry()

    def test_all_param_keys_are_non_empty(self) -> None:
        """全 param_key が空文字列でないことを確認"""
        for sig in SIGNAL_REGISTRY:
            assert sig.param_key, f"Empty param_key for signal: {sig.name}"
