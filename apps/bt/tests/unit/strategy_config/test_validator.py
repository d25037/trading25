"""
戦略設定バリデーション Unit Tests
"""

import pytest

from src.strategy_config.validator import (
    is_editable_category,
    validate_strategy_config,
    validate_strategy_name,
)


class TestValidateStrategyName:
    """validate_strategy_name関数のテスト"""

    @pytest.mark.parametrize(
        "name",
        [
            "range_break_v5",
            "my-strategy",
            "Strategy123",
            "production/range_break_v5",
            "experimental/my-strategy",
        ],
        ids=[
            "underscore",
            "hyphen",
            "alphanumeric",
            "category_slash",
            "category_slash_hyphen",
        ],
    )
    def test_valid_names(self, name: str) -> None:
        """正常な戦略名はエラーにならない"""
        validate_strategy_name(name)  # should not raise

    def test_empty_string_raises(self) -> None:
        """空文字列はValueErrorを送出"""
        with pytest.raises(ValueError, match="無効な戦略名"):
            validate_strategy_name("")

    def test_path_traversal_raises(self) -> None:
        """パストラバーサルはValueErrorを送出"""
        with pytest.raises(ValueError):
            validate_strategy_name("../etc/passwd")

    def test_too_long_raises(self) -> None:
        """長すぎる名前はValueErrorを送出"""
        with pytest.raises(ValueError, match="戦略名が長すぎます"):
            validate_strategy_name("a" * 101)

    @pytest.mark.parametrize(
        "name",
        [
            "name with spaces",
            "name@special",
            "名前",
        ],
        ids=["spaces", "at_sign", "non_ascii"],
    )
    def test_invalid_characters_raises(self, name: str) -> None:
        """不正な文字を含む名前はValueErrorを送出"""
        with pytest.raises(ValueError, match="無効な戦略名"):
            validate_strategy_name(name)

    def test_backslash_raises(self) -> None:
        """バックスラッシュはValueErrorを送出"""
        with pytest.raises(ValueError):
            validate_strategy_name("path\\name")

    def test_double_slash_raises(self) -> None:
        """ダブルスラッシュはValueErrorを送出"""
        with pytest.raises(ValueError, match="不正な文字"):
            validate_strategy_name("category//name")

    def test_tilde_raises(self) -> None:
        """チルダはValueErrorを送出"""
        with pytest.raises(ValueError):
            validate_strategy_name("~admin")


class TestValidateStrategyConfig:
    """validate_strategy_config関数のテスト"""

    def test_valid_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """妥当な設定はTrueを返す"""
        monkeypatch.setattr(
            "src.strategy_config.validator.try_validate_strategy_config_dict",
            lambda c: (True, None),
        )
        config: dict = {
            "entry_filter_params": {
                "volume": {},
                "trend": {},
                "fundamental": {},
                "volatility": {},
                "relative_performance": {},
                "margin": {},
            }
        }
        assert validate_strategy_config(config) is True

    def test_invalid_config_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """バリデーション失敗はFalseを返す"""
        monkeypatch.setattr(
            "src.strategy_config.validator.try_validate_strategy_config_dict",
            lambda c: (False, "validation error"),
        )
        assert validate_strategy_config({}) is False

    def test_missing_filter_types_warns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """フィルター欠落時も警告のみでTrueを返す"""
        monkeypatch.setattr(
            "src.strategy_config.validator.try_validate_strategy_config_dict",
            lambda c: (True, None),
        )
        config: dict = {"entry_filter_params": {}}
        assert validate_strategy_config(config) is True


class TestIsEditableCategory:
    """is_editable_category関数のテスト"""

    def test_experimental_is_editable(self) -> None:
        assert is_editable_category("experimental") is True

    def test_production_not_editable(self) -> None:
        assert is_editable_category("production") is False

    def test_reference_not_editable(self) -> None:
        assert is_editable_category("reference") is False

    def test_legacy_not_editable(self) -> None:
        assert is_editable_category("legacy") is False

    def test_empty_not_editable(self) -> None:
        assert is_editable_category("") is False
