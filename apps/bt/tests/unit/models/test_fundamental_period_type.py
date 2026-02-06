"""
FundamentalSignalParams period_type テスト
"""

from src.models.signals.fundamental import FundamentalSignalParams
from src.models.types import StatementsPeriodType


class TestFundamentalPeriodType:
    """period_type フィールドのテスト"""

    def test_default_value_is_fy(self):
        """デフォルト値が 'FY' であること"""
        params = FundamentalSignalParams()
        assert params.period_type == "FY"

    def test_accepts_all_valid_values(self):
        """全ての有効な値を受け入れること"""
        for value in ("all", "FY", "1Q", "2Q", "3Q"):
            params = FundamentalSignalParams(period_type=value)
            assert params.period_type == value

    def test_invalid_value_raises(self):
        """無効な値でバリデーションエラーが発生すること"""
        import pytest
        with pytest.raises(Exception):
            FundamentalSignalParams(period_type="4Q")  # type: ignore[arg-type]

    def test_existing_yaml_compatibility(self):
        """既存YAML（period_type未指定）との互換性"""
        # period_type を指定しないで構築
        params = FundamentalSignalParams(enabled=True)
        assert params.period_type == "FY"
        assert params.enabled is True

    def test_model_dump_includes_period_type(self):
        """model_dump() に period_type が含まれること"""
        params = FundamentalSignalParams(period_type="1Q")
        dump = params.model_dump()
        assert "period_type" in dump
        assert dump["period_type"] == "1Q"

    def test_from_dict_with_period_type(self):
        """辞書から period_type 付きで構築できること"""
        data = {"enabled": True, "period_type": "2Q"}
        params = FundamentalSignalParams(**data)
        assert params.period_type == "2Q"

    def test_from_dict_without_period_type(self):
        """辞書から period_type なしで構築できること（デフォルト適用）"""
        data = {"enabled": True}
        params = FundamentalSignalParams(**data)
        assert params.period_type == "FY"


class TestStatementsPeriodType:
    """StatementsPeriodType 共通型のテスト"""

    def test_type_alias_values(self):
        """StatementsPeriodType が正しい値を許容すること"""
        from typing import get_args
        expected = ("all", "FY", "1Q", "2Q", "3Q")
        assert get_args(StatementsPeriodType) == expected


class TestResolvePeriodType:
    """DataManagerMixin._resolve_period_type のテスト"""

    def test_resolve_from_entry_params(self):
        """entry_filter_params から period_type が解決されること"""
        from unittest.mock import MagicMock
        from src.strategies.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(period_type="1Q")
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "1Q"

    def test_resolve_from_exit_params(self):
        """exit_trigger_params から period_type が解決されること"""
        from unittest.mock import MagicMock
        from src.strategies.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = None
        mock_self.exit_trigger_params = MagicMock()
        mock_self.exit_trigger_params.fundamental = FundamentalSignalParams(period_type="2Q")

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "2Q"

    def test_resolve_default_when_no_params(self):
        """パラメータ未設定時のデフォルト 'FY'"""
        from unittest.mock import MagicMock
        from src.strategies.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = None
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "FY"

    def test_entry_priority_over_exit(self):
        """entry側が exit側より優先されること"""
        from unittest.mock import MagicMock
        from src.strategies.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(period_type="3Q")
        mock_self.exit_trigger_params = MagicMock()
        mock_self.exit_trigger_params.fundamental = FundamentalSignalParams(period_type="1Q")

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "3Q"
