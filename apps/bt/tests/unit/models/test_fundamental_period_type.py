"""
FundamentalSignalParams period_type テスト
"""

from src.shared.models.signals.fundamental import FundamentalSignalParams
from src.shared.models.types import StatementsPeriodType


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
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(period_type="1Q")
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "1Q"

    def test_resolve_from_exit_params(self):
        """exit_trigger_params から period_type が解決されること"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = None
        mock_self.exit_trigger_params = MagicMock()
        mock_self.exit_trigger_params.fundamental = FundamentalSignalParams(period_type="2Q")

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "2Q"

    def test_resolve_default_when_no_params(self):
        """パラメータ未設定時のデフォルト 'FY'"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = None
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "FY"

    def test_entry_priority_over_exit(self):
        """entry側が exit側より優先されること"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(period_type="3Q")
        mock_self.exit_trigger_params = MagicMock()
        mock_self.exit_trigger_params.fundamental = FundamentalSignalParams(period_type="1Q")

        result = DataManagerMixin._resolve_period_type(mock_self)
        assert result == "3Q"

    def test_should_include_forecast_revision_detects_forward_or_peg(self):
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(
            enabled=True,
            forward_eps_growth={"enabled": False},
            peg_ratio={"enabled": True},
        )
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._should_include_forecast_revision(mock_self)
        assert result is True

    def test_should_include_forecast_revision_false_when_fundamental_disabled(self):
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        mock_self = MagicMock()
        mock_self.entry_filter_params = MagicMock()
        mock_self.entry_filter_params.fundamental = FundamentalSignalParams(
            enabled=False,
            forward_eps_growth={"enabled": True},
            peg_ratio={"enabled": True},
        )
        mock_self.exit_trigger_params = None

        result = DataManagerMixin._should_include_forecast_revision(mock_self)
        assert result is False

    def test_load_multi_data_disables_optional_sources_when_signals_unused(
        self, monkeypatch
    ):
        """依存シグナルが無い場合、margin/statementsロードを無効化すること"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        captured: dict[str, object] = {}

        def fake_prepare_multi_data(**kwargs):
            captured.update(kwargs)
            return {"7203": {"daily": MagicMock()}}

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.prepare_multi_data",
            fake_prepare_multi_data,
        )

        mock_self = MagicMock()
        mock_self.multi_data_dict = None
        mock_self.dataset = "primeExTopix500"
        mock_self.stock_codes = ["7203"]
        mock_self.start_date = None
        mock_self.end_date = None
        mock_self.timeframe = "daily"
        mock_self.include_margin_data = True
        mock_self.include_statements_data = True
        mock_self._resolve_period_type.return_value = "FY"
        mock_self._should_include_forecast_revision.return_value = False
        mock_self._should_load_margin_data.return_value = False
        mock_self._should_load_statements_data.return_value = False

        DataManagerMixin.load_multi_data(mock_self)

        assert captured["include_margin_data"] is False
        assert captured["include_statements_data"] is False
        assert captured["include_forecast_revision"] is False

    def test_load_multi_data_keeps_optional_sources_when_required(self, monkeypatch):
        """依存シグナルが有効な場合、margin/statementsロードを維持すること"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        captured: dict[str, object] = {}

        def fake_prepare_multi_data(**kwargs):
            captured.update(kwargs)
            return {"7203": {"daily": MagicMock()}}

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.prepare_multi_data",
            fake_prepare_multi_data,
        )

        mock_self = MagicMock()
        mock_self.multi_data_dict = None
        mock_self.dataset = "primeExTopix500"
        mock_self.stock_codes = ["7203"]
        mock_self.start_date = None
        mock_self.end_date = None
        mock_self.timeframe = "daily"
        mock_self.include_margin_data = True
        mock_self.include_statements_data = True
        mock_self._resolve_period_type.return_value = "FY"
        mock_self._should_include_forecast_revision.return_value = False
        mock_self._should_load_margin_data.return_value = True
        mock_self._should_load_statements_data.return_value = True

        DataManagerMixin.load_multi_data(mock_self)

        assert captured["include_margin_data"] is True
        assert captured["include_statements_data"] is True
        assert captured["include_forecast_revision"] is False

    def test_load_multi_data_enables_forecast_revision_when_forecast_signals_used(
        self, monkeypatch
    ):
        """forward/PEGシグナル利用時にinclude_forecast_revisionを有効化すること"""
        from unittest.mock import MagicMock
        from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin

        captured: dict[str, object] = {}

        def fake_prepare_multi_data(**kwargs):
            captured.update(kwargs)
            return {"7203": {"daily": MagicMock()}}

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.prepare_multi_data",
            fake_prepare_multi_data,
        )

        mock_self = MagicMock()
        mock_self.multi_data_dict = None
        mock_self.dataset = "primeExTopix500"
        mock_self.stock_codes = ["7203"]
        mock_self.start_date = None
        mock_self.end_date = None
        mock_self.timeframe = "daily"
        mock_self.include_margin_data = False
        mock_self.include_statements_data = True
        mock_self._resolve_period_type.return_value = "FY"
        mock_self._should_include_forecast_revision.return_value = True
        mock_self._should_load_margin_data.return_value = False
        mock_self._should_load_statements_data.return_value = True

        DataManagerMixin.load_multi_data(mock_self)

        assert captured["include_forecast_revision"] is True
