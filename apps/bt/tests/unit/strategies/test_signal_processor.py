"""
SignalProcessor unit tests

SignalProcessorクラスの基本機能・エラーハンドリング・統合処理をテスト
"""

from collections.abc import Callable

import pytest
import pandas as pd
from unittest.mock import patch

from src.domains.strategy.runtime.compiler import compile_runtime_strategy
from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalAvailability,
    CompiledSignalIR,
    CompiledSignalScope,
    CompiledStrategyIR,
)
from src.domains.strategy.signals.registry import SignalDefinition
from src.shared.models.config import SharedConfig
from src.domains.strategy.signals.processor import SignalProcessor
from src.shared.models.signals import SignalParams, Signals


def _signal_definition(
    *,
    name: str = "TestSignal",
    signal_func: Callable[..., pd.Series] | None = None,
    enabled_checker: Callable[[SignalParams], bool] | None = None,
    param_builder: Callable[[SignalParams, dict], dict] | None = None,
    entry_purpose: str = "",
    exit_purpose: str = "",
    category: str = "test",
    description: str = "",
    param_key: str = "test",
    data_checker: Callable[[dict], bool] | None = None,
    exit_disabled: bool = False,
    data_requirements: list[str] | None = None,
) -> SignalDefinition:
    return SignalDefinition(
        name=name,
        signal_func=signal_func or (lambda **_kwargs: pd.Series(dtype=bool)),
        enabled_checker=enabled_checker or (lambda _params: True),
        param_builder=param_builder or (lambda _params, _data: {}),
        entry_purpose=entry_purpose,
        exit_purpose=exit_purpose,
        category=category,
        description=description,
        param_key=param_key,
        data_checker=data_checker,
        exit_disabled=exit_disabled,
        data_requirements=[] if data_requirements is None else data_requirements,
    )


def _compiled_strategy_for_signal_ids(*signal_ids: str) -> CompiledStrategyIR:
    signals: list[CompiledSignalIR] = []
    for signal_id in signal_ids:
        scope_name, _, param_key = signal_id.partition(".")
        scope = (
            CompiledSignalScope.ENTRY
            if scope_name == "entry"
            else CompiledSignalScope.EXIT
        )
        signals.append(
            CompiledSignalIR(
                signal_id=signal_id,
                scope=scope,
                param_key=param_key,
                signal_name=param_key,
                category="test",
                description="",
                data_requirements=[],
                availability=CompiledSignalAvailability(
                    observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
                    available_at=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
                    decision_cutoff=CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
                    execution_session=CompiledExecutionSession.NEXT_SESSION,
                ),
            )
        )

    return CompiledStrategyIR(
        strategy_name="test",
        execution_semantics="standard",
        dataset_name="sample",
        timeframe="daily",
        signals=signals,
        signal_ids=[signal.signal_id for signal in signals],
        required_data_domains=[],
        required_features=[],
        required_fundamental_fields=[],
    )


class TestSignalProcessor:
    """SignalProcessor テストクラス"""

    def setup_method(self):
        """テストセットアップ"""
        self.processor = SignalProcessor()

        # テスト用データ作成
        self.test_data = pd.DataFrame(
            {
                "Close": [100.0, 101.0, 102.0, 103.0, 104.0],
                "High": [101.0, 102.0, 103.0, 104.0, 105.0],
                "Low": [99.0, 100.0, 101.0, 102.0, 103.0],
                "Volume": [1000.0, 1100.0, 1200.0, 1300.0, 1400.0],
            },
            index=pd.date_range("2023-01-01", periods=5),
        )

        self.base_signal = pd.Series(
            [True, False, True, False, True], index=self.test_data.index
        )
        self.signal_params = SignalParams()
        self.standard_compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                    "execution_policy": {"mode": "standard"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=self.signal_params,
            exit_signal_params=self.signal_params,
        )

    def test_processor_initialization(self):
        """プロセッサー初期化テスト"""
        processor = SignalProcessor()
        assert processor is not None

    def test_apply_entry_signals_basic(self):
        """エントリーシグナル基本処理テスト"""
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, pd.Series)
        assert len(result) == len(self.base_signal)
        assert result.dtype == bool

    def test_apply_exit_signals_basic(self):
        """エグジットシグナル基本処理テスト"""
        result = self.processor.apply_exit_signals(
            base_exits=self.base_signal,
            data=self.test_data,
            signal_params=self.signal_params,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, pd.Series)
        assert len(result) == len(self.base_signal)
        assert result.dtype == bool

    def test_generate_signals_integration(self):
        """統合シグナル生成テスト"""
        result = self.processor.generate_signals(
            strategy_entries=self.base_signal,
            strategy_exits=self.base_signal,
            ohlc_data=self.test_data,
            entry_signal_params=self.signal_params,
            exit_signal_params=self.signal_params,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, Signals)
        assert len(result.entries) == len(self.base_signal)
        assert len(result.exits) == len(self.base_signal)
        assert result.entries.dtype == bool
        assert result.exits.dtype == bool

    def test_apply_signals_entry_type(self):
        """エントリータイプシグナル処理テスト"""
        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="entry",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_apply_signals_exit_type(self):
        """エグジットタイプシグナル処理テスト"""
        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="exit",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_missing_ohlc_data_validation(self):
        """OHLCデータ不足時のバリデーションテスト"""
        with pytest.raises(ValueError, match="OHLCデータが提供されていません"):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=None,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

    def test_missing_required_columns_validation(self):
        """必須カラム不足時のバリデーションテスト"""
        invalid_data = pd.DataFrame({"Price": [100, 101, 102]})

        with pytest.raises(ValueError, match="必須カラムが不足しています"):
            self.processor.apply_signals(
                base_signal=pd.Series([True, False, True]),
                signal_type="entry",
                ohlc_data=invalid_data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

    def test_error_handling_in_signal_processing(self):
        """シグナル処理でのエラーハンドリングテスト"""
        # エラーが発生する可能性のあるシグナルパラメータを有効化
        params = SignalParams()
        params.volume_ratio_above.enabled = True

        # モックでエラーを発生させる（データ駆動設計対応: registry経由のシグナル関数）
        with patch(
            "src.domains.strategy.signals.volume.volume_ratio_above_signal"
        ) as mock_volume:
            mock_volume.side_effect = Exception("テストエラー")

            # エラーが発生してもプロセシングが継続することを確認
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
                compiled_strategy=self.standard_compiled_strategy,
            )

            assert isinstance(result, pd.Series)
            assert result.dtype == bool

    def test_with_optional_data(self):
        """オプションデータ付きシグナル処理テスト"""
        # ダミーの財務データ作成
        statements_data = pd.DataFrame(
            {
                "EPS": [10.0, 11.0, 12.0, 13.0, 14.0],
                "ROE": [15.0, 16.0, 17.0, 18.0, 19.0],
            },
            index=self.test_data.index,
        )

        # ダミーのベンチマークデータ作成
        benchmark_data = pd.DataFrame(
            {"Close": [2000.0, 2010.0, 2020.0, 2030.0, 2040.0]},
            index=self.test_data.index,
        )

        # ダミーの信用残高データ作成
        margin_data = pd.DataFrame(
            {"margin_balance": [1000000, 1100000, 1200000, 1300000, 1400000]},
            index=self.test_data.index,
        )

        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="entry",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
            statements_data=statements_data,
            benchmark_data=benchmark_data,
            margin_data=margin_data,
            compiled_strategy=self.standard_compiled_strategy,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_signal_logging(self):
        """シグナルログ記録テスト"""
        # ボリュームシグナルを有効化してログテスト
        params = SignalParams()
        params.volume_ratio_above.enabled = True

        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
                compiled_strategy=self.standard_compiled_strategy,
            )

            # ログが呼ばれたことを確認
            assert mock_logger.debug.called

    def test_relative_mode_skips_execution_required_signals(self):
        """相対価格モード時に実価格必須シグナルがスキップされることを確認"""
        data = pd.DataFrame(
            {
                "Close": [1.0, 1.0, 1.0],
                "Volume": [1.0, 1.0, 1.0],
            },
            index=pd.date_range("2023-01-01", periods=3),
        )
        base_signal = pd.Series([True, True, True], index=data.index)

        params = SignalParams()
        params.trading_value.enabled = True
        params.trading_value.period = 1
        params.trading_value.threshold_value = 1.0  # 1e-8 << 1.0 => Falseになる
        params.trading_value.direction = "above"
        compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                    "execution_policy": {"mode": "standard"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=params,
            exit_signal_params=SignalParams(),
        )

        # 通常モード: シグナルが適用され、全てFalseになる
        result_normal = self.processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=data,
            signal_params=params,
            relative_mode=False,
            compiled_strategy=compiled_strategy,
        )
        assert result_normal.sum() == 0

        # 相対価格モード（実価格なし）: 実価格必須シグナルはスキップされ、baseが維持される
        result_relative = self.processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=data,
            signal_params=params,
            relative_mode=True,
            compiled_strategy=compiled_strategy,
        )
        assert result_relative.equals(base_signal)

    def test_missing_data_warning_reports_requirement_name(self):
        """必須データ不足ログが実際の要件を示すことを確認"""
        dummy_signal = _signal_definition(
            name="Forward EPS成長率",
            signal_func=lambda **_kwargs: self.base_signal,
            param_key="test.forward",
            data_checker=lambda _data: False,
            data_requirements=["statements:ForwardForecastEPS"],
        )

        with (
            patch("src.domains.strategy.signals.processor.SIGNAL_REGISTRY", [dummy_signal]),
            patch("src.domains.strategy.signals.processor.logger") as mock_logger,
        ):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

        warning_messages = [call.args[0] for call in mock_logger.warning.call_args_list]
        assert any("statements:ForwardForecastEPS" in message for message in warning_messages)
        assert all("ベンチマークデータ" not in message for message in warning_messages)

    def test_missing_required_data_fails_closed_for_entry(self):
        """有効なentryシグナルで必須データ不足なら全期間Falseに倒す"""
        dummy_signal = _signal_definition(
            name="Forward EPS成長率",
            signal_func=lambda **_kwargs: self.base_signal,
            param_key="test.forward",
            data_checker=lambda _data: False,
            data_requirements=["statements:ForwardForecastEPS"],
        )

        with patch("src.domains.strategy.signals.processor.SIGNAL_REGISTRY", [dummy_signal]):
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

        assert result.dtype == bool
        assert not result.any()

    def test_missing_required_data_keeps_base_signal_for_exit(self):
        """有効なexitシグナルで必須データ不足でも fail-closed は False 発火に留める"""
        dummy_signal = _signal_definition(
            name="Forward EPS成長率",
            signal_func=lambda **_kwargs: self.base_signal,
            param_key="test.forward",
            data_checker=lambda _data: False,
            data_requirements=["statements:ForwardForecastEPS"],
        )

        with patch("src.domains.strategy.signals.processor.SIGNAL_REGISTRY", [dummy_signal]):
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="exit",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

        pd.testing.assert_series_equal(result, self.base_signal)

    def test_apply_signals_raises_when_close_all_nan(self):
        data = self.test_data.copy()
        data["Close"] = [float("nan")] * len(data)
        with pytest.raises(ValueError, match="Close価格データが全てNaN"):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

    def test_apply_signals_warns_when_volume_all_nan(self):
        data = self.test_data.copy()
        data["Volume"] = [float("nan")] * len(data)
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=data,
                signal_params=self.signal_params,
                compiled_strategy=self.standard_compiled_strategy,
            )

        assert result.dtype == bool
        assert any("Volumeデータが全てNaN" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_requirement_satisfied_covers_supported_requirements(self):
        benchmark = pd.DataFrame({"Close": [1.0, 2.0]}, index=self.test_data.index[:2])
        statements = pd.DataFrame({"EPS": [1.0, 2.0]}, index=self.test_data.index[:2])
        margin = pd.DataFrame({"x": [1.0]}, index=self.test_data.index[:1])
        sector_data = {"情報・通信業": pd.DataFrame({"Close": [1.0]}, index=self.test_data.index[:1])}
        universe_multi_data = {
            "1111": {
                "daily": pd.DataFrame(
                    {"Close": [1.0, 2.0], "Volume": [100.0, 110.0]},
                    index=self.test_data.index[:2],
                )
            }
        }
        ohlc = self.test_data.copy()
        ohlc["Open"] = ohlc["Close"]
        sources = {
            "benchmark_data": benchmark,
            "statements_data": statements,
            "margin_data": margin,
            "sector_data": sector_data,
            "stock_sector_name": "情報・通信業",
            "stock_code": "1111",
            "universe_multi_data": universe_multi_data,
            "ohlc_data": ohlc[["Open", "High", "Low", "Close"]],
            "volume": self.test_data["Volume"],
        }

        assert self.processor._is_requirement_satisfied("benchmark_close", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("statements", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("statements:EPS", sources)  # noqa: SLF001
        assert not self.processor._is_requirement_satisfied("statements:ForwardForecastEPS", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("margin", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("sector", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("universe_ohlcv", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("ohlc", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("volume", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("unknown", sources)  # noqa: SLF001

    def test_apply_signals_reuses_cached_universe_rank_bucket_panel(self):
        index = pd.date_range("2025-01-01", periods=3)
        ohlc_data = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0],
                "High": [101.0, 102.0, 103.0],
                "Low": [99.0, 100.0, 101.0],
                "Close": [100.0, 101.0, 102.0],
                "Volume": [1000.0, 1010.0, 1020.0],
            },
            index=index,
        )
        params = SignalParams.model_validate(
            {"universe_rank_bucket": {"enabled": True, "min_constituents": 2}}
        )
        compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111", "2222"],
                    "execution_policy": {"mode": "standard"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=params,
        )
        universe_multi_data = {
            "1111": {"daily": ohlc_data},
            "2222": {"daily": ohlc_data},
        }
        feature_panel = pd.DataFrame(
            {
                "date": [*index, *index],
                "stock_code": ["1111", "1111", "1111", "2222", "2222", "2222"],
                "price_count": [2, 2, 2, 2, 2, 2],
                "price_bucket": ["q1", "q1", "q1", "q10", "q10", "q10"],
            }
        )

        with patch(
            "src.domains.strategy.signals.processor.build_universe_rank_bucket_feature_panel",
            return_value=feature_panel,
        ) as mock_builder:
            result_1 = self.processor.apply_signals(
                base_signal=pd.Series(True, index=index),
                signal_type="entry",
                ohlc_data=ohlc_data,
                signal_params=params,
                stock_code="1111",
                universe_multi_data=universe_multi_data,
                universe_member_codes=("1111", "2222"),
                compiled_strategy=compiled_strategy,
            )
            result_2 = self.processor.apply_signals(
                base_signal=pd.Series(True, index=index),
                signal_type="entry",
                ohlc_data=ohlc_data,
                signal_params=params,
                stock_code="2222",
                universe_multi_data=universe_multi_data,
                universe_member_codes=("1111", "2222"),
                compiled_strategy=compiled_strategy,
            )

        assert mock_builder.call_count == 1
        assert bool(result_1.all()) is True
        assert bool(result_2.any()) is False

    def test_universe_rank_bucket_cache_evicts_oldest_panel(self):
        index = pd.date_range("2025-01-01", periods=3)
        ohlc_data = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0],
                "High": [101.0, 102.0, 103.0],
                "Low": [99.0, 100.0, 101.0],
                "Close": [100.0, 101.0, 102.0],
                "Volume": [1000.0, 1010.0, 1020.0],
            },
            index=index,
        )
        universe_multi_data = {"1111": {"daily": ohlc_data}}
        feature_panel = pd.DataFrame(
            {
                "date": index,
                "stock_code": ["1111"] * len(index),
                "price_count": [1] * len(index),
                "price_bucket": ["q1"] * len(index),
            }
        )

        with patch(
            "src.domains.strategy.signals.processor.build_universe_rank_bucket_feature_panel",
            return_value=feature_panel,
        ) as mock_builder:
            for price_sma_period in range(
                1,
                self.processor._UNIVERSE_BUCKET_CACHE_LIMIT + 2,  # noqa: SLF001
            ):
                self.processor._get_cached_universe_rank_bucket_feature_panel(  # noqa: SLF001
                    universe_multi_data=universe_multi_data,
                    universe_member_codes=("1111",),
                    price_sma_period=price_sma_period,
                )

        assert mock_builder.call_count == self.processor._UNIVERSE_BUCKET_CACHE_LIMIT + 1  # noqa: SLF001
        assert len(self.processor._universe_rank_bucket_cache) == self.processor._UNIVERSE_BUCKET_CACHE_LIMIT  # noqa: SLF001

    def test_describe_missing_requirements_fallback_message(self):
        no_requirements = _signal_definition(data_requirements=[])
        assert (
            self.processor._describe_missing_requirements(no_requirements, {"volume": self.test_data["Volume"]})  # noqa: SLF001
            == "data checker returned False"
        )

        satisfied = _signal_definition(data_requirements=["volume"])
        assert (
            self.processor._describe_missing_requirements(  # noqa: SLF001
                satisfied,
                {"volume": self.test_data["Volume"]},
            )
            == "data checker returned False"
        )

    def test_apply_unified_signal_exit_disabled_is_skipped(self):
        dummy_signal = _signal_definition(
            name="BuyAndHold",
            signal_func=lambda **_kwargs: self.base_signal,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test",
            exit_disabled=True,
        )

        signal_conditions = [self.base_signal]
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=signal_conditions,
                signal_type="exit",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
                compiled_strategy=self.standard_compiled_strategy,
            )

        assert len(signal_conditions) == 1
        assert any("Exit用途では使用不可" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_apply_unified_signal_reindexes_when_index_mismatch(self):
        result_index = self.base_signal.index[2:]
        dummy_signal = _signal_definition(
            name="MismatchSignal",
            signal_func=lambda **_kwargs: pd.Series([True, False, True], index=result_index),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test",
        )

        signal_conditions = [self.base_signal]
        compiled_strategy = _compiled_strategy_for_signal_ids("entry.test")
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
                compiled_strategy=compiled_strategy,
            )

        assert len(signal_conditions) == 2
        aligned = signal_conditions[1]
        assert aligned.index.equals(self.base_signal.index)
        assert aligned.isna().sum() == 2
        assert any("日付不一致" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_apply_unified_signal_lags_non_same_day_entry_in_current_session_round_trip(self):
        dummy_signal = _signal_definition(
            name="RSI",
            signal_func=lambda **_kwargs: pd.Series(
                [True, True, False, True, False],
                index=self.base_signal.index,
            ),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="rsi_threshold",
        )

        signal_conditions = [self.base_signal]
        compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                    "execution_policy": {"mode": "current_session_round_trip"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=SignalParams.model_validate(
                {"rsi_threshold": {"enabled": True}}
            ),
        )
        self.processor._apply_unified_signal(  # noqa: SLF001
            signal_def=dummy_signal,
            signal_conditions=signal_conditions,
            signal_type="entry",
            signal_params=self.signal_params,
            base_signal=self.base_signal,
            data_sources={"is_relative_mode": False},
            compiled_strategy=compiled_strategy,
        )

        assert len(signal_conditions) == 2
        expected = pd.Series(
            [False, True, True, False, True],
            index=self.base_signal.index,
        )
        pd.testing.assert_series_equal(signal_conditions[-1], expected)

    def test_apply_signals_current_session_round_trip_preserves_only_gap_signal_same_day(self):
        base_signal = pd.Series([True, True, True, True, True], index=self.test_data.index)
        params = SignalParams()
        params.index_open_gap_regime.enabled = True
        params.rsi_threshold.enabled = True

        same_day_signal = _signal_definition(
            name="TOPIX Gap Same-Day",
            signal_func=lambda **_kwargs: pd.Series(
                [False, True, True, False, True],
                index=self.test_data.index,
            ),
            enabled_checker=lambda signal_params: signal_params.index_open_gap_regime.enabled,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="index_open_gap_regime",
        )
        technical_signal = _signal_definition(
            name="RSI",
            signal_func=lambda **_kwargs: pd.Series(
                [True, False, True, True, False],
                index=self.test_data.index,
            ),
            enabled_checker=lambda signal_params: signal_params.rsi_threshold.enabled,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="rsi_threshold",
        )

        compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                    "execution_policy": {"mode": "current_session_round_trip"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=params,
        )

        with patch(
            "src.domains.strategy.signals.processor.SIGNAL_REGISTRY",
            [same_day_signal, technical_signal],
        ):
            result = self.processor.apply_signals(
                base_signal=base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
                compiled_strategy=compiled_strategy,
            )

        expected = pd.Series(
            [False, True, False, False, True],
            index=self.test_data.index,
            dtype=bool,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_apply_signals_uses_compiled_strategy_availability_without_boolean_flag(self):
        base_signal = pd.Series([True, True, True, True, True], index=self.test_data.index)
        params = SignalParams()
        params.index_open_gap_regime.enabled = True
        params.rsi_threshold.enabled = True

        compiled_strategy = compile_runtime_strategy(
            strategy_name="demo",
            shared_config=SharedConfig.model_validate(
                {
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                    "execution_policy": {"mode": "current_session_round_trip"},
                },
                context={"resolve_stock_codes": False},
            ),
            entry_signal_params=params,
        )

        same_day_signal = _signal_definition(
            name="TOPIX Gap Same-Day",
            signal_func=lambda **_kwargs: pd.Series(
                [False, True, True, False, True],
                index=self.test_data.index,
            ),
            enabled_checker=lambda signal_params: signal_params.index_open_gap_regime.enabled,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="index_open_gap_regime",
        )
        technical_signal = _signal_definition(
            name="RSI",
            signal_func=lambda **_kwargs: pd.Series(
                [True, False, True, True, False],
                index=self.test_data.index,
            ),
            enabled_checker=lambda signal_params: signal_params.rsi_threshold.enabled,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="rsi_threshold",
        )

        with patch(
            "src.domains.strategy.signals.processor.SIGNAL_REGISTRY",
            [same_day_signal, technical_signal],
        ):
            result = self.processor.apply_signals(
                base_signal=base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
                compiled_strategy=compiled_strategy,
            )

        expected = pd.Series(
            [False, True, False, False, True],
            index=self.test_data.index,
            dtype=bool,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_apply_unified_signal_keyerror_is_reraised(self):
        dummy_signal = _signal_definition(
            name="KeyErrorSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(KeyError("missing key")),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test",
        )

        with pytest.raises(KeyError):
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=[self.base_signal],
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
                compiled_strategy=self.standard_compiled_strategy,
            )

    def test_apply_unified_signal_value_and_unknown_errors_are_swallowed(self):
        value_error_signal = _signal_definition(
            name="ValueErrorSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(ValueError("invalid")),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test",
        )
        unexpected_error_signal = _signal_definition(
            name="UnexpectedSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test",
        )

        signal_conditions = [self.base_signal]
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=value_error_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
                compiled_strategy=self.standard_compiled_strategy,
            )
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=unexpected_error_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
                compiled_strategy=self.standard_compiled_strategy,
            )

        assert len(signal_conditions) == 1
        warning_messages = [str(call.args[0]) for call in mock_logger.warning.call_args_list]
        assert any("ValueError" in message for message in warning_messages)
        assert any("予期しないエラー" in message for message in warning_messages)

    def test_apply_signals_entry_early_stop_when_recent_window_exhausted(self):
        base_signal = pd.Series([True, True, True, True, True], index=self.test_data.index)

        first_signal = _signal_definition(
            name="FirstGate",
            signal_func=lambda **_kwargs: pd.Series(
                [True, True, False, False, False],
                index=self.test_data.index,
            ),
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test.first",
        )

        second_called = {"value": False}

        def _second_signal(**_kwargs):
            second_called["value"] = True
            return pd.Series([True, True, True, True, True], index=self.test_data.index)

        second_signal = _signal_definition(
            name="SecondGate",
            signal_func=_second_signal,
            entry_purpose="entry",
            exit_purpose="exit",
            param_key="test.second",
        )

        with patch(
            "src.domains.strategy.signals.processor.SIGNAL_REGISTRY",
            [first_signal, second_signal],
        ):
            compiled_strategy = _compiled_strategy_for_signal_ids(
                "entry.test.first",
                "entry.test.second",
            )
            result = self.processor.apply_signals(
                base_signal=base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
                entry_recent_days_for_early_stop=2,
                compiled_strategy=compiled_strategy,
            )

        assert second_called["value"] is False
        assert not result.tail(2).any()

    def test_generate_signals_skips_exit_when_recent_entry_is_empty(self):
        index = self.test_data.index
        entry_only_old = pd.Series([True, True, False, False, False], index=index)

        with (
            patch.object(
                self.processor,
                "apply_entry_signals",
                return_value=entry_only_old,
            ) as mock_entry,
            patch.object(self.processor, "apply_exit_signals") as mock_exit,
        ):
            result = self.processor.generate_signals(
                strategy_entries=self.base_signal,
                strategy_exits=self.base_signal,
                ohlc_data=self.test_data,
                entry_signal_params=self.signal_params,
                exit_signal_params=self.signal_params,
                screening_recent_days=2,
                skip_exit_when_no_recent_entry=True,
                compiled_strategy=self.standard_compiled_strategy,
            )

        assert mock_entry.called
        assert mock_entry.call_args.kwargs["screening_recent_days"] == 2
        assert not mock_exit.called
        assert result.entries.equals(entry_only_old)
        assert result.exits.dtype == bool
        assert result.exits.sum() == 0


if __name__ == "__main__":
    pytest.main([__file__])
