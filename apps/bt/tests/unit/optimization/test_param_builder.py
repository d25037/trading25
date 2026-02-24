"""
Tests for SignalParams dynamic builder
"""

from loguru import logger

from src.shared.models.signals import (
    SignalParams,
    PeriodBreakoutParams,
    VolumeSignalParams,
    CrossoverSignalParams,
    FundamentalSignalParams,
)
from src.domains.optimization.param_builder import (
    build_signal_params,
    _unflatten_params,
    _deep_merge,
)


class TestParamBuilder:
    """SignalParams動的構築のテスト"""

    def test_build_signal_params_basic(self):
        """基本的なSignalParams構築テスト"""
        # ベース設定
        base_params = SignalParams(
            period_breakout=PeriodBreakoutParams(
                enabled=True,
                direction="high",
                condition="break",
                lookback_days=10,
                period=100,
            )
        )

        # グリッドパラメータ
        grid_params = {
            "entry_filter_params.period_breakout.period": 200,
            "entry_filter_params.volume.threshold": 2.0,
        }

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証
        assert result.period_breakout is not None
        assert result.period_breakout.enabled is True  # ベースから継承
        assert result.period_breakout.direction == "high"  # ベースから継承
        assert result.period_breakout.period == 200  # グリッドで上書き

    def test_build_signal_params_preserve_base_settings(self):
        """ベース設定の保持テスト（enabled, direction等）"""
        # ベース設定（enabled=True, direction="high"）
        base_params = SignalParams(
            period_breakout=PeriodBreakoutParams(
                enabled=True,
                direction="high",
                condition="break",
                lookback_days=5,
                period=50,
            )
        )

        # グリッドパラメータ（periodのみ上書き）
        grid_params = {"entry_filter_params.period_breakout.period": 100}

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証: enabled, direction, conditionはベースから継承
        assert result.period_breakout is not None
        assert result.period_breakout.enabled is True
        assert result.period_breakout.direction == "high"
        assert result.period_breakout.condition == "break"
        assert result.period_breakout.period == 100  # グリッドで上書き

    def test_build_signal_params_multiple_signals(self):
        """複数シグナルの構築テスト"""
        # ベース設定
        base_params = SignalParams(
            period_breakout=PeriodBreakoutParams(
                enabled=True,
                direction="high",
                condition="break",
                lookback_days=10,
                period=100,
            ),
            volume=VolumeSignalParams(
                enabled=True,
                direction="surge",
                threshold=1.5,
                short_period=20,
                long_period=100,
            ),
        )

        # グリッドパラメータ
        grid_params = {
            "entry_filter_params.period_breakout.period": 200,
            "entry_filter_params.volume.threshold": 2.5,
        }

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証
        assert result.period_breakout is not None
        assert result.period_breakout.period == 200
        assert result.volume is not None
        assert result.volume.threshold == 2.5
        assert result.volume.direction == "surge"  # ベースから継承

    def test_build_signal_params_empty_grid(self):
        """グリッドパラメータが空の場合のテスト"""
        # ベース設定
        base_params = SignalParams(
            period_breakout=PeriodBreakoutParams(
                enabled=True,
                direction="high",
                condition="break",
                lookback_days=10,
                period=100,
            )
        )

        # グリッドパラメータが空
        grid_params = {}

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証: ベース設定がそのまま返される
        assert result.period_breakout is not None
        assert result.period_breakout.period == 100

    def test_build_signal_params_wrong_section(self):
        """間違ったセクションでフィルタリングされるテスト"""
        # ベース設定
        base_params = SignalParams(
            period_breakout=PeriodBreakoutParams(
                enabled=True,
                direction="high",
                condition="break",
                lookback_days=10,
                period=100,
            )
        )

        # グリッドパラメータ（exit_trigger_paramsセクション）
        grid_params = {"exit_trigger_params.period_breakout.period": 200}

        # entry_filter_paramsセクションでフィルタ
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証: グリッドパラメータが適用されない（セクションが違う）
        assert result.period_breakout is not None
        assert result.period_breakout.period == 100  # ベース設定のまま

    def test_validate_signal_names_mismatch_warning(self, caplog):
        """
        グリッドYAMLに存在しないシグナル名がある場合の警告テスト

        これは今回のバグケース（macd_cross vs crossover）を再現するテスト。
        グリッドYAMLで指定されたシグナル名がベースYAMLに存在しない場合、
        警告が出力され、パラメータが無視されることを確認する。
        """
        # ベース設定（"crossover" シグナル）
        base_params = SignalParams(
            crossover=CrossoverSignalParams(
                enabled=True,
                type="macd",
                direction="golden",
                fast_period=60,
                slow_period=130,
                signal_period=54,
            )
        )

        # グリッドパラメータ（"macd_cross" という存在しないシグナル名）
        grid_params = {
            "entry_filter_params.macd_cross.fast_period": 36,
            "entry_filter_params.macd_cross.slow_period": 78,
        }

        # loguruログキャプチャ設定
        log_messages = []

        def log_sink(message):
            log_messages.append(message)

        handler_id = logger.add(log_sink, level="WARNING")

        try:
            # SignalParams構築
            result = build_signal_params(
                grid_params, "entry_filter_params", base_params
            )

            # 検証1: 警告メッセージが出力されている
            assert len(log_messages) > 0
            warning_text = "".join(log_messages)
            assert "macd_cross" in warning_text
            assert "ベースYAMLに存在しません" in warning_text

            # 検証2: グリッドパラメータが無視され、ベース設定がそのまま使われる
            assert result.crossover is not None
            assert result.crossover.fast_period == 60  # グリッド値36が無視される
            assert result.crossover.slow_period == 130  # グリッド値78が無視される
            assert result.crossover.signal_period == 54  # ベース設定のまま

        finally:
            # ログハンドラーをクリーンアップ
            logger.remove(handler_id)

    def test_validate_signal_names_match_no_warning(self, caplog):
        """
        グリッドYAMLとベースYAMLのシグナル名が一致する場合の正常テスト

        シグナル名が正しく一致している場合、警告が出力されず、
        パラメータが正しく反映されることを確認する。
        """
        # ベース設定（"crossover" シグナル）
        base_params = SignalParams(
            crossover=CrossoverSignalParams(
                enabled=True,
                type="macd",
                direction="golden",
                fast_period=60,
                slow_period=130,
                signal_period=54,
            )
        )

        # グリッドパラメータ（正しい "crossover" シグナル名）
        grid_params = {
            "entry_filter_params.crossover.fast_period": 36,
            "entry_filter_params.crossover.slow_period": 78,
        }

        # loguruログキャプチャ設定
        log_messages = []

        def log_sink(message):
            log_messages.append(message)

        handler_id = logger.add(log_sink, level="WARNING")

        try:
            # SignalParams構築
            result = build_signal_params(
                grid_params, "entry_filter_params", base_params
            )

            # 検証1: 警告メッセージが出力されていない
            assert len(log_messages) == 0

            # 検証2: グリッドパラメータが正しく反映される
            assert result.crossover is not None
            assert result.crossover.fast_period == 36  # グリッド値が反映される
            assert result.crossover.slow_period == 78  # グリッド値が反映される
            assert (
                result.crossover.signal_period == 54
            )  # ベース設定のまま（上書きなし）
            assert result.crossover.enabled is True  # ベースから継承
            assert result.crossover.type == "macd"  # ベースから継承

        finally:
            # ログハンドラーをクリーンアップ
            logger.remove(handler_id)

    def test_build_signal_params_nested_fundamental(self):
        """
        4階層ネストパラメータ（fundamental.per.threshold）のマージテスト

        財務指標シグナルがネストされた構造に対応できることを確認する。
        entry_filter_params.fundamental.per.threshold のような4階層パスを
        正しく処理してSignalParamsを構築する。
        """
        # ベース設定（FundamentalSignalParams）
        base_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                per=FundamentalSignalParams.PERParams(
                    enabled=True,
                    threshold=15.0,  # ベース値
                ),
                pbr=FundamentalSignalParams.PBRParams(
                    enabled=True,
                    threshold=1.0,  # ベース値
                ),
                roe=FundamentalSignalParams.ROEParams(
                    enabled=True,
                    threshold=10.0,  # ベース値
                ),
            )
        )

        # グリッドパラメータ（4階層: entry_filter_params.fundamental.per.threshold）
        grid_params = {
            "entry_filter_params.fundamental.per.threshold": 20.0,
            "entry_filter_params.fundamental.pbr.threshold": 2.0,
        }

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証: グリッドパラメータが正しく反映される
        assert result.fundamental is not None
        assert result.fundamental.enabled is True  # ベースから継承
        assert result.fundamental.per.enabled is True  # ベースから継承
        assert result.fundamental.per.threshold == 20.0  # グリッドで上書き
        assert result.fundamental.pbr.enabled is True  # ベースから継承
        assert result.fundamental.pbr.threshold == 2.0  # グリッドで上書き
        assert result.fundamental.roe.threshold == 10.0  # ベース設定のまま（上書きなし）

    def test_build_signal_params_nested_fundamental_with_periods(self):
        """
        ネストされたパラメータで複数フィールド（threshold, periods）を持つケースのテスト

        profit_growth.threshold と profit_growth.periods のように、
        同一サブシグナル内の複数フィールドを同時に上書きできることを確認する。
        """
        # ベース設定
        base_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                profit_growth=FundamentalSignalParams.ProfitGrowthParams(
                    enabled=True,
                    threshold=0.1,  # ベース値
                    periods=4,  # ベース値
                ),
            )
        )

        # グリッドパラメータ（threshold と periods を両方上書き）
        grid_params = {
            "entry_filter_params.fundamental.profit_growth.threshold": 0.2,
            "entry_filter_params.fundamental.profit_growth.periods": 8,
        }

        # SignalParams構築
        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        # 検証
        assert result.fundamental is not None
        assert result.fundamental.profit_growth.enabled is True  # ベースから継承
        assert result.fundamental.profit_growth.threshold == 0.2  # グリッドで上書き
        assert result.fundamental.profit_growth.periods == 8  # グリッドで上書き


class TestHelperFunctions:
    """ヘルパー関数のテスト"""

    def test_unflatten_params_basic(self):
        """_unflatten_params: 基本的な平坦化解除テスト"""
        flat_params = {"per.threshold": 15.0}
        result = _unflatten_params(flat_params)

        assert result == {"per": {"threshold": 15.0}}

    def test_unflatten_params_multiple(self):
        """_unflatten_params: 複数パラメータの平坦化解除テスト"""
        flat_params = {
            "per.threshold": 15.0,
            "pbr.threshold": 1.0,
        }
        result = _unflatten_params(flat_params)

        assert result == {
            "per": {"threshold": 15.0},
            "pbr": {"threshold": 1.0},
        }

    def test_unflatten_params_deep_nesting(self):
        """_unflatten_params: 深いネストの平坦化解除テスト"""
        flat_params = {"a.b.c.d": 123}
        result = _unflatten_params(flat_params)

        assert result == {"a": {"b": {"c": {"d": 123}}}}

    def test_deep_merge_basic(self):
        """_deep_merge: 基本的なマージテスト"""
        base = {"enabled": True, "threshold": 15.0}
        updates = {"threshold": 20.0}
        result = _deep_merge(base, updates)

        assert result == {"enabled": True, "threshold": 20.0}
        # 元の辞書は変更されていないことを確認
        assert base == {"enabled": True, "threshold": 15.0}

    def test_deep_merge_nested(self):
        """_deep_merge: ネストされた辞書のマージテスト"""
        base = {
            "per": {"enabled": True, "threshold": 15.0},
            "pbr": {"enabled": False, "threshold": 1.0},
        }
        updates = {
            "per": {"threshold": 20.0},
        }
        result = _deep_merge(base, updates)

        assert result == {
            "per": {"enabled": True, "threshold": 20.0},  # thresholdのみ更新
            "pbr": {"enabled": False, "threshold": 1.0},  # 変更なし
        }

    def test_deep_merge_add_new_key(self):
        """_deep_merge: 新しいキーの追加テスト"""
        base = {"existing": 1}
        updates = {"new_key": 2}
        result = _deep_merge(base, updates)

        assert result == {"existing": 1, "new_key": 2}
