"""
セクターシグナル ユニットテスト

sector_strength_ranking_signal, sector_rotation_phase_signal,
sector_volatility_regime_signal の基本動作・エッジケーステスト
"""

import numpy as np
import pandas as pd
import pytest

from src.strategies.signals.sector_strength import (
    sector_rotation_phase_signal,
    sector_strength_ranking_signal,
    sector_volatility_regime_signal,
)


class TestSectorStrengthRankingSignal:
    """sector_strength_ranking_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=200)

        # ベンチマーク（TOPIX代替）: 緩やかな上昇
        self.benchmark_close = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.005, 200)),
            index=self.dates,
        )

        # 複数セクターのOHLCデータ
        self.sector_data = {}
        sector_names = ["電気機器", "化学", "医薬品", "銀行業", "輸送用機器"]
        for i, name in enumerate(sector_names):
            # 各セクターに異なるトレンドを設定
            drift = 0.003 - i * 0.001  # 電気機器が最強、輸送用機器が最弱
            close = pd.Series(
                1000 * np.cumprod(1 + np.random.normal(drift, 0.008, 200)),
                index=self.dates,
            )
            self.sector_data[name] = pd.DataFrame(
                {
                    "Open": close * 0.999,
                    "High": close * 1.005,
                    "Low": close * 0.995,
                    "Close": close,
                    "Volume": np.random.randint(1000000, 5000000, 200),
                },
                index=self.dates,
            )

    def test_basic_functionality(self):
        """基本動作テスト: 強いセクターはTrue"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            momentum_period=20,
            sharpe_period=60,
            top_n=3,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.benchmark_close)

    def test_top_n_sensitivity(self):
        """top_n感度テスト: Nが大きいほどTrueが多い"""
        signal_top2 = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="化学",
            benchmark_close=self.benchmark_close,
            top_n=2,
        )
        signal_top4 = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="化学",
            benchmark_close=self.benchmark_close,
            top_n=4,
        )
        # top_n=4の方がtrueの割合が高いか同等
        assert signal_top4.sum() >= signal_top2.sum()

    def test_unknown_sector(self):
        """未知のセクター名テスト: 全False"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="不動産業",
            benchmark_close=self.benchmark_close,
            top_n=3,
        )
        assert signal.sum() == 0  # 未知セクターは全False

    def test_empty_sector_data(self):
        """空のセクターデータテスト"""
        signal = sector_strength_ranking_signal(
            sector_data={},
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=3,
        )
        assert signal.sum() == 0

    def test_all_top_n_equals_total_sectors(self):
        """top_n=全セクター数のテスト: ほぼ全True"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="銀行業",
            benchmark_close=self.benchmark_close,
            top_n=len(self.sector_data),
        )
        # 全セクターが含まれるので、ウォームアップ期間以降は全True
        assert isinstance(signal, pd.Series)
        # ウォームアップ後はTrueが多数
        assert signal.iloc[70:].sum() > 0

    def test_weight_parameters(self):
        """重みパラメータテスト"""
        # モメンタム重視
        signal_mom = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            momentum_weight=1.0,
            sharpe_weight=0.0,
            relative_weight=0.0,
            top_n=3,
        )
        assert isinstance(signal_mom, pd.Series)
        assert signal_mom.dtype == bool

    def test_nan_in_close(self):
        """NaN含むデータの処理テスト"""
        sector_data_with_nan = dict(self.sector_data)
        nan_df = sector_data_with_nan["電気機器"].copy()
        nan_df.loc[nan_df.index[:20], "Close"] = np.nan
        sector_data_with_nan["電気機器"] = nan_df

        signal = sector_strength_ranking_signal(
            sector_data=sector_data_with_nan,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=3,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool


class TestSectorRotationPhaseSignal:
    """sector_rotation_phase_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=200)

        # ベンチマーク: 緩やかな上昇
        self.benchmark_close = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.005, 200)),
            index=self.dates,
        )

        # セクター: ベンチマークより強い
        self.sector_close_strong = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.003, 0.005, 200)),
            index=self.dates,
        )

        # セクター: ベンチマークより弱い
        self.sector_close_weak = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(-0.001, 0.005, 200)),
            index=self.dates,
        )

    def test_leading_basic(self):
        """leading条件基本テスト"""
        signal = sector_rotation_phase_signal(
            sector_close=self.sector_close_strong,
            benchmark_close=self.benchmark_close,
            rs_period=20,
            direction="leading",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.sector_close_strong)

    def test_weakening_basic(self):
        """weakening条件基本テスト"""
        signal = sector_rotation_phase_signal(
            sector_close=self.sector_close_strong,
            benchmark_close=self.benchmark_close,
            rs_period=20,
            direction="weakening",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_invalid_direction(self):
        """不正なdirectionテスト"""
        with pytest.raises(ValueError):
            sector_rotation_phase_signal(
                sector_close=self.sector_close_strong,
                benchmark_close=self.benchmark_close,
                direction="invalid",
            )

    def test_period_effect(self):
        """期間効果テスト"""
        signal_short = sector_rotation_phase_signal(
            sector_close=self.sector_close_strong,
            benchmark_close=self.benchmark_close,
            rs_period=10,
            direction="leading",
        )
        signal_long = sector_rotation_phase_signal(
            sector_close=self.sector_close_strong,
            benchmark_close=self.benchmark_close,
            rs_period=40,
            direction="leading",
        )
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)

    def test_no_common_dates(self):
        """共通日付なしテスト"""
        other_dates = pd.date_range("2020-01-01", periods=100)
        other_close = pd.Series(
            1000 * np.ones(100), index=other_dates
        )
        signal = sector_rotation_phase_signal(
            sector_close=other_close,
            benchmark_close=self.benchmark_close,
            direction="leading",
        )
        # 共通日付がないので全False
        assert signal.sum() == 0

    def test_nan_handling(self):
        """NaN処理テスト"""
        sector_with_nan = self.sector_close_strong.copy()
        sector_with_nan.iloc[10:20] = np.nan
        signal = sector_rotation_phase_signal(
            sector_close=sector_with_nan,
            benchmark_close=self.benchmark_close,
            direction="leading",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool


class TestSectorVolatilityRegimeSignal:
    """sector_volatility_regime_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=200)

        # 安定したセクター（低ボラ）
        self.sector_close_stable = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.003, 200)),
            index=self.dates,
        )

        # 不安定なセクター（高ボラ）
        volatility = np.ones(200) * 0.005
        volatility[100:150] = 0.03  # 期間中にボラティリティスパイク
        self.sector_close_volatile = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, volatility, 200)),
            index=self.dates,
        )

    def test_low_vol_basic(self):
        """low_vol条件基本テスト"""
        signal = sector_volatility_regime_signal(
            sector_close=self.sector_close_stable,
            vol_period=20,
            vol_ma_period=60,
            direction="low_vol",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.sector_close_stable)

    def test_high_vol_basic(self):
        """high_vol条件基本テスト"""
        signal = sector_volatility_regime_signal(
            sector_close=self.sector_close_volatile,
            vol_period=20,
            vol_ma_period=60,
            direction="high_vol",
            spike_multiplier=1.5,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_invalid_direction(self):
        """不正なdirectionテスト"""
        with pytest.raises(ValueError):
            sector_volatility_regime_signal(
                sector_close=self.sector_close_stable,
                direction="medium",
            )

    def test_low_vol_vs_high_vol_complementary(self):
        """low_volとhigh_volの相補性テスト（spike_multiplier=1.0で完全相補）"""
        # spike_multiplier=1.0の場合、low_volとhigh_volは概ね相補的
        # ただしNaNやedge caseがあるため完全ではない
        signal_low = sector_volatility_regime_signal(
            sector_close=self.sector_close_stable,
            direction="low_vol",
        )
        signal_high = sector_volatility_regime_signal(
            sector_close=self.sector_close_stable,
            direction="high_vol",
            spike_multiplier=1.5,
        )
        # 両方Trueの日は少ない（spike_multiplier > 1.0のため）
        both_true = (signal_low & signal_high).sum()
        assert both_true == 0  # spike_multiplier > 1.0なら同時Trueはない

    def test_spike_multiplier_sensitivity(self):
        """spike_multiplier感度テスト"""
        signal_low_mult = sector_volatility_regime_signal(
            sector_close=self.sector_close_volatile,
            direction="high_vol",
            spike_multiplier=1.2,
        )
        signal_high_mult = sector_volatility_regime_signal(
            sector_close=self.sector_close_volatile,
            direction="high_vol",
            spike_multiplier=2.0,
        )
        # 低い倍率の方がTrueが多い
        assert signal_low_mult.sum() >= signal_high_mult.sum()

    def test_nan_handling(self):
        """NaN処理テスト"""
        close_with_nan = self.sector_close_stable.copy()
        close_with_nan.iloc[:10] = np.nan
        signal = sector_volatility_regime_signal(
            sector_close=close_with_nan,
            direction="low_vol",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaN期間はFalse
        assert not signal.iloc[:10].any()

    def test_short_series(self):
        """短いSeriesテスト"""
        short_dates = pd.date_range("2024-01-01", periods=10)
        short_close = pd.Series(
            1000 * np.ones(10) + np.random.randn(10),
            index=short_dates,
        )
        signal = sector_volatility_regime_signal(
            sector_close=short_close,
            vol_period=20,
            vol_ma_period=60,
            direction="low_vol",
        )
        assert isinstance(signal, pd.Series)
        assert len(signal) == 10


class TestSectorStrengthEdgeCases:
    """セクターシグナル エッジケーステスト"""

    def test_single_sector_data(self):
        """セクターが1つだけの場合"""
        dates = pd.date_range("2024-01-01", periods=100)
        benchmark = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.005, 100)),
            index=dates,
        )
        sector_data = {
            "電気機器": pd.DataFrame(
                {
                    "Close": pd.Series(
                        1000 * np.cumprod(1 + np.random.normal(0.002, 0.008, 100)),
                        index=dates,
                    )
                },
                index=dates,
            )
        }
        signal = sector_strength_ranking_signal(
            sector_data=sector_data,
            stock_sector_name="電気機器",
            benchmark_close=benchmark,
            top_n=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 1セクターでtop_n=1なら、ウォームアップ後は全True
        assert signal.iloc[70:].sum() > 0

    def test_identical_sector_returns(self):
        """全セクター同一リターンの場合"""
        dates = pd.date_range("2024-01-01", periods=100)
        benchmark = pd.Series(1000.0, index=dates)
        constant_close = pd.Series(
            1000 * np.cumprod(1 + np.full(100, 0.001)),
            index=dates,
        )
        sector_data = {}
        for name in ["A", "B", "C"]:
            sector_data[name] = pd.DataFrame({"Close": constant_close.copy()}, index=dates)

        signal = sector_strength_ranking_signal(
            sector_data=sector_data,
            stock_sector_name="A",
            benchmark_close=benchmark,
            top_n=2,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_warmup_period_is_false(self):
        """ウォームアップ期間のNaN安全性テスト"""
        dates = pd.date_range("2024-01-01", periods=100)
        benchmark = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.005, 100)),
            index=dates,
        )
        sector_data = {}
        for name in ["A", "B", "C"]:
            close = pd.Series(
                1000 * np.cumprod(1 + np.random.normal(0.001, 0.008, 100)),
                index=dates,
            )
            sector_data[name] = pd.DataFrame({"Close": close}, index=dates)

        # sharpe_period=60 なので最初の約30日はNaN → Falseになるはず
        signal = sector_strength_ranking_signal(
            sector_data=sector_data,
            stock_sector_name="A",
            benchmark_close=benchmark,
            momentum_period=20,
            sharpe_period=60,
            top_n=2,
        )
        # 最初の数日は確実にFalse（全構成要素がNaN）
        assert not signal.iloc[0]

    def test_rotation_with_equal_index_sizes(self):
        """rotation_phaseの異なるインデックスサイズ"""
        dates1 = pd.date_range("2024-01-01", periods=100)
        dates2 = pd.date_range("2024-01-15", periods=100)  # 2週間ずれ
        sector = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.002, 0.005, 100)),
            index=dates1,
        )
        benchmark = pd.Series(
            1000 * np.cumprod(1 + np.random.normal(0.001, 0.005, 100)),
            index=dates2,
        )
        signal = sector_rotation_phase_signal(
            sector_close=sector,
            benchmark_close=benchmark,
            direction="leading",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(sector)

    def test_volatility_regime_constant_price(self):
        """一定価格でのボラティリティレジーム（ボラ=0）"""
        dates = pd.date_range("2024-01-01", periods=100)
        constant = pd.Series(1000.0, index=dates)
        signal = sector_volatility_regime_signal(
            sector_close=constant,
            direction="low_vol",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool


class TestSectorStrengthRankingBottomMode:
    """sector_strength_ranking_signal() bottom mode テスト"""

    def setup_method(self):
        """決定的テストデータ作成（フレーク防止のため単調データ使用）"""
        self.dates = pd.date_range("2024-01-01", periods=100)

        # ベンチマーク: 一定
        self.benchmark_close = pd.Series(1000.0, index=self.dates)

        # 5セクター: 決定的な強度順（電気機器が最強、輸送用機器が最弱）
        self.sector_data = {}
        sector_names = ["電気機器", "化学", "医薬品", "銀行業", "輸送用機器"]
        for i, name in enumerate(sector_names):
            # 線形上昇: 電気機器が最も急上昇、輸送用機器が最も緩やか（または下落）
            daily_return = 0.005 - i * 0.002  # 0.005, 0.003, 0.001, -0.001, -0.003
            close = pd.Series(
                1000.0 * np.cumprod(np.full(100, 1.0 + daily_return)),
                index=self.dates,
            )
            self.sector_data[name] = pd.DataFrame(
                {
                    "Open": close * 0.999,
                    "High": close * 1.001,
                    "Low": close * 0.999,
                    "Close": close,
                    "Volume": np.full(100, 1000000),
                },
                index=self.dates,
            )

    def test_bottom_mode_basic(self):
        """bottom mode 基本動作: 最弱セクターがTrue"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="輸送用機器",
            benchmark_close=self.benchmark_close,
            top_n=2,
            selection_mode="bottom",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 輸送用機器は最弱セクターなのでbottom_n=2に含まれる
        # ウォームアップ後にTrueが出るはず
        assert signal.iloc[70:].sum() > 0

    def test_bottom_mode_strongest_sector_excluded(self):
        """bottom mode: 最強セクターはbottomに含まれない"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=2,
            selection_mode="bottom",
        )
        # 電気機器は最強なのでbottom_n=2に含まれない
        # ウォームアップ後はFalseが多い
        assert signal.iloc[70:].sum() == 0

    def test_top_and_bottom_exclusivity(self):
        """top mode と bottom mode の排他性: 同一日・同一セクターで同時Trueにならない"""
        signal_top = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="医薬品",
            benchmark_close=self.benchmark_close,
            top_n=2,
            selection_mode="top",
        )
        signal_bottom = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="医薬品",
            benchmark_close=self.benchmark_close,
            top_n=2,
            selection_mode="bottom",
        )
        # 5セクターでtop_n=2なので、中間の医薬品はどちらにも入らないか、片方のみ
        both_true = (signal_top & signal_bottom).sum()
        assert both_true == 0

    def test_bottom_top_n_sensitivity(self):
        """bottom mode: Nが大きいほどTrueが増加"""
        signal_n1 = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="銀行業",
            benchmark_close=self.benchmark_close,
            top_n=1,
            selection_mode="bottom",
        )
        signal_n3 = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="銀行業",
            benchmark_close=self.benchmark_close,
            top_n=3,
            selection_mode="bottom",
        )
        assert signal_n3.sum() >= signal_n1.sum()

    def test_invalid_selection_mode(self):
        """不正な selection_mode で ValueError"""
        with pytest.raises(ValueError, match="Invalid selection_mode"):
            sector_strength_ranking_signal(
                sector_data=self.sector_data,
                stock_sector_name="電気機器",
                benchmark_close=self.benchmark_close,
                top_n=2,
                selection_mode="middle",
            )

    def test_bottom_top_n_exceeds_total_sectors(self):
        """bottom mode: top_n > 有効セクター数のエッジケース（全Trueになるべき）"""
        signal = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=10,  # 5セクターに対して10
            selection_mode="bottom",
        )
        # top_n > total_sectorsなので全セクターが含まれる
        # ウォームアップ後は全True
        assert signal.iloc[70:].all()

    def test_default_selection_mode_is_top(self):
        """デフォルトのselection_modeがtopであること"""
        # selection_mode未指定で呼び出し（既存動作と同一）
        signal_default = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=2,
        )
        signal_top = sector_strength_ranking_signal(
            sector_data=self.sector_data,
            stock_sector_name="電気機器",
            benchmark_close=self.benchmark_close,
            top_n=2,
            selection_mode="top",
        )
        pd.testing.assert_series_equal(signal_default, signal_top)


class TestSectorSignalParamsValidation:
    """Pydanticパラメータバリデーションテスト"""

    def test_sector_strength_ranking_params_defaults(self):
        """SectorStrengthRankingParams デフォルト値テスト"""
        from src.models.signals.sector import SectorStrengthRankingParams

        params = SectorStrengthRankingParams()
        assert params.enabled is False
        assert params.momentum_period == 20
        assert params.sharpe_period == 60
        assert params.top_n == 10
        assert params.selection_mode == "top"
        assert params.momentum_weight == 0.4
        assert params.sharpe_weight == 0.4
        assert params.relative_weight == 0.2

    def test_sector_strength_ranking_params_validation(self):
        """SectorStrengthRankingParams バリデーションテスト"""
        from src.models.signals.sector import SectorStrengthRankingParams

        with pytest.raises(Exception):
            SectorStrengthRankingParams(top_n=0)  # gt=0 違反

        with pytest.raises(Exception):
            SectorStrengthRankingParams(top_n=34)  # le=33 違反

        with pytest.raises(Exception):
            SectorStrengthRankingParams(momentum_weight=-0.1)  # ge=0.0 違反

    def test_sector_strength_ranking_selection_mode_validation(self):
        """SectorStrengthRankingParams selection_mode バリデーションテスト"""
        from src.models.signals.sector import SectorStrengthRankingParams

        # 有効な値
        params_top = SectorStrengthRankingParams(selection_mode="top")
        assert params_top.selection_mode == "top"

        params_bottom = SectorStrengthRankingParams(selection_mode="bottom")
        assert params_bottom.selection_mode == "bottom"

        # 不正な値（Literal型なのでValidationError）
        with pytest.raises(Exception):
            SectorStrengthRankingParams(selection_mode="invalid")

    def test_sector_rotation_phase_params_defaults(self):
        """SectorRotationPhaseParams デフォルト値テスト"""
        from src.models.signals.sector import SectorRotationPhaseParams

        params = SectorRotationPhaseParams()
        assert params.enabled is False
        assert params.rs_period == 20
        assert params.direction == "leading"

    def test_sector_rotation_phase_params_direction_validation(self):
        """SectorRotationPhaseParams direction バリデーションテスト"""
        from src.models.signals.sector import SectorRotationPhaseParams

        with pytest.raises(Exception):
            SectorRotationPhaseParams(direction="invalid")

        # 有効な値
        params = SectorRotationPhaseParams(direction="weakening")
        assert params.direction == "weakening"

    def test_sector_volatility_regime_params_defaults(self):
        """SectorVolatilityRegimeParams デフォルト値テスト"""
        from src.models.signals.sector import SectorVolatilityRegimeParams

        params = SectorVolatilityRegimeParams()
        assert params.enabled is False
        assert params.vol_period == 20
        assert params.vol_ma_period == 60
        assert params.direction == "low_vol"
        assert params.spike_multiplier == 1.5

    def test_sector_volatility_regime_params_validation(self):
        """SectorVolatilityRegimeParams バリデーションテスト"""
        from src.models.signals.sector import SectorVolatilityRegimeParams

        with pytest.raises(Exception):
            SectorVolatilityRegimeParams(direction="invalid")

        with pytest.raises(Exception):
            SectorVolatilityRegimeParams(spike_multiplier=0.5)  # gt=1.0 違反

        with pytest.raises(Exception):
            SectorVolatilityRegimeParams(spike_multiplier=6.0)  # le=5.0 違反


class TestSectorSignalCompositeIntegration:
    """SignalParams統合テスト"""

    def test_signal_params_has_sector_fields(self):
        """SignalParams にセクターフィールドが存在するか"""
        from src.models.signals import SignalParams

        params = SignalParams()
        assert hasattr(params, "sector_strength_ranking")
        assert hasattr(params, "sector_rotation_phase")
        assert hasattr(params, "sector_volatility_regime")

    def test_signal_params_sector_disabled_by_default(self):
        """セクターシグナルがデフォルトで無効"""
        from src.models.signals import SignalParams

        params = SignalParams()
        assert params.sector_strength_ranking.enabled is False
        assert params.sector_rotation_phase.enabled is False
        assert params.sector_volatility_regime.enabled is False

    def test_signal_params_yaml_roundtrip(self):
        """YAML用のdictシリアライズ・デシリアライズ"""
        from src.models.signals import SignalParams

        params = SignalParams()
        params.sector_strength_ranking.enabled = True
        params.sector_strength_ranking.top_n = 5
        params.sector_strength_ranking.selection_mode = "bottom"

        dumped = params.model_dump()
        assert dumped["sector_strength_ranking"]["enabled"] is True
        assert dumped["sector_strength_ranking"]["top_n"] == 5
        assert dumped["sector_strength_ranking"]["selection_mode"] == "bottom"

        restored = SignalParams(**dumped)
        assert restored.sector_strength_ranking.enabled is True
        assert restored.sector_strength_ranking.top_n == 5
        assert restored.sector_strength_ranking.selection_mode == "bottom"

    def test_has_any_enabled_with_sector(self):
        """has_any_enabled() がセクターシグナルを検出"""
        from src.models.signals import SignalParams

        params = SignalParams()
        assert params.has_any_enabled() is False

        params.sector_strength_ranking.enabled = True
        assert params.has_any_enabled() is True


class TestHasSectorDataChecker:
    """_has_sector_data data_checker のテスト"""

    def test_returns_false_when_no_sector_data(self):
        """sector_dataが空の場合はFalse"""
        from src.strategies.signals.registry import _has_sector_data

        assert _has_sector_data({}) is False
        assert _has_sector_data({"sector_data": {}}) is False
        assert _has_sector_data({"sector_data": None}) is False

    def test_returns_false_when_no_stock_sector_name(self):
        """stock_sector_nameがない場合はFalse"""
        from src.strategies.signals.registry import _has_sector_data

        d = {"sector_data": {"電気機器": pd.DataFrame()}, "stock_sector_name": ""}
        assert _has_sector_data(d) is False

    def test_returns_false_when_sector_not_in_data(self):
        """銘柄セクターがsector_dataに含まれない場合はFalse（warningログ）"""
        from src.strategies.signals.registry import _has_sector_data

        d = {
            "sector_data": {"化学": pd.DataFrame(), "銀行業": pd.DataFrame()},
            "stock_sector_name": "電気機器",
        }
        assert _has_sector_data(d) is False

    def test_returns_true_when_sector_present(self):
        """銘柄セクターがsector_dataに含まれる場合はTrue"""
        from src.strategies.signals.registry import _has_sector_data

        d = {
            "sector_data": {"電気機器": pd.DataFrame(), "化学": pd.DataFrame()},
            "stock_sector_name": "電気機器",
        }
        assert _has_sector_data(d) is True
