"""
Agent models のユニットテスト
"""

import pytest
from pydantic import ValidationError

from src.agent.models import (
    EvolutionConfig,
    EvaluationResult,
    GeneratorConfig,
    Improvement,
    OptunaConfig,
    SignalConstraints,
    StrategyCandidate,
    WeaknessReport,
)


class TestGeneratorConfig:
    """GeneratorConfig のテスト"""

    def test_default_values(self):
        """デフォルト値が正しく設定されることを確認"""
        config = GeneratorConfig()
        assert config.n_strategies == 100
        assert config.entry_signal_min == 2
        assert config.entry_signal_max == 5
        assert config.exit_signal_min == 2
        assert config.exit_signal_max == 4
        assert config.seed is None
        assert config.entry_filter_only is False
        assert config.allowed_categories == []

    def test_custom_values(self):
        """カスタム値が正しく設定されることを確認"""
        config = GeneratorConfig(
            n_strategies=50,
            entry_signal_min=3,
            entry_signal_max=4,
            seed=42,
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )
        assert config.n_strategies == 50
        assert config.entry_signal_min == 3
        assert config.seed == 42
        assert config.entry_filter_only is True
        assert config.allowed_categories == ["fundamental"]

    def test_validation_n_strategies_min(self):
        """n_strategies の最小値バリデーション"""
        with pytest.raises(ValidationError):
            GeneratorConfig(n_strategies=0)

    def test_validation_n_strategies_max(self):
        """n_strategies の最大値バリデーション"""
        with pytest.raises(ValidationError):
            GeneratorConfig(n_strategies=100001)


class TestEvolutionConfig:
    """EvolutionConfig のテスト"""

    def test_default_values(self):
        """デフォルト値が正しく設定されることを確認"""
        config = EvolutionConfig()
        assert config.population_size == 50
        assert config.generations == 20
        assert config.mutation_rate == 0.1
        assert config.crossover_rate == 0.7
        assert config.elite_ratio == 0.1
        assert config.entry_filter_only is False
        assert config.allowed_categories == []

    def test_mutation_rate_range(self):
        """mutation_rate の範囲バリデーション"""
        # 有効な値
        config = EvolutionConfig(mutation_rate=0.5)
        assert config.mutation_rate == 0.5

        # 無効な値
        with pytest.raises(ValidationError):
            EvolutionConfig(mutation_rate=1.5)


class TestOptunaConfig:
    """OptunaConfig のテスト"""

    def test_default_values(self):
        """デフォルト値が正しく設定されることを確認"""
        config = OptunaConfig()
        assert config.n_trials == 100
        assert config.sampler == "tpe"
        assert config.pruning is True
        assert config.entry_filter_only is False
        assert config.allowed_categories == []

    def test_sampler_options(self):
        """サンプラー設定"""
        config = OptunaConfig(sampler="cmaes")
        assert config.sampler == "cmaes"


class TestStrategyCandidate:
    """StrategyCandidate のテスト"""

    def test_minimal_creation(self):
        """最小限の情報で作成できることを確認"""
        candidate = StrategyCandidate(
            strategy_id="test_strategy",
            entry_filter_params={"volume": {"enabled": True}},
            exit_trigger_params={"atr_support_break": {"enabled": True}},
        )
        assert candidate.strategy_id == "test_strategy"
        assert "volume" in candidate.entry_filter_params

    def test_with_metadata(self):
        """メタデータ付きで作成できることを確認"""
        candidate = StrategyCandidate(
            strategy_id="test_strategy",
            entry_filter_params={},
            exit_trigger_params={},
            metadata={"generation": 5, "score": 0.8},
        )
        assert candidate.metadata["generation"] == 5


class TestEvaluationResult:
    """EvaluationResult のテスト"""

    def test_successful_result(self):
        """成功した評価結果を作成できることを確認"""
        candidate = StrategyCandidate(
            strategy_id="test",
            entry_filter_params={},
            exit_trigger_params={},
        )
        result = EvaluationResult(
            candidate=candidate,
            score=0.85,
            sharpe_ratio=1.5,
            calmar_ratio=2.0,
            total_return=0.25,
            success=True,
        )
        assert result.success is True
        assert result.sharpe_ratio == 1.5

    def test_failed_result(self):
        """失敗した評価結果を作成できることを確認"""
        candidate = StrategyCandidate(
            strategy_id="test",
            entry_filter_params={},
            exit_trigger_params={},
        )
        result = EvaluationResult(
            candidate=candidate,
            score=-999.0,
            success=False,
            error_message="Backtest failed",
        )
        assert result.success is False
        assert result.error_message == "Backtest failed"


class TestWeaknessReport:
    """WeaknessReport のテスト"""

    def test_creation(self):
        """弱点レポートを作成できることを確認"""
        report = WeaknessReport(
            strategy_name="test_strategy",
            max_drawdown=0.35,
            max_drawdown_duration_days=45,
        )
        assert report.strategy_name == "test_strategy"
        assert report.max_drawdown == 0.35

    def test_with_patterns(self):
        """パターン付きで作成できることを確認"""
        report = WeaknessReport(
            strategy_name="test",
            losing_trade_patterns=[
                {"type": "consecutive_losses", "count": 5},
            ],
        )
        assert len(report.losing_trade_patterns) == 1


class TestImprovement:
    """Improvement のテスト"""

    def test_add_signal_improvement(self):
        """シグナル追加改善を作成できることを確認"""
        improvement = Improvement(
            improvement_type="add_signal",
            target="entry",
            signal_name="volume",
            changes={"enabled": True, "threshold": 1.5},
            reason="エントリー精度向上",
            expected_impact="勝率向上",
        )
        assert improvement.improvement_type == "add_signal"
        assert improvement.signal_name == "volume"

    def test_adjust_param_improvement(self):
        """パラメータ調整改善を作成できることを確認"""
        improvement = Improvement(
            improvement_type="adjust_param",
            target="exit",
            signal_name="atr_support_break",
            changes={"atr_multiplier": 4.0},
            reason="早期損切り",
            expected_impact="ドローダウン軽減",
        )
        assert improvement.improvement_type == "adjust_param"


class TestSignalConstraints:
    """SignalConstraints のテスト"""

    def test_basic_signal(self):
        """基本的なシグナル制約を作成できることを確認"""
        constraint = SignalConstraints(
            name="volume",
            usage="both",
        )
        assert constraint.name == "volume"
        assert constraint.usage == "both"
        assert constraint.required_data == []

    def test_signal_with_requirements(self):
        """要件付きシグナル制約を作成できることを確認"""
        constraint = SignalConstraints(
            name="beta",
            usage="entry",
            required_data=["benchmark_data"],
            recommended_with=["volume"],
        )
        assert "benchmark_data" in constraint.required_data
        assert "volume" in constraint.recommended_with
