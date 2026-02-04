# pyright: reportUnusedImport=false
"""
戦略自動生成・改善エージェントパッケージ

LLMコストなしのルールベース戦略自動生成・改善機能を提供

主要コンポーネント:
- StrategyGenerator: シグナル組み合わせの自動生成
- StrategyEvaluator: 生成戦略のバックテスト評価
- ParameterEvolver: 遺伝的アルゴリズムによるパラメータ最適化
- OptunaOptimizer: ベイズ最適化によるパラメータ探索
- StrategyImprover: 既存戦略の弱点分析と改善提案
- YamlUpdater: 戦略設定のYAML自動更新

使用例:
    # 新規戦略生成
    from src.agent import StrategyGenerator, StrategyEvaluator
    generator = StrategyGenerator()
    candidates = generator.generate(n_strategies=100)
    evaluator = StrategyEvaluator()
    results = evaluator.evaluate_batch(candidates, top_k=10)

    # 遺伝的アルゴリズム最適化
    from src.agent import ParameterEvolver
    evolver = ParameterEvolver()
    best, history = evolver.evolve("range_break_v15")

CLI使用:
    uv run bt lab generate --count 100 --top 10
    uv run bt lab evolve range_break_v15 --generations 20
    uv run bt lab optimize range_break_v15 --trials 100
    uv run bt lab improve range_break_v15 --auto-apply
"""

from .models import (
    EvaluationResult,
    EvolutionConfig,
    GeneratorConfig,
    Improvement,
    OptunaConfig,
    SignalConstraints,
    StrategyCandidate,
    WeaknessReport,
)
from .parameter_evolver import ParameterEvolver
from .strategy_evaluator import StrategyEvaluator
from .strategy_generator import StrategyGenerator
from .strategy_improver import StrategyImprover
from .yaml_updater import YamlUpdater

# Optunaは任意依存のため、インポートエラーを許容
try:
    from .optuna_optimizer import OptunaOptimizer  # noqa: F401

    __all_optuna__ = ["OptunaOptimizer"]
except ImportError:
    __all_optuna__ = []

__all__ = [
    # Generators
    "StrategyGenerator",
    "StrategyEvaluator",
    # Optimizers
    "ParameterEvolver",
    # Improvers
    "StrategyImprover",
    "YamlUpdater",
    # Models
    "StrategyCandidate",
    "EvaluationResult",
    "GeneratorConfig",
    "EvolutionConfig",
    "OptunaConfig",
    "SignalConstraints",
    "WeaknessReport",
    "Improvement",
] + __all_optuna__
