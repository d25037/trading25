# Agent System (Strategy Auto-Generation & Optimization)

## 概要

LLMコストなしのルールベース戦略自動生成・最適化システム。
遺伝的アルゴリズムとベイズ最適化を活用した完全自動化パイプライン。

**実装箇所**: `src/agent/`

## 主要コンポーネント

### StrategyGenerator

シグナル組み合わせの自動生成

```python
from src.agent import StrategyGenerator, GeneratorConfig

config = GeneratorConfig(
    n_strategies=100,
    entry_signal_min=2,
    entry_signal_max=5,
    exit_signal_min=2,
    exit_signal_max=4,
    exclude_signals=["beta_filter"],  # 除外シグナル
    required_signals=["volume_filter"],  # 必須シグナル
)

generator = StrategyGenerator(config)
candidates = generator.generate()
```

### StrategyEvaluator

生成戦略のバックテスト評価

```python
from src.agent import StrategyEvaluator

evaluator = StrategyEvaluator(
    dataset="stocks",
    start_date="2020-01-01",
    end_date="2024-12-31",
)
results = evaluator.evaluate_batch(candidates, top_k=10)

# 結果はスコア順でソート済み
for result in results:
    print(f"{result.candidate.strategy_id}: {result.score:.3f}")
```

### ParameterEvolver

遺伝的アルゴリズムによるパラメータ最適化

```python
from src.agent import ParameterEvolver, EvolutionConfig

config = EvolutionConfig(
    population_size=50,
    generations=20,
    mutation_rate=0.1,
    crossover_rate=0.7,
    elite_ratio=0.1,
)

evolver = ParameterEvolver(config)
best_candidate, history = evolver.evolve(
    base_strategy="range_break_v15",
    dataset="stocks",
)
```

### OptunaOptimizer

ベイズ最適化によるパラメータ探索（任意依存）

```python
from src.agent import OptunaOptimizer, OptunaConfig

config = OptunaConfig(
    n_trials=100,
    sampler="tpe",  # tpe, random, cmaes
    pruning=True,
)

optimizer = OptunaOptimizer(config)
best_params, study = optimizer.optimize(
    strategy_name="range_break_v15",
    dataset="stocks",
)
```

### StrategyImprover

既存戦略の弱点分析と改善提案

```python
from src.agent import StrategyImprover

improver = StrategyImprover()
weakness_report = improver.analyze("range_break_v15")
improvements = improver.suggest_improvements(weakness_report)

# 自動適用
if auto_apply:
    improver.apply_improvements(improvements, strategy_name="range_break_v15")
```

### YamlUpdater

戦略設定のYAML自動更新

```python
from src.agent import YamlUpdater

updater = YamlUpdater()
updater.update_strategy(
    strategy_name="range_break_v15",
    entry_filter_params=new_entry_params,
    exit_trigger_params=new_exit_params,
)
```

## CLI使用方法

```bash
# 新規戦略生成（100個生成、上位10個を保持）
uv run bt lab generate --count 100 --top 10

# 遺伝的アルゴリズム最適化
uv run bt lab evolve range_break_v15 --generations 20 --population 50

# Optuna最適化
uv run bt lab optimize range_break_v15 --trials 100

# 弱点分析と改善
uv run bt lab improve range_break_v15 --auto-apply

# 生成戦略の評価
uv run bt lab evaluate --strategy-dir config/strategies/experimental/evolved/
```

## データモデル

### StrategyCandidate

戦略候補の構造

```python
class StrategyCandidate(BaseModel):
    strategy_id: str
    entry_filter_params: dict[str, Any]
    exit_trigger_params: dict[str, Any]
    shared_config: dict[str, Any] = {}
    metadata: dict[str, Any] = {}
```

### EvaluationResult

評価結果の構造

```python
class EvaluationResult(BaseModel):
    candidate: StrategyCandidate
    score: float  # 複合スコア（正規化済み）
    sharpe_ratio: float
    calmar_ratio: float
    total_return: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    success: bool
    error_message: str | None
```

### EvolutionConfig

遺伝的アルゴリズム設定

```python
class EvolutionConfig(BaseModel):
    population_size: int = 50
    generations: int = 20
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elite_ratio: float = 0.1
    tournament_size: int = 3
    n_jobs: int = -1  # 全CPU使用
    timeout_seconds: int = 600
```

## 生成戦略の保存先

- 生成された戦略: `config/strategies/experimental/evolved/`
- 命名規則: `evolved_{base_strategy}_{timestamp}.yaml`

## 関連ファイル

- `src/agent/__init__.py`: パッケージエントリ
- `src/agent/models.py`: Pydanticモデル定義
- `src/agent/strategy_generator.py`: 戦略生成ロジック
- `src/agent/strategy_evaluator.py`: バックテスト評価
- `src/agent/parameter_evolver.py`: 遺伝的アルゴリズム
- `src/agent/optuna_optimizer.py`: Optuna最適化
- `src/agent/strategy_improver.py`: 弱点分析・改善
- `src/agent/yaml_updater.py`: YAML更新ユーティリティ
- `src/cli_bt/lab.py`: CLIコマンド
- `tests/unit/agent/`: テストスイート
