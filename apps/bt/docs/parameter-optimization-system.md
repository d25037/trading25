# パラメータ最適化システム

## Source of Truth

実行時仕様はstrategy YAMLトップレベルの`optimization` blockです。

```yaml
optimization:
  description: "optional description"
  parameter_ranges:
    entry_filter_params:
      sma_cross:
        short_period: [5, 10, 20]
        long_period: [50, 100, 200]
```

`ParameterOptimizationEngine(strategy_name, verbose=False)`は`ConfigLoader`で
strategy YAMLを一度読み、`analyze_saved_strategy_optimization()`と
`validate_parameter_ranges()`で検証します。診断上の仕様位置は
`<strategy path>#optimization`です。

## 実行フロー

1. routeまたはCLIがstrategy nameをworkerへ渡す
2. engineがstrategy YAMLとdefault optimization settingsを読む
3. parameter combinationsを生成する
4. OHLCVとbenchmarkを一度prefetchしてworker間で再利用する
5. vectorbt fast pathで候補をscoreする
6. ranked `fast_candidates`、metrics JSON、HTML reportを成果物として保存する

routeとCLIはparameter rangeやscore計算を再実装しません。

## 境界

- Strategy仕様CRUD/draft:
  `src/application/services/strategy_optimization_service.py`
- 実行service:
  `src/application/services/optimization_service.py`
- Worker:
  `src/application/workers/optimization_worker.py`
- Domain engine:
  `src/domains/optimization`
- HTTP:
  `src/entrypoints/http/routes/optimize.py`,
  `src/entrypoints/http/routes/strategies.py`
- CLI:
  `src/entrypoints/cli/optimize.py`

## 移行

過去形式の入力は`bt migrate-optimization-specs`だけが読み、strategy YAMLの
`optimization` blockへone-shot変換します。通常実行、API、Web editorは移行元を
探索せず、互換引数も持ちません。

## Verification

```bash
uv run pytest \
  tests/unit/optimization \
  tests/unit/server/services/test_optimization_service.py \
  tests/unit/server/test_optimization_worker.py \
  tests/unit/server/routes/test_optimize.py \
  tests/unit/cli/test_optimize_command.py \
  tests/unit/strategies/utils/test_optimization.py

uv run ruff check \
  src/domains/optimization \
  src/application/services/optimization_service.py \
  src/application/workers/optimization_worker.py \
  src/entrypoints/http/routes/optimize.py
```
