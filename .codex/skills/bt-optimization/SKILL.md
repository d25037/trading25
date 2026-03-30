---
name: bt-optimization
description: bt の最適化エンジン（グリッド探索・スコアリング・可視化）を扱うスキル。optimization domain、worker、`/api/optimize/*` と strategy-scoped optimization API を変更するときに使用する。
---

# bt-optimization

## When to use

- パラメータ空間生成、スコアリング、verification 付き optimize job、HTML 成果物を変更するとき。

## Source of Truth

- `apps/bt/src/domains/optimization`
- `apps/bt/src/application/services/optimization_service.py`
- `apps/bt/src/application/services/strategy_optimization_service.py`
- `apps/bt/src/application/workers/optimization_worker.py`
- `apps/bt/src/entrypoints/http/routes/optimize.py`
- `apps/bt/src/entrypoints/http/routes/strategies.py`
- `apps/bt/src/entrypoints/cli/optimize.py`

## Workflow

1. route -> service -> worker -> domain の順で責務を確認する。strategy-scoped optimization spec の CRUD/draft は `routes/strategies.py`、実行ジョブは `routes/optimize.py` を見る。
2. パラメータ空間、strategy YAML 内の `optimization` block、スコア指標、verification stage の整合を確認する。
3. 並列実行や timeout 変更時は worker の terminal state を確認する。
4. legacy `*_grid.yaml` は migration 専用経路以外から読まない。新規仕様は strategy YAML トップレベル `optimization` を SoT にする。
5. 成果物（Notebook/HTML、metrics JSON）の保存規約を崩さない。

## Guardrails

- `optimization` job type と backtest family の run contract を壊さない。
- `engine_policy` と verification payload の互換を維持する。
- score 計算を route や CLI に重複実装しない。

## Verification

- `uv run --project apps/bt pytest tests/unit/optimization tests/unit/server/services/test_optimization_service.py tests/unit/server/test_optimization_worker.py tests/unit/server/routes/test_optimize.py`
- `uv run --project apps/bt pytest tests/unit/cli/test_optimize_command.py tests/unit/strategies/utils/test_optimization.py`
- `uv run --project apps/bt ruff check src/domains/optimization src/application/services/optimization_service.py src/application/workers/optimization_worker.py src/entrypoints/http/routes/optimize.py`
