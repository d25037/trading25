---
name: bt-optimization
description: bt の最適化エンジン（グリッド探索・スコアリング・可視化）を扱うスキル。`src/optimization` や `/api/optimize/*` 変更時に使う。
---

# bt-optimization

## Scope

- `apps/bt/src/optimization/**`
- `apps/bt/src/server/routes/optimize.py`
- `apps/bt/config/optimization/**`

## Focus

1. パラメータ空間生成の妥当性。
2. スコア指標の定義と重みの説明可能性。
3. 並列実行時の安定性と再現性。
4. 生成成果物（Notebook/HTML）の命名・保存規約。
