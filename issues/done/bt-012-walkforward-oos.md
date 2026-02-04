---
id: bt-012
title: "Walk-forward / OOS evaluation"
status: done
priority: medium
labels: []
project: bt
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# bt-012 Walk-forward / OOS evaluation

## 目的
Add walk-forward or out-of-sample evaluation to reduce overfitting risk.

## 受け入れ条件
- Configurable train/test splits or rolling windows.
- Metrics aggregated across folds and exposed in output.

## 実施内容
- src/strategies/core/mixins/
- src/backtest/runner.py
- tests/

## 結果
（完了済み）

## 補足
