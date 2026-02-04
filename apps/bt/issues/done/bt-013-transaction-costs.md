---
id: bt-013
title: "Transaction cost model expansion"
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

# bt-013 Transaction cost model expansion

## 目的
Add slippage/spread/borrow fee options to SharedConfig and apply them in backtests.

## 受け入れ条件
- New cost parameters appear in SharedConfig.
- Portfolio construction uses these costs consistently.

## 実施内容
- src/models/config.py
- src/strategies/core/mixins/backtest_executor_mixin.py
- docs update

## 結果
（完了済み）

## 補足
