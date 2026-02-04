---
id: bt-014
title: "Max concurrent / exposure limits"
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

# bt-014 Max concurrent / exposure limits

## 目的
Add max concurrent positions and max exposure controls to prevent over-allocation.

## 受け入れ条件
- Exceeding limits reduces new entries or scales sizes.
- Behavior is documented and tested.

## 実施内容
- src/models/config.py
- src/strategies/core/mixins/backtest_executor_mixin.py
- tests/

## 結果
（完了済み）

## 補足
