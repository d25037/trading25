---
id: bt-006
title: "Relative mode signal skipping bug"
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

# bt-006 Relative mode signal skipping bug

## 目的
Fix relative mode detection so β値/売買代金シグナル are only skipped in actual relative mode.

## 受け入れ条件
- Normal mode applies execution-price-required signals.
- Relative mode skips them when execution prices are unavailable.

## 実施内容
- src/strategies/signals/processor.py
- src/strategies/core/yaml_configurable_strategy.py
- tests/unit/strategies/test_signal_processor.py

## 結果
（完了済み）

## 補足
