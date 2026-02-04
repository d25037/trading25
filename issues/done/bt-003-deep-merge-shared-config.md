---
id: bt-003
title: "Deep merge shared_config"
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

# bt-003 Deep merge shared_config

## 目的
Implement deep merge for shared_config so nested settings (e.g., parameter_optimization.scoring_weights) are not lost.

## 受け入れ条件
- Nested dict overrides preserve unspecified defaults.
- Unit tests cover nested override behavior.

## 実施内容
- src/strategy_config/parameter_extractor.py
- tests/unit/strategy_config/test_loader.py

## 結果
（完了済み）

## 補足
