---
id: bt-002
title: "SharedConfig defaults mismatch"
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

# bt-002 SharedConfig defaults mismatch

## 目的
Align SharedConfig defaults with config/default.yaml to avoid divergent behavior between YAML-driven runs and direct model usage.

Current defaults in `src/models/config.py` differ from `config/default.yaml`, causing inconsistent backtest behavior.

## 受け入れ条件
- SharedConfig default values match config/default.yaml defaults for shared_config.
- Tests updated to assert the unified defaults.

## 実施内容
- src/models/config.py
- config/default.yaml (if needed)
- tests/unit/strategy_config/test_loader.py (update expectations if required)

## 結果
（完了済み）

## 補足
