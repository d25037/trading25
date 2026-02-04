---
id: bt-015
title: "Regression tests for key behaviors"
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

# bt-015 Regression tests for key behaviors

## 目的
Add tests for relative mode handling, deep merge, timeout enforcement, and backtest execution config.

## 受け入れ条件
- Tests cover the specified behaviors and pass.

## 実施内容
- tests/unit/strategies/test_signal_processor.py
- tests/unit/strategy_config/test_loader.py
- tests/unit/optimization/test_timeout.py
- tests/unit/backtest/test_backtest_runner.py
- tests/unit/server/services/test_backtest_service.py

## 結果
（完了済み）

## 補足
