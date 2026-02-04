---
id: bt-007
title: "BacktestRunner ignores execution config"
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

# bt-007 BacktestRunner ignores execution config

## 目的
BacktestRunner should respect strategy execution settings (template path and output directory).

## 受け入れ条件
- Runner uses ConfigLoader.get_template_notebook_path/get_output_directory.
- Unit test verifies custom template/output usage.

## 実施内容
- src/backtest/runner.py
- tests/unit/backtest/test_backtest_runner.py

## 結果
（完了済み）

## 補足
