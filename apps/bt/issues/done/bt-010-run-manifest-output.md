---
id: bt-010
title: "Backtest run manifest output"
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

# bt-010 Backtest run manifest output

## 目的
Emit a machine-readable manifest with config, dataset, timestamps, and versions for each run.

## 受け入れ条件
- Each run outputs a JSON (or YAML) manifest alongside HTML.
- Manifest includes strategy, dataset, shared_config, and git commit hash (if available).

## 実施内容
- src/backtest/runner.py or src/backtest/marimo_executor.py
- docs update

## 結果
（完了済み）

## 補足
