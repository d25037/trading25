---
id: bt-004
title: "Optimization timeout enforcement"
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

# bt-004 Optimization timeout enforcement

## 目的
Ensure per-combination timeout is actually enforced during optimization.

`as_completed()` with `future.result(timeout=...)` does not enforce per-task timeouts as intended.

## 受け入れ条件
- A long-running evaluation is terminated after OPTIMIZATION_TIMEOUT_SECONDS.
- Unit tests verify timeout behavior.

## 実施内容
- src/optimization/engine.py
- tests/unit/optimization/test_timeout.py

## 結果
（完了済み）

## 補足
