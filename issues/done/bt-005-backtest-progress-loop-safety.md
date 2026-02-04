---
id: bt-005
title: "Backtest progress callback loop safety"
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

# bt-005 Backtest progress callback loop safety

## 目的
Make progress notifications thread-safe by using the main event loop instead of `get_event_loop()` inside worker threads.

## 受け入れ条件
- Progress updates use `asyncio.run_coroutine_threadsafe` with the running loop.
- Unit tests verify loop usage.

## 実施内容
- src/server/services/backtest_service.py
- tests/unit/server/services/test_backtest_service.py

## 結果
（完了済み）

## 補足
