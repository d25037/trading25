---
id: bt-011
title: "Metrics JSON output (avoid HTML scraping)"
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

# bt-011 Metrics JSON output (avoid HTML scraping)

## 目的
Export backtest metrics as JSON to avoid brittle HTML regex extraction.

## 受け入れ条件
- JSON metrics are emitted during Marimo runs.
- Server uses JSON metrics when available; HTML scraping is fallback only.

## 実施内容
- notebooks/templates/marimo/strategy_analysis.py
- src/data/metrics_extractor.py (prefer JSON)
- src/server/services/backtest_service.py (use JSON if present)

## 結果
（完了済み）

## 補足
