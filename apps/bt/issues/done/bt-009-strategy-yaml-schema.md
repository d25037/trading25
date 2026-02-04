---
id: bt-009
title: "Strategy YAML schema validation"
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

# bt-009 Strategy YAML schema validation

## 目的
Add pydantic schemas for strategy YAML to validate shared_config and signal params together.

## 受け入れ条件
- Invalid YAML yields clear validation errors.
- Schema covers shared_config, entry_filter_params, exit_trigger_params, execution.

## 実施内容
- src/strategy_config/models.py (new)
- src/strategy_config/loader.py (validation hook)
- tests/strategy_config/

## 結果
（完了済み）

## 補足
