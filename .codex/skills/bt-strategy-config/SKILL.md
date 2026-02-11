---
name: bt-strategy-config
description: bt の戦略 YAML 設定とローダーを扱うスキル。`config/strategies` と `strategy_config` の変更、戦略追加、検証ロジック更新時に使用する。
---

# bt-strategy-config

## Scope

- `apps/bt/config/strategies/**`
- `apps/bt/src/strategy_config/loader.py`
- `apps/bt/src/strategies/core/yaml_configurable_strategy.py`

## Rules

- 3層構造（`experimental/`, `production/`, `legacy/`, `reference/`）を維持する。
- カテゴリ省略時の探索順を壊さない。
- YAML 主導設計を維持し、戦略固有ロジックのハードコードを避ける。
