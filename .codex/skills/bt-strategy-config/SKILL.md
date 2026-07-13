---
name: bt-strategy-config
description: bt の戦略 YAML 設定とローダーを扱うスキル。`config/strategies`、runtime loader、strict validation を変更するときに使用する。
---

# bt-strategy-config

## When to use

- 戦略 YAML、loader、validator、category 運用ルールを変更するとき。
- strategy rename/move/delete や production/experimental 境界を見直すとき。

## Source of Truth

- `apps/bt/config/strategies`
- `apps/bt/src/domains/strategy/runtime`
- `apps/bt/src/domains/strategy/core/yaml_configurable_strategy.py`
- `apps/bt/src/entrypoints/http/routes/strategies.py`
- `contracts/strategy-config-v3.schema.json`

## Workflow

1. YAML schema、runtime loader、strategy route の順で影響範囲を確認する。
2. category、探索順、rename/delete 権限の互換を確認する。
3. validation 変更時は backend strict validation、OpenAPI/schema、web 表示の整合を確認する。

## Guardrails

- 3層構造（`experimental/`, `production/`, `legacy/`, `reference/`）を維持する。
- カテゴリ省略時の探索順を壊さない。
- `shared_config.dataset` は unsupported。market run は `shared_config.data_source: market` + `universe_preset`、archived reproducibility は `data_source: dataset_snapshot` + `dataset_snapshot` + `static_universe: true` を使う。
- frontend-local validation を再導入しない。web は backend validation result と metadata-driven guidance を表示する。
- YAML 主導設計を維持し、戦略固有ロジックのハードコードを避ける。

## Verification

- `uv run --directory apps/bt pytest tests/unit/strategy_config tests/unit/server/routes/test_strategies.py`
- `uv run --directory apps/bt ruff check src/domains/strategy/runtime src/domains/strategy/core/yaml_configurable_strategy.py src/entrypoints/http/routes/strategies.py`
