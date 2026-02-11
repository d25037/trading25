---
name: bt-agent-system
description: bt の戦略自動生成・進化最適化（agent/lab 系）を扱うスキル。`src/agent` と `/api/lab/*`、`bt lab *` コマンドを実装/レビューするときに使う。
---

# bt-agent-system

## Scope

- `apps/bt/src/agent/**`
- `apps/bt/src/server/routes/lab.py`
- `apps/bt/src/cli_bt/lab.py`

## Review Focus

1. 生成候補の妥当性（制約、シグナル整合、再現性）。
2. 評価メトリクスとランキングの一貫性。
3. 長時間ジョブの進捗・キャンセル・エラー処理。

## Guardrails

- API 契約は OpenAPI を正とする。
- `uv run bt server --port 3002` 前提を崩さない。
