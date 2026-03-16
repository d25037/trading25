---
name: bt-agent-system
description: bt の戦略自動生成・進化最適化（agent/lab 系）を扱うスキル。lab domain、worker、`/api/lab/*`、`bt lab *` を変更するときに使用する。
---

# bt-agent-system

## When to use

- Lab generate/evolve/optimize/improve の実装を変更するとき。
- `target_scope`、async job、verification stage、worker orchestration を見直すとき。

## Source of Truth

- `apps/bt/src/domains/lab_agent`
- `apps/bt/src/application/services/lab_service.py`
- `apps/bt/src/application/workers/lab_worker.py`
- `apps/bt/src/entrypoints/http/routes/lab.py`
- `apps/bt/src/entrypoints/cli/lab.py`

## Workflow

1. route -> service -> worker -> domain の順で責務境界を確認する。
2. `target_scope`、`entry_filter_only`、`engine_policy` の整合を崩さない。
3. 候補生成ロジックは `domains/lab_agent` に寄せ、route に計算を載せない。
4. 長時間ジョブの progress、cancel、terminal state を worker 経由で確認する。

## Guardrails

- API 契約は OpenAPI を正とする。
- `uv run bt server --port 3002` 前提を崩さない。
- `bt lab generate/improve` の `entry_filter_only` と `allowed_category` 互換を維持する。

## Verification

- `uv run --project apps/bt pytest tests/unit/agent tests/unit/server/test_lab_worker.py tests/unit/server/services/test_lab_service_worker.py`
- `uv run --project apps/bt pytest tests/unit/cli_bt/test_lab_cli.py`
- `uv run --project apps/bt ruff check src/domains/lab_agent src/application/services/lab_service.py src/application/workers/lab_worker.py`
