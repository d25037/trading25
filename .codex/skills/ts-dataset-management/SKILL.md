---
name: ts-dataset-management
description: Use when ts/web の `/api/dataset*` integration、dataset作成・参照・検証、preset変更、またはpath safetyをレビューするとき。
---

# ts-dataset-management

## When to use

- Dataset create/status/info/validation を ts/web から扱う実装を変更するとき。
- preset、error handling、dataset path safety を見直すとき。

## Source of Truth

- `apps/ts/packages/contracts/src/types/api-response-types.ts`
- `apps/ts/packages/contracts/openapi/bt-openapi.json`
- `apps/ts/packages/web/src/hooks/useDataset.ts`

## Workflow

1. `bt-openapi.json` で dataset endpoint と schema を確認する。
2. `DATASET_PRESETS` と web 側の preset 表示が一致しているか確認する。
3. `dataset.duckdb + manifest.v2.json` bundle 前提、`overwrite=true` 再試行、job cancel の即時返却に UI が合っているか確認する。
4. path safety と XDG 配下制約を UI と request payload で崩していないか確認する。
5. 契約変更後は `bun run --filter @trading25/contracts bt:sync` を実行して型を同期する。

## Guardrails

- Dataset 操作は FastAPI (`:3002`) 経由を前提にする。
- 任意パス入力、旧 `dataset.db`、root-level DB artifact、`timeoutMinutes` request、resume route を再導入しない。
- snapshot 名は backend resolver に任せ、UI で絶対パスや `..` を受け付けない。
- `ts/web` が `@trading25/contracts` の shared 型を参照していること。
- エラーレスポンスが統一フォーマットと整合していること。

## Verification

```bash
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
bun --cwd="$PWD/apps/ts" run quality:typecheck
bun --cwd="$PWD/apps/ts" run workspace:test
```
