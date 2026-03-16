---
name: ts-dataset-management
description: ts 側の dataset 運用（作成・参照・検証）を FastAPI 契約に合わせて扱うスキル。`/api/dataset*` を使う web 実装、preset 変更、パス安全性レビューで使用する。
---

# ts-dataset-management

## When to use

- Dataset create/status/info/validation を ts/web から扱う実装を変更するとき。
- preset、error handling、dataset path safety を見直すとき。

## Source of Truth

- `apps/ts/packages/contracts/src/types/api-response-types.ts`
- `apps/ts/packages/contracts/openapi/bt-openapi.json`
- `apps/ts/packages/web/src/hooks/useDataset.ts`
- `apps/ts/packages/web/src/types/dataset.ts`

## Workflow

1. `bt-openapi.json` で dataset endpoint と schema を確認する。
2. `DATASET_PRESETS` と web 側の preset 表示が一致しているか確認する。
3. path safety と XDG 配下制約を UI と request payload で崩していないか確認する。
4. 契約変更後は `bun run --filter @trading25/contracts bt:sync` を実行して型を同期する。

## Guardrails

- Dataset 操作は FastAPI (`:3002`) 経由を前提にする。
- 任意パス入力は `datasets/` 配下に制限し、絶対パスや `..` を許可しない。
- `ts/web` が `@trading25/contracts` の shared 型を参照していること。
- エラーレスポンスが統一フォーマットと整合していること。

## Verification

- `bun run --filter @trading25/contracts bt:check`
- `bun run quality:typecheck`
- `bun run workspace:test`
