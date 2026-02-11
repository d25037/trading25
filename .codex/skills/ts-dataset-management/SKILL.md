---
name: ts-dataset-management
description: ts 側の dataset 運用（作成・参照・検証）を FastAPI 契約に合わせて扱うスキル。`/api/dataset*` を使う web/cli 実装、preset 変更、パス安全性レビューで使用する。
---

# ts-dataset-management

## Source of Truth

- Dataset preset SSOT: `apps/ts/packages/shared/src/dataset/config/presets/metadata.ts`
- Dataset CLI group: `apps/ts/packages/cli/src/commands/dataset/index.ts`
- Dataset API hooks: `apps/ts/packages/web/src/hooks/useDataset.ts`
- OpenAPI snapshot: `apps/ts/packages/shared/openapi/bt-openapi.json`

## Presets (Current)

- `fullMarket`
- `primeMarket`
- `standardMarket`
- `growthMarket`
- `quickTesting`
- `topix100`
- `topix500`
- `mid400`
- `primeExTopix500`

## Operational Rules

- Dataset 操作は FastAPI (`:3002`) 経由を前提にする。
- パスは XDG 配下 (`~/.local/share/trading25/datasets/`) を前提にする。
- 任意パス入力は `datasets/` 配下に制限し、絶対パスや `..` を許可しない。
- API 契約変更後は `bun run --filter @trading25/shared bt:sync` を実行して型を同期する。

## Key Checks

1. `/api/dataset`, `/api/dataset/jobs/{jobId}`, `/api/dataset/{name}/*` の契約整合。
2. `ts/web` と `ts/cli` が同じ shared 型を参照していること。
3. preset 名が `DATASET_PRESET_NAMES` と一致していること。
4. エラーレスポンスが統一フォーマットと整合していること。
