---
name: ts-financial-analysis
description: bt FastAPI 分析APIを ts/web と ts/cli から利用する統合スキル。UI表示、CLI出力、型同期、API呼び出し整合の実装/修正/レビュー時に使用する。
---

# ts-financial-analysis

`apps/bt` が financial-analysis の Single Source of Truth。  
このスキルは ts 側の **consumer/integration** のみを扱う。

## Source of Truth

- OpenAPI snapshot: `apps/ts/packages/shared/openapi/bt-openapi.json`
- Web hooks: `apps/ts/packages/web/src/hooks/useRanking.ts`, `useScreening.ts`, `useFundamentals.ts`, `useFactorRegression.ts`, `usePortfolioFactorRegression.ts`
- CLI commands: `apps/ts/packages/cli/src/commands/analysis/**`
- Shared types: `@trading25/shared/types/api-types`, `@trading25/shared/types/api-response-types`

## Workflow

1. `bt-openapi.json` で対象エンドポイントの path/query/response を確認する。
2. ts 側の呼び出し実装（web hooks / cli api-client）が契約通りか確認する。
3. 契約変更時は `bun run --filter @trading25/shared bt:sync` を実行して型同期する。
4. Web/CLI の表示・整形・エラーハンドリングの破綻を確認する。

## Guardrails

- ts 側で financial-analysis の計算ロジックを再実装しない。
- Archived Hono analytics サービス層は bt プロキシ方針を維持する。
- ts 側は「API呼び出し・型整合・表示整形」に責務を限定する。

## Notes

- bt 側の分析ロジック実装変更は `bt-financial-analysis` スキルを使用する。
