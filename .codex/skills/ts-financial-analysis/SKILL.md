---
name: ts-financial-analysis
description: ts 側の分析機能（ROE, ranking, screening, factor regression, portfolio factor regression）を現行 API 契約に合わせて扱うスキル。分析表示、CLI出力、型整合の実装/修正/レビュー時に使用する。
---

# ts-financial-analysis

## Source of Truth

- OpenAPI snapshot: `apps/ts/packages/shared/openapi/bt-openapi.json`
- Web hooks: `apps/ts/packages/web/src/hooks/useRanking.ts`, `useFundamentals.ts`, `useFactorRegression.ts`, `usePortfolioFactorRegression.ts`
- CLI commands: `apps/ts/packages/cli/src/commands/analysis/**`
- Shared types: `@trading25/shared/types/api-types`, `@trading25/shared/types/api-response-types`

## Checks

1. `/api/analytics/*` の path/query と呼び出し実装が一致していること。
2. 欠損値、ゼロ除算、時系列欠落に対する表示/集計が破綻しないこと。
3. web と cli が同じ shared 型を使っていること。
4. 並び順やランキング項目が API レスポンス仕様と一致していること。

## Notes

- 計算ロジックが bt API 側にある機能は ts で再実装しない。
- 契約更新時は `bt:sync` 後に分析系型とテストを確認する。
