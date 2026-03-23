---
name: ts-financial-analysis
description: bt FastAPI 分析APIを ts/web と shared api-clients から利用する統合スキル。UI表示、型同期、API呼び出し整合の実装・修正・レビュー時に使用する。
---

# ts-financial-analysis

## When to use

- Screening / Ranking 画面や charts 内 fundamentals / factor regression 系 UI を変更するとき。
- backend の分析 API 契約変更に追従して web 側の integration を直すとき。

## Source of Truth

- `apps/ts/packages/contracts/openapi/bt-openapi.json`
- `apps/ts/packages/web/src/hooks`
- `apps/ts/packages/web/src/lib/analytics-client.ts`
- `apps/ts/packages/api-clients/src/analytics`

## Workflow

1. `bt-openapi.json` で対象エンドポイントの path、query、response を確認する。
2. web hook と shared analytics client のどちらに責務があるかを切り分ける。
3. 契約変更時は `bun run --filter @trading25/contracts bt:sync` を実行して型同期する。
4. Screening / Ranking UI の表示、整形、エラーハンドリングの破綻を確認する。

## Guardrails

- `apps/bt` が financial-analysis の Single Source of Truth。
- ts 側で financial-analysis の計算ロジックを再実装しない。
- Ranking の indices view は backend `indexPerformance` をそのまま表示し、`lookbackDays` / `date` の解釈や騰落計算を frontend で再実装しない。
- Archived legacy analytics サービス層は bt プロキシ方針を維持する。
- ts 側は API 呼び出し、型整合、表示整形に責務を限定する。

## Verification

- `bun run --filter @trading25/contracts bt:check`
- `bun run quality:typecheck`
- `bun run workspace:test`
