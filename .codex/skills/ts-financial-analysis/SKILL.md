---
name: ts-financial-analysis
description: Use when bt FastAPI analytics を ts/web・shared api-clients から利用する UI、型同期、API呼び出し、またはresponse mappingを変更するとき。
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
   Fundamentals GET の `from` / `to` / `periodType` / `preferConsolidated` / `tradingValuePeriod` / `forecastEpsLookbackFyCount` は GET request query はすべて optional。指定された値だけを contract 名のまま forward する。POST request body は `symbol` のみ required で、対応する snake_case options は optional。
   response の `asOfDate` は required。optional request field と required response field を混同しない。
2. web hook と shared analytics client のどちらに責務があるかを切り分ける。
3. 契約変更時は `bun run --filter @trading25/contracts bt:sync` を実行して型同期する。
4. Screening は async jobs/result API、Ranking は current OpenAPI response を使っていることを確認する。旧 screening query は 410 のため呼び出さない。
5. Screening / Ranking UI の表示、整形、typed recovery エラーハンドリングの破綻を確認する。

## Guardrails

- `apps/bt` が financial-analysis の Single Source of Truth。
- ts 側で financial-analysis の計算ロジックを再実装しない。
- `asOfDate`（effective market date）、`provenance.reference_date`（knowledge cutoff）、`fundamentalsAdjustmentBasisDate`（fundamentals adjustment basis frontier）の意味をfrontendで再解釈・代替しない。
- Market v5 / `provider_adjusted_v1` の exact provider-window/current-basis response を前提とし、欠損を current/latest 値、frontend-local adjustment、deterministic retryで補完しない。pre-v5/incompatible response は typed recovery error として表示する。
- Ranking の indices view は backend `indexPerformance` をそのまま表示し、`lookbackDays` / `date` の解釈や騰落計算を frontend で再実装しない。
- 旧 analytics service compatibility layer を再導入しない。必要なら `@trading25/api-clients/analytics` と web hook の責務分担で直す。
- 旧 `GET /api/analytics/screening` query を復活させない。`POST /api/analytics/screening/jobs`、job status/cancel、`GET /api/analytics/screening/result/{job_id}` が唯一の実行経路。
- ts 側は API 呼び出し、型整合、表示整形に責務を限定する。

## Verification

```bash
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
bun --cwd="$PWD/apps/ts" run quality:typecheck
bun --cwd="$PWD/apps/ts" run workspace:test
```
