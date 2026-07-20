---
name: ts-api-endpoints
description: Use when FastAPI (:3002) 契約に沿った ts/web・shared api-clients の route追加、型不整合、endpoint調査、または通信バグ修正を行うとき。
---

# ts-api-endpoints

## When to use

- ts/web の `/api` 呼び出し、shared api-clients、OpenAPI generated types を調整するとき。
- endpoint lookup や request/response 契約の確認が必要なとき。

## Source of Truth

- `apps/ts/packages/contracts/openapi/bt-openapi.json`
- `references/openapi-paths.md`
- `apps/ts/packages/contracts/scripts/fetch-bt-openapi.ts`
- `apps/ts/packages/web/src/lib`
- `apps/ts/packages/api-clients/src`

## Workflow

1. `references/openapi-paths.md` で対象 path/method を確認する。path の掲載は runtime support を意味しないため、deprecated response（例: 410）も OpenAPI で確認する。
2. `bt-openapi.json` で request/response schema と status code を確認する。
3. web 側の `/api/*` 呼び出しと shared api-clients のどちらを触るべきか切り分ける。
4. contracts 型（`@trading25/contracts/types/api-types`, `api-response-types`）の整合を取る。
5. 契約更新時は `bun run --filter @trading25/contracts bt:sync` を実行して型同期する。

## Guardrails

- `apps/bt` が唯一のバックエンド。
- ts 側でバックエンド実装を新設しない。
- 接続先ポートは `3002` を前提にする。
- API ドキュメント UI は `/doc` 前提で扱う。
- 旧 `GET /api/analytics/screening` は 410。screening integration は `POST /api/analytics/screening/jobs`、job status/cancel、`GET /api/analytics/screening/result/{job_id}` を使い、deprecated query を再導入しない。
- Market-backed response は backend の schema v5 / `provider_adjusted_v1` 契約を前提とする。provider vintage と current-basis state は FastAPI response を SoT とし、ts 側で旧DB compatibility、legacy basis、current/latest fallback、standalone adjusted-metrics recovery を実装しない。

## Verification

```bash
python3 scripts/skills/refresh_skill_references.py --check
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
bun --cwd="$PWD/apps/ts" run quality:typecheck
```
