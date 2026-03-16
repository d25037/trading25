---
name: ts-api-endpoints
description: FastAPI (:3002) 契約に沿って ts/web と shared api-clients の API 呼び出しを実装・レビューするスキル。ルート追加、型不整合、エンドポイント調査、通信バグ修正時に使用する。
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

1. `references/openapi-paths.md` で対象 path/method を確認する。
2. `bt-openapi.json` で request/response schema を確認する。
3. web 側の `/api/*` 呼び出しと shared api-clients のどちらを触るべきか切り分ける。
4. contracts 型（`@trading25/contracts/types/api-types`, `api-response-types`）の整合を取る。
5. 契約更新時は `bun run --filter @trading25/contracts bt:sync` を実行して型同期する。

## Guardrails

- `apps/bt` が唯一のバックエンド。
- ts 側でバックエンド実装を新設しない。
- 接続先ポートは `3002` を前提にする。
- API ドキュメント UI は `/doc` 前提で扱う。

## Verification

- `python3 scripts/skills/refresh_skill_references.py --check`
- `bun run --filter @trading25/contracts bt:check`
- `bun run quality:typecheck`
