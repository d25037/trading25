---
name: ts-api-endpoints
description: FastAPI (:3002) 契約に沿って ts/web と ts/cli の API 呼び出しを実装・レビューするスキル。ルート追加、型不整合、エンドポイント調査、通信バグ修正時に使用する。
---

# ts-api-endpoints

`apps/bt` が唯一のバックエンド。ts 側は `/api` を FastAPI (`:3002`) に向けるクライアント実装のみを扱う。

## Source of Truth

- OpenAPI snapshot: `apps/ts/packages/shared/openapi/bt-openapi.json`
- Generated index: `references/openapi-paths.md`
- Type generation scripts: `apps/ts/packages/shared/scripts/fetch-bt-openapi.ts`
- Web proxy config: `apps/ts/packages/web/vite.config.ts`
- CLI base URL and request handling: `apps/ts/packages/cli/src/utils/api-client.ts`

## Workflow

1. `references/openapi-paths.md` で対象 path/method を確認する。
2. `bt-openapi.json` で request/response schema を確認する。
3. ts 側呼び出し先 (`/api/*`) と shared 型 (`@trading25/shared/types/api-types`, `api-response-types`) の整合を取る。
4. 契約更新時は `bun run --filter @trading25/shared bt:sync` を実行して型同期する。
5. 生成型の健全性は `bun run --filter @trading25/shared bt:check` で確認する。

## Guardrails

- ts 側でバックエンド実装を新設しない。
- 接続先ポートは `3002` を前提にする。
- API ドキュメント UI は `/doc` 前提で扱う。
- market filter は legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う。
