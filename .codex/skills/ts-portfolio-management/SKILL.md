---
name: ts-portfolio-management
description: ポートフォリオ CRUD と保有銘柄管理を現行 API 契約に合わせて扱うスキル。`/api/portfolio*` を使う web 実装、表示、型整合レビューで使用する。
---

# ts-portfolio-management

## When to use

- Portfolio / Watchlist UI の CRUD、保有銘柄管理、watchlist ranking 連携を変更するとき。

## Source of Truth

- `apps/ts/packages/contracts/src/types/api-response-types.ts`
- `apps/ts/packages/contracts/openapi/bt-openapi.json`
- `apps/ts/packages/web/src/hooks/useWatchlist.ts`
- `apps/ts/packages/web/src/pages/WatchlistPage.tsx`
- `apps/ts/packages/web/src/components/Watchlist/WatchlistDetail.tsx`

## Workflow

1. `bt-openapi.json` で portfolio / watchlist endpoint の request/response を確認する。
2. id ベース API と code/companyName ベース API の使い分けを hook 単位で確認する。
3. CRUD、銘柄追加/削除、memo 更新、Ranking 連携の表示整形と型拘束を合わせる。
4. shared 型で足りる場合は web ローカル型を増やさない。

## Guardrails

- `portfolio.db` は FastAPI 管理。
- 追加、更新、削除時の整合性（重複銘柄、負数、null）を崩さない。
- エラーレスポンス統一フォーマット準拠を維持する。
- watchlist item 追加時は 4 桁 code と `companyName` payload を維持する。

## Verification

- `bun run --filter @trading25/contracts bt:check`
- `bun run quality:typecheck`
- `bun run workspace:test`
