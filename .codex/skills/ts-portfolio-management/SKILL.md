---
name: ts-portfolio-management
description: ポートフォリオ CRUD と保有銘柄管理を現行 API 契約に合わせて扱うスキル。`/api/portfolio*` 利用実装、CLI操作、web表示、型整合レビューで使用する。
---

# ts-portfolio-management

## Source of Truth

- OpenAPI snapshot: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Web hooks: `apps/ts/packages/web/src/hooks/usePortfolio.ts`, `usePortfolioPerformance.ts`
- Web contract types: `apps/ts/packages/web/src/types/portfolio.ts`
- Shared response types: `apps/ts/packages/contracts/src/types/api-response-types.ts`

## API Surface

- `/api/portfolio`
- `/api/portfolio/{id}`
- `/api/portfolio/{id}/items`
- `/api/portfolio/{id}/items/{itemId}`
- `/api/portfolio/{portfolioName}/stocks/{code}`
- `/api/portfolio/{portfolioName}/codes`
- `/api/portfolio/{id}/performance`

## Rules

- `portfolio.db` は FastAPI 管理。
- id ベース API と name+code ベース API の使い分けを明確にする。
- shared の response 型を再利用し、重複型定義を増やさない。
- performance と factor regression の入力パラメータを型で拘束する。

## Review Checklist

1. 追加/更新/削除時の整合性（重複銘柄、負数、null）。
2. エラーレスポンス統一フォーマット準拠。
3. web の API クライアント実装と OpenAPI 契約に差分がないこと。
