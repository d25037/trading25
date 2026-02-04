---
id: ts-002
title: "API レスポンス型の型回避 (`as never`) 排除"
status: closed
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-02-01
depends_on: []
blocks: []
parent: null
---

# ts-002 API レスポンス型の型回避 (`as never`) 排除

## 目的
OpenAPI の型安全性を回復し、`as never` による型回避を削除する。

## 受け入れ条件
- `as never` を使用しているルートがゼロ
- `handleRouteError` などの共通関数で型整合が保証される

## 実施内容
- `handleRouteError` をジェネリクス化し `ErrorResponseResult<Code>` を返すように変更
- `handlePortfolioError`、`handleWatchlistError` も同パターンでジェネリクス化
- 各ルートハンドラ（13ファイル、26箇所）から `as never` を削除し、`allowedStatusCodes` を渡すように修正
- `withErrorHandling` の戻り値型を `RouteHandler<T | ErrorResponseResult>` に更新
- `as ErrorResponseResult<Code>` キャストはヘルパー関数内の return 箇所に集約（各1-2箇所）

## 結果
- `as never` 使用箇所: 26 → 0
- typecheck, test, lint すべてパス
- `as` キャストは共通ヘルパー関数内に限定（route-handler.ts, portfolio-helpers.ts, watchlist-helpers.ts）

## 補足
`resolveAllowedStatus` を活用し、OpenAPI 宣言と一致しないステータスコードが返されそうな場合は自動的に許可されたコードにフォールバックする。
