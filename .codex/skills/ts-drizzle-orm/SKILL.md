---
name: ts-drizzle-orm
description: ts/shared の Drizzle ORM スキーマと SQLite 境界を扱うスキル。DB テーブル定義変更、クエリ実装、データ型整合性レビュー、shared エクスポート更新時に使用する。
---

# ts-drizzle-orm

## Source of Truth

- DB modules: `apps/ts/packages/shared/src/db/**`
- Portfolio modules: `apps/ts/packages/shared/src/portfolio/**`
- Dataset modules: `apps/ts/packages/shared/src/dataset/**`
- Export surface: `apps/ts/packages/shared/src/index.ts`

## Rules

- DB の単一事実源は `packages/shared`。
- 銘柄コード正規化（5桁 -> 4桁）を壊さない。
- `market.db` / `portfolio.db` / `datasets` の責務分離を維持する。
- market filter は legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を意識して変更する。

## Review Focus

1. 変更が additive か breaking かを明示する。
2. `src/index.ts` の公開 API と実装を一致させる。
3. shared 型 (`api-types`, `api-response-types`) と DB 取得結果の null/optional 整合を確認する。
