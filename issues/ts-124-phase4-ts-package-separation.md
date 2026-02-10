---
id: ts-124
title: "Phase 4: TS パッケージ責務分離"
status: in-progress
priority: medium
labels: [architecture, refactor]
project: ts
created: 2026-02-09
updated: 2026-02-10
depends_on: []
blocks: []
parent: null
---

# ts-124 Phase 4: TS パッケージ責務分離（4B は削除中心）

## 目的
`apps/ts/packages/shared` に集約された責務を整理し、`web`/`cli` が用途別境界を直接参照する構成へ移行する。FastAPI 一本化後の重複ドメイン実装は新設移管ではなく削除を優先する。

## 受け入れ条件
- `clients-ts`, `market-db-ts`, `dataset-db-ts`, `portfolio-db-ts` の境界が作成される
- 4D Step1 で `market-db-ts` / `dataset-db-ts` / `portfolio-db-ts` を `shared` に再統合し、`web`/`cli` は `@trading25/shared/*` 参照へ統一される
- `apps/ts/packages/web` と `apps/ts/packages/cli` が `shared/src/*` の深いパスを直接参照しない
- `apps/ts/packages/shared/src/factor-regression`, `screening`, `market-sync` の実装本体が段階削除される（`analytics-ts` / `market-sync-ts` は作成しない）
- `apps/ts/packages/shared` が TS ドメイン重複実装を持たない共通境界（DB/dataset/portfolio + `bt:sync` 補助・型公開）になる
- `apps/ts/packages/web` と `apps/ts/packages/cli` の実行ロジックが FastAPI endpoint + OpenAPI generated types を優先利用する
- lint/typecheck/test が通る

## 実施内容
- Phase 4A: データアクセス + クライアント境界の作成と移管
- Phase 4B: ドメインロジック（analytics / market-sync）の削減・撤去（削除中心）
- 互換 re-export の導入と段階撤去計画の策定
- import 依存と dep-direction チェックの更新

## 結果
- 2026-02-09: `@trading25/clients-ts` パッケージを新設し、`backtest` クライアント実装とテストを移管。
- 2026-02-09: `web`/`cli` の `@trading25/shared/clients/backtest` import を `@trading25/clients-ts/backtest` へ切替。
- 2026-02-09: `@trading25/shared/clients/backtest` は互換 re-export レイヤへ変更（Phase 4 中は互換維持）。
- 2026-02-09: `@trading25/market-db-ts` / `@trading25/dataset-db-ts` / `@trading25/portfolio-db-ts` を新設し、`web`/`cli` の dataset/portfolio/watchlist import を新境界へ切替。
- 2026-02-09: 新設 3 パッケージの `typecheck` を追加（`tsc --noEmit --rootDir ../..`）し、`apps/ts` の `typecheck:all` に統合。
- 2026-02-09: `shared/src/clients/backtest` の旧実装・旧テストを削除し、互換 re-export + generated 型のみに整理。
- 2026-02-09: `shared/src/db` / `dataset` / `portfolio` / `watchlist` / `clients(base,markets,JQuants)` を互換 re-export に置換し、実装本体を新パッケージへ移管。
- 2026-02-09: `clients-ts` に JQuants client 群（base/markets/JQuantsClient）を移管し、`dataset-db-ts` / `portfolio-db-ts` は `market-db-ts` / `clients-ts` を参照する構成に整理。
- 2026-02-09: lint / typecheck / test を通過し、Phase 4A の受け入れ条件（deep import 解消 + shared の薄い互換レイヤ化）を満たした。
- 2026-02-09: `apps/ts` の `test`/`typecheck:all`/`test:backend`/`test:coverage` から archived `@trading25/api` を除外。
- 2026-02-09: dep-direction ルールに `@trading25/clients-ts/backtest` を追加し allowlist を更新。
- 2026-02-09: Phase 4B は「`analytics-ts` / `market-sync-ts` を新設して移管」から「FastAPI 一本化に合わせて TS 重複ドメイン実装を削除」に方針転換。
- 2026-02-09: Phase 4B の削除タスクを完了。`shared/src/factor-regression` / `screening` / `market-sync` の実装本体を削除。
- 2026-02-09: `shared/src/index.ts` / `shared/package.json` の export 面を整理し、削除済みドメインへの公開経路を除去。
- 2026-02-09: `cli` の screening 実行経路を API レスポンス型（`ScreeningResultItem`）に統一し、旧 local conversion を削除。
- 2026-02-09: `web` の Vite/Vitest alias に `@trading25/shared/*` を明示追加し、`shared/dist` 非依存でテストを安定化。
- 2026-02-09: 4B 変更後の検証完了（`bun run typecheck:all` / `bun run lint` / `bun run test` pass）。
- 2026-02-10: `@trading25/market-db-ts` / `@trading25/dataset-db-ts` / `@trading25/portfolio-db-ts` を `shared` へ再統合し、3パッケージを削除。
- 2026-02-10: `web`/`cli` の import を `@trading25/shared/portfolio|watchlist|dataset` に統一し、`vite`/`vitest`/`tsconfig` の不要 alias を削除。
- 2026-02-10: `apps/ts` の scripts / workspace 依存を整理（build/typecheck 対象の再定義）し、lint / typecheck / test のグリーンを確認。

## 補足
- 元タスク `ts-117` は archived API package 前提のため 2026-02-09 にクローズ済み
- 参照: `docs/unified-roadmap.md` Phase 4（再ベースライン）
