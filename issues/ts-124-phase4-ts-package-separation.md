---
id: ts-124
title: "Phase 4: TS パッケージ責務分離"
status: in-progress
priority: medium
labels: [architecture, refactor]
project: ts
created: 2026-02-09
updated: 2026-02-09
depends_on: []
blocks: []
parent: null
---

# ts-124 Phase 4: TS パッケージ責務分離

## 目的
`apps/ts/packages/shared` に集約された責務を分離し、`web`/`cli` が用途別パッケージを直接参照する構成へ移行する。

## 受け入れ条件
- `clients-ts`, `market-db-ts`, `dataset-db-ts`, `portfolio-db-ts`, `analytics-ts`, `market-sync-ts` の境界が作成される
- `apps/ts/packages/web` と `apps/ts/packages/cli` が `shared/src/*` の深いパスを直接参照しない
- `apps/ts/packages/shared` が互換 re-export と `bt:sync` 補助中心の薄いファサードになる
- lint/typecheck/test が通る

## 実施内容
- Phase 4A: データアクセス + クライアント境界の作成と移管
- Phase 4B: ドメインロジック（analytics / market-sync）の移管
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

## 補足
- 元タスク `ts-117` は archived API package 前提のため 2026-02-09 にクローズ済み
- 参照: `docs/unified-roadmap.md` Phase 4（再ベースライン）
