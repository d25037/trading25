---
id: ts-014a
title: "Coverage Gate: shared 70/70"
status: done
priority: medium
labels: [test]
project: ts
created: 2026-01-31
updated: 2026-02-01
depends_on: []
blocks: []
---

# ts-014a Coverage Gate: shared 70/70

## 現状
- 閾値: lines 60% / functions 60%
- 実績: lines 60.3% / functions 66.9%

## 目標
- 閾値を **lines 70% / functions 70%** に引き上げ、テストを追加して通す

## 変更対象
- `scripts/check-coverage.ts` — `shared: { lines: 0.7, functions: 0.7 }`
- `packages/shared/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 結果
PR #13 でmerge完了。以下のテストを追加:
- BacktestClient.test.ts（型定義に合わせたモックデータ補完）
- BatchExecutor.test.ts
- progress.test.ts
- drizzle-market-reader.test.ts
- drizzle-watchlist-database.test.ts
- monthly.test.ts（timeframe変換テスト）
- dataset-paths.test.ts / find-project-root.test.ts

非nullアサーション(`!`)を `getFirstElementOrFail` / `getElementOrFail` に置換し、Biome lint準拠に。
