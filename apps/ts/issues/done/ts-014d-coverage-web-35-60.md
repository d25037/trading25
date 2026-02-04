---
id: ts-014d
title: "Coverage Gate: web 35/60"
status: done
priority: medium
labels: [test]
project: ts
created: 2026-01-31
updated: 2026-02-01
depends_on: []
blocks: []
---

# ts-014d Coverage Gate: web 35/60

## 現状
- 閾値: lines 25% / functions 50%
- 実績: lines 27.7% / functions 54.8%

## 目標
- 閾値を **lines 35% / functions 60%** に引き上げ、テストを追加して通す

## 主な未テスト領域
- `useDataset.ts` (143行) — 未テスト
- `useHistorySync.ts` (174行) — 未テスト
- `usePortfolio.ts` — 部分テスト (50%)、残りの mutation フック
- `useBacktest.ts` — 部分テスト (47%)
- `useOptimization.ts` — 部分テスト (49%)
- `chartStore.ts` — 部分テスト (89%)

## 変更対象
- `scripts/check-coverage.ts` — `web: { lines: 0.35, functions: 0.6 }`
- `packages/web/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 結果
PR #11 でmerge完了。以下のテストを追加:
- JobsTable, Header, RankingFilters, RankingSummary, ScreeningFilters, ScreeningSummary コンポーネントテスト
- useBacktest, useDataset, useHistorySync, useOptimization, usePortfolio フックテスト
- HistoryPage ページテスト
- chartStore ストアテスト
