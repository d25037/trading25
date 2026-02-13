---
id: ts-114
title: "Coverage Gate: web 45/70"
status: done
priority: medium
labels: [test, coverage]
project: ts
created: 2026-02-01
updated: 2026-02-13
depends_on: []
blocks: []
parent: ts-129
---

# ts-114 Coverage Gate: web 45/70

## 現状
- 閾値: lines 35% / functions 60%
- 実績: ts-014d で達成済み

## 目標
- 閾値を **lines 45% / functions 70%** に引き上げ、テストを追加して通す

## 主な未テスト領域（推定）
- `src/components/` 配下のUI コンポーネント（Ranking, Screening, Watchlist, Settings等）
- `src/pages/IndicesPage.tsx`, `src/pages/SettingsPage.tsx`
- `src/hooks/useDataset.ts` — 0% カバレッジ

## 変更対象
- `scripts/check-coverage.ts` — `web: { lines: 0.45, functions: 0.7 }`
- `packages/web/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 実施内容
- `apps/ts/scripts/check-coverage.ts` を更新し、`web` の閾値を `lines 45%` / `functions 70%` に変更
- `packages/web/src/pages/SettingsPage.test.tsx` を追加し、Sync 開始/進行中/失敗時の描画とキャンセル実行をカバー
- `packages/web/src/pages/IndicesPage.test.tsx` を追加し、インデックス一覧・セクタ株一覧・選択イベントをカバー
- `src/components` / `src/pages` の主要 UI 未テスト領域に対する回帰テストを追加

## 結果
対応済み

## 整理メモ
- coverage backlog 整理用の親Issue: `ts-129`
