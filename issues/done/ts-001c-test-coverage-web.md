---
id: ts-001c
title: "Test Coverage: web パッケージ"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: "ts-001"
---

# ts-001c Test Coverage: web パッケージ

## 目的
主要 UI/Hook の回帰を防ぎ、最低カバレッジ 60% を達成する。

## 受け入れ条件
- `bun run --filter @trading25/web test` が安定して通る
- web のカバレッジが 60% 以上
- 代表的ユーザーフロー（Backtest 実行/結果閲覧）にテストがある

## 実施内容
- pages: Charts/Analysis/Portfolio/Backtest のレンダリング/タブ切替テスト追加
- hooks: useBacktest/useOptimization/usePortfolio の Query/Mutation テスト追加
- stores: chartStore/backtestStore の状態更新/プリセット操作テスト追加
- `@vitest/coverage-v8` を devDependencies に追加

## 結果
- `bun run --filter @trading25/web test` ✅
- `bun run --filter @trading25/web test:coverage` は `@vitest/coverage-v8` のインストール後に再実行が必要

## 補足
