---
id: ts-113
title: "Coverage Gate: cli 70/70"
status: open
priority: medium
labels: [test]
project: ts
created: 2026-02-01
updated: 2026-02-01
depends_on: []
blocks: []
---

# ts-113 Coverage Gate: cli 70/70

## 現状
- 閾値: lines 60% / functions 60%

## 目標
- 閾値を **lines 70% / functions 70%** に引き上げ、テストを追加して通す

## 変更対象
- `scripts/check-coverage.ts` — `cli: { lines: 0.7, functions: 0.7 }`
- `packages/cli/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る
