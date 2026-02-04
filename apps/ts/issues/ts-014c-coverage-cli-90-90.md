---
id: ts-014c
title: "Coverage Gate: cli 90/90"
status: open
priority: medium
labels: [test]
project: ts
created: 2026-01-31
updated: 2026-01-31
depends_on: []
blocks: []
---

# ts-014c Coverage Gate: cli 90/90

## 現状
- 閾値: lines 60% / functions 60%
- 実績: lines 82.4% / functions 94.4%

## 目標
- 閾値を **lines 90% / functions 90%** に引き上げ、テストを追加して通す

## 備考
実績は lines 82.4% / functions 94.4%。functions は既に超えているが、lines は追加テストが必要。

## 変更対象
- `scripts/check-coverage.ts` — `cli: { lines: 0.9, functions: 0.9 }`
- `packages/cli/src/` 配下のテスト追加

## 受け入れ条件
- `bun run check:coverage` が新閾値で通る
