---
id: ts-111
title: "Coverage Gate: shared 80/80"
status: open
priority: medium
labels: [test, coverage]
project: ts
created: 2026-02-01
updated: 2026-02-13
depends_on: []
blocks: []
parent: ts-129
---

# ts-111 Coverage Gate: shared 80/80

## 現状
- 閾値: lines 70% / functions 70%
- 実績: ts-014a で達成済み

## 目標
- 閾値を **lines 80% / functions 80%** に引き上げ、テストを追加して通す

## 変更対象
- `scripts/check-coverage.ts` — `shared: { lines: 0.8, functions: 0.8 }`
- `packages/shared/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 整理メモ
- coverage backlog 整理用の親Issue: `ts-129`
