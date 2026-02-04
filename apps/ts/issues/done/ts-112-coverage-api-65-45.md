---
id: ts-112
title: "Coverage Gate: api 65/45"
status: closed
priority: medium
labels: [test]
project: ts
created: 2026-02-01
updated: 2026-02-01
depends_on: []
blocks: []
---

# ts-112 Coverage Gate: api 65/45

## 現状
- 閾値: lines 55% / functions 35%
- 実績: ts-014b で達成済み

## 目標
- 閾値を **lines 65% / functions 45%** に引き上げ、テストを追加して通す

## 補足
- ルートハンドラのテストは `mock.module` + dynamic import パターンを使用すること（`roe.test.ts` が模範）
- サービスがモジュールロード時にインスタンス化されるルートでは、DI化の検討が必要

## 変更対象
- `scripts/check-coverage.ts` — `api: { lines: 0.65, functions: 0.45 }`
- `packages/api/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る
